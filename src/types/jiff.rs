use std::error::Error;

use bytes::BytesMut;
use jiff::{
    Timestamp,
    civil::{Date, DateTime, Time},
    fmt::strtime::{self, BrokenDownTime},
    tz::{Offset, TimeZone},
};
use postgres_types::{IsNull, Type, WrongType};

use super::{FromSqlText, ToSqlText, format::FormatOptions, format::date_style::DateStyle};

// PostgreSQL has microsecond precision. Use the same text precision as Chrono.
const TIME_FORMAT: &str = "%H:%M:%S%.6f";

macro_rules! impl_jiff_text {
    ($rust:ty, $pg:ident, $array:ident, $parse:expr, $format:expr) => {
        impl<'a> FromSqlText<'a> for $rust {
            fn from_sql_text(
                _ty: &Type,
                input: &'a [u8],
                options: &FormatOptions,
            ) -> Result<Self, Box<dyn Error + Sync + Send>> {
                let parse: fn(&str, &FormatOptions) -> Result<Self, jiff::Error> = $parse;
                Ok(parse(std::str::from_utf8(input)?, options)?)
            }
        }

        impl ToSqlText for $rust {
            fn to_sql_text(
                &self,
                ty: &Type,
                out: &mut BytesMut,
                options: &FormatOptions,
            ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                if !matches!(*ty, Type::$pg | Type::$array) {
                    return Err(Box::new(WrongType::new::<Self>(ty.clone())));
                }
                let format: fn(&Self, &FormatOptions) -> Result<String, jiff::Error> = $format;
                out.extend_from_slice(format(self, options)?.as_bytes());
                Ok(IsNull::No)
            }
        }
    };
}

impl_jiff_text!(
    Date,
    DATE,
    DATE_ARRAY,
    |input, options| input
        .parse()
        .or_else(|_| Date::strptime(DateStyle::new(&options.date_style).date_format_str(), input)),
    |date, options| strtime::format(DateStyle::new(&options.date_style).date_format_str(), *date)
);

impl_jiff_text!(
    Time,
    TIME,
    TIME_ARRAY,
    |input, _| input.parse(),
    |time, _| strtime::format(TIME_FORMAT, *time)
);

impl_jiff_text!(
    DateTime,
    TIMESTAMP,
    TIMESTAMP_ARRAY,
    |input, options| input.parse().or_else(|_| DateTime::strptime(
        DateStyle::new(&options.date_style).full_format_str(),
        input
    )),
    |datetime, options| strtime::format(
        DateStyle::new(&options.date_style).full_format_str(),
        *datetime
    )
);

impl_jiff_text!(
    Timestamp,
    TIMESTAMPTZ,
    TIMESTAMPTZ_ARRAY,
    parse_timestamp,
    |timestamp, options| strtime::format(
        DateStyle::new(&options.date_style).full_tz_format_str(),
        &timestamp.to_zoned_datetime(TimeZone::UTC)
    )
);

fn parse_timestamp(input: &str, options: &FormatOptions) -> Result<Timestamp, jiff::Error> {
    // PostgreSQL's ISO format uses numeric offsets, which Jiff parses directly.
    input.parse().or_else(|_| {
        let style = DateStyle::new(&options.date_style);
        let format = style.full_format_str();
        // Non-ISO DateStyles can end in a zone abbreviation. Support UTC
        // (the output of our encoder), but don't guess at other abbreviations.
        if let Some(datetime) = input.strip_suffix(" UTC") {
            let mut parsed = BrokenDownTime::parse(format, datetime)?;
            parsed.set_offset(Some(Offset::UTC));
            parsed.to_timestamp()
        } else {
            Timestamp::strptime(format!("{format} %:::z"), input)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use postgres_types::{FromSql, ToSql};

    fn roundtrip<T>(ty: &Type, value: T, options: &FormatOptions)
    where
        T: for<'a> FromSqlText<'a> + ToSqlText + for<'a> FromSql<'a> + ToSql + PartialEq,
    {
        let mut text = BytesMut::new();
        value.to_sql_text(ty, &mut text, options).unwrap();
        assert_eq!(T::from_sql_text(ty, &text, options).unwrap(), value);

        let mut binary = BytesMut::new();
        value.to_sql_checked(ty, &mut binary).unwrap();
        assert_eq!(T::from_sql(ty, &binary).unwrap(), value);
    }

    #[test]
    fn text_and_binary_roundtrips() {
        let date: Date = "1969-07-20".parse().unwrap();
        let time: Time = "20:17:40.123456".parse().unwrap();
        let datetime: DateTime = "1969-07-20 20:17:40.123456".parse().unwrap();
        let timestamp: Timestamp = "1969-07-20 21:17:40.123456+01".parse().unwrap();

        for style in ["ISO", "SQL, DMY", "German", "Postgres, MDY"] {
            let options = FormatOptions {
                date_style: style.into(),
                ..FormatOptions::default()
            };
            roundtrip(&Type::DATE, date, &options);
            roundtrip(&Type::TIME, time, &options);
            roundtrip(&Type::TIMESTAMP, datetime, &options);
            roundtrip(&Type::TIMESTAMPTZ, timestamp, &options);
            roundtrip(
                &Type::TIMESTAMPTZ_ARRAY,
                vec![Some(timestamp), None],
                &options,
            );
        }
    }

    #[test]
    fn parses_postgres_iso_timestamp_with_offset() {
        let options = FormatOptions::default();
        let timestamp = Timestamp::from_sql_text(
            &Type::TIMESTAMPTZ,
            b"2026-03-01 10:00:00.123456+05:30",
            &options,
        )
        .unwrap();
        assert_eq!(timestamp, "2026-03-01T04:30:00.123456Z".parse().unwrap());

        let mut out = BytesMut::new();
        timestamp
            .to_sql_text(&Type::TIMESTAMPTZ, &mut out, &options)
            .unwrap();
        assert_eq!(&out[..], b"2026-03-01 04:30:00.123456+00");
        assert!(
            timestamp
                .to_sql_text(&Type::DATE, &mut out, &options)
                .is_err()
        );
    }
}

use std::error::Error;
use std::fmt;
#[cfg(feature = "pg-type-chrono")]
use std::time::SystemTime;

use bytes::{BufMut, BytesMut};
#[cfg(feature = "pg-type-chrono")]
use chrono::offset::Utc;
#[cfg(feature = "pg-type-chrono")]
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use lazy_regex::{lazy_regex, Lazy, Regex};
#[cfg(feature = "pg-type-serde-json")]
use postgres_types::Json;
use postgres_types::{IsNull, Kind, Type};
#[cfg(feature = "pg-type-rust-decimal")]
use rust_decimal::Decimal;
#[cfg(feature = "pg-type-serde-json")]
use serde::Serialize;
#[cfg(feature = "pg-type-serde-json")]
use serde_json::Value;

use crate::types::format::bytea_output::ByteaOutput;
use crate::types::format::float_digits::ExtraFloatDigits;
use crate::types::format::FormatOptions;

pub static QUOTE_CHECK: Lazy<Regex> = lazy_regex!(r#"^$|["{},\\\s]|^null$"#i);
pub static QUOTE_ESCAPE: Lazy<Regex> = lazy_regex!(r#"(["\\])"#);

pub trait ToSqlText: fmt::Debug {
    /// Converts value to text format of Postgres type.
    ///
    /// This trait is modelled after `ToSql` from postgres-types, which is
    /// for binary encoding.
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized;
}

impl<T> ToSqlText for &T
where
    T: ToSqlText,
{
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        (*self).to_sql_text(ty, out, format_options)
    }
}

impl<T: ToSqlText> ToSqlText for Option<T> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match *self {
            Some(ref val) => val.to_sql_text(ty, out, format_options),
            None => Ok(IsNull::Yes),
        }
    }
}

impl ToSqlText for bool {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *self {
            out.put_slice(b"t");
        } else {
            out.put_slice(b"f");
        }
        Ok(IsNull::No)
    }
}

impl ToSqlText for String {
    fn to_sql_text(
        &self,
        ty: &Type,
        w: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&str as ToSqlText>::to_sql_text(&&**self, ty, w, format_options)
    }
}

impl ToSqlText for &str {
    fn to_sql_text(
        &self,
        ty: &Type,
        w: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let quote = matches!(ty.kind(), Kind::Array(_)) && QUOTE_CHECK.is_match(self);

        if quote {
            w.put_u8(b'"');
            w.put_slice(QUOTE_ESCAPE.replace_all(self, r#"\$1"#).as_bytes());
            w.put_u8(b'"');
        } else {
            w.put_slice(self.as_bytes());
        }

        Ok(IsNull::No)
    }
}

macro_rules! impl_to_sql_text {
    ($t:ty) => {
        impl ToSqlText for $t {
            fn to_sql_text(
                &self,
                _ty: &Type,
                w: &mut BytesMut,
                _format_options: &FormatOptions,
            ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                use std::fmt::Write;
                write!(w, "{}", self)?;
                Ok(IsNull::No)
            }
        }
    };
}

impl_to_sql_text!(i8);
impl_to_sql_text!(i16);
impl_to_sql_text!(i32);
impl_to_sql_text!(i64);
impl_to_sql_text!(u32);
impl_to_sql_text!(char);

impl ToSqlText for f32 {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let extra_float_digits = ExtraFloatDigits(format_options.extra_float_digits);
        out.put_slice(extra_float_digits.format_f32(*self).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for f64 {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let extra_float_digits = ExtraFloatDigits(format_options.extra_float_digits);
        out.put_slice(extra_float_digits.format_f64(*self).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for &[u8] {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let bytea_output =
            ByteaOutput::try_from(format_options.bytea_output.as_str()).map_err(Box::new)?;

        // for a bytea_array, the bytes are enclosed in quotes,
        // and \x is escaped
        if ty == &Type::BYTEA_ARRAY {
            out.put_slice(b"\"\\");
        }

        match bytea_output {
            ByteaOutput::Hex => {
                out.put_slice(b"\\x");
                out.put_slice(hex::encode(self).as_bytes());
            }
            ByteaOutput::Escape => {
                self.iter().for_each(|b| match b {
                    0..=31 | 127..=255 => {
                        out.put_slice(b"\\");
                        out.put_slice(format!("{b:03o}").as_bytes());
                    }
                    92 => out.put_slice(b"\\\\"),
                    32..=126 => out.put_u8(*b),
                });
            }
        }

        if ty == &Type::BYTEA_ARRAY {
            out.put_slice(b"\"");
        }

        Ok(IsNull::No)
    }
}

impl ToSqlText for Vec<u8> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[u8] as ToSqlText>::to_sql_text(&&**self, ty, out, format_options)
    }
}

impl<const N: usize> ToSqlText for [u8; N] {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[u8] as ToSqlText>::to_sql_text(&&self[..], ty, out, format_options)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for SystemTime {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let datetime: DateTime<Utc> = DateTime::<Utc>::from(*self);
        datetime.to_sql_text(ty, out, format_options)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl<Tz: TimeZone> ToSqlText for DateTime<Tz>
where
    Tz::Offset: std::fmt::Display,
{
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        use crate::types::format::date_style::DateStyle;
        use std::fmt::Write;

        let date_style = DateStyle::new(&format_options.date_style);

        let fmt = match *ty {
            Type::TIMESTAMP | Type::TIMESTAMP_ARRAY => date_style.full_format_str(),
            Type::TIMESTAMPTZ | Type::TIMESTAMPTZ_ARRAY => &date_style.full_tz_format_smolstr(),
            Type::DATE | Type::DATE_ARRAY => date_style.date_format_str(),
            Type::TIME | Type::TIME_ARRAY => "%H:%M:%S%.6f",
            Type::TIMETZ | Type::TIMETZ_ARRAY => date_style.time_tz_format_str(),
            _ => Err(Box::new(postgres_types::WrongType::new::<DateTime<Tz>>(
                ty.clone(),
            )))?,
        };
        write!(out, "{}", self.format(fmt))?;
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for NaiveDateTime {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        use crate::types::format::date_style::DateStyle;
        use std::fmt::Write;

        let date_style = DateStyle::new(&format_options.date_style);

        let fmt = match *ty {
            Type::TIMESTAMP | Type::TIMESTAMP_ARRAY => date_style.full_format_str(),
            Type::DATE | Type::DATE_ARRAY => date_style.date_format_str(),
            Type::TIME | Type::TIME_ARRAY => "%H:%M:%S%.6f",
            _ => Err(Box::new(postgres_types::WrongType::new::<NaiveDateTime>(
                ty.clone(),
            )))?,
        };
        write!(out, "{}", self.format(fmt))?;
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for NaiveDate {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        use crate::types::format::date_style::DateStyle;
        use std::fmt::Write;

        if !matches!(*ty, Type::DATE | Type::DATE_ARRAY) {
            return Err(Box::new(postgres_types::WrongType::new::<NaiveDate>(
                ty.clone(),
            )));
        }

        let date_style = DateStyle::new(&format_options.date_style);
        write!(out, "{}", self.format(date_style.date_format_str()))?;
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for NaiveTime {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        use std::fmt::Write;

        if !matches!(*ty, Type::TIME | Type::TIME_ARRAY) {
            return Err(Box::new(postgres_types::WrongType::new::<NaiveTime>(
                ty.clone(),
            )));
        }
        write!(out, "{}", self.format("%H:%M:%S%.6f"))?;
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for Duration {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        use crate::types::format::interval_style::IntervalStyle;
        use smol_str::format_smolstr;

        let interval_style =
            IntervalStyle::try_from(format_options.interval_style.as_str()).map_err(Box::new)?;

        let total_seconds = self.num_seconds();
        let microseconds = self.num_microseconds().unwrap_or(0) % 1_000_000;

        // Extract components
        let sign = if total_seconds < 0 { "-" } else { "" };
        let abs_seconds = total_seconds.abs();
        let days = abs_seconds / 86400;
        let hours = (abs_seconds % 86400) / 3600;
        let minutes = (abs_seconds % 3600) / 60;
        let seconds = abs_seconds % 60;

        let output = match interval_style {
            IntervalStyle::Postgres => {
                let mut parts = Vec::new();

                if days != 0 {
                    parts.push(format_smolstr!("{days} days"));
                }
                if hours != 0 || minutes != 0 || seconds != 0 || microseconds != 0 {
                    let time_str = if microseconds == 0 {
                        format_smolstr!("{hours:02}:{minutes:02}:{seconds:02}")
                    } else {
                        format_smolstr!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                    };
                    parts.push(time_str);
                }

                if parts.is_empty() {
                    format_smolstr!("{sign}00:00:00")
                } else {
                    format_smolstr!("{sign}{}", parts.join(" "))
                }
            }
            IntervalStyle::ISO8601 => {
                let mut parts = Vec::new();

                if days != 0 {
                    parts.push(format_smolstr!("{days}D"));
                }

                let mut time_parts = Vec::new();
                if hours != 0 {
                    time_parts.push(format_smolstr!("{hours}H"));
                }
                if minutes != 0 {
                    time_parts.push(format_smolstr!("{minutes}M",));
                }
                if seconds != 0 || microseconds != 0 {
                    if microseconds == 0 {
                        time_parts.push(format_smolstr!("{seconds}S",));
                    } else {
                        time_parts.push(format_smolstr!("{seconds}.{microseconds:06}S"));
                    }
                }

                if !time_parts.is_empty() {
                    parts.push(format_smolstr!("T{}", time_parts.join("")));
                }

                if parts.is_empty() {
                    format_smolstr!("{sign}PT0S",)
                } else {
                    format_smolstr!("{sign}P{}", parts.join(""))
                }
            }
            IntervalStyle::SQLStandard => {
                let mut parts = Vec::new();

                if days != 0 {
                    parts.push(format_smolstr!("{days} {hours}"));
                } else if hours != 0 || minutes != 0 || seconds != 0 || microseconds != 0 {
                    if microseconds == 0 {
                        parts.push(format_smolstr!("{hours:02}:{minutes:02}:{seconds:02}"));
                    } else {
                        parts.push(format_smolstr!(
                            "{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}",
                        ));
                    }
                }

                if parts.is_empty() {
                    format_smolstr!("{sign}00:00:00")
                } else {
                    format_smolstr!("{sign}{}", parts.join(" "))
                }
            }
            IntervalStyle::PostgresVerbose => {
                let mut parts = Vec::new();

                if days != 0 {
                    parts.push(format_smolstr!(
                        "{days} day{}",
                        if days != 1 { "s" } else { "" }
                    ));
                }
                if hours != 0 {
                    parts.push(format_smolstr!(
                        "{hours} hour{}",
                        if hours != 1 { "s" } else { "" }
                    ));
                }
                if minutes != 0 {
                    parts.push(format_smolstr!(
                        "{minutes} min{}",
                        if minutes != 1 { "s" } else { "" }
                    ));
                }
                if seconds != 0 || microseconds != 0 {
                    if microseconds == 0 {
                        parts.push(format_smolstr!(
                            "{seconds} sec{}",
                            if seconds != 1 { "s" } else { "" }
                        ));
                    } else {
                        let total_seconds = seconds as f64 + microseconds as f64 / 1_000_000.0;
                        parts.push(format_smolstr!(
                            "{total_seconds} sec{}",
                            if total_seconds != 1.0 { "s" } else { "" }
                        ));
                    }
                }

                if parts.is_empty() {
                    format_smolstr!("{sign}@ 0")
                } else {
                    format_smolstr!("{sign}@ {}", parts.join(" "))
                }
            }
        };

        out.put_slice(output.as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-rust-decimal")]
impl ToSqlText for Decimal {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        use std::fmt::Write;

        if !matches!(*ty, Type::NUMERIC | Type::NUMERIC_ARRAY) {
            return Err(Box::new(postgres_types::WrongType::new::<Decimal>(
                ty.clone(),
            )));
        }

        write!(out, "{}", self)?;
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-serde-json")]
impl<T: Serialize + fmt::Debug> ToSqlText for Json<T> {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        serde_json::ser::to_writer(out.writer(), &self.0)?;
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-serde-json")]
impl ToSqlText for Value {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        serde_json::ser::to_writer(out.writer(), &self)?;
        Ok(IsNull::No)
    }
}

impl<T: ToSqlText> ToSqlText for &[T] {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(b"{");
        for (i, val) in self.iter().enumerate() {
            if i > 0 {
                out.put_slice(b",");
            }
            // put NULL for null value in array
            if let IsNull::Yes = val.to_sql_text(ty, out, format_options)? {
                out.put_slice(b"NULL");
            }
        }
        out.put_slice(b"}");
        Ok(IsNull::No)
    }
}

impl<T: ToSqlText> ToSqlText for Vec<T> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[T] as ToSqlText>::to_sql_text(&&**self, ty, out, format_options)
    }
}

impl<T: ToSqlText, const N: usize> ToSqlText for [T; N] {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
        format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[T] as ToSqlText>::to_sql_text(&&self[..], ty, out, format_options)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[cfg(feature = "pg-type-chrono")]
    use chrono::offset::FixedOffset;

    #[test]
    #[cfg(feature = "pg-type-chrono")]
    fn test_date_time_format() {
        let date = NaiveDate::from_ymd_opt(2023, 3, 5).unwrap();
        let mut buf = BytesMut::new();
        date.to_sql_text(&Type::DATE, &mut buf, &FormatOptions::default())
            .unwrap();
        assert_eq!("2023-03-05", String::from_utf8_lossy(buf.freeze().as_ref()));

        let date = NaiveDate::from_ymd_opt(2023, 3, 5).unwrap();
        let mut buf = BytesMut::new();
        assert!(date
            .to_sql_text(&Type::INT8, &mut buf, &FormatOptions::default())
            .is_err());

        let date = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2023, 3, 5).unwrap(),
            NaiveTime::from_hms_opt(10, 20, 00).unwrap(),
        )
        .and_local_timezone(FixedOffset::east_opt(8 * 3600).unwrap())
        .unwrap();

        let mut buf = BytesMut::new();
        date.to_sql_text(&Type::TIMESTAMPTZ, &mut buf, &FormatOptions::default())
            .unwrap();
        // format: 2023-02-01 22:31:49.479895+08
        assert_eq!(
            "2023-03-05 10:20:00.000000+08",
            String::from_utf8_lossy(buf.freeze().as_ref())
        );
    }

    #[test]
    fn test_null() {
        let data = vec![None::<i8>, Some(8)];
        let mut buf = BytesMut::new();
        data.to_sql_text(&Type::INT2, &mut buf, &FormatOptions::default())
            .unwrap();
        assert_eq!("{NULL,8}", String::from_utf8_lossy(buf.freeze().as_ref()));
    }

    #[test]
    fn test_bool() {
        let yes = true;
        let no = false;

        let mut buf = BytesMut::new();
        yes.to_sql_text(&Type::BOOL, &mut buf, &FormatOptions::default())
            .unwrap();
        assert_eq!("t", String::from_utf8_lossy(buf.freeze().as_ref()));

        let mut buf = BytesMut::new();
        no.to_sql_text(&Type::BOOL, &mut buf, &FormatOptions::default())
            .unwrap();
        assert_eq!("f", String::from_utf8_lossy(buf.freeze().as_ref()));
    }

    #[test]
    #[cfg(feature = "pg-type-chrono")]
    fn test_array() {
        let date = &[
            NaiveDate::from_ymd_opt(2023, 3, 5).unwrap(),
            NaiveDate::from_ymd_opt(2023, 3, 6).unwrap(),
        ];
        let mut buf = BytesMut::new();
        date.to_sql_text(&Type::DATE_ARRAY, &mut buf, &FormatOptions::default())
            .unwrap();
        assert_eq!(
            "{2023-03-05,2023-03-06}",
            String::from_utf8_lossy(buf.freeze().as_ref())
        );

        let chars = &[
            "{", "abc", "}", "\"", "", "a,b", "null", "NULL", "NULL!", "\\", " ", "\"\"",
        ];
        let mut buf = BytesMut::new();
        chars
            .to_sql_text(&Type::VARCHAR_ARRAY, &mut buf, &FormatOptions::default())
            .unwrap();
        assert_eq!(
            r#"{"{",abc,"}","\"","","a,b","null","NULL",NULL!,"\\"," ","\"\""}"#,
            String::from_utf8_lossy(buf.freeze().as_ref())
        );
    }

    #[test]
    fn test_bytea_array() {
        let data = vec![b"hello", b"world"];
        let mut buf = BytesMut::new();
        data.to_sql_text(&Type::BYTEA_ARRAY, &mut buf, &FormatOptions::default())
            .unwrap();

        assert_eq!(
            "{\"\\\\x68656c6c6f\",\"\\\\x776f726c64\"}",
            String::from_utf8_lossy(buf.freeze().as_ref())
        );
    }
}

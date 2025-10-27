use std::error::Error;
#[cfg(feature = "pg-type-chrono")]
use std::time::SystemTime;

use bytes::{BufMut, BytesMut};
#[cfg(feature = "pg-type-chrono")]
use chrono::offset::Utc;
#[cfg(feature = "pg-type-chrono")]
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone};
use postgres_types::{IsNull, Type};

#[cfg(feature = "pg-type-chrono")]
use crate::types::ToSqlText;
use crate::{api::ClientInfo, error::PgWireError};

pub const BYTEA_OUTPUT_HEX: &str = "hex";
pub const BYTEA_OUTPUT_ESCAPE: &str = "escape";

pub const DATE_STYLE_ORDER_DMY: &str = "DMY";
pub const DATE_STYLE_ORDER_MDY: &str = "MDY";
pub const DATE_STYLE_ORDER_YMD: &str = "YMD";

pub const DATE_STYLE_DISPLAY_ISO: &str = "ISO";
pub const DATE_STYLE_DISPLAY_SQL: &str = "SQL";
pub const DATE_STYLE_DISPLAY_GERMAN: &str = "german";
pub const DATE_STYLE_DISPLAY_POSTGRES: &str = "postgres";

#[derive(Debug, Default, Copy, Clone)]
pub enum DateStyleOrder {
    DMY,
    MDY,
    #[default]
    YMD,
}

impl TryFrom<&str> for DateStyleOrder {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim() {
            DATE_STYLE_ORDER_DMY => Ok(Self::DMY),
            DATE_STYLE_ORDER_MDY => Ok(Self::MDY),
            DATE_STYLE_ORDER_YMD => Ok(Self::YMD),
            _ => Err(PgWireError::InvalidOptionValue(value.to_string())),
        }
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub enum DateStyleDisplayStyle {
    #[default]
    ISO,
    SQL,
    GERMAN,
    POSTGRES,
}

impl TryFrom<&str> for DateStyleDisplayStyle {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim() {
            DATE_STYLE_DISPLAY_ISO => Ok(Self::ISO),
            DATE_STYLE_DISPLAY_SQL => Ok(Self::SQL),
            DATE_STYLE_DISPLAY_GERMAN => Ok(Self::GERMAN),
            DATE_STYLE_DISPLAY_POSTGRES => Ok(Self::POSTGRES),
            _ => Err(PgWireError::InvalidOptionValue(value.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct DateStyle<T> {
    style: DateStyleDisplayStyle,
    order: Option<DateStyleOrder>,
    data: T,
}

impl<T> DateStyle<T> {
    /// Create DateStyle wrapped datetime for serialization
    ///
    /// This function will ignore invalid config and fallback to default.
    /// It is possible the config only provides style without custom order.
    pub fn new(data: T, config: &str) -> Self {
        let (style, order) = config
            .split_once(", ")
            .map(|(style, order)| (style, Some(order)))
            .unwrap_or((config, None));
        DateStyle {
            order: order.map(|order| DateStyleOrder::try_from(order).unwrap_or_default()),
            style: DateStyleDisplayStyle::try_from(style).unwrap_or_default(),
            data,
        }
    }

    pub fn new_with_client_info<C>(data: T, client_info: C) -> Self
    where
        C: ClientInfo,
    {
        let display_config = client_info
            .metadata()
            .get("DateStyle")
            .map(|s| s.as_str())
            .unwrap_or("ISO");
        Self::new(data, display_config)
    }

    /// Get datetime format str for current style
    pub fn full_format_str(&self) -> &str {
        match (&self.style, &self.order) {
            (DateStyleDisplayStyle::SQL, Some(DateStyleOrder::DMY)) => "%d/%m/%Y %H:%M:%S%.f",
            (DateStyleDisplayStyle::SQL, _) => "%m/%d/%Y %H:%M:%S%.f",

            (DateStyleDisplayStyle::ISO, _) => "%Y-%m-%d %H:%M:%S%.f",

            (DateStyleDisplayStyle::POSTGRES, _) => "%a %b %e %H:%M:%S %.f %Y",

            (DateStyleDisplayStyle::GERMAN, _) => "%d.%m.%Y %H:%M:%S%.f",
        }
    }

    /// Get datetime with tz format str for current style
    pub fn full_tz_format_str(&self) -> String {
        match self.style {
            DateStyleDisplayStyle::ISO => format!("{}%:::z", self.full_format_str()),
            _ => format!("{}%Z", self.full_format_str()),
        }
    }

    /// Get date format str for current style
    pub fn date_format_str(&self) -> &str {
        match (&self.style, &self.order) {
            (DateStyleDisplayStyle::SQL, Some(DateStyleOrder::DMY)) => "%d/%m/%Y",
            (DateStyleDisplayStyle::SQL, _) => "%m/%d/%Y",

            (DateStyleDisplayStyle::ISO, _) => "%Y-%m-%d",

            (DateStyleDisplayStyle::POSTGRES, Some(DateStyleOrder::DMY)) => "%d-%m-%Y",
            (DateStyleDisplayStyle::POSTGRES, _) => "%m-%d-%Y",

            (DateStyleDisplayStyle::GERMAN, _) => "%d.%m.%Y",
        }
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for DateStyle<SystemTime> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let datetime: DateTime<Utc> = DateTime::<Utc>::from(self.data);
        DateStyle {
            data: datetime,
            style: self.style,
            order: self.order,
        }
        .to_sql_text(ty, out)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl<Tz: TimeZone> ToSqlText for DateStyle<DateTime<Tz>>
where
    Tz::Offset: std::fmt::Display,
{
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::TIMESTAMP | Type::TIMESTAMP_ARRAY => self.full_format_str(),
            Type::TIMESTAMPTZ | Type::TIMESTAMPTZ_ARRAY => &self.full_tz_format_str(),
            Type::DATE | Type::DATE_ARRAY => self.date_format_str(),
            Type::TIME | Type::TIME_ARRAY => "%H:%M:%S%.6f",
            Type::TIMETZ | Type::TIMETZ_ARRAY => "%H:%M:%S%.6f%:::z",
            _ => Err(Box::new(postgres_types::WrongType::new::<DateTime<Tz>>(
                ty.clone(),
            )))?,
        };
        out.put_slice(self.data.format(fmt).to_string().as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for DateStyle<NaiveDateTime> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::TIMESTAMP | Type::TIMESTAMP_ARRAY => self.full_format_str(),
            Type::DATE | Type::DATE_ARRAY => self.date_format_str(),
            Type::TIME | Type::TIME_ARRAY => "%H:%M:%S%.6f",
            _ => Err(Box::new(postgres_types::WrongType::new::<NaiveDateTime>(
                ty.clone(),
            )))?,
        };
        out.put_slice(self.data.format(fmt).to_string().as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for DateStyle<NaiveDate> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::DATE | Type::DATE_ARRAY => self.data.format(self.date_format_str()).to_string(),
            _ => Err(Box::new(postgres_types::WrongType::new::<NaiveDate>(
                ty.clone(),
            )))?,
        };

        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "pg-type-chrono")]
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};

    #[test]
    fn test_date_style_order_from_str() {
        assert!(matches!(
            DateStyleOrder::try_from("DMY").unwrap(),
            DateStyleOrder::DMY
        ));
        assert!(matches!(
            DateStyleOrder::try_from("MDY").unwrap(),
            DateStyleOrder::MDY
        ));
        assert!(matches!(
            DateStyleOrder::try_from("YMD").unwrap(),
            DateStyleOrder::YMD
        ));
        assert!(DateStyleOrder::try_from("invalid").is_err());
    }

    #[test]
    fn test_date_style_display_style_from_str() {
        assert!(matches!(
            DateStyleDisplayStyle::try_from("ISO").unwrap(),
            DateStyleDisplayStyle::ISO
        ));
        assert!(matches!(
            DateStyleDisplayStyle::try_from("SQL").unwrap(),
            DateStyleDisplayStyle::SQL
        ));
        assert!(matches!(
            DateStyleDisplayStyle::try_from("german").unwrap(),
            DateStyleDisplayStyle::GERMAN
        ));
        assert!(matches!(
            DateStyleDisplayStyle::try_from("postgres").unwrap(),
            DateStyleDisplayStyle::POSTGRES
        ));
        assert!(DateStyleDisplayStyle::try_from("invalid").is_err());
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_date_style_new() {
        let dt = NaiveDate::from_ymd_opt(2023, 12, 25)
            .unwrap()
            .and_hms_opt(14, 30, 45)
            .unwrap();

        let date_style = DateStyle::new(dt, "ISO");
        assert!(matches!(date_style.style, DateStyleDisplayStyle::ISO));
        assert!(date_style.order.is_none());

        let date_style = DateStyle::new(dt, "SQL, DMY");
        assert!(matches!(date_style.style, DateStyleDisplayStyle::SQL));
        assert!(matches!(date_style.order.unwrap(), DateStyleOrder::DMY));

        let date_style = DateStyle::new(dt, "invalid");
        assert!(matches!(date_style.style, DateStyleDisplayStyle::ISO));
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_date_style_format_strs() {
        let dt = NaiveDate::from_ymd_opt(2023, 12, 25)
            .unwrap()
            .and_hms_micro_opt(14, 30, 45, 123456)
            .unwrap();

        let test_cases = vec![
            (
                "ISO",
                None,
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M:%S%.f%:::z",
                "%Y-%m-%d",
            ),
            (
                "SQL",
                Some("DMY"),
                "%d/%m/%Y %H:%M:%S%.f",
                "%d/%m/%Y %H:%M:%S%.f%Z",
                "%d/%m/%Y",
            ),
            (
                "SQL",
                None,
                "%m/%d/%Y %H:%M:%S%.f",
                "%m/%d/%Y %H:%M:%S%.f%Z",
                "%m/%d/%Y",
            ),
            (
                "german",
                None,
                "%d.%m.%Y %H:%M:%S%.f",
                "%d.%m.%Y %H:%M:%S%.f%Z",
                "%d.%m.%Y",
            ),
            (
                "postgres",
                None,
                "%a %b %e %H:%M:%S %.f %Y",
                "%a %b %e %H:%M:%S %.f %Y%Z",
                "%m-%d-%Y",
            ),
            (
                "postgres",
                Some("DMY"),
                "%a %b %e %H:%M:%S %.f %Y",
                "%a %b %e %H:%M:%S %.f %Y%Z",
                "%d-%m-%Y",
            ),
        ];

        for (style, order, expected_full, expected_tz, expected_date) in test_cases {
            let config = if let Some(order) = order {
                format!("{}, {}", style, order)
            } else {
                style.to_string()
            };

            let date_style = DateStyle::new(dt, &config);
            assert_eq!(date_style.full_format_str(), expected_full);
            assert_eq!(date_style.full_tz_format_str(), expected_tz);
            assert_eq!(date_style.date_format_str(), expected_date);
        }
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_datetime_to_sql_text() {
        let dt = NaiveDate::from_ymd_opt(2023, 12, 25)
            .unwrap()
            .and_hms_micro_opt(14, 30, 45, 123456)
            .unwrap();
        let datetime: DateTime<Utc> = Utc.from_utc_datetime(&dt);

        let mut out = BytesMut::new();

        let date_style = DateStyle::new(datetime, "ISO");
        date_style.to_sql_text(&Type::TIMESTAMP, &mut out).unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "2023-12-25 14:30:45.123456"
        );

        out.clear();
        date_style
            .to_sql_text(&Type::TIMESTAMPTZ, &mut out)
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "2023-12-25 14:30:45.123456+00"
        );

        out.clear();
        date_style.to_sql_text(&Type::DATE, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "2023-12-25");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_naive_datetime_to_sql_text() {
        let dt = NaiveDate::from_ymd_opt(2023, 12, 25)
            .unwrap()
            .and_hms_micro_opt(14, 30, 45, 123456)
            .unwrap();

        let mut out = BytesMut::new();

        let date_style = DateStyle::new(dt, "SQL, DMY");
        date_style.to_sql_text(&Type::TIMESTAMP, &mut out).unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "25/12/2023 14:30:45.123456"
        );

        out.clear();
        date_style.to_sql_text(&Type::DATE, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "25/12/2023");

        out.clear();
        date_style.to_sql_text(&Type::TIME, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "14:30:45.123456");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_system_time_to_sql_text() {
        use std::time::{Duration, UNIX_EPOCH};

        let system_time = UNIX_EPOCH + Duration::from_secs(1703514645); // 2023-12-25 14:30:45 UTC
        let mut out = BytesMut::new();

        let date_style = DateStyle::new(system_time, "german");
        date_style.to_sql_text(&Type::TIMESTAMP, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "25.12.2023 14:30:45");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_naive_date_to_sql_text() {
        let date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
        let mut out = BytesMut::new();

        let date_style = DateStyle::new(date, "postgres, DMY");
        date_style.to_sql_text(&Type::DATE, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "25-12-2023");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_wrong_type_error() {
        let dt = NaiveDate::from_ymd_opt(2023, 12, 25)
            .unwrap()
            .and_hms_opt(14, 30, 45)
            .unwrap();
        let mut out = BytesMut::new();

        let date_style = DateStyle::new(dt, "ISO");
        let result = date_style.to_sql_text(&Type::BOOL, &mut out);
        assert!(result.is_err());
    }
}

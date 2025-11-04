use crate::error::PgWireError;

pub const DATE_STYLE_ORDER_DMY: &str = "dmy";
pub const DATE_STYLE_ORDER_MDY: &str = "mdy";
pub const DATE_STYLE_ORDER_YMD: &str = "ymd";

pub const DATE_STYLE_DISPLAY_ISO: &str = "iso";
pub const DATE_STYLE_DISPLAY_SQL: &str = "sql";
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
        match value.trim().to_lowercase().as_ref() {
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
    German,
    Postgres,
}

impl TryFrom<&str> for DateStyleDisplayStyle {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_lowercase().as_ref() {
            DATE_STYLE_DISPLAY_ISO => Ok(Self::ISO),
            DATE_STYLE_DISPLAY_SQL => Ok(Self::SQL),
            DATE_STYLE_DISPLAY_GERMAN => Ok(Self::German),
            DATE_STYLE_DISPLAY_POSTGRES => Ok(Self::Postgres),
            _ => Err(PgWireError::InvalidOptionValue(value.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct DateStyle {
    style: DateStyleDisplayStyle,
    order: Option<DateStyleOrder>,
}

impl DateStyle {
    /// Create DateStyle wrapped datetime for serialization
    ///
    /// This function will ignore invalid config and fallback to default.
    /// It is possible the config only provides style without custom order.
    pub fn new(config: &str) -> Self {
        let (style, order) = config
            .split_once(", ")
            .map(|(style, order)| (style, Some(order)))
            .unwrap_or((config, None));
        DateStyle {
            order: order.map(|order| DateStyleOrder::try_from(order).unwrap_or_default()),
            style: DateStyleDisplayStyle::try_from(style).unwrap_or_default(),
        }
    }

    /// Get datetime format str for current style
    pub fn full_format_str(&self) -> &str {
        match (&self.style, &self.order) {
            (DateStyleDisplayStyle::SQL, Some(DateStyleOrder::DMY)) => "%d/%m/%Y %H:%M:%S%.f",
            (DateStyleDisplayStyle::SQL, _) => "%m/%d/%Y %H:%M:%S%.f",

            (DateStyleDisplayStyle::ISO, _) => "%Y-%m-%d %H:%M:%S%.f",

            (DateStyleDisplayStyle::Postgres, _) => "%a %b %e %H:%M:%S %.f %Y",

            (DateStyleDisplayStyle::German, _) => "%d.%m.%Y %H:%M:%S%.f",
        }
    }

    /// Get datetime with tz format str for current style
    pub fn full_tz_format_str(&self) -> String {
        match self.style {
            DateStyleDisplayStyle::ISO => format!("{}%:::z", self.full_format_str()),
            _ => format!("{} %Z", self.full_format_str()),
        }
    }

    /// Get time with tz format string for current style
    pub fn time_tz_format_str(&self) -> &str {
        match self.style {
            DateStyleDisplayStyle::ISO => "%H:%M:%S%.f%:::z",
            _ => "%H:%M:%S%.f %Z",
        }
    }

    /// Get date format str for current style
    pub fn date_format_str(&self) -> &str {
        match (&self.style, &self.order) {
            (DateStyleDisplayStyle::SQL, Some(DateStyleOrder::DMY)) => "%d/%m/%Y",
            (DateStyleDisplayStyle::SQL, _) => "%m/%d/%Y",

            (DateStyleDisplayStyle::ISO, _) => "%Y-%m-%d",

            (DateStyleDisplayStyle::Postgres, Some(DateStyleOrder::DMY)) => "%d-%m-%Y",
            (DateStyleDisplayStyle::Postgres, _) => "%m-%d-%Y",

            (DateStyleDisplayStyle::German, _) => "%d.%m.%Y",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "pg-type-chrono")]
    use bytes::BytesMut;
    #[cfg(feature = "pg-type-chrono")]
    use chrono::offset::Utc;
    #[cfg(feature = "pg-type-chrono")]
    use chrono::{DateTime, NaiveDate, TimeZone};
    #[cfg(feature = "pg-type-chrono")]
    use postgres_types::Type;

    #[cfg(feature = "pg-type-chrono")]
    use crate::types::ToSqlText;

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
            DateStyleDisplayStyle::German
        ));
        assert!(matches!(
            DateStyleDisplayStyle::try_from("postgres").unwrap(),
            DateStyleDisplayStyle::Postgres
        ));
        assert!(DateStyleDisplayStyle::try_from("invalid").is_err());
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_date_style_new() {
        let date_style = DateStyle::new("ISO");
        assert!(matches!(date_style.style, DateStyleDisplayStyle::ISO));
        assert!(date_style.order.is_none());

        let date_style = DateStyle::new("SQL, DMY");
        assert!(matches!(date_style.style, DateStyleDisplayStyle::SQL));
        assert!(matches!(date_style.order.unwrap(), DateStyleOrder::DMY));

        let date_style = DateStyle::new("invalid");
        assert!(matches!(date_style.style, DateStyleDisplayStyle::ISO));
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_date_style_format_strs() {
        let test_cases = vec![
            (
                "ISO",
                None,
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%d %H:%M:%S%.f%:::z",
                "%Y-%m-%d",
                "%H:%M:%S%.f%:::z",
            ),
            (
                "SQL",
                Some("DMY"),
                "%d/%m/%Y %H:%M:%S%.f",
                "%d/%m/%Y %H:%M:%S%.f %Z",
                "%d/%m/%Y",
                "%H:%M:%S%.f %Z",
            ),
            (
                "SQL",
                None,
                "%m/%d/%Y %H:%M:%S%.f",
                "%m/%d/%Y %H:%M:%S%.f %Z",
                "%m/%d/%Y",
                "%H:%M:%S%.f %Z",
            ),
            (
                "german",
                None,
                "%d.%m.%Y %H:%M:%S%.f",
                "%d.%m.%Y %H:%M:%S%.f %Z",
                "%d.%m.%Y",
                "%H:%M:%S%.f %Z",
            ),
            (
                "postgres",
                None,
                "%a %b %e %H:%M:%S %.f %Y",
                "%a %b %e %H:%M:%S %.f %Y %Z",
                "%m-%d-%Y",
                "%H:%M:%S%.f %Z",
            ),
            (
                "postgres",
                Some("DMY"),
                "%a %b %e %H:%M:%S %.f %Y",
                "%a %b %e %H:%M:%S %.f %Y %Z",
                "%d-%m-%Y",
                "%H:%M:%S%.f %Z",
            ),
        ];

        for (style, order, expected_full, expected_tz, expected_date, expected_time_tz) in
            test_cases
        {
            let config = if let Some(order) = order {
                format!("{}, {}", style, order)
            } else {
                style.to_string()
            };

            let date_style = DateStyle::new(&config);
            assert_eq!(date_style.full_format_str(), expected_full);
            assert_eq!(date_style.full_tz_format_str(), expected_tz);
            assert_eq!(date_style.date_format_str(), expected_date);
            assert_eq!(date_style.time_tz_format_str(), expected_time_tz);
        }
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_datetime_to_sql_text() {
        use chrono_tz::US::Pacific;

        use crate::types::format::FormatOptions;

        let dt = NaiveDate::from_ymd_opt(2023, 12, 25)
            .unwrap()
            .and_hms_micro_opt(14, 30, 45, 123456)
            .unwrap();
        let datetime: DateTime<Utc> = Utc.from_utc_datetime(&dt);

        let mut out = BytesMut::new();
        let mut format_option = FormatOptions::default();
        format_option.date_style = "ISO".to_string();

        datetime
            .to_sql_text(&Type::TIMESTAMP, &mut out, &format_option)
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "2023-12-25 14:30:45.123456"
        );

        out.clear();
        datetime
            .to_sql_text(&Type::TIMESTAMPTZ, &mut out, &format_option)
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "2023-12-25 14:30:45.123456+00"
        );

        out.clear();
        datetime
            .to_sql_text(&Type::DATE, &mut out, &format_option)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "2023-12-25");

        let dt = Pacific.from_local_datetime(&dt).unwrap();

        out.clear();
        dt.to_sql_text(&Type::TIMESTAMPTZ, &mut out, &format_option)
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "2023-12-25 14:30:45.123456-08"
        );
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_naive_datetime_to_sql_text() {
        use chrono_tz::US::Pacific;

        use crate::types::format::FormatOptions;

        let dt = NaiveDate::from_ymd_opt(2023, 12, 25)
            .unwrap()
            .and_hms_micro_opt(14, 30, 45, 123456)
            .unwrap();

        let mut out = BytesMut::new();
        let mut format_options = FormatOptions::default();
        format_options.date_style = "SQL, DMY".to_string();

        dt.to_sql_text(&Type::TIMESTAMP, &mut out, &format_options)
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "25/12/2023 14:30:45.123456"
        );

        out.clear();
        dt.to_sql_text(&Type::DATE, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "25/12/2023");

        out.clear();
        dt.to_sql_text(&Type::TIME, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "14:30:45.123456");

        let dt = Pacific.from_local_datetime(&dt).unwrap();

        out.clear();
        dt.to_sql_text(&Type::TIMESTAMPTZ, &mut out, &format_options)
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "25/12/2023 14:30:45.123456 PST"
        );
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_system_time_to_sql_text() {
        use std::time::{Duration, UNIX_EPOCH};

        use crate::types::format::FormatOptions;

        let system_time = UNIX_EPOCH + Duration::from_secs(1703514645); // 2023-12-25 14:30:45 UTC
        let mut out = BytesMut::new();
        let mut format_options = FormatOptions::default();
        format_options.date_style = "german".to_string();

        system_time
            .to_sql_text(&Type::TIMESTAMP, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "25.12.2023 14:30:45");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_naive_date_to_sql_text() {
        use crate::types::format::FormatOptions;

        let date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
        let mut out = BytesMut::new();
        let mut format_options = FormatOptions::default();
        format_options.date_style = "postgres, DMY".to_string();

        date.to_sql_text(&Type::DATE, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "25-12-2023");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_wrong_type_error() {
        use crate::types::format::FormatOptions;

        let dt = NaiveDate::from_ymd_opt(2023, 12, 25)
            .unwrap()
            .and_hms_opt(14, 30, 45)
            .unwrap();
        let mut out = BytesMut::new();
        let mut format_options = FormatOptions::default();
        format_options.date_style = "ISO".to_string();

        let result = dt.to_sql_text(&Type::BOOL, &mut out, &format_options);
        assert!(result.is_err());
    }
}

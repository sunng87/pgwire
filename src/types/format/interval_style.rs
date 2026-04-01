use crate::error::PgWireError;

pub const INTERVAL_STYLE_POSTGRES: &str = "postgres";
pub const INTERVAL_STYLE_ISO_8601: &str = "iso_8601";
pub const INTERVAL_STYLE_SQL_STANDARD: &str = "sql_standard";
pub const INTERVAL_STYLE_POSTGRES_VERBOSE: &str = "postgres_verbose";

#[derive(Debug, Default, Copy, Clone)]
pub enum IntervalStyle {
    #[default]
    Postgres,
    ISO8601,
    SQLStandard,
    PostgresVerbose,
}

impl TryFrom<&str> for IntervalStyle {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_lowercase().as_ref() {
            INTERVAL_STYLE_POSTGRES => Ok(Self::Postgres),
            INTERVAL_STYLE_ISO_8601 => Ok(Self::ISO8601),
            INTERVAL_STYLE_SQL_STANDARD => Ok(Self::SQLStandard),
            INTERVAL_STYLE_POSTGRES_VERBOSE => Ok(Self::PostgresVerbose),
            _ => Err(PgWireError::InvalidOptionValue(value.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "pg-type-chrono")]
    use bytes::BytesMut;
    #[cfg(feature = "pg-type-chrono")]
    use chrono::Duration;
    #[cfg(feature = "pg-type-chrono")]
    use postgres_types::Type;

    use super::*;
    #[cfg(feature = "pg-type-chrono")]
    use crate::types::ToSqlText;

    #[test]
    fn test_interval_style_from_str() {
        assert!(matches!(
            IntervalStyle::try_from("postgres").unwrap(),
            IntervalStyle::Postgres
        ));
        assert!(matches!(
            IntervalStyle::try_from("iso_8601").unwrap(),
            IntervalStyle::ISO8601
        ));
        assert!(matches!(
            IntervalStyle::try_from("sql_standard").unwrap(),
            IntervalStyle::SQLStandard
        ));
        assert!(matches!(
            IntervalStyle::try_from("postgres_verbose").unwrap(),
            IntervalStyle::PostgresVerbose
        ));
        assert!(IntervalStyle::try_from("invalid").is_err());
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_duration_to_sql_text() {
        use crate::types::format::FormatOptions;

        let duration = Duration::seconds(3661); // 1 hour, 1 minute, 1 second
        let mut out = BytesMut::new();
        let mut format_options = FormatOptions::default();

        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "01:01:01");

        out.clear();
        format_options.interval_style = "iso_8601".to_string();
        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "PT1H1M1S");

        out.clear();
        format_options.interval_style = "sql_standard".to_string();
        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "1:01:01");

        out.clear();
        format_options.interval_style = "postgres_verbose".to_string();
        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "@ 1 hour 1 min 1 sec");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_duration_with_microseconds() {
        use crate::types::format::FormatOptions;

        let duration = Duration::microseconds(1234567); // 1 second, 234567 microseconds
        let mut out = BytesMut::new();
        let mut format_options = FormatOptions::default();

        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "00:00:01.234567");

        out.clear();
        format_options.interval_style = "iso_8601".to_string();
        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();

        out.clear();
        format_options.interval_style = "sql_standard".to_string();
        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "0:00:01.234567");

        out.clear();
        format_options.interval_style = "postgres_verbose".to_string();
        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "@ 1.234567 secs");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_zero_duration() {
        use crate::types::format::FormatOptions;

        let duration = Duration::zero();
        let mut out = BytesMut::new();
        let mut format_options = FormatOptions::default();

        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "00:00:00");

        out.clear();
        format_options.interval_style = "iso_8601".to_string();
        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "PT0S");

        out.clear();
        format_options.interval_style = "sql_standard".to_string();
        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "0");

        out.clear();
        format_options.interval_style = "postgres_verbose".to_string();
        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "@ 0");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_postgres_verbose_pluralization() {
        use crate::types::format::FormatOptions;

        let duration = Duration::seconds(86400 + 7200 + 120 + 2); // 1 day, 2 hours, 2 minutes, 2 seconds
        let mut out = BytesMut::new();
        let mut format_options = FormatOptions::default();
        format_options.interval_style = "postgres_verbose".to_string();

        duration
            .to_sql_text(&Type::INTERVAL, &mut out, &format_options)
            .unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "@ 1 day 2 hours 2 mins 2 secs"
        );
    }
}

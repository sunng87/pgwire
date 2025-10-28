#[cfg(feature = "pg-type-chrono")]
use std::error::Error;

#[cfg(feature = "pg-type-chrono")]
use bytes::{BufMut, BytesMut};
#[cfg(feature = "pg-type-chrono")]
use chrono::Duration;
#[cfg(feature = "pg-type-chrono")]
use postgres_types::{IsNull, Type};

use crate::{api::ClientInfo, error::PgWireError};

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

#[derive(Debug)]
pub struct IntervalStyleWrapper<T> {
    style: IntervalStyle,
    data: T,
}

impl<T> IntervalStyleWrapper<T> {
    pub fn new(data: T, config: &str) -> Self {
        let style = IntervalStyle::try_from(config).unwrap_or_default();

        Self { style, data }
    }

    pub fn new_with_client_info<C>(data: T, client_info: C) -> Self
    where
        C: ClientInfo,
    {
        let config = client_info
            .metadata()
            .get("intervalstyle")
            .map(|s| s.as_str())
            .unwrap_or(INTERVAL_STYLE_POSTGRES);
        Self::new(data, config)
    }

    pub fn data(&self) -> &T {
        &self.data
    }

    pub fn style(&self) -> IntervalStyle {
        self.style
    }
}

#[cfg(feature = "pg-type-chrono")]
impl crate::types::ToSqlText for IntervalStyleWrapper<Duration> {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let duration = &self.data;
        let total_seconds = duration.num_seconds();
        let microseconds = duration.num_microseconds().unwrap_or(0) % 1_000_000;

        // Extract components
        let sign = if total_seconds < 0 { "-" } else { "" };
        let abs_seconds = total_seconds.abs();
        let days = abs_seconds / 86400;
        let hours = (abs_seconds % 86400) / 3600;
        let minutes = (abs_seconds % 3600) / 60;
        let seconds = abs_seconds % 60;

        let output = match self.style {
            IntervalStyle::Postgres => {
                let mut parts = Vec::new();

                if days != 0 {
                    parts.push(format!("{days} days"));
                }
                if hours != 0 || minutes != 0 || seconds != 0 || microseconds != 0 {
                    let time_str = if microseconds == 0 {
                        format!("{hours:02}:{minutes:02}:{seconds:02}")
                    } else {
                        format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}",)
                    };
                    parts.push(time_str);
                }

                if parts.is_empty() {
                    format!("{sign}00:00:00")
                } else {
                    format!("{sign}{}", parts.join(" "))
                }
            }
            IntervalStyle::ISO8601 => {
                let mut parts = Vec::new();

                if days != 0 {
                    parts.push(format!("{days}D"));
                }

                let mut time_parts = Vec::new();
                if hours != 0 {
                    time_parts.push(format!("{hours}H"));
                }
                if minutes != 0 {
                    time_parts.push(format!("{minutes}M",));
                }
                if seconds != 0 || microseconds != 0 {
                    if microseconds == 0 {
                        time_parts.push(format!("{seconds}S",));
                    } else {
                        time_parts.push(format!("{seconds}.{microseconds:06}S"));
                    }
                }

                if !time_parts.is_empty() {
                    parts.push(format!("T{}", time_parts.join("")));
                }

                if parts.is_empty() {
                    format!("{sign}PT0S",)
                } else {
                    format!("{sign}P{}", parts.join(""))
                }
            }
            IntervalStyle::SQLStandard => {
                let mut parts = Vec::new();

                if days != 0 {
                    parts.push(format!("{days} {hours}"));
                } else if hours != 0 || minutes != 0 || seconds != 0 || microseconds != 0 {
                    if microseconds == 0 {
                        parts.push(format!("{hours:02}:{minutes:02}:{seconds:02}"));
                    } else {
                        parts.push(format!(
                            "{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}",
                        ));
                    }
                }

                if parts.is_empty() {
                    format!("{sign}00:00:00")
                } else {
                    format!("{sign}{}", parts.join(" "))
                }
            }
            IntervalStyle::PostgresVerbose => {
                let mut parts = Vec::new();

                if days != 0 {
                    parts.push(format!("{days} day{}", if days != 1 { "s" } else { "" }));
                }
                if hours != 0 {
                    parts.push(format!("{hours} hour{}", if hours != 1 { "s" } else { "" }));
                }
                if minutes != 0 {
                    parts.push(format!(
                        "{minutes} min{}",
                        if minutes != 1 { "s" } else { "" }
                    ));
                }
                if seconds != 0 || microseconds != 0 {
                    if microseconds == 0 {
                        parts.push(format!(
                            "{seconds} sec{}",
                            if seconds != 1 { "s" } else { "" }
                        ));
                    } else {
                        let total_seconds = seconds as f64 + microseconds as f64 / 1_000_000.0;
                        parts.push(format!(
                            "{total_seconds} sec{}",
                            if total_seconds != 1.0 { "s" } else { "" }
                        ));
                    }
                }

                if parts.is_empty() {
                    format!("{sign}@ 0")
                } else {
                    format!("{sign}@ {}", parts.join(" "))
                }
            }
        };

        out.put_slice(output.as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "pg-type-chrono")]
    use crate::types::ToSqlText;
    #[cfg(feature = "pg-type-chrono")]
    use chrono::Duration;

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
    fn test_interval_style_wrapper_new() {
        let duration = Duration::seconds(3600);

        let wrapper = IntervalStyleWrapper::new(duration, "postgres");
        assert!(matches!(wrapper.style, IntervalStyle::Postgres));

        let wrapper = IntervalStyleWrapper::new(duration, "iso_8601");
        assert!(matches!(wrapper.style, IntervalStyle::ISO8601));

        let wrapper = IntervalStyleWrapper::new(duration, "postgres_verbose");
        assert!(matches!(wrapper.style, IntervalStyle::PostgresVerbose));

        let wrapper = IntervalStyleWrapper::new(duration, "invalid");
        assert!(matches!(wrapper.style, IntervalStyle::Postgres));
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_duration_to_sql_text() {
        let duration = Duration::seconds(3661); // 1 hour, 1 minute, 1 second
        let mut out = BytesMut::new();

        let wrapper = IntervalStyleWrapper::new(duration, "postgres");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "01:01:01");

        out.clear();
        let wrapper = IntervalStyleWrapper::new(duration, "iso_8601");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "PT1H1M1S");

        out.clear();
        let wrapper = IntervalStyleWrapper::new(duration, "sql_standard");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "01:01:01");

        out.clear();
        let wrapper = IntervalStyleWrapper::new(duration, "postgres_verbose");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "@ 1 hour 1 min 1 sec");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_duration_with_microseconds() {
        let duration = Duration::microseconds(1234567); // 1 second, 234567 microseconds
        let mut out = BytesMut::new();

        let wrapper = IntervalStyleWrapper::new(duration, "postgres");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "00:00:01.234567");

        out.clear();
        let wrapper = IntervalStyleWrapper::new(duration, "iso_8601");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "PT1.234567S");

        out.clear();
        let wrapper = IntervalStyleWrapper::new(duration, "sql_standard");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "00:00:01.234567");

        out.clear();
        let wrapper = IntervalStyleWrapper::new(duration, "postgres_verbose");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "@ 1.234567 secs");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_zero_duration() {
        let duration = Duration::zero();
        let mut out = BytesMut::new();

        let wrapper = IntervalStyleWrapper::new(duration, "postgres");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "00:00:00");

        out.clear();
        let wrapper = IntervalStyleWrapper::new(duration, "iso_8601");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "PT0S");

        out.clear();
        let wrapper = IntervalStyleWrapper::new(duration, "sql_standard");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "00:00:00");

        out.clear();
        let wrapper = IntervalStyleWrapper::new(duration, "postgres_verbose");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "@ 0");
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_postgres_verbose_pluralization() {
        let duration = Duration::seconds(86400 + 7200 + 120 + 2); // 1 day, 2 hours, 2 minutes, 2 seconds
        let mut out = BytesMut::new();

        let wrapper = IntervalStyleWrapper::new(duration, "postgres_verbose");
        wrapper.to_sql_text(&Type::INTERVAL, &mut out).unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            "@ 1 day 2 hours 2 mins 2 secs"
        );
    }
}

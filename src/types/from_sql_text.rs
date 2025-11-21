use std::error::Error;
use std::fmt;
#[cfg(feature = "pg-type-chrono")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "pg-type-chrono")]
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, Utc};
#[cfg(feature = "pg-type-serde-json")]
use postgres_types::Json;
use postgres_types::Type;
#[cfg(feature = "pg-type-rust-decimal")]
use rust_decimal::Decimal;
#[cfg(feature = "pg-type-serde-json")]
use serde::Deserialize;
#[cfg(feature = "pg-type-serde-json")]
use serde_json::Value;

use crate::types::format::{string::parse_string_postgres, FormatOptions};

pub trait FromSqlText<'a>: fmt::Debug {
    /// Converts value from postgres text format to rust.
    ///
    /// This trait is modelled after `FromSql` from postgres-types, which is
    /// for binary encoding.
    fn from_sql_text(
        ty: &Type,
        input: &'a [u8],
        format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized;
}

fn to_str(f: &[u8]) -> Result<&str, Box<dyn Error + Sync + Send>> {
    std::str::from_utf8(f).map_err(Into::into)
}

impl<'a> FromSqlText<'a> for bool {
    fn from_sql_text(
        _ty: &Type,
        input: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let len = input.len();

        if len == 0 {
            return Ok(false);
        }

        // Helper function for case-insensitive comparison of a byte slice against an ASCII string
        // This function returns true only if the slices are of the same length and content (case-insensitive)
        let byte_case_cmp = |slice: &[u8], target: &str| -> bool {
            slice.len() == target.len()
                && slice
                    .iter()
                    .zip(target.as_bytes().iter())
                    .all(|(b1, b2)| b1.to_ascii_lowercase() == b2.to_ascii_lowercase())
        };

        match input[0].to_ascii_lowercase() {
            // --- Cases 't' and 'f' ---
            b't' => {
                if byte_case_cmp(input, "true") || byte_case_cmp(input, "t") {
                    return Ok(true);
                }
            }
            b'f' => {
                if byte_case_cmp(input, "false") || byte_case_cmp(input, "f") {
                    return Ok(false);
                }
            }

            // --- Cases 'y' and 'n' ---
            b'y' => {
                if byte_case_cmp(input, "yes") || byte_case_cmp(input, "y") {
                    return Ok(true);
                }
            }
            b'n' => {
                if byte_case_cmp(input, "no") || byte_case_cmp(input, "n") {
                    return Ok(false);
                }
            }

            // --- Case 'o' ---
            b'o' => {
                // Check 'on' (length must be exactly 2)
                if len == 2 && byte_case_cmp(input, "on") {
                    return Ok(true);
                }
                // Check 'off' (length must be exactly 3)
                if len == 3 && byte_case_cmp(input, "off") {
                    return Ok(false);
                }
            }

            // --- Cases '1' and '0' ---
            b'1' => {
                if len == 1 {
                    return Ok(true);
                }
            }
            b'0' => {
                if len == 1 {
                    return Ok(false);
                }
            }

            _ => {}
        }

        Err("Invalid text value for bool".into())
    }
}

impl<'a> FromSqlText<'a> for String {
    fn from_sql_text(
        _ty: &Type,
        input: &[u8],
        format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        to_str(input).map(|s| parse_string_postgres(s, format_options.standard_conforming_strings))
    }
}

macro_rules! impl_from_sql_text {
    ($t:ty) => {
        impl<'a> FromSqlText<'a> for $t {
            fn from_sql_text(
                _ty: &Type,
                input: &[u8],
                _format_options: &FormatOptions,
            ) -> Result<Self, Box<dyn Error + Sync + Send>> {
                to_str(input).and_then(|s| s.parse::<$t>().map_err(Into::into))
            }
        }
    };
}

impl_from_sql_text!(i8);
impl_from_sql_text!(i16);
impl_from_sql_text!(i32);
impl_from_sql_text!(i64);
impl_from_sql_text!(u32);
impl_from_sql_text!(f32);
impl_from_sql_text!(f64);
impl_from_sql_text!(char);

#[cfg(feature = "pg-type-rust-decimal")]
impl<'a> FromSqlText<'a> for Decimal {
    fn from_sql_text(
        _ty: &Type,
        input: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        Decimal::from_str_exact(to_str(input)?).map_err(Into::into)
    }
}

impl<'a> FromSqlText<'a> for Vec<u8> {
    fn from_sql_text(
        _ty: &Type,
        input: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let data = input
            .strip_prefix(b"\\x")
            .ok_or("\\x prefix expected for bytea")?;

        hex::decode(data).map_err(|e| e.to_string().into())
    }
}

#[cfg(feature = "pg-type-chrono")]
impl<'a> FromSqlText<'a> for SystemTime {
    fn from_sql_text(
        _ty: &Type,
        value: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let datetime = NaiveDateTime::parse_from_str(to_str(value)?, "%Y-%m-%d %H:%M:%S.6f")?;
        let system_time =
            UNIX_EPOCH + Duration::from_millis(datetime.and_utc().timestamp_millis() as u64);

        Ok(system_time)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl<'a> FromSqlText<'a> for DateTime<FixedOffset> {
    fn from_sql_text(
        ty: &Type,
        value: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match *ty {
            Type::TIMESTAMP | Type::TIMESTAMP_ARRAY => {
                let fmt = "%Y-%m-%d %H:%M:%S%.6f";
                let datetime = NaiveDateTime::parse_from_str(to_str(value)?, fmt)?;

                Ok(DateTime::from_naive_utc_and_offset(datetime, Utc.fix()))
            }
            Type::TIMESTAMPTZ | Type::TIMESTAMPTZ_ARRAY => {
                let fmt = "%Y-%m-%d %H:%M:%S%.6f%:::z";
                let datetime = DateTime::parse_from_str(to_str(value)?, fmt)?;
                Ok(datetime)
            }
            Type::DATE | Type::DATE_ARRAY => {
                let fmt = "%Y-%m-%d";
                let datetime = NaiveDateTime::parse_from_str(to_str(value)?, fmt)?;
                Ok(DateTime::from_naive_utc_and_offset(datetime, Utc.fix()))
            }
            _ => Err(Box::new(postgres_types::WrongType::new::<DateTime<Utc>>(
                ty.clone(),
            ))),
        }
    }
}

#[cfg(feature = "pg-type-chrono")]
impl<'a> FromSqlText<'a> for NaiveDate {
    fn from_sql_text(
        _ty: &Type,
        value: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let date = NaiveDate::parse_from_str(to_str(value)?, "%Y-%m-%d")?;
        Ok(date)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl<'a> FromSqlText<'a> for NaiveTime {
    fn from_sql_text(
        _ty: &Type,
        value: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let time = NaiveTime::parse_from_str(to_str(value)?, "%H:%M:%S")?;
        Ok(time)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl<'a> FromSqlText<'a> for NaiveDateTime {
    fn from_sql_text(
        _ty: &Type,
        value: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let datetime = NaiveDateTime::parse_from_str(to_str(value)?, "%Y-%m-%d %H:%M:%S")?;
        Ok(datetime)
    }
}

#[cfg(feature = "pg-type-serde-json")]
impl<'a, T: Deserialize<'a> + fmt::Debug> FromSqlText<'a> for Json<T> {
    fn from_sql_text(
        _ty: &Type,
        input: &'a [u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        serde_json::de::from_slice::<T>(input)
            .map(Json)
            .map_err(Into::into)
    }
}

#[cfg(feature = "pg-type-serde-json")]
impl<'a> FromSqlText<'a> for Value {
    fn from_sql_text(
        _ty: &Type,
        input: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        serde_json::de::from_slice::<Value>(input).map_err(Into::into)
    }
}

impl<'a, T> FromSqlText<'a> for Option<T>
where
    T: FromSqlText<'a>,
{
    fn from_sql_text(
        ty: &Type,
        input: &'a [u8],
        format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if input.is_empty() {
            Ok(None)
        } else {
            T::from_sql_text(ty, input, format_options).map(Some)
        }
    }
}

//TODO: array types

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_sql_text_for_string() {
        let sql_text = "Hello, World!".as_bytes();
        let result =
            String::from_sql_text(&Type::VARCHAR, sql_text, &FormatOptions::default()).unwrap();
        assert_eq!(result, "Hello, World!");
    }

    #[test]
    fn test_from_sql_text_for_i32() {
        let sql_text = "42".as_bytes();
        let result =
            i32::from_sql_text(&Type::VARCHAR, sql_text, &FormatOptions::default()).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_from_sql_text_for_i32_invalid() {
        let sql_text = "not_a_number".as_bytes();
        let result = i32::from_sql_text(&Type::INT4, sql_text, &FormatOptions::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_text_for_f64() {
        let sql_text = "1.23".as_bytes();
        let result =
            f64::from_sql_text(&Type::FLOAT8, sql_text, &FormatOptions::default()).unwrap();
        assert_eq!(result, 1.23);
    }

    #[test]
    fn test_from_sql_text_for_f64_invalid() {
        let sql_text = "not_a_number".as_bytes();
        let result = f64::from_sql_text(&Type::FLOAT8, sql_text, &FormatOptions::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_text_for_bool() {
        let sql_text = "t".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(result);

        let sql_text = "f".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(!result);

        let sql_text = "TRUE".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(result);

        let sql_text = "FALSE".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(!result);

        let sql_text = "on".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(result);

        let sql_text = "off".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(!result);

        let sql_text = "1".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(result);

        let sql_text = "0".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(!result);

        let sql_text = "y".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(result);

        let sql_text = "n".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(!result);

        let sql_text = "yes".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(result);

        let sql_text = "no".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default()).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_from_sql_text_for_bool_invalid() {
        let sql_text = "not_a_boolean".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_text_for_option_string() {
        let sql_text = "Some text".as_bytes();
        let result =
            Option::<String>::from_sql_text(&Type::VARCHAR, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, Some("Some text".to_string()));

        let sql_text = "".as_bytes();
        let result =
            Option::<String>::from_sql_text(&Type::VARCHAR, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_from_sql_text_for_option_i32() {
        let sql_text = "42".as_bytes();
        let result =
            Option::<i32>::from_sql_text(&Type::INT4, sql_text, &FormatOptions::default()).unwrap();
        assert_eq!(result, Some(42));

        let sql_text = "".as_bytes();
        let result =
            Option::<i32>::from_sql_text(&Type::INT4, sql_text, &FormatOptions::default()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_from_sql_text_for_option_f64() {
        let sql_text = "1.23".as_bytes();
        let result =
            Option::<f64>::from_sql_text(&Type::FLOAT8, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, Some(1.23));

        let sql_text = "".as_bytes();
        let result =
            Option::<f64>::from_sql_text(&Type::FLOAT8, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_from_sql_text_for_option_bool() {
        let sql_text = "t".as_bytes();
        let result =
            Option::<bool>::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, Some(true));

        let sql_text = "".as_bytes();
        let result =
            Option::<bool>::from_sql_text(&Type::BOOL, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, None);
    }
}

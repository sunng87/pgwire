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

        match input[0].to_ascii_lowercase() {
            // --- Cases 't' and 'f' ---
            b't' => {
                if input.eq_ignore_ascii_case(b"true") || input.eq_ignore_ascii_case(b"t") {
                    return Ok(true);
                }
            }
            b'f' => {
                if input.eq_ignore_ascii_case(b"false") || input.eq_ignore_ascii_case(b"f") {
                    return Ok(false);
                }
            }

            // --- Cases 'y' and 'n' ---
            b'y' => {
                if input.eq_ignore_ascii_case(b"yes") || input.eq_ignore_ascii_case(b"y") {
                    return Ok(true);
                }
            }
            b'n' => {
                if input.eq_ignore_ascii_case(b"no") || input.eq_ignore_ascii_case(b"n") {
                    return Ok(false);
                }
            }

            // --- Case 'o' ---
            b'o' => {
                // Check 'on' (length must be exactly 2)
                if len == 2 && input.eq_ignore_ascii_case(b"on") {
                    return Ok(true);
                }
                // Check 'off' (length must be exactly 3)
                if len == 3 && input.eq_ignore_ascii_case(b"off") {
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
        ty: &Type,
        value: &[u8],
        format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let datetime = NaiveDateTime::from_sql_text(ty, value, format_options)?;
        let system_time =
            UNIX_EPOCH + Duration::from_millis(datetime.and_utc().timestamp_millis() as u64);

        Ok(system_time)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl<'a> FromSqlText<'a> for DateTime<FixedOffset> {
    fn from_sql_text(
        _ty: &Type,
        value: &[u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let value_str = to_str(value)?;

        // For TIMESTAMPTZ, parse with timezone and preserve it
        let timezone_formats = [
            "%Y-%m-%d %H:%M:%S%.6f%:::z", // "2024-11-24 11:00:00.123456+08:00:00"
            "%Y-%m-%d %H:%M:%S%.6f%:z",   // "2024-11-24 11:00:00.123456+08:00"
            "%Y-%m-%d %H:%M:%S%.6f%#z",   // "2024-11-24 11:00:00.123456+08"
            "%Y-%m-%d %H:%M:%S%.6f%z",    // "2024-11-24 11:00:00.123456+0800"
            "%Y-%m-%d %H:%M:%S%:z",       // "2024-11-24 11:00:00+08:00"
            "%Y-%m-%d %H:%M:%S%#z",       // "2024-11-24 11:00:00+08"
            "%Y-%m-%d %H:%M:%S%z",        // "2024-11-24 11:00:00+0800"
        ];

        for format in &timezone_formats {
            if let Ok(datetime) = DateTime::parse_from_str(value_str, format) {
                return Ok(datetime);
            }
        }

        // Fallback: try without timezone and assume UTC
        let naive_formats = [
            "%Y-%m-%d %H:%M:%S%.6f", // "2024-11-24 11:00:00.123456"
            "%Y-%m-%d %H:%M:%S",     // "2024-11-24 11:00:00"
        ];

        for format in &naive_formats {
            if let Ok(datetime) = NaiveDateTime::parse_from_str(value_str, format) {
                return Ok(DateTime::from_naive_utc_and_offset(datetime, Utc.fix()));
            }
        }

        // Handle DATE types
        if let Ok(date) = NaiveDate::parse_from_str(value_str, "%Y-%m-%d") {
            let datetime = date.and_hms_opt(0, 0, 0).unwrap();
            return Ok(DateTime::from_naive_utc_and_offset(datetime, Utc.fix()));
        }

        Err(format!("Unable to parse datetime: {}", value_str).into())
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
        let value_str = to_str(value)?;

        // Try parsing date without timezone first
        if let Ok(date) = NaiveDate::parse_from_str(value_str, "%Y-%m-%d") {
            return Ok(date);
        }

        // Handle pgjdbc format with timezone: "2024-06-20 +08"
        // Split on whitespace to separate date from timezone
        if let Some((date_part, _tz_part)) = value_str.split_once(' ') {
            if let Ok(date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
                return Ok(date);
            }
        }

        // Handle formats with time components that might include timezone
        let datetime_formats = [
            "%Y-%m-%d %H:%M:%S%.f%:z", // "2024-06-20 11:00:00.123456+08:00"
            "%Y-%m-%d %H:%M:%S%.f%#z", // "2024-06-20 11:00:00.123456+08"
            "%Y-%m-%d %H:%M:%S%.f%z",  // "2024-06-20 11:00:00.123456+0800"
            "%Y-%m-%d %H:%M:%S%:z",    // "2024-06-20 11:00:00+08:00"
            "%Y-%m-%d %H:%M:%S%#z",    // "2024-06-20 11:00:00+08"
            "%Y-%m-%d %H:%M:%S%z",     // "2024-06-20 11:00:00+0800"
            "%Y-%m-%d %H:%M:%S%.f",    // "2024-06-20 11:00:00.123456"
            "%Y-%m-%d %H:%M:%S",       // "2024-06-20 11:00:00"
        ];

        for format in &datetime_formats {
            if let Ok(datetime) = NaiveDateTime::parse_from_str(value_str, format) {
                return Ok(datetime.date());
            }
        }

        Err(format!("Unable to parse date: {}", value_str).into())
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
        let time = NaiveTime::parse_from_str(to_str(value)?, "%H:%M:%S%.6f")?;
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
        let value_str = to_str(value)?;

        // For TIMESTAMP, ignore timezone and parse as naive datetime
        let naive_formats = [
            "%Y-%m-%d %H:%M:%S%.f", // "2024-11-24 11:00:00.123456"
            "%Y-%m-%d %H:%M:%S%.f", // "2024-11-24 11:00:00.123456"
            "%Y-%m-%d %H:%M:%S",    // "2024-11-24 11:00:00"
        ];

        for format in &naive_formats {
            if let Ok(parsed) = NaiveDateTime::parse_from_str(value_str, format) {
                return Ok(parsed);
            }
        }

        // Fallback: try to parse with timezone and extract naive part
        let timezone_formats = [
            "%Y-%m-%d %H:%M:%S%.f%:z", // "2024-11-24 11:00:00.123456+08:00"
            "%Y-%m-%d %H:%M:%S%.f%#z", // "2024-11-24 11:00:00.123456+08"
            "%Y-%m-%d %H:%M:%S%.f%z",  // "2024-11-24 11:00:00.123456+0800"
            "%Y-%m-%d %H:%M:%S%:z",    // "2024-11-24 11:00:00+08:00"
            "%Y-%m-%d %H:%M:%S%#z",    // "2024-11-24 11:00:00+08"
            "%Y-%m-%d %H:%M:%S%z",     // "2024-11-24 11:00:00+0800"
        ];

        for format in &timezone_formats {
            if let Ok(parsed) = DateTime::<FixedOffset>::parse_from_str(value_str, format) {
                return Ok(parsed.naive_local());
            }
        }

        Err(format!("Unable to parse datetime: {}", value_str).into())
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

// Helper function to extract array elements
fn extract_array_elements(input: &str) -> Result<Vec<String>, Box<dyn Error + Sync + Send>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    let mut depth = 0; // For nested arrays

    for ch in input.chars() {
        match ch {
            '\\' if !escape_next => {
                escape_next = true;
                if in_quotes {
                    current.push(ch);
                }
            }
            '"' if !escape_next => {
                in_quotes = !in_quotes;
                // Don't include the quotes in the output
            }
            '{' if !in_quotes && !escape_next => {
                depth += 1;
                current.push(ch);
            }
            '}' if !in_quotes && !escape_next => {
                depth -= 1;
                current.push(ch);
            }
            ',' if !in_quotes && depth == 0 && !escape_next => {
                // End of current element
                if !current.trim().eq_ignore_ascii_case("NULL") {
                    elements.push(std::mem::take(&mut current));
                }
            }
            _ => {
                current.push(ch);
                escape_next = false;
            }
        }
    }

    // Process the last element
    if !current.is_empty() && !current.trim().eq_ignore_ascii_case("NULL") {
        elements.push(current);
    }

    Ok(elements)
}

macro_rules! impl_vec_from_sql_text {
    ($t:ty) => {
        impl<'a> FromSqlText<'a> for Vec<$t> {
            fn from_sql_text(
                ty: &Type,
                input: &'a [u8],
                format_options: &FormatOptions,
            ) -> Result<Self, Box<dyn Error + Sync + Send>> {
                // PostgreSQL array text format: {elem1,elem2,elem3}
                // Remove the outer braces
                let input_str = to_str(input)?;

                if input_str.is_empty() {
                    return Ok(Vec::new());
                }

                // Check if it's an array format
                if !input_str.starts_with('{') || !input_str.ends_with('}') {
                    return Err("Invalid array format: must start with '{' and end with '}'".into());
                }

                let inner = &input_str[1..input_str.len() - 1];

                if inner.is_empty() {
                    return Ok(Vec::new());
                }

                let elements = extract_array_elements(inner)?;
                let mut result = Vec::new();

                for element_str in elements {
                    let element = <$t>::from_sql_text(ty, element_str.as_bytes(), format_options)?;
                    result.push(element);
                }

                Ok(result)
            }
        }
    };
}

impl_vec_from_sql_text!(i8);
impl_vec_from_sql_text!(i16);
impl_vec_from_sql_text!(i32);
impl_vec_from_sql_text!(i64);
impl_vec_from_sql_text!(u32);
impl_vec_from_sql_text!(f32);
impl_vec_from_sql_text!(f64);
impl_vec_from_sql_text!(char);
impl_vec_from_sql_text!(bool);
impl_vec_from_sql_text!(String);

#[cfg(feature = "pg-type-chrono")]
impl_vec_from_sql_text!(NaiveDate);
#[cfg(feature = "pg-type-chrono")]
impl_vec_from_sql_text!(NaiveTime);
#[cfg(feature = "pg-type-chrono")]
impl_vec_from_sql_text!(NaiveDateTime);
#[cfg(feature = "pg-type-chrono")]
impl_vec_from_sql_text!(DateTime<FixedOffset>);
#[cfg(feature = "pg-type-chrono")]
impl_vec_from_sql_text!(SystemTime);

// Helper function to extract array elements including NULL values for Option types
fn extract_array_elements_with_nulls(
    input: &str,
) -> Result<Vec<String>, Box<dyn Error + Sync + Send>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    let mut depth = 0; // For nested arrays

    for ch in input.chars() {
        match ch {
            '\\' if !escape_next => {
                escape_next = true;
                if in_quotes {
                    current.push(ch);
                }
            }
            '"' if !escape_next => {
                in_quotes = !in_quotes;
                // Don't include the quotes in the output
            }
            '{' if !in_quotes && !escape_next => {
                depth += 1;
                current.push(ch);
            }
            '}' if !in_quotes && !escape_next => {
                depth -= 1;
                current.push(ch);
            }
            ',' if !in_quotes && depth == 0 && !escape_next => {
                // End of current element - include NULL values for Option types
                elements.push(std::mem::take(&mut current));
            }
            _ => {
                current.push(ch);
                escape_next = false;
            }
        }
    }

    // Process the last element
    if !current.is_empty() {
        elements.push(current);
    }

    Ok(elements)
}

macro_rules! impl_vec_option_from_sql_text {
    ($t:ty) => {
        impl<'a> FromSqlText<'a> for Vec<Option<$t>> {
            fn from_sql_text(
                ty: &Type,
                input: &'a [u8],
                format_options: &FormatOptions,
            ) -> Result<Self, Box<dyn Error + Sync + Send>> {
                // PostgreSQL array text format: {elem1,elem2,elem3}
                // Remove the outer braces
                let input_str = to_str(input)?;

                if input_str.is_empty() {
                    return Ok(Vec::new());
                }

                // Check if it's an array format
                if !input_str.starts_with('{') || !input_str.ends_with('}') {
                    return Err("Invalid array format: must start with '{' and end with '}'".into());
                }

                let inner = &input_str[1..input_str.len() - 1];

                if inner.is_empty() {
                    return Ok(Vec::new());
                }

                let elements = extract_array_elements_with_nulls(inner)?;
                let mut result = Vec::new();

                for element_str in elements {
                    if element_str.trim().eq_ignore_ascii_case("NULL") {
                        result.push(None);
                    } else if element_str.is_empty() {
                        result.push(None);
                    } else {
                        let element =
                            <$t>::from_sql_text(ty, element_str.as_bytes(), format_options)
                                .map(Some)?;
                        result.push(element);
                    }
                }

                Ok(result)
            }
        }
    };
}

impl_vec_option_from_sql_text!(i8);
impl_vec_option_from_sql_text!(i16);
impl_vec_option_from_sql_text!(i32);
impl_vec_option_from_sql_text!(i64);
impl_vec_option_from_sql_text!(u32);
impl_vec_option_from_sql_text!(f32);
impl_vec_option_from_sql_text!(f64);
impl_vec_option_from_sql_text!(char);
impl_vec_option_from_sql_text!(bool);
impl_vec_option_from_sql_text!(String);

#[cfg(feature = "pg-type-chrono")]
impl_vec_option_from_sql_text!(NaiveDate);
#[cfg(feature = "pg-type-chrono")]
impl_vec_option_from_sql_text!(NaiveTime);
#[cfg(feature = "pg-type-chrono")]
impl_vec_option_from_sql_text!(NaiveDateTime);
#[cfg(feature = "pg-type-chrono")]
impl_vec_option_from_sql_text!(DateTime<FixedOffset>);
#[cfg(feature = "pg-type-chrono")]
impl_vec_option_from_sql_text!(SystemTime);

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

    #[test]
    fn test_from_sql_text_for_vec_i32() {
        let sql_text = "{1,2,3}".as_bytes();
        let result =
            Vec::<i32>::from_sql_text(&Type::INT4_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec![1, 2, 3]);

        let sql_text = "{}".as_bytes();
        let result =
            Vec::<i32>::from_sql_text(&Type::INT4_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, Vec::<i32>::new());

        let sql_text = "".as_bytes();
        let result =
            Vec::<i32>::from_sql_text(&Type::INT4_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, Vec::<i32>::new());
    }

    #[test]
    fn test_from_sql_text_for_vec_i64() {
        let sql_text = "{100,200,300}".as_bytes();
        let result =
            Vec::<i64>::from_sql_text(&Type::INT8_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec![100, 200, 300]);
    }

    #[test]
    fn test_from_sql_text_for_vec_i16() {
        let sql_text = "{10,20,30}".as_bytes();
        let result =
            Vec::<i16>::from_sql_text(&Type::INT2_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[test]
    fn test_from_sql_text_for_vec_i8() {
        let sql_text = "{1,2,3}".as_bytes();
        let result =
            Vec::<i8>::from_sql_text(&Type::INT2_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_from_sql_text_for_vec_string() {
        let sql_text = "{hello,world}".as_bytes();
        let result =
            Vec::<String>::from_sql_text(&Type::VARCHAR_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec!["hello".to_string(), "world".to_string()]);

        // Test with quoted strings containing commas
        let sql_text = r#"{"hello,world",test}"#.as_bytes();
        let result =
            Vec::<String>::from_sql_text(&Type::VARCHAR_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec!["hello,world".to_string(), "test".to_string()]);

        // Test with empty array
        let sql_text = "{}".as_bytes();
        let result =
            Vec::<String>::from_sql_text(&Type::VARCHAR_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, Vec::<String>::new());

        // Test with single element
        let sql_text = "{single}".as_bytes();
        let result =
            Vec::<String>::from_sql_text(&Type::VARCHAR_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec!["single".to_string()]);
    }

    #[test]
    fn test_from_sql_text_for_vec_f32() {
        let sql_text = "{1.5,2.5,3.5}".as_bytes();
        let result =
            Vec::<f32>::from_sql_text(&Type::FLOAT4_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec![1.5, 2.5, 3.5]);
    }

    #[test]
    fn test_from_sql_text_for_vec_f64() {
        let sql_text = "{1.23,4.56,7.89}".as_bytes();
        let result =
            Vec::<f64>::from_sql_text(&Type::FLOAT8_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec![1.23, 4.56, 7.89]);
    }

    #[test]
    fn test_from_sql_text_for_vec_u32() {
        let sql_text = "{100,200,300}".as_bytes();
        let result =
            Vec::<u32>::from_sql_text(&Type::INT4_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec![100, 200, 300]);
    }

    #[test]
    fn test_from_sql_text_for_vec_char() {
        let sql_text = "{a,b,c}".as_bytes();
        let result =
            Vec::<char>::from_sql_text(&Type::CHAR_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec!['a', 'b', 'c']);
    }

    #[test]
    fn test_from_sql_text_for_vec_bool() {
        let sql_text = "{t,f,true,false}".as_bytes();
        let result =
            Vec::<bool>::from_sql_text(&Type::BOOL_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, vec![true, false, true, false]);
    }

    #[test]
    fn test_from_sql_text_for_vec_invalid_array() {
        let sql_text = "[1,2,3]".as_bytes();
        let result =
            Vec::<i32>::from_sql_text(&Type::INT4_ARRAY, sql_text, &FormatOptions::default());
        assert!(result.is_err());

        let sql_text = "{1,2,3".as_bytes();
        let result =
            Vec::<i32>::from_sql_text(&Type::INT4_ARRAY, sql_text, &FormatOptions::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_text_for_vec_option_i32() {
        let sql_text = "{1,2,3}".as_bytes();
        let result = Vec::<Option<i32>>::from_sql_text(
            &Type::INT4_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(result, vec![Some(1), Some(2), Some(3)]);

        let sql_text = "{}".as_bytes();
        let result = Vec::<Option<i32>>::from_sql_text(
            &Type::INT4_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(result, Vec::<Option<i32>>::new());

        let sql_text = "".as_bytes();
        let result = Vec::<Option<i32>>::from_sql_text(
            &Type::INT4_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(result, Vec::<Option<i32>>::new());
    }

    #[test]
    fn test_from_sql_text_for_vec_option_string() {
        let sql_text = "{hello,world}".as_bytes();
        let result = Vec::<Option<String>>::from_sql_text(
            &Type::VARCHAR_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(
            result,
            vec![Some("hello".to_string()), Some("world".to_string())]
        );

        // Test with quoted strings containing commas
        let sql_text = r#"{"hello,world",test}"#.as_bytes();
        let result = Vec::<Option<String>>::from_sql_text(
            &Type::VARCHAR_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(
            result,
            vec![Some("hello,world".to_string()), Some("test".to_string())]
        );

        // Test with empty array
        let sql_text = "{}".as_bytes();
        let result = Vec::<Option<String>>::from_sql_text(
            &Type::VARCHAR_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(result, Vec::<Option<String>>::new());
    }

    #[test]
    fn test_from_sql_text_for_vec_option_bool() {
        let sql_text = "{t,f,true,false}".as_bytes();
        let result = Vec::<Option<bool>>::from_sql_text(
            &Type::BOOL_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(
            result,
            vec![Some(true), Some(false), Some(true), Some(false)]
        );
    }

    #[test]
    fn test_from_sql_text_for_vec_option_f64() {
        let sql_text = "{1.23,4.56,7.89}".as_bytes();
        let result = Vec::<Option<f64>>::from_sql_text(
            &Type::FLOAT8_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(result, vec![Some(1.23), Some(4.56), Some(7.89)]);
    }

    #[test]
    fn test_from_sql_text_for_vec_option_with_nulls() {
        // Test that NULL values are converted to None
        let sql_text = "{1,NULL,3,NULL,5}".as_bytes();
        let result = Vec::<Option<i32>>::from_sql_text(
            &Type::INT4_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(result, vec![Some(1), None, Some(3), None, Some(5)]);
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_naive_datetime_with_timezone() {
        // Test NaiveDateTime: should ignore timezone and parse as naive
        let sql_text = "2024-11-24 11:00:00+08".as_bytes();
        let result =
            NaiveDateTime::from_sql_text(&Type::TIMESTAMP, sql_text, &FormatOptions::default())
                .unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 11, 24)
            .unwrap()
            .and_hms_opt(11, 0, 0)
            .unwrap(); // Should ignore timezone, keep 11:00
        assert_eq!(result, expected);

        // Test with microseconds
        let sql_text = "2024-11-24 11:00:00.123456+08:00".as_bytes();
        let result =
            NaiveDateTime::from_sql_text(&Type::TIMESTAMP, sql_text, &FormatOptions::default())
                .unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 11, 24)
            .unwrap()
            .and_hms_micro_opt(11, 0, 0, 123456)
            .unwrap(); // Should ignore timezone
        assert_eq!(result, expected);

        // Test without timezone
        let sql_text = "2024-11-24 11:00:00".as_bytes();
        let result =
            NaiveDateTime::from_sql_text(&Type::TIMESTAMP, sql_text, &FormatOptions::default())
                .unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 11, 24)
            .unwrap()
            .and_hms_opt(11, 0, 0)
            .unwrap();
        assert_eq!(result, expected);
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_datetime_fixed_offset_with_timezone() {
        // Test parsing DateTime<FixedOffset> from various timezone formats
        let sql_text = "2024-11-24 11:00:00+08".as_bytes();
        let result = DateTime::<FixedOffset>::from_sql_text(
            &Type::TIMESTAMP,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 11, 24)
            .unwrap()
            .and_hms_opt(11, 0, 0)
            .unwrap();
        assert_eq!(result.naive_local(), expected);
        assert_eq!(result.offset(), &offset);

        // Test with full timezone format
        let sql_text = "2024-11-24 11:00:00+08:00".as_bytes();
        let result = DateTime::<FixedOffset>::from_sql_text(
            &Type::TIMESTAMP,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 11, 24)
            .unwrap()
            .and_hms_opt(11, 0, 0)
            .unwrap();
        assert_eq!(result.naive_local(), expected);
        assert_eq!(result.offset(), &offset);

        // Test with microseconds
        let sql_text = "2024-11-24 11:00:00.123456+08:00".as_bytes();
        let result = DateTime::<FixedOffset>::from_sql_text(
            &Type::TIMESTAMP,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 11, 24)
            .unwrap()
            .and_hms_micro_opt(11, 0, 0, 123456)
            .unwrap();
        assert_eq!(result.naive_local(), expected);
        assert_eq!(result.offset(), &offset);

        // Test without timezone (should assume UTC)
        let sql_text = "2024-11-24 11:00:00".as_bytes();
        let result = DateTime::<FixedOffset>::from_sql_text(
            &Type::TIMESTAMP,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 11, 24)
            .unwrap()
            .and_hms_opt(11, 0, 0)
            .unwrap();
        assert_eq!(result.naive_utc(), expected);
        assert_eq!(result.offset(), &Utc.fix());

        // Test negative timezone
        let sql_text = "2024-11-24 11:00:00-05:00".as_bytes();
        let result = DateTime::<FixedOffset>::from_sql_text(
            &Type::TIMESTAMP,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        let offset = FixedOffset::west_opt(5 * 3600).unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 11, 24)
            .unwrap()
            .and_hms_opt(11, 0, 0)
            .unwrap();
        assert_eq!(result.naive_local(), expected);
        assert_eq!(result.offset(), &offset);
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_datetime_various_formats() {
        // Test various timezone formats that pgjdbc might send
        let test_cases = vec![
            ("2024-11-24 11:00:00+08", "UTC+8"),
            ("2024-11-24 11:00:00+0800", "UTC+8"),
            ("2024-11-24 11:00:00+08:00", "UTC+8"),
            ("2024-11-24 11:00:00-05", "UTC-5"),
            ("2024-11-24 11:00:00-0500", "UTC-5"),
            ("2024-11-24 11:00:00-05:00", "UTC-5"),
            (
                "2024-11-24 11:00:00.123456+08:00",
                "UTC+8 with microseconds",
            ),
            ("2024-11-24 11:00:00", "no timezone (UTC)"),
        ];

        for (input, description) in test_cases {
            let result = DateTime::<FixedOffset>::from_sql_text(
                &Type::TIMESTAMP,
                input.as_bytes(),
                &FormatOptions::default(),
            );
            assert!(
                result.is_ok(),
                "Failed to parse {}: {}",
                description,
                result.unwrap_err()
            );
        }
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_datetime_type_specific_behavior() {
        use chrono::Timelike;

        // Test that DateTime<FixedOffset> preserves timezone
        let sql_text = "2024-11-24 11:00:00+08";

        let result = DateTime::<FixedOffset>::from_sql_text(
            &Type::TIMESTAMPTZ,
            sql_text.as_bytes(),
            &FormatOptions::default(),
        )
        .unwrap();
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        assert_eq!(result.offset(), &offset);
        assert_eq!(result.hour(), 11); // Should preserve local time
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_naive_date_with_timezone() {
        // Test parsing date with pgjdbc timezone format: "2024-06-20 +08"
        let sql_text = "2024-06-20 +08".as_bytes();
        let result =
            NaiveDate::from_sql_text(&Type::DATE, sql_text, &FormatOptions::default()).unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 6, 20).unwrap();
        assert_eq!(result, expected);

        // Test parsing date with negative timezone: "2024-06-20 -05"
        let sql_text = "2024-06-20 -05".as_bytes();
        let result =
            NaiveDate::from_sql_text(&Type::DATE, sql_text, &FormatOptions::default()).unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 6, 20).unwrap();
        assert_eq!(result, expected);

        // Test parsing date with datetime and timezone (should extract date part)
        let sql_text = "2024-06-20 11:00:00+08:00".as_bytes();
        let result =
            NaiveDate::from_sql_text(&Type::DATE, sql_text, &FormatOptions::default()).unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 6, 20).unwrap();
        assert_eq!(result, expected);

        // Test parsing date without timezone (should still work)
        let sql_text = "2024-06-20".as_bytes();
        let result =
            NaiveDate::from_sql_text(&Type::DATE, sql_text, &FormatOptions::default()).unwrap();
        let expected = NaiveDate::from_ymd_opt(2024, 6, 20).unwrap();
        assert_eq!(result, expected);
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_vec_naive_date() {
        let sql_text = "{2024-01-01,2024-12-31}".as_bytes();
        let result =
            Vec::<NaiveDate>::from_sql_text(&Type::DATE_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(
            result,
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()
            ]
        );

        // Test empty array
        let sql_text = "{}".as_bytes();
        let result =
            Vec::<NaiveDate>::from_sql_text(&Type::DATE_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(result, Vec::<NaiveDate>::new());
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_vec_naive_time() {
        let sql_text = "{12:30:45,23:59:59.123456}".as_bytes();
        let result =
            Vec::<NaiveTime>::from_sql_text(&Type::TIME_ARRAY, sql_text, &FormatOptions::default())
                .unwrap();
        assert_eq!(
            result,
            vec![
                NaiveTime::from_hms_opt(12, 30, 45).unwrap(),
                NaiveTime::from_hms_micro_opt(23, 59, 59, 123456).unwrap()
            ]
        );
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_vec_naive_datetime() {
        let sql_text = "{2024-01-01 12:30:45,2024-12-31 23:59:59.123456}".as_bytes();
        let result = Vec::<NaiveDateTime>::from_sql_text(
            &Type::TIMESTAMP_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(
            result,
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(12, 30, 45)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 12, 31)
                    .unwrap()
                    .and_hms_micro_opt(23, 59, 59, 123456)
                    .unwrap()
            ]
        );

        // Test with timezone info (should be ignored for NaiveDateTime)
        let sql_text = "{2024-01-01 12:30:45+08:00,2024-12-31 23:59:59.123456-05:00}".as_bytes();
        let result = Vec::<NaiveDateTime>::from_sql_text(
            &Type::TIMESTAMP_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(
            result,
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 1)
                    .unwrap()
                    .and_hms_opt(12, 30, 45)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2024, 12, 31)
                    .unwrap()
                    .and_hms_micro_opt(23, 59, 59, 123456)
                    .unwrap()
            ]
        );
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_vec_datetime_fixed_offset() {
        use chrono::Timelike;

        let sql_text = "{2024-01-01 12:30:45+08:00,2024-12-31 23:59:59.123456-05:00}".as_bytes();
        let result = Vec::<DateTime<FixedOffset>>::from_sql_text(
            &Type::TIMESTAMPTZ_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();

        let offset_plus_8 = FixedOffset::east_opt(8 * 3600).unwrap();
        let offset_minus_5 = FixedOffset::west_opt(5 * 3600).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].offset(), &offset_plus_8);
        assert_eq!(result[0].hour(), 12);
        assert_eq!(result[1].offset(), &offset_minus_5);
        assert_eq!(result[1].hour(), 23);
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_vec_option_naive_date() {
        let sql_text = "{2024-01-01,NULL,2024-12-31}".as_bytes();
        let result = Vec::<Option<NaiveDate>>::from_sql_text(
            &Type::DATE_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(
            result,
            vec![
                Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
                None,
                Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
            ]
        );
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_vec_option_naive_datetime() {
        let sql_text =
            "{2024-01-01 12:30:45,NULL,2024-12-31 23:59:59.123456,2024-12-31 23:59:59.0}"
                .as_bytes();
        let result = Vec::<Option<NaiveDateTime>>::from_sql_text(
            &Type::TIMESTAMP_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();
        assert_eq!(
            result,
            vec![
                Some(
                    NaiveDate::from_ymd_opt(2024, 1, 1)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap()
                ),
                None,
                Some(
                    NaiveDate::from_ymd_opt(2024, 12, 31)
                        .unwrap()
                        .and_hms_micro_opt(23, 59, 59, 123456)
                        .unwrap()
                ),
                Some(
                    NaiveDate::from_ymd_opt(2024, 12, 31)
                        .unwrap()
                        .and_hms_micro_opt(23, 59, 59, 0)
                        .unwrap()
                )
            ]
        );
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_from_sql_text_for_vec_option_datetime_fixed_offset() {
        let sql_text =
            "{2024-01-01 12:30:45+08:00,NULL,2024-12-31 23:59:59.123456-05:00}".as_bytes();
        let result = Vec::<Option<DateTime<FixedOffset>>>::from_sql_text(
            &Type::TIMESTAMPTZ_ARRAY,
            sql_text,
            &FormatOptions::default(),
        )
        .unwrap();

        let offset_plus_8 = FixedOffset::east_opt(8 * 3600).unwrap();
        let offset_minus_5 = FixedOffset::west_opt(5 * 3600).unwrap();

        assert_eq!(result.len(), 3);
        assert!(result[0].is_some());
        assert!(result[1].is_none());
        assert!(result[2].is_some());

        assert_eq!(result[0].as_ref().unwrap().offset(), &offset_plus_8);
        assert_eq!(result[2].as_ref().unwrap().offset(), &offset_minus_5);
    }
}

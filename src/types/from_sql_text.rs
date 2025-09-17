use std::error::Error;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, Utc};
use postgres_types::{Type, WrongType};
use rust_decimal::Decimal;

pub trait FromSqlText: fmt::Debug {
    /// Converts value from postgres text format to rust.
    ///
    /// This trait is modelled after `FromSql` from postgres-types, which is
    /// for binary encoding.
    fn from_sql_text(ty: &Type, input: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized;
}

fn to_str(f: &[u8]) -> Result<&str, Box<dyn Error + Sync + Send>> {
    std::str::from_utf8(f).map_err(Into::into)
}

impl FromSqlText for bool {
    fn from_sql_text(_ty: &Type, input: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match input {
            b"t" => Ok(true),
            b"f" => Ok(false),
            _ => Err("Invalid text value for bool".into()),
        }
    }
}

impl FromSqlText for String {
    fn from_sql_text(_ty: &Type, input: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        to_str(input).map(|s| s.to_owned())
    }
}

macro_rules! impl_from_sql_text {
    ($t:ty) => {
        impl FromSqlText for $t {
            fn from_sql_text(
                _ty: &Type,
                input: &[u8],
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

impl FromSqlText for Decimal {
    fn from_sql_text(_ty: &Type, input: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        Decimal::from_str_exact(to_str(input)?).map_err(Into::into)
    }
}

impl FromSqlText for Vec<u8> {
    fn from_sql_text(_ty: &Type, input: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let data = input
            .strip_prefix(b"\\x")
            .ok_or("\\x prefix expected for bytea")?;

        hex::decode(data).map_err(|e| e.to_string().into())
    }
}

impl FromSqlText for SystemTime {
    fn from_sql_text(_ty: &Type, value: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let datetime = NaiveDateTime::parse_from_str(to_str(value)?, "%Y-%m-%d %H:%M:%S.6f")?;
        let system_time =
            UNIX_EPOCH + Duration::from_millis(datetime.and_utc().timestamp_millis() as u64);

        Ok(system_time)
    }
}

impl FromSqlText for DateTime<FixedOffset> {
    fn from_sql_text(ty: &Type, value: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
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
            _ => Err(Box::new(WrongType::new::<DateTime<Utc>>(ty.clone()))),
        }
    }
}

impl FromSqlText for NaiveDate {
    fn from_sql_text(_ty: &Type, value: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let date = NaiveDate::parse_from_str(to_str(value)?, "%Y-%m-%d")?;
        Ok(date)
    }
}

impl FromSqlText for NaiveTime {
    fn from_sql_text(_ty: &Type, value: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let time = NaiveTime::parse_from_str(to_str(value)?, "%H:%M:%S")?;
        Ok(time)
    }
}

impl FromSqlText for NaiveDateTime {
    fn from_sql_text(_ty: &Type, value: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let datetime = NaiveDateTime::parse_from_str(to_str(value)?, "%Y-%m-%d %H:%M:%S")?;
        Ok(datetime)
    }
}

impl<T> FromSqlText for Option<T>
where
    T: FromSqlText,
{
    fn from_sql_text(ty: &Type, input: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if input.is_empty() {
            Ok(None)
        } else {
            T::from_sql_text(ty, input).map(Some)
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
        let result = String::from_sql_text(&Type::VARCHAR, sql_text).unwrap();
        assert_eq!(result, "Hello, World!");
    }

    #[test]
    fn test_from_sql_text_for_i32() {
        let sql_text = "42".as_bytes();
        let result = i32::from_sql_text(&Type::VARCHAR, sql_text).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_from_sql_text_for_i32_invalid() {
        let sql_text = "not_a_number".as_bytes();
        let result = i32::from_sql_text(&Type::INT4, sql_text);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_text_for_f64() {
        let sql_text = "1.23".as_bytes();
        let result = f64::from_sql_text(&Type::FLOAT8, sql_text).unwrap();
        assert_eq!(result, 1.23);
    }

    #[test]
    fn test_from_sql_text_for_f64_invalid() {
        let sql_text = "not_a_number".as_bytes();
        let result = f64::from_sql_text(&Type::FLOAT8, sql_text);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_text_for_bool() {
        let sql_text = "t".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text).unwrap();
        assert!(result);

        let sql_text = "f".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_from_sql_text_for_bool_invalid() {
        let sql_text = "not_a_boolean".as_bytes();
        let result = bool::from_sql_text(&Type::BOOL, sql_text);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_text_for_option_string() {
        let sql_text = "Some text".as_bytes();
        let result = Option::<String>::from_sql_text(&Type::VARCHAR, sql_text).unwrap();
        assert_eq!(result, Some("Some text".to_string()));

        let sql_text = "".as_bytes();
        let result = Option::<String>::from_sql_text(&Type::VARCHAR, sql_text).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_from_sql_text_for_option_i32() {
        let sql_text = "42".as_bytes();
        let result = Option::<i32>::from_sql_text(&Type::INT4, sql_text).unwrap();
        assert_eq!(result, Some(42));

        let sql_text = "".as_bytes();
        let result = Option::<i32>::from_sql_text(&Type::INT4, sql_text).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_from_sql_text_for_option_f64() {
        let sql_text = "1.23".as_bytes();
        let result = Option::<f64>::from_sql_text(&Type::FLOAT8, sql_text).unwrap();
        assert_eq!(result, Some(1.23));

        let sql_text = "".as_bytes();
        let result = Option::<f64>::from_sql_text(&Type::FLOAT8, sql_text).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_from_sql_text_for_option_bool() {
        let sql_text = "t".as_bytes();
        let result = Option::<bool>::from_sql_text(&Type::BOOL, sql_text).unwrap();
        assert_eq!(result, Some(true));

        let sql_text = "".as_bytes();
        let result = Option::<bool>::from_sql_text(&Type::BOOL, sql_text).unwrap();
        assert_eq!(result, None);
    }
}

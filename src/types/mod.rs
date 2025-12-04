pub mod format;
mod from_sql_text;
mod to_sql_text;

pub use from_sql_text::FromSqlText;
pub use to_sql_text::{ToSqlText, QUOTE_CHECK, QUOTE_ESCAPE};

#[cfg(test)]
mod roundtrip_tests {
    use super::*;
    use crate::types::format::FormatOptions;
    use bytes::BytesMut;
    use postgres_types::Type;

    macro_rules! test_roundtrip {
        ($ty:ty, $value:expr, $pg_type:expr) => {
            let mut buf = BytesMut::new();
            let format_options = FormatOptions::default();

            // Test encoding
            $value
                .to_sql_text($pg_type, &mut buf, &format_options)
                .unwrap();
            let encoded = buf.freeze();

            // Test decoding
            let decoded: $ty = <$ty>::from_sql_text($pg_type, &encoded, &format_options).unwrap();

            assert_eq!(
                $value,
                decoded,
                "Roundtrip failed for {}: {:?} -> {} -> {:?}",
                stringify!($ty),
                $value,
                String::from_utf8_lossy(&encoded),
                decoded
            );
        };
    }

    macro_rules! test_roundtrip_float {
        ($ty:ty, $value:expr, $pg_type:expr) => {
            let mut buf = BytesMut::new();
            let format_options = FormatOptions::default();

            // Test encoding
            $value
                .to_sql_text($pg_type, &mut buf, &format_options)
                .unwrap();
            let encoded = buf.freeze();

            // Test decoding
            let decoded: $ty = <$ty>::from_sql_text($pg_type, &encoded, &format_options).unwrap();

            // Use approximate comparison for floating point
            assert!(
                ($value - decoded).abs() < f64::EPSILON as $ty,
                "Roundtrip failed for {}: {:?} -> {} -> {:?}",
                stringify!($ty),
                $value,
                String::from_utf8_lossy(&encoded),
                decoded
            );
        };
    }

    #[test]
    fn test_roundtrip_bool() {
        test_roundtrip!(bool, true, &Type::BOOL);
        test_roundtrip!(bool, false, &Type::BOOL);
    }

    #[test]
    fn test_roundtrip_string() {
        test_roundtrip!(String, "hello".to_string(), &Type::VARCHAR);
        test_roundtrip!(String, "".to_string(), &Type::VARCHAR);
        test_roundtrip!(String, "with spaces".to_string(), &Type::VARCHAR);
        test_roundtrip!(String, "with'quote".to_string(), &Type::VARCHAR);
        test_roundtrip!(String, "with\\backslash".to_string(), &Type::VARCHAR);
    }

    #[test]
    fn test_roundtrip_str() {
        let mut buf = BytesMut::new();
        let format_options = FormatOptions::default();
        let value = "hello world";

        value
            .to_sql_text(&Type::VARCHAR, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();

        let decoded: String =
            String::from_sql_text(&Type::VARCHAR, &encoded, &format_options).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_roundtrip_char() {
        test_roundtrip!(char, 'a', &Type::CHAR);
        test_roundtrip!(char, 'Z', &Type::CHAR);
        test_roundtrip!(char, '🦀', &Type::CHAR);
    }

    #[test]
    fn test_roundtrip_integers() {
        test_roundtrip!(i8, 0, &Type::INT2);
        test_roundtrip!(i8, 127, &Type::INT2);
        test_roundtrip!(i8, -128, &Type::INT2);

        test_roundtrip!(i16, 0, &Type::INT2);
        test_roundtrip!(i16, 32767, &Type::INT2);
        test_roundtrip!(i16, -32768, &Type::INT2);

        test_roundtrip!(i32, 0, &Type::INT4);
        test_roundtrip!(i32, 2147483647, &Type::INT4);
        test_roundtrip!(i32, -2147483648, &Type::INT4);

        test_roundtrip!(i64, 0, &Type::INT8);
        test_roundtrip!(i64, 9223372036854775807i64, &Type::INT8);
        test_roundtrip!(i64, -9223372036854775808i64, &Type::INT8);

        test_roundtrip!(u32, 0, &Type::INT4);
        test_roundtrip!(u32, 4294967295u32, &Type::INT4);
    }

    #[test]
    fn test_roundtrip_floats() {
        test_roundtrip_float!(f32, 0.0, &Type::FLOAT4);
        test_roundtrip_float!(f32, 1.5, &Type::FLOAT4);
        test_roundtrip_float!(f32, -3.14, &Type::FLOAT4);

        test_roundtrip_float!(f64, 0.0, &Type::FLOAT8);
        test_roundtrip_float!(f64, 1.5, &Type::FLOAT8);
        test_roundtrip_float!(f64, -3.14159265359, &Type::FLOAT8);
    }

    #[test]
    fn test_roundtrip_bytes() {
        let test_data: Vec<u8> = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello" in bytes
        test_roundtrip!(Vec<u8>, test_data.clone(), &Type::BYTEA);

        // Test empty bytes
        test_roundtrip!(Vec<u8>, Vec::<u8>::new(), &Type::BYTEA);

        // Test slice reference
        let mut buf = BytesMut::new();
        let format_options = FormatOptions::default();
        let slice: &[u8] = &test_data;

        slice
            .to_sql_text(&Type::BYTEA, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();

        let decoded: Vec<u8> =
            Vec::<u8>::from_sql_text(&Type::BYTEA, &encoded, &format_options).unwrap();
        assert_eq!(slice, &decoded[..]);
    }

    #[test]
    fn test_roundtrip_option() {
        test_roundtrip!(Option<i32>, Some::<i32>(42), &Type::INT4);
        test_roundtrip!(Option<i32>, None::<i32>, &Type::INT4);

        test_roundtrip!(
            Option<String>,
            Some::<String>("hello".to_string()),
            &Type::VARCHAR
        );
        test_roundtrip!(Option<String>, None::<String>, &Type::VARCHAR);

        test_roundtrip!(Option<bool>, Some::<bool>(true), &Type::BOOL);
        test_roundtrip!(Option<bool>, None::<bool>, &Type::BOOL);
    }

    #[test]
    fn test_roundtrip_arrays() {
        // Test integer arrays
        test_roundtrip!(Vec<i32>, vec![1i32, 2i32, 3i32], &Type::INT4_ARRAY);
        test_roundtrip!(Vec<i32>, Vec::<i32>::new(), &Type::INT4_ARRAY);
        test_roundtrip!(Vec<i32>, vec![0i32, -1i32, 42i32], &Type::INT4_ARRAY);

        // Test string arrays
        test_roundtrip!(
            Vec<String>,
            vec!["hello".to_string(), "world".to_string()],
            &Type::VARCHAR_ARRAY
        );
        test_roundtrip!(Vec<String>, Vec::<String>::new(), &Type::VARCHAR_ARRAY);

        // Test boolean arrays
        test_roundtrip!(Vec<bool>, vec![true, false, true], &Type::BOOL_ARRAY);

        // Test float arrays
        test_roundtrip!(Vec<f64>, vec![1.1f64, 2.2f64, 3.3f64], &Type::FLOAT8_ARRAY);

        // Test char arrays
        test_roundtrip!(Vec<char>, vec!['a', 'b', 'c'], &Type::CHAR_ARRAY);

        // Test byte arrays
        test_roundtrip!(
            Vec<Vec<u8>>,
            vec![b"item1".to_vec(), b"item2".to_vec(), b"item3".to_vec()],
            &Type::BYTEA_ARRAY
        );
    }

    #[test]
    fn test_roundtrip_option_arrays() {
        test_roundtrip!(
            Vec<Option<i32>>,
            vec![Some(1i32), None::<i32>, Some(3i32)],
            &Type::INT4_ARRAY
        );
        test_roundtrip!(
            Vec<Option<String>>,
            vec![
                Some("hello".to_string()),
                None::<String>,
                Some("world".to_string())
            ],
            &Type::VARCHAR_ARRAY
        );
        test_roundtrip!(
            Vec<Option<bool>>,
            vec![Some(true), None::<bool>, Some(false)],
            &Type::BOOL_ARRAY
        );
    }

    #[cfg(feature = "pg-type-chrono")]
    #[test]
    fn test_roundtrip_chrono_types() {
        use std::time::SystemTime;

        use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};

        // Test NaiveDate
        let date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
        test_roundtrip!(NaiveDate, date, &Type::DATE);

        let time = NaiveTime::from_hms_opt(14, 30, 45).unwrap();
        test_roundtrip!(NaiveTime, time, &Type::TIME);

        // Test NaiveDateTime
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2023, 12, 25).unwrap(),
            NaiveTime::from_hms_opt(14, 30, 45).unwrap(),
        );
        test_roundtrip!(NaiveDateTime, datetime, &Type::TIMESTAMP);

        // Test DateTime<FixedOffset>
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let datetime_tz: DateTime<FixedOffset> = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2023, 12, 25)
                .unwrap()
                .and_hms_opt(14, 30, 45)
                .unwrap(),
            offset,
        );
        test_roundtrip!(DateTime<FixedOffset>, datetime_tz, &Type::TIMESTAMPTZ);

        let system_time = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1703505045);
        test_roundtrip!(SystemTime, system_time, &Type::TIMESTAMP);
    }

    #[cfg(feature = "pg-type-rust-decimal")]
    #[test]
    fn test_roundtrip_decimal() {
        use rust_decimal::Decimal;

        let decimal1 = Decimal::new(12345, 2); // 123.45
        test_roundtrip!(Decimal, decimal1, &Type::NUMERIC);

        let decimal2 = Decimal::new(-67890, 3); // -67.890
        test_roundtrip!(Decimal, decimal2, &Type::NUMERIC);

        let decimal3 = Decimal::ZERO;
        test_roundtrip!(Decimal, decimal3, &Type::NUMERIC);
    }

    #[cfg(feature = "pg-type-serde-json")]
    #[test]
    fn test_roundtrip_json() {
        use postgres_types::Json;
        use serde_json::{json, Value};

        // Test Value
        let json_value = json!({"key": "value", "number": 42, "array": [1, 2, 3]});
        test_roundtrip!(Value, json_value.clone(), &Type::JSONB);

        // Test Json<T>
        let json_wrapped: Json<serde_json::Map<String, Value>> =
            Json(json!({"test": "data"}).as_object().unwrap().clone());
        test_roundtrip!(
            Json<serde_json::Map<String, Value>>,
            json_wrapped,
            &Type::JSONB
        );

        // Test simple JSON value
        let simple_json = json!("hello world");
        test_roundtrip!(Value, simple_json, &Type::JSONB);
    }

    #[test]
    fn test_roundtrip_edge_cases() {
        // Test string with special characters that need quoting in arrays
        let special_string = "hello,world".to_string();
        test_roundtrip!(String, special_string, &Type::VARCHAR);

        // Test string with braces
        let braces_string = "{test}".to_string();
        test_roundtrip!(String, braces_string, &Type::VARCHAR);

        // Test string with quotes
        let quote_string = "\"quoted\"".to_string();
        test_roundtrip!(String, quote_string, &Type::VARCHAR);

        // Test empty string
        test_roundtrip!(String, String::new(), &Type::VARCHAR);

        // Test maximum values
        test_roundtrip!(i64, i64::MAX, &Type::INT8);
        test_roundtrip!(i64, i64::MIN, &Type::INT8);
        test_roundtrip!(u32, u32::MAX, &Type::INT4);
    }

    #[test]
    fn test_roundtrip_array_edge_cases() {
        // Test array with special strings that need quoting - simplified for now
        let special_strings: Vec<String> = vec![
            "hello,world".to_string(),
            "{braced}".to_string(),
            "simple".to_string(),
            "nonempty".to_string(),
        ];
        test_roundtrip!(Vec<String>, special_strings, &Type::VARCHAR_ARRAY);

        // Test mixed boolean values
        let mixed_bools = vec![true, false, true, false];
        test_roundtrip!(Vec<bool>, mixed_bools, &Type::BOOL_ARRAY);

        // Test array with negative numbers
        let negative_numbers = vec![-1i32, -2i32, -3i32];
        test_roundtrip!(Vec<i32>, negative_numbers, &Type::INT4_ARRAY);
    }
}

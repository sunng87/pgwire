pub mod format;
mod from_sql_text;
#[cfg(feature = "pg-type-postgis")]
pub mod postgis;
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
        test_roundtrip!(
            Vec<String>,
            vec!["hello\\".to_string(), "world\\".to_string()],
            &Type::VARCHAR_ARRAY
        );
        test_roundtrip!(Vec<String>, Vec::<String>::new(), &Type::VARCHAR_ARRAY);
        test_roundtrip!(Vec<String>, vec![String::new()], &Type::VARCHAR_ARRAY);

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

    #[test]
    #[cfg(feature = "pg-type-postgis")]
    fn test_geometry_point() {
        let point = ::postgis::ewkb::Point {
            x: 1.0,
            y: 2.0,
            srid: None,
        };

        test_roundtrip!(::postgis::ewkb::Point, point, &Type::TEXT);
    }

    fn test_roundtrip_interval_postgres_style() {
        use pg_interval::Interval;

        let format_options = FormatOptions::default();

        // Test interval with months
        let interval1 = Interval::new(6, 0, 0);
        let mut buf = BytesMut::new();
        interval1
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval1, decoded);

        // Test interval with days
        let interval2 = Interval::new(0, 15, 0);
        let mut buf = BytesMut::new();
        interval2
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval2, decoded);

        // Test interval with months and days
        let interval3 = Interval::new(6, 15, 0);
        let mut buf = BytesMut::new();
        interval3
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval3, decoded);

        // Test interval with time component
        let interval4 = Interval::new(0, 0, 3661000000i64);
        let mut buf = BytesMut::new();
        interval4
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval4, decoded);

        // Test complex interval with all components
        let interval5 = Interval::new(12, 15, 1296060000000i64);
        let mut buf = BytesMut::new();
        interval5
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval5, decoded);

        // Test zero interval
        let interval6 = Interval::new(0, 0, 0);
        let mut buf = BytesMut::new();
        interval6
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval6, decoded);
    }

    #[test]
    fn test_roundtrip_interval_iso8601_style() {
        use pg_interval::Interval;

        let mut format_options = FormatOptions::default();
        format_options.interval_style = "iso_8601".to_string();

        // Test simple time interval
        let interval1 = Interval::new(0, 0, 3661000000i64);
        let mut buf = BytesMut::new();
        interval1
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval1, decoded);

        // Test interval with days
        let interval2 = Interval::new(0, 1, 86400000000i64);
        let mut buf = BytesMut::new();
        interval2
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval2, decoded);

        // Test interval with months
        let interval3 = Interval::new(6, 0, 0);
        let mut buf = BytesMut::new();
        interval3
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval3, decoded);

        // Test complex interval
        let interval4 = Interval::new(12, 15, 1296060000000i64);
        let mut buf = BytesMut::new();
        interval4
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval4, decoded);

        // Test zero interval
        let interval5 = Interval::new(0, 0, 0);
        let mut buf = BytesMut::new();
        interval5
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval5, decoded);
    }

    #[test]
    fn test_roundtrip_interval_postgres_verbose_style() {
        use pg_interval::Interval;

        let mut format_options = FormatOptions::default();
        format_options.interval_style = "postgres_verbose".to_string();

        // Test interval with months
        let interval1 = Interval::new(6, 0, 0);
        let mut buf = BytesMut::new();
        interval1
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval1, decoded);

        // Test interval with days
        let interval2 = Interval::new(0, 15, 0);
        let mut buf = BytesMut::new();
        interval2
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval2, decoded);

        // Test interval with months and days
        let interval3 = Interval::new(6, 15, 0);
        let mut buf = BytesMut::new();
        interval3
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval3, decoded);

        // Test interval with time component
        let interval4 = Interval::new(0, 0, 3661000000i64);
        let mut buf = BytesMut::new();
        interval4
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval4, decoded);

        // Test complex interval with all components
        let interval5 = Interval::new(12, 15, 1296060000000i64);
        let mut buf = BytesMut::new();
        interval5
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval5, decoded);

        // Test zero interval
        let interval6 = Interval::new(0, 0, 0);
        let mut buf = BytesMut::new();
        interval6
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval6, decoded);
    }

    #[test]
    fn test_roundtrip_interval_negative_values() {
        use pg_interval::Interval;

        let format_options = FormatOptions::default();

        let interval1 = Interval::new(-6, 0, 0);
        let mut buf = BytesMut::new();
        interval1
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval1, decoded);

        let interval2 = Interval::new(0, -15, 0);
        let mut buf = BytesMut::new();
        interval2
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval2, decoded);

        let interval3 = Interval::new(0, 0, -3600000000i64);
        let mut buf = BytesMut::new();
        interval3
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval3, decoded);

        let interval4 = Interval::new(-12, -15, -86400000000i64);
        let mut buf = BytesMut::new();
        interval4
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval4, decoded);
    }

    #[test]
    fn test_roundtrip_interval_negative_values_iso8601() {
        use pg_interval::Interval;

        let mut format_options = FormatOptions::default();
        format_options.interval_style = "iso_8601".to_string();

        let interval1 = Interval::new(-6, 0, 0);
        let mut buf = BytesMut::new();
        interval1
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval1, decoded);

        let interval2 = Interval::new(0, -15, 0);
        let mut buf = BytesMut::new();
        interval2
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval2, decoded);
    }

    #[test]
    fn test_roundtrip_interval_negative_values_postgres_verbose() {
        use pg_interval::Interval;

        let mut format_options = FormatOptions::default();
        format_options.interval_style = "postgres_verbose".to_string();

        let interval1 = Interval::new(-6, 0, 0);
        let mut buf = BytesMut::new();
        interval1
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval1, decoded);

        let interval2 = Interval::new(0, -15, 0);
        let mut buf = BytesMut::new();
        interval2
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval2, decoded);
    }

    #[test]
    fn test_interval_encoding_formats() {
        use pg_interval::Interval;

        let interval = Interval::new(12, 15, 1296060000000i64);

        let mut format_options = FormatOptions::default();

        // Test postgres encoding
        format_options.interval_style = "postgres".to_string();
        let mut buf = BytesMut::new();
        interval
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let postgres_str = String::from_utf8_lossy(&buf);
        eprintln!("postgres format: {}", postgres_str);

        let parsed = Interval::from_postgres(&postgres_str);
        assert!(parsed.is_ok(), "postgres style should roundtrip");
        assert_eq!(
            parsed.unwrap(),
            interval,
            "postgres roundtrip should preserve value"
        );

        // Test iso_8601 encoding
        format_options.interval_style = "iso_8601".to_string();
        buf.clear();
        interval
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let iso_str = String::from_utf8_lossy(&buf);
        eprintln!("iso_8601 format: {}", iso_str);

        let parsed = Interval::from_iso(&iso_str);
        assert!(parsed.is_ok(), "iso_8601 style should roundtrip");
        assert_eq!(
            parsed.unwrap(),
            interval,
            "iso_8601 roundtrip should preserve value"
        );

        // Test postgres_verbose encoding
        format_options.interval_style = "postgres_verbose".to_string();
        buf.clear();
        interval
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let verbose_str = String::from_utf8_lossy(&buf);
        eprintln!("postgres_verbose format: {}", verbose_str);

        let parsed = Interval::from_postgres_verbose(&verbose_str);
        assert!(parsed.is_ok(), "postgres_verbose style should roundtrip");
        assert_eq!(
            parsed.unwrap(),
            interval,
            "postgres_verbose roundtrip should preserve value"
        );
    }

    #[test]
    fn test_roundtrip_interval_comprehensive() {
        use pg_interval::Interval;

        let format_options = FormatOptions::default();

        let interval1 = Interval::new(0, 0, 3661000000i64);
        let mut buf = BytesMut::new();
        interval1
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval1, decoded, "Time-only interval should roundtrip");

        let interval2 = Interval::new(12, 0, 0);
        let mut buf = BytesMut::new();
        interval2
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval2, decoded, "12 months (1 year) should roundtrip");

        let interval3 = Interval::new(0, 1, 0);
        let mut buf = BytesMut::new();
        interval3
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval3, decoded, "Single day should roundtrip");

        let interval4 = Interval::new(0, 0, 0);
        let mut buf = BytesMut::new();
        interval4
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval4, decoded, "Zero interval should roundtrip");

        let interval5 = Interval::new(12, 15, 1296060000000i64);
        let mut buf = BytesMut::new();
        interval5
            .to_sql_text(&Type::INTERVAL, &mut buf, &format_options)
            .unwrap();
        let encoded = buf.freeze();
        let decoded: Interval =
            Interval::from_sql_text(&Type::INTERVAL, &encoded, &format_options).unwrap();
        assert_eq!(interval5, decoded, "Complex interval should roundtrip");
    }
}

use bytes::Bytes;
use time::{Date, Duration, OffsetDateTime, PrimitiveDateTime, Time};

#[derive(Debug)]
pub enum Data {
    // null
    Null,

    // bool
    Bool(bool),

    // numberics
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Real(f32),
    Double(f64),
    SmallSerial(u16),
    Serial(u32),
    BigSerial(u64),

    // monetary
    Money(i64),

    // text
    Varchar(String),
    Character(String),
    Text(String),
    Char(u8),

    // bytes
    Bytes(Bytes),

    // time
    Timestamp(PrimitiveDateTime),
    TimestampWithTimeZone(OffsetDateTime),
    Date(Date),
    Time(Time),
    Interval(Duration),
    // TODO: more native types to be added
}

impl Data {
    fn into_bytes(&self) -> Vec<u8> {
        // TODO
        Vec::new()
    }

    fn into_text(&self) -> String {
        // TODO
        "".to_owned()
    }

    // TODO: optional or result
    fn from_bytes(type_oid: i32, bytes: &[u8]) -> Data {
        Data::Null
    }

    fn from_text(type_oid: i32, text: &str) -> Data {
        Data::Null
    }
}

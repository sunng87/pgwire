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
    Bytea(Bytes),

    // time
    Timestamp(PrimitiveDateTime),
    TimestampWithTimeZone(OffsetDateTime),
    Date(Date),
    Time(Time),
    Interval(Duration),
    // TODO: more native types to be added
}

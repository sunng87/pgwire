use bytes::{BufMut, Bytes, BytesMut};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

use crate::error::PgWireResult;

pub const TYPE_OID_UNSPECIFIED: i32 = 0;
pub const TYPE_OID_BOOL: i32 = 16;
pub const TYPE_OID_INT2: i32 = 21;
pub const TYPE_OID_INT4: i32 = 23;
pub const TYPE_OID_INT8: i32 = 20;
pub const TYPE_OID_FLOAT4: i32 = 700;
pub const TYPE_OID_FLOAT8: i32 = 701;
pub const TYPE_OID_VARCHAR: i32 = 104;
pub const TYPE_OID_TEXT: i32 = 25;
pub const TYPE_OID_BYTES: i32 = 17;
pub const TYPE_OID_TIMESTAMP: i32 = 1114;
pub const TYPE_OID_TIMESTAMPTZ: i32 = 1184;
pub const TYPE_OID_DATE: i32 = 1082;
pub const TYPE_OID_TIME: i32 = 1083;

#[derive(Debug)]
pub enum Data {
    // null
    Null,

    // bool
    Bool(bool), // oid: 16

    // numberics
    Int2(i16),   // oid: 21
    Int4(i32),   // oid: 23
    Int8(i64),   // oid: 20
    Float4(f32), // oid: 700
    Float8(f64), // oid: 701

    // text
    Varchar(String), // oid: 1043
    Text(String),    // oid: 25

    // bytes
    Bytes(Vec<u8>), // oid: 17

    // time
    Timestamp(PrimitiveDateTime),          // oid: 1114
    TimestampWithTimeZone(OffsetDateTime), // oid: 1184
    Date(Date),                            // oid: 1082
    Time(Time),                            // oid: 1083
}

impl Data {
    pub fn type_oid(&self) -> i32 {
        match self {
            Data::Null => TYPE_OID_UNSPECIFIED,
            Data::Bool(_) => TYPE_OID_BOOL,

            Data::Int2(_) => TYPE_OID_INT2,
            Data::Int4(_) => TYPE_OID_INT4,
            Data::Int8(_) => TYPE_OID_INT8,
            Data::Float4(_) => TYPE_OID_FLOAT4,
            Data::Float8(_) => TYPE_OID_FLOAT8,

            Data::Varchar(_) => TYPE_OID_VARCHAR,
            Data::Text(_) => TYPE_OID_TEXT,

            Data::Bytes(_) => TYPE_OID_BYTES,

            Data::Timestamp(_) => TYPE_OID_TIMESTAMP,
            Data::TimestampWithTimeZone(_) => TYPE_OID_TIMESTAMPTZ,
            Data::Date(_) => TYPE_OID_DATE,
            Data::Time(_) => TYPE_OID_TIME,
        }
    }

    pub fn write_bytes(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Data::Null => {}
            Data::Bool(v) => buf.put_u8(if *v { 1 } else { 0 }),

            Data::Int2(n) => buf.put_i16(*n),
            Data::Int4(n) => buf.put_i32(*n),
            Data::Int8(n) => buf.put_i64(*n),
            Data::Float4(n) => buf.put_f32(*n),
            Data::Float8(n) => buf.put_f64(*n),

            Data::Varchar(t) => buf.put_slice(t.as_bytes()),
            Data::Text(t) => buf.put_slice(t.as_bytes()),

            Data::Bytes(ba) => buf.put_slice(ba),

            Data::Timestamp(dt) => {}
            Data::TimestampWithTimeZone(dttz) => {}
            Data::Date(dt) => {}
            Data::Time(tm) => {}
        }

        Ok(())
    }

    pub fn write_text(&self, buf: &mut BytesMut) -> PgWireResult<()> {}

    // TODO: optional or result
    pub fn from_bytes(type_oid: i32, bytes: &[u8]) -> Data {
        Data::Null
    }

    pub fn from_text(type_oid: i32, text: &str) -> Data {
        Data::Null
    }
}

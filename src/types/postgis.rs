use std::error::Error;
use std::io::Cursor;

use bytes::{BufMut, BytesMut};
use postgis::ewkb::{self, AsEwkbPoint, EwkbRead, EwkbWrite};
use postgres_types::Type;

use crate::types::format::FormatOptions;
use crate::types::{FromSqlText, ToSqlText};

fn read_ewkb<T: EwkbRead>(input: &[u8]) -> Result<T, Box<dyn Error + Sync + Send>> {
    let bytes = hex::decode(input).map_err(|e| Box::new(e))?;
    let mut cursor = Cursor::new(bytes.as_slice());
    T::read_ewkb(&mut cursor).map_err(|e| Box::new(e) as Box<dyn Error + Sync + Send>)
}

fn write_ewkb<T: EwkbWrite>(
    data: &T,
    out: &mut BytesMut,
) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
    let hex = data.to_hex_ewkb();
    out.put_slice(hex.as_bytes());
    Ok(postgres_types::IsNull::No)
}

impl<'a> FromSqlText<'a> for ewkb::Point {
    fn from_sql_text(
        _ty: &Type,
        input: &'a [u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        read_ewkb::<Self>(input)
    }
}

impl ToSqlText for ewkb::Point {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        write_ewkb(&self.as_ewkb(), out)
    }
}

impl<'a> FromSqlText<'a> for ewkb::PointZ {
    fn from_sql_text(
        _ty: &Type,
        input: &'a [u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        read_ewkb::<Self>(input)
    }
}

impl ToSqlText for ewkb::PointZ {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        write_ewkb(&self.as_ewkb(), out)
    }
}

impl<'a> FromSqlText<'a> for ewkb::PointM {
    fn from_sql_text(
        _ty: &Type,
        input: &'a [u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        read_ewkb::<Self>(input)
    }
}

impl ToSqlText for ewkb::PointM {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        write_ewkb(&self.as_ewkb(), out)
    }
}

impl<'a> FromSqlText<'a> for ewkb::PointZM {
    fn from_sql_text(
        _ty: &Type,
        input: &'a [u8],
        _format_options: &FormatOptions,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        read_ewkb::<Self>(input)
    }
}

impl ToSqlText for ewkb::PointZM {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        write_ewkb(&self.as_ewkb(), out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_to_hex() {
        let point = ewkb::Point {
            x: 1.0,
            y: 2.0,
            srid: None,
        };
        let mut buf = BytesMut::new();
        point
            .to_sql_text(&Type::TEXT, &mut buf, &FormatOptions::default())
            .unwrap();
        let output = String::from_utf8_lossy(&buf[..]);
        assert_eq!(output, "0101000000000000000000F03F0000000000000040");
    }
}

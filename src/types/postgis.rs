use std::error::Error;
use std::io::Cursor;

use bytes::{BufMut, BytesMut};
use postgis::ewkb::{
    self, AsEwkbGeometry, AsEwkbGeometryCollection, AsEwkbLineString, AsEwkbMultiLineString,
    AsEwkbMultiPoint, AsEwkbMultiPolygon, AsEwkbPoint, AsEwkbPolygon, EwkbRead, EwkbWrite,
};
use postgres_types::Type;

use crate::types::format::FormatOptions;
use crate::types::{FromSqlText, ToSqlText};

macro_rules! impl_postgis_type {
    ($type:ty) => {
        impl<'a> FromSqlText<'a> for $type {
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

        impl ToSqlText for $type {
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
    };
}

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

impl_postgis_type!(ewkb::Point);
impl_postgis_type!(ewkb::PointZ);
impl_postgis_type!(ewkb::PointM);
impl_postgis_type!(ewkb::PointZM);
impl_postgis_type!(ewkb::LineString);
impl_postgis_type!(ewkb::LineStringZ);
impl_postgis_type!(ewkb::LineStringM);
impl_postgis_type!(ewkb::LineStringZM);
impl_postgis_type!(ewkb::Polygon);
impl_postgis_type!(ewkb::PolygonZ);
impl_postgis_type!(ewkb::PolygonM);
impl_postgis_type!(ewkb::PolygonZM);
impl_postgis_type!(ewkb::MultiPoint);
impl_postgis_type!(ewkb::MultiPointZ);
impl_postgis_type!(ewkb::MultiPointM);
impl_postgis_type!(ewkb::MultiPointZM);
impl_postgis_type!(ewkb::MultiLineString);
impl_postgis_type!(ewkb::MultiLineStringZ);
impl_postgis_type!(ewkb::MultiLineStringM);
impl_postgis_type!(ewkb::MultiLineStringZM);
impl_postgis_type!(ewkb::MultiPolygon);
impl_postgis_type!(ewkb::MultiPolygonZ);
impl_postgis_type!(ewkb::MultiPolygonM);
impl_postgis_type!(ewkb::MultiPolygonZM);
impl_postgis_type!(ewkb::Geometry);
impl_postgis_type!(ewkb::GeometryZ);
impl_postgis_type!(ewkb::GeometryM);
impl_postgis_type!(ewkb::GeometryZM);
impl_postgis_type!(ewkb::GeometryCollection);
impl_postgis_type!(ewkb::GeometryCollectionZ);
impl_postgis_type!(ewkb::GeometryCollectionM);
impl_postgis_type!(ewkb::GeometryCollectionZM);

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

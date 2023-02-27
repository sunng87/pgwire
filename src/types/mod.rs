use std::{error::Error, fmt};

use bytes::{BufMut, BytesMut};
use postgres_types::{IsNull, Type};

pub trait ToSqlText: fmt::Debug {
    /// Converts value to text format of Postgres type.
    ///
    /// This trait is modelled after `ToSql` from postgres-types, which is
    /// for binary encoding.
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized;
}

impl<'a, T> ToSqlText for &'a T
where
    T: ToSqlText,
{
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        (*self).to_sql(ty, out)
    }
}

impl<T: ToSqlText> ToSqlText for Option<T> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match *self {
            Some(ref val) => val.to_sql(ty, out),
            None => Ok(IsNull::Yes),
        }
    }
}

// TODO: array support

impl ToSqlText for String {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&str as ToSqlText>::to_sql(&&**self, ty, w)
    }
}

impl<'a> ToSqlText for &'a str {
    fn to_sql(&self, _ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        w.put_slice(self.as_bytes());
        Ok(IsNull::No)
    }
}

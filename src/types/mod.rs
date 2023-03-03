use std::time::SystemTime;
use std::{error::Error, fmt};

use bytes::{BufMut, BytesMut};
use chrono::offset::Utc;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use postgres_types::{IsNull, Type};

pub trait ToSqlText: fmt::Debug {
    /// Converts value to text format of Postgres type.
    ///
    /// This trait is modelled after `ToSql` from postgres-types, which is
    /// for binary encoding.
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized;
}

impl<'a, T> ToSqlText for &'a T
where
    T: ToSqlText,
{
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        (*self).to_sql_text(ty, out)
    }
}

impl<T: ToSqlText> ToSqlText for Option<T> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match *self {
            Some(ref val) => val.to_sql_text(ty, out),
            None => Ok(IsNull::Yes),
        }
    }
}

// TODO: array support

impl ToSqlText for String {
    fn to_sql_text(
        &self,
        ty: &Type,
        w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&str as ToSqlText>::to_sql_text(&&**self, ty, w)
    }
}

impl<'a> ToSqlText for &'a str {
    fn to_sql_text(
        &self,
        _ty: &Type,
        w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        w.put_slice(self.as_bytes());
        Ok(IsNull::No)
    }
}

macro_rules! impl_to_sql_text {
    ($t:ty) => {
        impl ToSqlText for $t {
            fn to_sql_text(
                &self,
                _ty: &Type,
                w: &mut BytesMut,
            ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
                w.put_slice(self.to_string().as_bytes());
                Ok(IsNull::No)
            }
        }
    };
}

impl_to_sql_text!(i8);
impl_to_sql_text!(i16);
impl_to_sql_text!(i32);
impl_to_sql_text!(i64);
impl_to_sql_text!(i128);
impl_to_sql_text!(u8);
impl_to_sql_text!(u16);
impl_to_sql_text!(u32);
impl_to_sql_text!(u64);
impl_to_sql_text!(u128);
impl_to_sql_text!(f32);
impl_to_sql_text!(f64);
impl_to_sql_text!(bool);
impl_to_sql_text!(char);

impl ToSqlText for Vec<u8> {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        out.put_slice(hex::encode(self).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for &[u8] {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        out.put_slice(hex::encode(self).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for SystemTime {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let datetime: DateTime<Utc> = DateTime::<Utc>::from(*self);
        let fmt = datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for NaiveDateTime {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let fmt = self.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for NaiveDate {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let fmt = self.format("%Y-%m-%d").to_string();
        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for NaiveTime {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let fmt = self.format("%H:%M:%S%.6f").to_string();
        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

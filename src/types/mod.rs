use std::time::SystemTime;
use std::{error::Error, fmt};

use bytes::{BufMut, BytesMut};
use chrono::offset::Utc;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use postgres_types::{IsNull, Type, WrongType};
use rust_decimal::Decimal;

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

impl ToSqlText for bool {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *self {
            out.put_slice(b"t");
        } else {
            out.put_slice(b"f");
        }
        Ok(IsNull::No)
    }
}

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
impl_to_sql_text!(u32);
impl_to_sql_text!(f32);
impl_to_sql_text!(f64);
impl_to_sql_text!(char);

impl ToSqlText for &[u8] {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(hex::encode(self).as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for Vec<u8> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[u8] as ToSqlText>::to_sql_text(&&**self, ty, out)
    }
}

impl<const N: usize> ToSqlText for [u8; N] {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[u8] as ToSqlText>::to_sql_text(&&self[..], ty, out)
    }
}

impl ToSqlText for SystemTime {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let datetime: DateTime<Utc> = DateTime::<Utc>::from(*self);
        let fmt = datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

impl<Tz: TimeZone> ToSqlText for DateTime<Tz>
where
    Tz::Offset: std::fmt::Display,
{
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::TIMESTAMP => "%Y-%m-%d %H:%M:%S%.6f",
            Type::TIMESTAMPTZ => "%Y-%m-%d %H:%M:%S%.6f%:::z",
            Type::DATE => "%Y-%m-%d",
            Type::TIME => "%H:%M:%S%.6f",
            Type::TIMETZ => "%H:%M:%S%.6f%:::z",
            _ => Err(Box::new(WrongType::new::<DateTime<Tz>>(ty.clone())))?,
        };
        out.put_slice(self.format(fmt).to_string().as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for NaiveDateTime {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::TIMESTAMP => "%Y-%m-%d %H:%M:%S%.6f",
            Type::DATE => "%Y-%m-%d",
            Type::TIME => "%H:%M:%S%.6f",
            _ => Err(Box::new(WrongType::new::<NaiveDateTime>(ty.clone())))?,
        };
        out.put_slice(self.format(fmt).to_string().as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for NaiveDate {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::DATE => self.format("%Y-%m-%d").to_string(),
            _ => Err(Box::new(WrongType::new::<NaiveDate>(ty.clone())))?,
        };

        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for NaiveTime {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::TIME => self.format("%H:%M:%S%.6f").to_string(),
            _ => Err(Box::new(WrongType::new::<NaiveTime>(ty.clone())))?,
        };
        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

impl ToSqlText for Decimal {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let fmt = match *ty {
            Type::NUMERIC => self.to_string(),
            _ => Err(Box::new(WrongType::new::<Decimal>(ty.clone())))?,
        };

        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

impl<T: ToSqlText> ToSqlText for &[T] {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(b"{");
        for (i, val) in self.iter().enumerate() {
            if i > 0 {
                out.put_slice(b",");
            }
            // put NULL for null value in array
            if let IsNull::Yes = val.to_sql_text(ty, out)? {
                out.put_slice(b"NULL");
            }
        }
        out.put_slice(b"}");
        Ok(IsNull::No)
    }
}

impl<T: ToSqlText> ToSqlText for Vec<T> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[T] as ToSqlText>::to_sql_text(&&**self, ty, out)
    }
}

impl<T: ToSqlText, const N: usize> ToSqlText for [T; N] {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&[T] as ToSqlText>::to_sql_text(&&self[..], ty, out)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::offset::Local;

    #[test]
    fn test_date_time_format() {
        let date = NaiveDate::from_ymd_opt(2023, 3, 5).unwrap();
        let mut buf = BytesMut::new();
        date.to_sql_text(&Type::DATE, &mut buf).unwrap();
        assert_eq!("2023-03-05", String::from_utf8_lossy(buf.freeze().as_ref()));

        let date = NaiveDate::from_ymd_opt(2023, 3, 5).unwrap();
        let mut buf = BytesMut::new();
        assert!(date.to_sql_text(&Type::INT8, &mut buf).is_err());

        let date = Local::now();
        let mut buf = BytesMut::new();
        date.to_sql_text(&Type::TIMESTAMPTZ, &mut buf).unwrap();
        // format: 2023-02-01 22:31:49.479895+08
        assert_eq!(29, String::from_utf8_lossy(buf.freeze().as_ref()).len());
    }

    #[test]
    fn test_null() {
        let data = vec![None::<i8>, Some(8)];
        let mut buf = BytesMut::new();
        data.to_sql_text(&Type::INT2, &mut buf).unwrap();
        assert_eq!("{NULL,8}", String::from_utf8_lossy(buf.freeze().as_ref()));
    }

    #[test]
    fn test_bool() {
        let yes = true;
        let no = false;

        let mut buf = BytesMut::new();
        yes.to_sql_text(&Type::BOOL, &mut buf).unwrap();
        assert_eq!("t", String::from_utf8_lossy(buf.freeze().as_ref()));

        let mut buf = BytesMut::new();
        no.to_sql_text(&Type::BOOL, &mut buf).unwrap();
        assert_eq!("f", String::from_utf8_lossy(buf.freeze().as_ref()));
    }
}

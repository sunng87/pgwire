use std::error::Error;
#[cfg(feature = "pg-type-chrono")]
use std::time::SystemTime;

use bytes::{BufMut, BytesMut};
#[cfg(feature = "pg-type-chrono")]
use chrono::offset::Utc;
#[cfg(feature = "pg-type-chrono")]
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone};
use postgres_types::{IsNull, Type};

#[cfg(feature = "pg-type-chrono")]
use crate::types::ToSqlText;
use crate::{api::ClientInfo, error::PgWireError};

pub const BYTEA_OUTPUT_HEX: &str = "hex";
pub const BYTEA_OUTPUT_ESCAPE: &str = "escape";

pub const DATE_STYLE_ORDER_DMY: &str = "DMY";
pub const DATE_STYLE_ORDER_MDY: &str = "MDY";
pub const DATE_STYLE_ORDER_YMD: &str = "YMD";

pub const DATE_STYLE_DISPLAY_ISO: &str = "ISO";
pub const DATE_STYLE_DISPLAY_SQL: &str = "SQL";
pub const DATE_STYLE_DISPLAY_GERMAN: &str = "german";
pub const DATE_STYLE_DISPLAY_POSTGRES: &str = "postgres";

pub const INTERVAL_STYLE_POSTGRES: &str = "postgres";
pub const INTERVAL_STYLE_ISO_8601: &str = "iso_8601";
pub const INTERVAL_STYLE_SQL_STANDARD: &str = "sql_standard";

#[derive(Debug, Default, Copy, Clone)]
pub enum DateStyleOrder {
    DMY,
    MDY,
    #[default]
    YMD,
}

impl TryFrom<&str> for DateStyleOrder {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim() {
            DATE_STYLE_ORDER_DMY => Ok(Self::DMY),
            DATE_STYLE_ORDER_MDY => Ok(Self::MDY),
            DATE_STYLE_ORDER_YMD => Ok(Self::YMD),
            _ => Err(PgWireError::InvalidOptionValue(value.to_string())),
        }
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub enum DateStyleDisplayStyle {
    #[default]
    ISO,
    SQL,
    GERMAN,
    POSTGRES,
}

impl TryFrom<&str> for DateStyleDisplayStyle {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim() {
            DATE_STYLE_DISPLAY_ISO => Ok(Self::ISO),
            DATE_STYLE_DISPLAY_SQL => Ok(Self::SQL),
            DATE_STYLE_DISPLAY_GERMAN => Ok(Self::GERMAN),
            DATE_STYLE_DISPLAY_POSTGRES => Ok(Self::POSTGRES),
            _ => Err(PgWireError::InvalidOptionValue(value.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct DateStyle<T> {
    style: DateStyleDisplayStyle,
    order: Option<DateStyleOrder>,
    data: T,
}

impl<T> DateStyle<T> {
    /// Create DateStyle wrapped datetime for serialization
    ///
    /// This function will ignore invalid config and fallback to default.
    /// It is possible the config only provides style without custom order.
    pub fn new(data: T, config: &str) -> Self {
        let (style, order) = config
            .split_once(", ")
            .map(|(style, order)| (style, Some(order)))
            .unwrap_or((config, None));
        DateStyle {
            order: order.map(|order| DateStyleOrder::try_from(order).unwrap_or_default()),
            style: DateStyleDisplayStyle::try_from(style).unwrap_or_default(),
            data,
        }
    }

    pub fn new_with_client_info<C>(data: T, client_info: C) -> Self
    where
        C: ClientInfo,
    {
        let display_config = client_info
            .metadata()
            .get("DateStyle")
            .map(|s| s.as_str())
            .unwrap_or("ISO");
        Self::new(data, display_config)
    }

    /// Get datetime format str for current style
    pub fn full_format_str(&self) -> &str {
        match (&self.style, &self.order) {
            (DateStyleDisplayStyle::SQL, Some(DateStyleOrder::DMY)) => "%d/%m/%Y %H:%M:%S%.f",
            (DateStyleDisplayStyle::SQL, _) => "%m/%d/%Y %H:%M:%S%.f",

            (DateStyleDisplayStyle::ISO, _) => "%Y-%m-%d %H:%M:%S%.f",

            (DateStyleDisplayStyle::POSTGRES, _) => "%a %b %e %H:%M:%S %.f %Y",

            (DateStyleDisplayStyle::GERMAN, _) => "%d.%m.%Y %H:%M:%S%.f",
        }
    }

    /// Get datetime with tz format str for current style
    pub fn full_tz_format_str(&self) -> String {
        match self.style {
            DateStyleDisplayStyle::SQL => format!("{}:::z", self.full_format_str()),
            _ => format!("{}:::Z", self.full_format_str()),
        }
    }

    /// Get date format str for current style
    pub fn date_format_str(&self) -> &str {
        match (&self.style, &self.order) {
            (DateStyleDisplayStyle::SQL, Some(DateStyleOrder::DMY)) => "%d/%m/%Y",
            (DateStyleDisplayStyle::SQL, _) => "%m/%d/%Y",

            (DateStyleDisplayStyle::ISO, _) => "%Y-%m-%d",

            (DateStyleDisplayStyle::POSTGRES, Some(DateStyleOrder::DMY)) => "%d-%m-%Y",
            (DateStyleDisplayStyle::POSTGRES, _) => "%m-%d-%Y",

            (DateStyleDisplayStyle::GERMAN, _) => "%d.%m.%Y",
        }
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for DateStyle<SystemTime> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let datetime: DateTime<Utc> = DateTime::<Utc>::from(self.data);
        DateStyle {
            data: datetime,
            style: self.style,
            order: self.order,
        }
        .to_sql_text(ty, out)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl<Tz: TimeZone> ToSqlText for DateStyle<DateTime<Tz>>
where
    Tz::Offset: std::fmt::Display,
{
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::TIMESTAMP | Type::TIMESTAMP_ARRAY => self.full_format_str(),
            Type::TIMESTAMPTZ | Type::TIMESTAMPTZ_ARRAY => &self.full_tz_format_str(),
            Type::DATE | Type::DATE_ARRAY => self.date_format_str(),
            Type::TIME | Type::TIME_ARRAY => "%H:%M:%S%.6f",
            Type::TIMETZ | Type::TIMETZ_ARRAY => "%H:%M:%S%.6f%:::z",
            _ => Err(Box::new(postgres_types::WrongType::new::<DateTime<Tz>>(
                ty.clone(),
            )))?,
        };
        out.put_slice(self.data.format(fmt).to_string().as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for DateStyle<NaiveDateTime> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::TIMESTAMP | Type::TIMESTAMP_ARRAY => self.full_format_str(),
            Type::DATE | Type::DATE_ARRAY => self.date_format_str(),
            Type::TIME | Type::TIME_ARRAY => "%H:%M:%S%.6f",
            _ => Err(Box::new(postgres_types::WrongType::new::<NaiveDateTime>(
                ty.clone(),
            )))?,
        };
        out.put_slice(self.data.format(fmt).to_string().as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(feature = "pg-type-chrono")]
impl ToSqlText for DateStyle<NaiveDate> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let fmt = match *ty {
            Type::DATE | Type::DATE_ARRAY => self.data.format(self.date_format_str()).to_string(),
            _ => Err(Box::new(postgres_types::WrongType::new::<NaiveDate>(
                ty.clone(),
            )))?,
        };

        out.put_slice(fmt.as_bytes());
        Ok(IsNull::No)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}

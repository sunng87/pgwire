use bytes::{BufMut, BytesMut};
use postgres_types::{IsNull, Type};

use crate::{error::PgWireError, types::ToSqlText};

pub const BYTEA_OUTPUT_HEX: &str = "hex";
pub const BYTEA_OUTPUT_ESCAPE: &str = "escape";

#[derive(Debug, Default, Copy, Clone)]
pub enum ByteaOutputFormat {
    #[default]
    Hex,
    Escape,
}

impl TryFrom<&str> for ByteaOutputFormat {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_lowercase().as_ref() {
            BYTEA_OUTPUT_HEX => Ok(Self::Hex),
            BYTEA_OUTPUT_ESCAPE => Ok(Self::Escape),
            _ => Err(PgWireError::InvalidOptionValue(value.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct ByteaOutput<T> {
    format: ByteaOutputFormat,
    data: T,
}

impl<T> ByteaOutput<T>
where
    T: AsRef<[u8]>,
{
    pub fn new(data: T, config: &str) -> Self {
        let format = ByteaOutputFormat::try_from(config).unwrap_or_default();

        Self { format, data }
    }

    pub fn format(&self) -> ByteaOutputFormat {
        self.format
    }
}

impl<T> ToSqlText for ByteaOutput<T>
where
    T: AsRef<[u8]> + std::fmt::Debug,
{
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        let data = self.data.as_ref();
        match self.format {
            ByteaOutputFormat::Hex => data.to_sql_text(ty, out),
            ByteaOutputFormat::Escape => {
                data.iter().for_each(|b| match b {
                    0..=31 | 127..=255 => {
                        out.put_slice(b"\\");
                        out.put_slice(format!("{b:03o}").as_bytes());
                    }
                    92 => out.put_slice(b"\\\\"),
                    32..=126 => out.put_u8(*b),
                });
                Ok(IsNull::No)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytea_output_format() {
        let data = "helloworld";

        let mut out = BytesMut::new();

        let hex_bytea = ByteaOutput::new(data.as_bytes(), "hex");
        hex_bytea.to_sql_text(&Type::BYTEA, &mut out).unwrap();

        assert_eq!(out.as_ref(), b"\\x68656c6c6f776f726c64");
        out.clear();

        let data = &[97, 98, 99, 107, 108, 109, 42, 169, 84];
        let hex_bytea = ByteaOutput::new(data, "escape");
        hex_bytea.to_sql_text(&Type::BYTEA, &mut out).unwrap();
        assert_eq!(out.as_ref(), b"abcklm*\\251T");
        out.clear();
    }
}

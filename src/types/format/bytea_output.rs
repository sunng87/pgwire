use crate::error::PgWireError;

pub const BYTEA_OUTPUT_HEX: &str = "hex";
pub const BYTEA_OUTPUT_ESCAPE: &str = "escape";

#[derive(Debug, Default, Copy, Clone)]
pub enum ByteaOutput {
    #[default]
    Hex,
    Escape,
}

impl TryFrom<&str> for ByteaOutput {
    type Error = PgWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_lowercase().as_ref() {
            BYTEA_OUTPUT_HEX => Ok(Self::Hex),
            BYTEA_OUTPUT_ESCAPE => Ok(Self::Escape),
            _ => Err(PgWireError::InvalidOptionValue(value.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use postgres_types::Type;

    use crate::types::format::FormatOptions;
    use crate::types::ToSqlText;

    #[test]
    fn test_bytea_output_format() {
        let data = "helloworld";

        let mut out = BytesMut::new();
        let mut format_options = FormatOptions::default();

        data.to_sql_text(&Type::BYTEA, &mut out, &format_options)
            .unwrap();

        assert_eq!(out.as_ref(), b"\\x68656c6c6f776f726c64");

        out.clear();
        let data = &[97, 98, 99, 107, 108, 109, 42, 169, 84];
        format_options.bytea_output = "escape".to_string();

        data.to_sql_text(&Type::BYTEA, &mut out, &format_options)
            .unwrap();
        assert_eq!(out.as_ref(), b"abcklm*\\251T");
        out.clear();
    }
}

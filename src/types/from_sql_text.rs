use std::error::Error;
use std::fmt;

use postgres_types::Type;

pub trait FromSqlText: fmt::Debug {
    /// Converts value from postgres text format to rust.
    ///
    /// This trait is modelled after `FromSql` from postgres-types, which is
    /// for binary encoding.
    fn from_sql_text(ty: &Type, input: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        Self: Sized;
}

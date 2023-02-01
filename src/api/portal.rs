use std::sync::Arc;

use bytes::Bytes;
use postgres_types::FromSqlOwned;

use crate::{
    error::{PgWireError, PgWireResult},
    messages::{
        data::{FORMAT_CODE_BINARY, FORMAT_CODE_TEXT},
        extendedquery::Bind,
    },
};

use super::{stmt::StoredStatement, DEFAULT_NAME};

/// Represent a prepared sql statement and its parameters bound by a `Bind`
/// request.
#[derive(Debug, CopyGetters, Default, Getters, Setters, Clone)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Portal<S> {
    name: String,
    statement: Arc<StoredStatement<S>>,
    parameter_format: Format,
    parameters: Vec<Option<Bytes>>,
    result_column_format_codes: Vec<i16>,
}

#[derive(Debug, Clone, Default)]
pub enum Format {
    #[default]
    UnifiedText,
    UnifiedBinary,
    Individual(Vec<i16>),
}

impl From<i16> for Format {
    fn from(v: i16) -> Format {
        if v == FORMAT_CODE_TEXT {
            Format::UnifiedText
        } else {
            Format::UnifiedBinary
        }
    }
}

impl Format {
    fn parameter_format_of(&self, idx: usize) -> i16 {
        match self {
            Format::UnifiedText => FORMAT_CODE_TEXT,
            Format::UnifiedBinary => FORMAT_CODE_BINARY,
            Format::Individual(ref fv) => fv[idx],
        }
    }
}

impl<S: Clone> Portal<S> {
    /// Try to create portal from bind command and current client state
    pub fn try_new(bind: &Bind, statement: Arc<StoredStatement<S>>) -> PgWireResult<Self> {
        let portal_name = bind
            .portal_name()
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_owned());

        // format
        let format = if bind.parameter_format_codes().is_empty() {
            Format::UnifiedText
        } else if bind.parameter_format_codes().len() == 1 {
            Format::from(bind.parameter_format_codes()[0])
        } else {
            Format::Individual(bind.parameter_format_codes().clone())
        };

        Ok(Portal {
            name: portal_name,
            statement,
            parameter_format: format,
            parameters: bind.parameters().clone(),
            result_column_format_codes: bind.result_column_format_codes().clone(),
        })
    }

    /// Get number of parameters
    pub fn parameter_len(&self) -> usize {
        self.parameters.len()
    }

    /// Attempt to get parameter at given index as type `T`.
    ///
    pub fn parameter<T>(&self, idx: usize) -> PgWireResult<Option<T>>
    where
        T: FromSqlOwned,
    {
        let param = self
            .parameters()
            .get(idx)
            .ok_or_else(|| PgWireError::ParameterIndexOutOfBound(idx))?;

        let _format = self.parameter_format().parameter_format_of(idx);

        let ty = self
            .statement
            .parameter_types()
            .get(idx)
            .ok_or_else(|| PgWireError::ParameterTypeIndexOutOfBound(idx))?;

        if !T::accepts(ty) {
            return Err(PgWireError::InvalidRustTypeForParameter(
                ty.name().to_owned(),
            ));
        }

        if let Some(ref param) = param {
            // TODO: from_sql only works with binary format
            // here we need to check format code first and seek to support text
            T::from_sql(ty, param)
                .map(|v| Some(v))
                .map_err(PgWireError::FailedToParseParameter)
        } else {
            // Null
            Ok(None)
        }
    }
}

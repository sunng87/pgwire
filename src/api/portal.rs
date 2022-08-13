use postgres_types::{FromSqlOwned, Type};

use crate::{
    error::{PgWireError, PgWireResult},
    messages::{
        data::{FORMAT_CODE_BINARY, FORMAT_CODE_TEXT},
        extendedquery::Bind,
    },
};

use super::{ClientInfo, DEFAULT_NAME};

/// Represent a prepared sql statement and its parameters bound by a `Bind`
/// request.
#[derive(Debug, Default, Getters, Setters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Portal {
    name: String,
    statement: String,
    parameter_types: Vec<Type>,
    parameter_format: Format,
    parameters: Vec<Option<Vec<u8>>>,
    result_column_format_codes: Vec<i16>,
}

#[derive(Debug)]
pub enum Format {
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

impl Default for Format {
    fn default() -> Self {
        Format::UnifiedText
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

impl Portal {
    /// Try to create portal from bind command and current client state
    pub fn try_new<C>(bind: &Bind, client: &C) -> PgWireResult<Portal>
    where
        C: ClientInfo,
    {
        let portal_name = bind
            .portal_name()
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_owned());
        let statement_name = bind
            .statement_name()
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_owned());
        let statement = client
            .stmt_store()
            .get(&statement_name)
            .ok_or_else(|| PgWireError::StatementNotFound(statement_name.clone()))?;

        // types
        let mut types = Vec::new();
        for oid in statement.type_oids() {
            // TODO: support non built-in types
            types.push(Type::from_oid(*oid).ok_or(PgWireError::UnknownTypeId(*oid))?);
        }

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
            statement: statement.statement().to_owned(),
            parameter_types: types,
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
            .ok_or(PgWireError::ParameterIndexOutOfBound(idx))?;

        let _format = self.parameter_format().parameter_format_of(idx);

        let ty = self
            .parameter_types()
            .get(idx)
            .ok_or(PgWireError::ParameterTypeIndexOutOfBound(idx))?;

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

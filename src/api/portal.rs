use std::fmt::Debug;
use std::ops::DerefMut;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::StreamExt;
use postgres_types::FromSqlOwned;
use tokio::sync::Mutex;

use crate::api::Type;
use crate::api::results::QueryResponse;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::data::FORMAT_CODE_BINARY;
use crate::messages::extendedquery::Bind;
use crate::types::FromSqlText;
use crate::types::format::FormatOptions;

use super::DEFAULT_NAME;
use super::results::FieldFormat;
use super::stmt::StoredStatement;

/// Represent a prepared sql statement and its parameters bound by a `Bind`
/// request.
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct Portal<S> {
    pub name: String,
    pub statement: Arc<StoredStatement<S>>,
    pub parameter_format: Format,
    pub parameters: Vec<Option<Bytes>>,
    pub result_column_format: Format,
    pub state: Arc<Mutex<PortalExecutionState>>,
}

/// Execution state of a portal during extended query processing.
#[derive(Default, Debug)]
pub enum PortalExecutionState {
    #[default]
    Initial,
    // tag and data stream
    Suspended(QueryResponse),
    Finished,
}

/// Result of fetching rows from a portal in `Suspended` state.
#[derive(Debug)]
pub struct FetchResult {
    pub response: QueryResponse,
    pub suspended: bool,
}

/// Column format specification for parameters or result columns.
#[derive(Debug, Clone, Default)]
pub enum Format {
    #[default]
    UnifiedText,
    UnifiedBinary,
    Individual(Vec<i16>),
}

impl From<i16> for Format {
    fn from(v: i16) -> Format {
        if v == FORMAT_CODE_BINARY {
            Format::UnifiedBinary
        } else {
            Format::UnifiedText
        }
    }
}

impl Format {
    /// Get format code for given index
    pub fn format_for(&self, idx: usize) -> FieldFormat {
        match self {
            Format::UnifiedText => FieldFormat::Text,
            Format::UnifiedBinary => FieldFormat::Binary,
            Format::Individual(fv) => FieldFormat::from(fv[idx]),
        }
    }

    /// Test if `idx` field is text format
    pub fn is_text(&self, idx: usize) -> bool {
        self.format_for(idx) == FieldFormat::Text
    }

    /// Test if `idx` field is binary format
    pub fn is_binary(&self, idx: usize) -> bool {
        self.format_for(idx) == FieldFormat::Binary
    }

    fn from_codes(codes: &[i16]) -> Self {
        if codes.is_empty() {
            Format::UnifiedText
        } else if codes.len() == 1 {
            Format::from(codes[0])
        } else {
            Format::Individual(codes.to_vec())
        }
    }
}

impl<S: Clone> Portal<S> {
    /// Try to create portal from bind command and current client state
    pub fn try_new(bind: &Bind, statement: Arc<StoredStatement<S>>) -> PgWireResult<Self> {
        let portal_name = bind
            .portal_name
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_owned());

        // param format
        let param_format = Format::from_codes(&bind.parameter_format_codes);

        // format
        let result_format = Format::from_codes(&bind.result_column_format_codes);

        Ok(Portal {
            name: portal_name,
            statement,
            parameter_format: param_format,
            parameters: bind.parameters.clone(),
            result_column_format: result_format,
            state: Arc::new(Mutex::new(PortalExecutionState::Initial)),
        })
    }

    /// Create a cursor-oriented portal with a stored statement.
    ///
    /// The portal starts in `Initial` state. The first call to [`fetch()`](Self::fetch)
    /// after [`start()`](Self::start) will begin returning rows.
    /// Use [`start()`](Self::start) to provide a `QueryResponse` before fetching.
    pub fn new_cursor(name: String, statement: Arc<StoredStatement<S>>) -> Self {
        Portal {
            name,
            statement,
            parameter_format: Format::UnifiedText,
            parameters: vec![],
            result_column_format: Format::UnifiedText,
            state: Arc::new(Mutex::new(PortalExecutionState::Initial)),
        }
    }

    /// Get number of parameters
    pub fn parameter_len(&self) -> usize {
        self.parameters.len()
    }

    /// Attempt to get parameter at given index as type `T`.
    ///
    pub fn parameter<'a, T>(&'a self, idx: usize, pg_type: &Type) -> PgWireResult<Option<T>>
    where
        T: FromSqlOwned + FromSqlText<'a>,
    {
        if !T::accepts(pg_type) {
            return Err(PgWireError::InvalidRustTypeForParameter(
                pg_type.name().to_owned(),
            ));
        }

        let param = self
            .parameters
            .get(idx)
            .ok_or_else(|| PgWireError::ParameterIndexOutOfBound(idx))?;

        let _format = self.parameter_format.format_for(idx);

        if let Some(param) = param {
            if self.parameter_format.is_binary(idx) {
                T::from_sql(pg_type, param)
                    .map(|v| Some(v))
                    .map_err(PgWireError::FailedToParseParameter)
            } else {
                T::from_sql_text(pg_type, param, &FormatOptions::default())
                    .map(|v| Some(v))
                    .map_err(PgWireError::FailedToParseParameter)
            }
        } else {
            // Null
            Ok(None)
        }
    }

    /// Get a handle to the portal's execution state.
    pub fn state(&self) -> Arc<Mutex<PortalExecutionState>> {
        self.state.clone()
    }

    /// Transition the portal from `Initial` to `Suspended` with the given
    /// query response.
    ///
    /// This is called by the query handler after executing the portal's
    /// statement, before calling [`fetch()`](Self::fetch) to retrieve rows.
    pub async fn start(&self, response: QueryResponse) {
        let mut state = self.state.lock().await;
        *state = PortalExecutionState::Suspended(response);
    }

    /// Fetch up to `max_rows` from a portal's suspended state.
    ///
    /// Returns a [`FetchResult`] containing the rows, the row schema, and
    /// whether the portal is still suspended (has more rows). When the
    /// underlying stream is exhausted, the portal transitions to `Finished`.
    /// When `max_rows` is 0, all remaining rows are fetched.
    ///
    /// Returns an error if the portal is in `Initial` state (call
    /// [`start()`](Self::start) first) or if the stream yields an error.
    pub async fn fetch(&self, max_rows: usize) -> PgWireResult<FetchResult> {
        let mut state = self.state.lock().await;

        match state.deref_mut() {
            PortalExecutionState::Initial => Err(PgWireError::PortalNotStarted),
            PortalExecutionState::Finished => Ok(FetchResult {
                response: QueryResponse::new(Arc::new(vec![]), futures::stream::empty()),
                suspended: false,
            }),
            PortalExecutionState::Suspended(response) => {
                let command_tag = response.command_tag().to_owned();
                let row_schema = response.row_schema();
                let data_rows = response.data_rows();

                let mut rows = Vec::with_capacity(if max_rows == 0 { 256 } else { max_rows });
                let mut suspended = true;
                while max_rows == 0 || rows.len() < max_rows {
                    if let Some(row) = data_rows.next().await {
                        rows.push(row?);
                    } else {
                        suspended = false;
                        break;
                    }
                }

                if !suspended {
                    *state = PortalExecutionState::Finished;
                }

                let result_response = QueryResponse {
                    command_tag,
                    row_schema,
                    data_rows: Box::pin(futures::stream::iter(rows.into_iter().map(Ok))),
                };

                Ok(FetchResult {
                    response: result_response,
                    suspended,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use postgres_types::FromSql;

    use super::*;

    #[test]
    fn test_from_sql() {
        assert_eq!(
            "helloworld",
            String::from_sql(&Type::UNKNOWN, "helloworld".as_bytes()).unwrap()
        )
    }
}

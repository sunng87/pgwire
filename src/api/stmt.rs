use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use postgres_types::Type;

use crate::error::PgWireResult;
use crate::messages::PgWireBackendMessage;
use crate::messages::extendedquery::Parse;

use super::portal::Format;
use super::results::FieldInfo;
use super::{ClientInfo, DEFAULT_NAME};

#[non_exhaustive]
#[derive(Debug, Default, new)]
pub struct StoredStatement<S> {
    /// name of the statement
    pub id: String,
    /// parsed query statement
    pub statement: S,
    /// type ids of query parameters, can be empty if frontend asks backend for
    /// type inference
    pub parameter_types: Vec<Option<Type>>,
}

impl<S> StoredStatement<S> {
    pub(crate) async fn parse<C, Q>(
        client: &C,
        parse: &Parse,
        parser: Q,
    ) -> PgWireResult<StoredStatement<S>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        Q: QueryParser<Statement = S>,
    {
        let types = parse
            .type_oids
            .iter()
            .map(|oid| Type::from_oid(*oid))
            .collect::<Vec<_>>();
        let statement = parser.parse_sql(client, &parse.query, &types).await?;
        Ok(StoredStatement {
            id: parse
                .name
                .clone()
                .unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            statement,
            parameter_types: types,
        })
    }
}

/// Trait for sql parser. The parser transforms string query into its statement
/// type.
#[async_trait]
pub trait QueryParser {
    type Statement;

    /// Convert sql string into the internal type of query execution
    ///
    /// The client may or may not provide type information with any parameters
    /// from the sql.
    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync;

    /// Get parameter type information from parsed statement
    ///
    /// Implement this if your query engine can provide type resolution for
    /// parameters. `ExtendedQueryHandler` will use this information for auto
    /// implementation of `do_describe_statement`
    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>>;

    /// Get query result type information from parsed statement
    ///
    /// Implement this if your query engine can provide resultset schema
    /// resolution. `ExtendedQueryHandler` will use this information for auto
    /// implementation of `do_describe_portal` and `do_describe_portal`
    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>>;
}

#[async_trait]
impl<QP> QueryParser for Arc<QP>
where
    QP: QueryParser + Send + Sync,
{
    type Statement = QP::Statement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        (**self).parse_sql(client, sql, types).await
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        (**self).get_parameter_types(stmt)
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        (**self).get_result_schema(stmt, column_format)
    }
}

/// A demo parser implementation. Never use it in serious application.
#[derive(new, Debug, Default)]
pub struct NoopQueryParser;

#[async_trait]
impl QueryParser for NoopQueryParser {
    type Statement = String;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(sql.to_owned())
    }

    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(vec![])
    }

    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(vec![])
    }
}

use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use postgres_types::Type;

use crate::messages::extendedquery::Parse;
use crate::{error::PgWireResult, messages::PgWireBackendMessage};

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
    pub parameter_types: Vec<Type>,
}

impl<S> StoredStatement<S> {
    pub(crate) async fn parse<C, Q>(
        client: &mut C,
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
            .map(|oid| Type::from_oid(*oid).unwrap_or(Type::UNKNOWN))
            .collect::<Vec<Type>>();
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

    async fn parse_sql<C>(
        &self,
        client: &mut C,
        sql: &str,
        types: &[Type],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync;
}

#[async_trait]
impl<QP> QueryParser for Arc<QP>
where
    QP: QueryParser + Send + Sync,
{
    type Statement = QP::Statement;

    async fn parse_sql<C>(
        &self,
        client: &mut C,
        sql: &str,
        types: &[Type],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        (**self).parse_sql(client, sql, types).await
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
        _client: &mut C,
        sql: &str,
        _types: &[Type],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(sql.to_owned())
    }
}

use postgres_types::Oid;

use crate::error::PgWireResult;
use crate::messages::extendedquery::Parse;

use super::DEFAULT_NAME;

#[derive(Debug, Default, new, Getters, Setters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct StoredStatement<S> {
    // name of the statement, empty string for unnamed
    id: String,
    // query statement
    statement: S,
    // type ids of query parameters
    type_oids: Vec<Oid>,
}

impl<S> StoredStatement<S> {
    pub(crate) fn parse<Q>(parse: &Parse, parser: &Q) -> PgWireResult<StoredStatement<S>>
    where
        Q: QueryParser<Statement = S>,
    {
        Ok(StoredStatement {
            id: parse
                .name()
                .clone()
                .unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            statement: parser.parse_sql(parse.query())?,
            type_oids: parse.type_oids().clone(),
        })
    }
}

/// Trait for sql parser. The parser transforms string query into its statement
/// type.
pub trait QueryParser {
    type Statement;

    fn parse_sql(&self, sql: &str) -> PgWireResult<Self::Statement>;
}

/// A demo parser implementation. Never use it in serious application.
#[derive(new, Debug)]
pub struct NoopQueryParser;

impl QueryParser for NoopQueryParser {
    type Statement = String;

    fn parse_sql(&self, sql: &str) -> PgWireResult<Self::Statement> {
        Ok(sql.to_owned())
    }
}

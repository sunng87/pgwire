use postgres_types::Oid;

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

pub(crate) fn parse<S>(
    parse: &Parse,
    parser: &QueryParser<Statement = S>,
) -> PgWireResult<StoredStatement<S>> {
    StoredStatement {
        id: parse
            .name()
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_owned()),
        statement: parser.parse_sql(parse.query())?,
        type_oids: parse.type_oids().clone(),
    }
}

pub trait QueryParser {
    type Statement;

    fn parse_sql(&self, sql: &str) -> PgWireResult<Self::Statement>;
}

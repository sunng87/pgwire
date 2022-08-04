use crate::messages::extendedquery::Parse;

use super::DEFAULT_NAME;

#[derive(Debug, Default, new, Getters, Setters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Statement {
    // name of the statement, empty string for unnamed
    id: String,
    // query statement
    statement: String,
    // type ids of query parameters
    type_oids: Vec<i32>,
}

impl From<&Parse> for Statement {
    fn from(parse: &Parse) -> Statement {
        Statement {
            id: parse
                .name()
                .clone()
                .unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            statement: parse.query().clone(),
            type_oids: parse.type_oids().clone(),
        }
    }
}

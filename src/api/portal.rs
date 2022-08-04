use crate::{messages::extendedquery::Bind, types::Data};

use super::DEFAULT_NAME;

#[derive(Debug, Default, new, Getters, Setters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Portal {
    id: String,
    stmt_id: String,
    parameters: Vec<Data>,
}

impl From<&Bind> for Portal {
    fn from(bind: &Bind) -> Portal {
        Portal {
            id: bind
                .portal_name()
                .clone()
                .unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            stmt_id: bind
                .statement_name()
                .clone()
                .unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            // FIXME:
            parameters: bind.parameters().clone(),
        }
    }
}

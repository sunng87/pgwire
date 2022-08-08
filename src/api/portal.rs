use itertools;

use crate::{
    messages::{data::FORMAT_CODE_TEXT, extendedquery::Bind},
    types::Data,
};

use super::{stmt::Statement, ClientInfo, DEFAULT_NAME};

#[derive(Debug, Default, Getters, Setters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Portal {
    id: String,
    stmt_id: String,
    parameters: Vec<Data>,
}

impl Portal {
    pub fn new(bind: &Bind, statement: &Statement) -> Portal {
        Portal {
            id: bind
                .portal_name()
                .clone()
                .unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            stmt_id: bind
                .statement_name()
                .clone()
                .unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            parameters: parse_parameters(bind, statement),
        }
    }

    pub fn statement<C>(&self, client: &C) -> Option<String>
    where
        C: ClientInfo,
    {
        client
            .stmt_store()
            .get(self.stmt_id())
            .map(|s| s.statement().clone())
    }
}

fn parse_parameters(bind: &Bind, statement: &Statement) -> Vec<Data> {
    let bind_format_codes = bind.parameter_format_codes();
    let unified_format_code = if bind_format_codes.is_empty() {
        Some(FORMAT_CODE_TEXT)
    } else if bind_format_codes.len() == 1 {
        Some(bind_format_codes.get(0).cloned().unwrap())
    } else {
        None
    };

    if let Some(format_code) = unified_format_code {
        itertools::zip(bind.parameters(), statement.type_oids())
            .map(|(value, type_oid)| parse_value(format_code, *type_oid, value))
            .collect::<Vec<Data>>()
    } else {
        itertools::izip!(
            bind.parameters(),
            bind.parameter_format_codes(),
            statement.type_oids(),
        )
        .map(|(value, format_code, type_oid)| parse_value(*format_code, *type_oid, value))
        .collect::<Vec<Data>>()
    }
}

fn parse_value(format_code: i16, type_oid: i32, value: &Option<Vec<u8>>) -> Data {
    if let Some(ref value) = value {
        if format_code == FORMAT_CODE_TEXT {
            // text
            Data::from_text(type_oid, String::from_utf8_lossy(value).as_ref())
        } else {
            // binary
            Data::from_bytes(type_oid, value)
        }
    } else {
        Data::Null
    }
}

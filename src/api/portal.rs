use crate::types::Data;

#[derive(Debug, Default, new, Getters, Setters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct Portal {
    id: String,
    stmt_id: String,
    parameters: Vec<Data>,
}

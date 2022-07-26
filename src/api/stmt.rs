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

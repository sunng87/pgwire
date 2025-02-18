use crate::api::results::FieldInfo;
use crate::error::PgWireClientResult;
use crate::messages::data::DataRow;

#[derive(new, Debug)]
pub struct DataRowDecoder<'a> {
    fields: &'a [FieldInfo],
    row: DataRow,
}

impl<'a> DataRowDecoder<'a> {
    /// Get value from data row
    pub fn next<T>(&mut self) -> PgWireClientResult<Option<T>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {}

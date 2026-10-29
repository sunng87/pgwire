use bytes::{Buf, BufMut, BytesMut};

use super::invalid_data;
use crate::error::PgWireResult;
use crate::messages::data::DataRow;

const SIGNATURE: &[u8] = b"PGCOPY\n\xFF\r\n\0";
const HEADER_LEN: usize = SIGNATURE.len() + 8;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Phase {
    Header,
    Extension(usize),
    TupleCount,
    FieldLength,
    FieldData(usize),
    Trailer,
}

#[derive(Debug)]
pub(super) struct BinaryDecoder {
    columns: i16,
    phase: Phase,
    buffer: BytesMut,
    row: BytesMut,
    field_index: usize,
}

impl BinaryDecoder {
    pub(super) fn new(columns: i16) -> Self {
        Self {
            columns,
            phase: Phase::Header,
            buffer: BytesMut::new(),
            row: BytesMut::new(),
            field_index: 0,
        }
    }

    pub(super) fn push(&mut self, data: &[u8]) -> PgWireResult<Vec<DataRow>> {
        if self.phase == Phase::Trailer {
            return Err(invalid_data(
                "COPY binary data was pushed after the trailer",
            ));
        }

        self.buffer.extend_from_slice(data);
        let mut rows = Vec::new();

        loop {
            match self.phase {
                Phase::Header => {
                    let prefix_len = self.buffer.len().min(SIGNATURE.len());
                    if self.buffer[..prefix_len] != SIGNATURE[..prefix_len] {
                        return Err(invalid_data("invalid COPY binary signature"));
                    }

                    let critical_flags_end = self
                        .buffer
                        .len()
                        .min(SIGNATURE.len() + std::mem::size_of::<u16>());
                    if critical_flags_end > SIGNATURE.len()
                        && self.buffer[SIGNATURE.len()..critical_flags_end]
                            .iter()
                            .any(|byte| *byte != 0)
                    {
                        return Err(invalid_data("unsupported critical COPY binary flag"));
                    }

                    let extension_length_offset = SIGNATURE.len() + std::mem::size_of::<u32>();
                    if self.buffer.len() > extension_length_offset
                        && self.buffer[extension_length_offset] & 0x80 != 0
                    {
                        return Err(invalid_data("negative COPY binary header extension length"));
                    }
                    if self.buffer.len() < HEADER_LEN {
                        break;
                    }

                    let mut header = self.buffer.split_to(HEADER_LEN);
                    header.advance(SIGNATURE.len());
                    header.advance(4);
                    let extension_length = header.get_i32();
                    if extension_length < 0 {
                        return Err(invalid_data("negative COPY binary header extension length"));
                    }
                    self.phase = Phase::Extension(extension_length as usize);
                }
                Phase::Extension(remaining) => {
                    let consumed = remaining.min(self.buffer.len());
                    self.buffer.advance(consumed);
                    let remaining = remaining - consumed;
                    if remaining != 0 {
                        self.phase = Phase::Extension(remaining);
                        break;
                    }
                    self.phase = Phase::TupleCount;
                }
                Phase::TupleCount => {
                    if let Some(first) = self.buffer.first() {
                        let expected_first = self.columns.to_be_bytes()[0];
                        if *first != expected_first && *first != 0xFF {
                            return Err(invalid_data("invalid COPY binary tuple count prefix"));
                        }
                    }
                    if self.buffer.len() < 2 {
                        break;
                    }
                    let count = self.buffer.get_i16();
                    if count == -1 {
                        self.phase = Phase::Trailer;
                    } else if count < -1 {
                        return Err(invalid_data("invalid negative COPY binary tuple count"));
                    } else if count != self.columns {
                        return Err(invalid_data(format!(
                            "COPY binary tuple has {count} fields, expected {}",
                            self.columns
                        )));
                    } else {
                        self.row.clear();
                        self.field_index = 0;
                        self.phase = Phase::FieldLength;
                    }
                }
                Phase::FieldLength => {
                    if self.field_index == self.columns as usize {
                        rows.push(DataRow::new(std::mem::take(&mut self.row), self.columns));
                        self.phase = Phase::TupleCount;
                        continue;
                    }
                    if self.buffer.len() < 4 {
                        if self.buffer.first().is_some_and(|byte| byte & 0x80 != 0)
                            && !self.buffer.iter().all(|byte| *byte == 0xFF)
                        {
                            return Err(invalid_data(
                                "invalid negative COPY binary field length prefix",
                            ));
                        }
                        break;
                    }

                    let length = self.buffer.get_i32();
                    if length < -1 {
                        return Err(invalid_data("invalid negative COPY binary field length"));
                    }
                    self.row.put_i32(length);
                    if length == -1 || length == 0 {
                        self.field_index += 1;
                    } else {
                        self.phase = Phase::FieldData(length as usize);
                    }
                }
                Phase::FieldData(remaining) => {
                    let consumed = remaining.min(self.buffer.len());
                    if consumed != 0 {
                        let value = self.buffer.split_to(consumed);
                        self.row.extend_from_slice(&value);
                    }
                    let remaining = remaining - consumed;
                    if remaining != 0 {
                        self.phase = Phase::FieldData(remaining);
                        break;
                    }
                    self.field_index += 1;
                    self.phase = Phase::FieldLength;
                }
                Phase::Trailer => {
                    if !self.buffer.is_empty() {
                        return Err(invalid_data("data follows the COPY binary trailer"));
                    }
                    break;
                }
            }
        }

        Ok(rows)
    }

    pub(super) fn finish(&mut self) -> PgWireResult<Vec<DataRow>> {
        if self.phase != Phase::Trailer || !self.buffer.is_empty() {
            return Err(invalid_data("incomplete COPY binary stream"));
        }
        Ok(Vec::new())
    }
}

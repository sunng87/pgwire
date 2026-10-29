use super::{RecordEnding, accept_record_ending, build_row, invalid_data};
use crate::error::PgWireResult;
use crate::messages::data::DataRow;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Pending {
    None,
    Quote,
    Escape,
}

#[derive(Debug)]
pub(super) struct CsvDecoder {
    columns: i16,
    delimiter: u8,
    quote: u8,
    escape: u8,
    null_string: Vec<u8>,
    fields: Vec<Option<Vec<u8>>>,
    field: Vec<u8>,
    quoted_section: bool,
    in_quotes: bool,
    pending: Pending,
    pending_cr: bool,
    ending: Option<RecordEnding>,
    record_started: bool,
}

impl CsvDecoder {
    pub(super) fn new(
        columns: i16,
        delimiter: u8,
        quote: u8,
        escape: u8,
        null_string: Vec<u8>,
    ) -> Self {
        Self {
            columns,
            delimiter,
            quote,
            escape,
            null_string,
            fields: Vec::with_capacity(columns as usize),
            field: Vec::new(),
            quoted_section: false,
            in_quotes: false,
            pending: Pending::None,
            pending_cr: false,
            ending: None,
            record_started: false,
        }
    }

    pub(super) fn push(&mut self, data: &[u8]) -> PgWireResult<Vec<DataRow>> {
        let mut rows = Vec::new();
        for &byte in data {
            self.push_byte(byte, &mut rows)?;
        }
        Ok(rows)
    }

    fn push_byte(&mut self, byte: u8, rows: &mut Vec<DataRow>) -> PgWireResult<()> {
        let mut pending_byte = Some(byte);
        while let Some(byte) = pending_byte.take() {
            if self.pending_cr {
                self.pending_cr = false;
                if byte == b'\n' {
                    rows.push(self.end_record(Some(RecordEnding::CrLf))?);
                    continue;
                }
                rows.push(self.end_record(Some(RecordEnding::Cr))?);
                pending_byte = Some(byte);
                continue;
            }

            match self.pending {
                Pending::Quote => {
                    self.pending = Pending::None;
                    if byte == self.quote {
                        self.field.push(byte);
                        continue;
                    }
                    self.in_quotes = false;
                    pending_byte = Some(byte);
                    continue;
                }
                Pending::Escape => {
                    self.pending = Pending::None;
                    if byte == self.quote || byte == self.escape {
                        self.field.push(byte);
                        continue;
                    }
                    self.field.push(self.escape);
                    pending_byte = Some(byte);
                    continue;
                }
                Pending::None => {}
            }

            if self.in_quotes {
                if self.quote == self.escape && byte == self.quote {
                    self.pending = Pending::Quote;
                } else if self.quote != self.escape && byte == self.escape {
                    self.pending = Pending::Escape;
                } else if byte == self.quote {
                    self.in_quotes = false;
                } else {
                    self.field.push(byte);
                }
                continue;
            }

            if byte == self.quote {
                self.record_started = true;
                self.quoted_section = true;
                self.in_quotes = true;
            } else if byte == self.delimiter {
                self.record_started = true;
                self.end_field();
                if self.fields.len() >= self.columns as usize {
                    return Err(invalid_data("too many fields in COPY CSV row"));
                }
            } else if byte == b'\n' {
                rows.push(self.end_record(Some(RecordEnding::Lf))?);
            } else if byte == b'\r' {
                match self.ending {
                    Some(RecordEnding::Lf) => {
                        return Err(invalid_data("mixed COPY record-ending styles"));
                    }
                    Some(RecordEnding::Cr) => {
                        rows.push(self.end_record(Some(RecordEnding::Cr))?);
                    }
                    Some(RecordEnding::CrLf) | None => {
                        if self.fields.len() + 1 != self.columns as usize {
                            return Err(invalid_data("wrong number of fields in COPY CSV row"));
                        }
                        self.pending_cr = true;
                    }
                }
            } else {
                self.record_started = true;
                self.field.push(byte);
            }
        }
        Ok(())
    }

    fn end_field(&mut self) {
        let value = if !self.quoted_section && self.field == self.null_string {
            None
        } else {
            Some(std::mem::take(&mut self.field))
        };
        self.field.clear();
        self.quoted_section = false;
        self.fields.push(value);
    }

    fn end_record(&mut self, ending: Option<RecordEnding>) -> PgWireResult<DataRow> {
        if self.in_quotes || self.pending != Pending::None {
            return Err(invalid_data("unfinished COPY CSV quoted section"));
        }
        if let Some(ending) = ending {
            accept_record_ending(&mut self.ending, ending)?;
        }
        self.end_field();
        self.record_started = false;
        build_row(self.columns, std::mem::take(&mut self.fields))
    }

    pub(super) fn finish(&mut self) -> PgWireResult<Vec<DataRow>> {
        let mut rows = Vec::new();
        if self.pending_cr {
            self.pending_cr = false;
            rows.push(self.end_record(Some(RecordEnding::Cr))?);
            return Ok(rows);
        }

        if self.pending == Pending::Quote {
            self.pending = Pending::None;
            self.in_quotes = false;
        }
        if self.in_quotes || self.pending != Pending::None {
            return Err(invalid_data("unfinished COPY CSV quoted section"));
        }
        if self.record_started {
            rows.push(self.end_record(None)?);
        }
        Ok(rows)
    }
}

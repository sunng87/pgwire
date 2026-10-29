use super::{RecordEnding, accept_record_ending, build_row, invalid_data};
use crate::error::PgWireResult;
use crate::messages::data::DataRow;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum Escape {
    None,
    Slash,
    Octal { value: u16, digits: u8 },
    Hex { value: u16, digits: u8 },
}

#[derive(Debug)]
pub(super) struct TextDecoder {
    columns: i16,
    delimiter: u8,
    null_string: Vec<u8>,
    fields: Vec<Option<Vec<u8>>>,
    raw_field: Vec<u8>,
    field: Vec<u8>,
    escape: Escape,
    pending_cr: bool,
    ending: Option<RecordEnding>,
    record_started: bool,
}

impl TextDecoder {
    pub(super) fn new(columns: i16, delimiter: u8, null_string: Vec<u8>) -> Self {
        Self {
            columns,
            delimiter,
            null_string,
            fields: Vec::with_capacity(columns as usize),
            raw_field: Vec::new(),
            field: Vec::new(),
            escape: Escape::None,
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
        let mut pending = Some(byte);
        while let Some(byte) = pending.take() {
            if self.pending_cr {
                self.pending_cr = false;
                if byte == b'\n' {
                    rows.push(self.end_record(Some(RecordEnding::CrLf))?);
                    continue;
                }
                rows.push(self.end_record(Some(RecordEnding::Cr))?);
                pending = Some(byte);
                continue;
            }

            if self.escape == Escape::Slash
                && self.raw_field == self.null_string
                && (byte == self.delimiter || matches!(byte, b'\r' | b'\n'))
            {
                self.escape = Escape::None;
                pending = Some(byte);
                continue;
            }

            match self.escape {
                Escape::Slash => {
                    self.raw_field.push(byte);
                    match byte {
                        b'0'..=b'7' => {
                            self.escape = Escape::Octal {
                                value: u16::from(byte - b'0'),
                                digits: 1,
                            };
                        }
                        b'x' => {
                            self.escape = Escape::Hex {
                                value: 0,
                                digits: 0,
                            };
                        }
                        b'b' => {
                            self.field.push(8);
                            self.escape = Escape::None;
                        }
                        b'f' => {
                            self.field.push(12);
                            self.escape = Escape::None;
                        }
                        b'n' => {
                            self.field.push(b'\n');
                            self.escape = Escape::None;
                        }
                        b'r' => {
                            self.field.push(b'\r');
                            self.escape = Escape::None;
                        }
                        b't' => {
                            self.field.push(b'\t');
                            self.escape = Escape::None;
                        }
                        b'v' => {
                            self.field.push(11);
                            self.escape = Escape::None;
                        }
                        b'\\' => {
                            self.field.push(b'\\');
                            self.escape = Escape::None;
                        }
                        value => {
                            self.field.push(value);
                            self.escape = Escape::None;
                        }
                    }
                }
                Escape::Octal { value, digits } => {
                    if matches!(byte, b'0'..=b'7') && digits < 3 {
                        self.raw_field.push(byte);
                        let value = value * 8 + u16::from(byte - b'0');
                        let digits = digits + 1;
                        if digits == 3 {
                            self.field.push(value as u8);
                            self.escape = Escape::None;
                        } else {
                            self.escape = Escape::Octal { value, digits };
                        }
                    } else {
                        self.field.push(value as u8);
                        self.escape = Escape::None;
                        pending = Some(byte);
                    }
                }
                Escape::Hex { value, digits } => {
                    if let Some(digit) = hex_value(byte).filter(|_| digits < 2) {
                        self.raw_field.push(byte);
                        let value = value * 16 + u16::from(digit);
                        let digits = digits + 1;
                        if digits == 2 {
                            self.field.push(value as u8);
                            self.escape = Escape::None;
                        } else {
                            self.escape = Escape::Hex { value, digits };
                        }
                    } else {
                        self.field
                            .push(if digits == 0 { b'x' } else { value as u8 });
                        self.escape = Escape::None;
                        pending = Some(byte);
                    }
                }
                Escape::None => {
                    if byte == b'\\' {
                        self.record_started = true;
                        self.raw_field.push(byte);
                        self.escape = Escape::Slash;
                    } else if byte == self.delimiter {
                        self.record_started = true;
                        self.end_field();
                        if self.fields.len() >= self.columns as usize {
                            return Err(invalid_data("too many fields in COPY text row"));
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
                                    return Err(invalid_data(
                                        "wrong number of fields in COPY text row",
                                    ));
                                }
                                self.pending_cr = true;
                            }
                        }
                    } else {
                        self.record_started = true;
                        self.raw_field.push(byte);
                        self.field.push(byte);
                    }
                }
            }
        }
        Ok(())
    }

    fn end_field(&mut self) {
        let value = if self.raw_field == self.null_string {
            None
        } else {
            Some(std::mem::take(&mut self.field))
        };
        self.raw_field.clear();
        self.field.clear();
        self.fields.push(value);
    }

    fn end_record(&mut self, ending: Option<RecordEnding>) -> PgWireResult<DataRow> {
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

        match self.escape {
            Escape::Slash if self.raw_field == self.null_string => {}
            Escape::Slash => return Err(invalid_data("unfinished COPY text escape")),
            Escape::Octal { value, .. } => self.field.push(value as u8),
            Escape::Hex { value, digits } => {
                self.field
                    .push(if digits == 0 { b'x' } else { value as u8 });
            }
            Escape::None => {}
        }
        self.escape = Escape::None;

        if self.record_started {
            rows.push(self.end_record(None)?);
        }
        Ok(rows)
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

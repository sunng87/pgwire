use bytes::{Buf, BufMut, BytesMut};

use super::codec;
use super::DecodeContext;
use super::Message;
use crate::error::{PgWireError, PgWireResult};

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct CommandComplete {
    pub tag: String,
}

pub const MESSAGE_TYPE_BYTE_COMMAND_COMPLETE: u8 = b'C';

impl Message for CommandComplete {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_COMMAND_COMPLETE)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::SMALL_BACKEND_PACKET_SIZE_LIMIT
    }

    fn message_length(&self) -> usize {
        5 + self.tag.len()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        codec::put_cstring(buf, &self.tag);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let tag = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

        Ok(CommandComplete::new(tag))
    }
}

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct EmptyQueryResponse;

pub const MESSAGE_TYPE_BYTE_EMPTY_QUERY_RESPONSE: u8 = b'I';

impl Message for EmptyQueryResponse {
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_EMPTY_QUERY_RESPONSE)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::SMALL_BACKEND_PACKET_SIZE_LIMIT
    }

    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _buf: &mut BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    fn decode_body(
        _buf: &mut BytesMut,
        _full_len: usize,
        _ctx: &DecodeContext,
    ) -> PgWireResult<Self> {
        Ok(EmptyQueryResponse)
    }
}

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct ReadyForQuery {
    pub status: TransactionStatus,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum TransactionStatus {
    Idle = READY_STATUS_IDLE,
    Transaction = READY_STATUS_TRANSACTION_BLOCK,
    Error = READY_STATUS_FAILED_TRANSACTION_BLOCK,
}

pub const READY_STATUS_IDLE: u8 = b'I';
pub const READY_STATUS_TRANSACTION_BLOCK: u8 = b'T';
pub const READY_STATUS_FAILED_TRANSACTION_BLOCK: u8 = b'E';

pub const MESSAGE_TYPE_BYTE_READY_FOR_QUERY: u8 = b'Z';

impl Message for ReadyForQuery {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_READY_FOR_QUERY)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::SMALL_BACKEND_PACKET_SIZE_LIMIT
    }

    #[inline]
    fn message_length(&self) -> usize {
        5
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(self.status as u8);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let status = TransactionStatus::try_from(buf.get_u8())?;
        Ok(ReadyForQuery::new(status))
    }
}

impl TryFrom<u8> for TransactionStatus {
    type Error = PgWireError;
    fn try_from(value: u8) -> Result<Self, PgWireError> {
        match value {
            READY_STATUS_IDLE => Ok(Self::Idle),
            READY_STATUS_TRANSACTION_BLOCK => Ok(Self::Transaction),
            READY_STATUS_FAILED_TRANSACTION_BLOCK => Ok(Self::Error),
            _ => Err(PgWireError::InvalidTransactionStatus(value)),
        }
    }
}

/// postgres error response, sent from backend to frontend
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct ErrorResponse {
    pub fields: Vec<(u8, String)>,
}

pub const MESSAGE_TYPE_BYTE_ERROR_RESPONSE: u8 = b'E';

impl Message for ErrorResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_ERROR_RESPONSE)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::LONG_BACKEND_PACKET_SIZE_LIMIT
    }

    fn message_length(&self) -> usize {
        4 + self.fields.iter().map(|f| 1 + f.1.len() + 1).sum::<usize>() + 1
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        for (code, value) in &self.fields {
            buf.put_u8(*code);
            codec::put_cstring(buf, value);
        }

        buf.put_u8(b'\0');

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let mut fields = Vec::new();
        loop {
            let code = buf.get_u8();

            if code == b'\0' {
                return Ok(ErrorResponse { fields });
            } else {
                let value = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
                fields.push((code, value));
            }
        }
    }
}

/// postgres error response, sent from backend to frontend
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct NoticeResponse {
    pub fields: Vec<(u8, String)>,
}

pub const MESSAGE_TYPE_BYTE_NOTICE_RESPONSE: u8 = b'N';

impl Message for NoticeResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_NOTICE_RESPONSE)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::LONG_BACKEND_PACKET_SIZE_LIMIT
    }

    fn message_length(&self) -> usize {
        4 + self.fields.iter().map(|f| 1 + f.1.len() + 1).sum::<usize>() + 1
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        for (code, value) in &self.fields {
            buf.put_u8(*code);
            codec::put_cstring(buf, value);
        }

        buf.put_u8(b'\0');

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let mut fields = Vec::new();
        loop {
            let code = buf.get_u8();

            if code == b'\0' {
                return Ok(NoticeResponse { fields });
            } else {
                let value = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
                fields.push((code, value));
            }
        }
    }
}

/// Response to SSLRequest.
///
/// To initiate an SSL-encrypted connection, the frontend initially sends an
/// SSLRequest message rather than a StartupMessage. The server then responds
/// with a single byte containing 'S' or 'N', indicating that it is willing or
/// unwilling to perform SSL, respectively.
#[non_exhaustive]
#[derive(Debug, PartialEq)]
pub enum SslResponse {
    Accept,
    Refuse,
}

impl SslResponse {
    pub const BYTE_ACCEPT: u8 = b'S';
    pub const BYTE_REFUSE: u8 = b'N';
    // The whole message takes only one byte and has no size field.
    pub const MESSAGE_LENGTH: usize = 1;
}

impl Message for SslResponse {
    fn message_length(&self) -> usize {
        Self::MESSAGE_LENGTH
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Self::Accept => buf.put_u8(Self::BYTE_ACCEPT),
            Self::Refuse => buf.put_u8(Self::BYTE_REFUSE),
        }
        Ok(())
    }

    fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        self.encode_body(buf)
    }

    fn decode_body(_: &mut BytesMut, _: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        unreachable!()
    }

    fn decode(buf: &mut BytesMut, _ctx: &DecodeContext) -> PgWireResult<Option<Self>> {
        if buf.remaining() >= Self::MESSAGE_LENGTH {
            match buf[0] {
                Self::BYTE_ACCEPT => {
                    buf.advance(Self::MESSAGE_LENGTH);
                    Ok(Some(SslResponse::Accept))
                }
                Self::BYTE_REFUSE => {
                    buf.advance(Self::MESSAGE_LENGTH);
                    Ok(Some(SslResponse::Refuse))
                }
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

/// NotificationResponse
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, Default, new)]
pub struct NotificationResponse {
    pub pid: i32,
    pub channel: String,
    pub payload: String,
}

pub const MESSAGE_TYPE_BYTE_NOTIFICATION_RESPONSE: u8 = b'A';

impl Message for NotificationResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_NOTIFICATION_RESPONSE)
    }

    #[inline]
    fn max_message_length() -> usize {
        super::LONG_BACKEND_PACKET_SIZE_LIMIT
    }

    fn message_length(&self) -> usize {
        8 + self.channel.len() + 1 + self.payload.len() + 1
    }

    fn encode_body(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i32(self.pid);
        codec::put_cstring(buf, &self.channel);
        codec::put_cstring(buf, &self.payload);

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize, _ctx: &DecodeContext) -> PgWireResult<Self> {
        let pid = buf.get_i32();
        let channel = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
        let payload = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());

        Ok(NotificationResponse {
            pid,
            channel,
            payload,
        })
    }
}

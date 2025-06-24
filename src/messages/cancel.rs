use bytes::{Buf, BufMut};

use super::{codec, startup::SecretKey, DecodeContext, Message};
use crate::error::{PgWireError, PgWireResult};

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct CancelRequest {
    pub pid: i32,
    pub secret_key: SecretKey,
}

impl CancelRequest {
    const CANCEL_REQUEST_CODE: i32 = 80877102;

    const MINIMUM_CANCEL_REQUEST_MESSAGE_LEN: usize = 16;

    /// try to inspect the buf if it's a cancel request packet
    /// return None if there is not enough bytes to give the result
    pub fn is_cancel_request_packet(buf: &[u8]) -> Option<bool> {
        if buf.len() >= 8 {
            let cancel_code = (&buf[4..8]).get_i32();
            Some(cancel_code == CancelRequest::CANCEL_REQUEST_CODE)
        } else {
            None
        }
    }
}

impl Message for CancelRequest {
    #[inline]
    fn message_length(&self) -> usize {
        match &self.secret_key {
            SecretKey::I32(_) => 16,
            SecretKey::Bytes(bytes) => 12 + bytes.len(),
        }
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        buf.put_i32(CancelRequest::CANCEL_REQUEST_CODE);
        buf.put_i32(self.pid);

        match &self.secret_key {
            SecretKey::I32(key) => buf.put_i32(*key),
            SecretKey::Bytes(key) => buf.put_slice(key),
        }

        Ok(())
    }

    fn decode(buf: &mut bytes::BytesMut, ctx: &DecodeContext) -> PgWireResult<Option<Self>> {
        if let Some(is_cancel) = Self::is_cancel_request_packet(buf) {
            if is_cancel {
                codec::decode_packet(buf, 0, |buf, full_len| {
                    Self::decode_body(buf, full_len, ctx)
                })
            } else {
                Err(PgWireError::InvalidCancelRequest)
            }
        } else {
            Ok(None)
        }
    }

    fn decode_body(
        buf: &mut bytes::BytesMut,
        msg_len: usize,
        _ctx: &DecodeContext,
    ) -> PgWireResult<Self> {
        if msg_len < CancelRequest::MINIMUM_CANCEL_REQUEST_MESSAGE_LEN {
            return Err(PgWireError::InvalidCancelRequest);
        }

        // skip length and cancel code
        buf.advance(4);
        let pid = buf.get_i32();
        let secret_key = buf.split_to(msg_len - 12).freeze();

        Ok(CancelRequest {
            pid,
            secret_key: SecretKey::Bytes(secret_key),
        })
    }
}

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
    pub fn is_cancel_request_packet(buf: &[u8]) -> bool {
        if buf.remaining() < 8 {
            return false;
        }

        let cancel_code = (&buf[4..8]).get_i32();
        cancel_code == CancelRequest::CANCEL_REQUEST_CODE
    }
}

impl Message for CancelRequest {
    #[inline]
    fn message_length(&self) -> usize {
        12 + self.secret_key.len()
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        buf.put_i32(CancelRequest::CANCEL_REQUEST_CODE);
        buf.put_i32(self.pid);

        self.secret_key.encode(buf)?;

        Ok(())
    }

    /// Please call `is_cancel_request_packet` before calling this if you don't
    /// want to get an error for non-CancelRequest packet.
    fn decode(buf: &mut bytes::BytesMut, ctx: &DecodeContext) -> PgWireResult<Option<Self>> {
        if buf.remaining() >= 8 {
            if Self::is_cancel_request_packet(buf) {
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
        ctx: &DecodeContext,
    ) -> PgWireResult<Self> {
        if msg_len < CancelRequest::MINIMUM_CANCEL_REQUEST_MESSAGE_LEN {
            return Err(PgWireError::InvalidCancelRequest);
        }

        // skip cancel code
        buf.advance(4);
        let pid = buf.get_i32();
        let secret_key = SecretKey::decode(buf, msg_len - 12, ctx)?;

        Ok(CancelRequest { pid, secret_key })
    }
}

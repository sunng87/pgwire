use super::Message;

#[derive(Default, PartialEq, Eq, Debug, new)]
pub struct Terminate;

pub const MESSAGE_TYPE_BYTE_TERMINATE: u8 = b'X';

impl Message for Terminate {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_TERMINATE)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _: &mut bytes::BytesMut) -> std::io::Result<()> {
        Ok(())
    }

    fn decode_body(_: &mut bytes::BytesMut) -> std::io::Result<Self> {
        Ok(Terminate)
    }
}

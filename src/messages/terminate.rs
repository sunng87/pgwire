use super::Message;

#[derive(Default, PartialEq, Eq, Debug, new)]
pub struct Terminate;

impl Message for Terminate {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(b'X')
    }

    #[inline]
    fn message_length(&self) -> i32 {
        4
    }

    fn encode_body(&self, _: &mut bytes::BytesMut) -> std::io::Result<()> {
        Ok(())
    }

    fn decode_body(_: &mut bytes::BytesMut) -> std::io::Result<Self> {
        Ok(Terminate)
    }
}

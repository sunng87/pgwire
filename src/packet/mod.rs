pub(crate) trait MessageType {
    #[inline]
    fn message_type(&self) -> Option<u8> {
        None
    }
}

pub(crate) trait MessageLength {
    fn message_length(&self) -> i32;
}

mod startup;

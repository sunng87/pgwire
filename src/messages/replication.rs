use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::codec;
use crate::error::{PgWireError, PgWireResult};

pub const MSG_TYPE_BEGIN: u8 = b'B';
pub const MSG_TYPE_COMMIT: u8 = b'C';
pub const MSG_TYPE_ORIGIN: u8 = b'O';
pub const MSG_TYPE_RELATION: u8 = b'R';
pub const MSG_TYPE_TYPE: u8 = b'Y';
pub const MSG_TYPE_INSERT: u8 = b'I';
pub const MSG_TYPE_UPDATE: u8 = b'U';
pub const MSG_TYPE_DELETE: u8 = b'D';
pub const MSG_TYPE_TRUNCATE: u8 = b'T';
pub const MSG_TYPE_MESSAGE: u8 = b'M';

pub const MSG_TYPE_STREAM_START: u8 = b'S';
pub const MSG_TYPE_STREAM_STOP: u8 = b'E';
pub const MSG_TYPE_STREAM_COMMIT: u8 = b'c';
pub const MSG_TYPE_STREAM_ABORT: u8 = b'A';

pub const MSG_TYPE_BEGIN_PREPARE: u8 = b'b';
pub const MSG_TYPE_PREPARE: u8 = b'P';
pub const MSG_TYPE_COMMIT_PREPARED: u8 = b'K';
pub const MSG_TYPE_ROLLBACK_PREPARED: u8 = b'r';
pub const MSG_TYPE_STREAM_PREPARE: u8 = b'p';

pub const MSG_TYPE_XLOG_DATA: u8 = b'w';
pub const MSG_TYPE_PRIMARY_KEEPALIVE: u8 = b'k';
pub const MSG_TYPE_STANDBY_STATUS_UPDATE: u8 = b'r';
pub const MSG_TYPE_HOT_STANDBY_FEEDBACK: u8 = b'h';

pub const TUPLE_DATA_NULL: u8 = b'n';
pub const TUPLE_DATA_UNCHANGED_TOAST: u8 = b'u';
pub const TUPLE_DATA_TEXT: u8 = b't';
pub const TUPLE_DATA_BINARY: u8 = b'b';

pub const UPDATE_OLD_TUPLE_KEY: u8 = b'K';
pub const UPDATE_OLD_TUPLE: u8 = b'O';
pub const UPDATE_NEW_TUPLE: u8 = b'N';

pub const DELETE_OLD_TUPLE_KEY: u8 = b'K';
pub const DELETE_OLD_TUPLE: u8 = b'O';

pub const TRUNCATE_OPTION_CASCADE: i8 = 1;
pub const TRUNCATE_OPTION_RESTART_IDENTITY: i8 = 2;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TupleDataColumn {
    Null,
    UnchangedToast,
    Text(Bytes),
    Binary(Bytes),
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct TupleData {
    pub columns: Vec<TupleDataColumn>,
}

impl TupleData {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i16(self.columns.len() as i16);
        for col in &self.columns {
            match col {
                TupleDataColumn::Null => {
                    buf.put_u8(TUPLE_DATA_NULL);
                }
                TupleDataColumn::UnchangedToast => {
                    buf.put_u8(TUPLE_DATA_UNCHANGED_TOAST);
                }
                TupleDataColumn::Text(data) => {
                    buf.put_u8(TUPLE_DATA_TEXT);
                    buf.put_i32(data.len() as i32);
                    buf.put_slice(data.as_ref());
                }
                TupleDataColumn::Binary(data) => {
                    buf.put_u8(TUPLE_DATA_BINARY);
                    buf.put_i32(data.len() as i32);
                    buf.put_slice(data.as_ref());
                }
            }
        }
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let num_columns = buf.get_i16();
        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            let col_type = buf.get_u8();
            match col_type {
                TUPLE_DATA_NULL => {
                    columns.push(TupleDataColumn::Null);
                }
                TUPLE_DATA_UNCHANGED_TOAST => {
                    columns.push(TupleDataColumn::UnchangedToast);
                }
                TUPLE_DATA_TEXT => {
                    let len = buf.get_i32() as usize;
                    let data = buf.split_to(len).freeze();
                    columns.push(TupleDataColumn::Text(data));
                }
                TUPLE_DATA_BINARY => {
                    let len = buf.get_i32() as usize;
                    let data = buf.split_to(len).freeze();
                    columns.push(TupleDataColumn::Binary(data));
                }
                _ => {
                    return Err(PgWireError::InvalidMessageType(col_type));
                }
            }
        }
        Ok(TupleData::new(columns))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct RelationColumn {
    pub flags: i8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

impl RelationColumn {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_i8(self.flags);
        codec::put_cstring(buf, &self.name);
        buf.put_u32(self.type_oid);
        buf.put_i32(self.type_modifier);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let flags = buf.get_i8();
        let name = codec::get_cstring(buf).unwrap_or_default();
        let type_oid = buf.get_u32();
        let type_modifier = buf.get_i32();
        Ok(RelationColumn::new(flags, name, type_oid, type_modifier))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct Begin {
    pub final_lsn: i64,
    pub commit_timestamp: i64,
    pub transaction_id: i32,
}

impl Begin {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_BEGIN);
        buf.put_i64(self.final_lsn);
        buf.put_i64(self.commit_timestamp);
        buf.put_i32(self.transaction_id);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let final_lsn = buf.get_i64();
        let commit_timestamp = buf.get_i64();
        let transaction_id = buf.get_i32();
        Ok(Begin::new(final_lsn, commit_timestamp, transaction_id))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct Commit {
    pub flags: i8,
    pub commit_lsn: i64,
    pub end_lsn: i64,
    pub commit_timestamp: i64,
}

impl Commit {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_COMMIT);
        buf.put_i8(self.flags);
        buf.put_i64(self.commit_lsn);
        buf.put_i64(self.end_lsn);
        buf.put_i64(self.commit_timestamp);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let flags = buf.get_i8();
        let commit_lsn = buf.get_i64();
        let end_lsn = buf.get_i64();
        let commit_timestamp = buf.get_i64();
        Ok(Commit::new(flags, commit_lsn, end_lsn, commit_timestamp))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct Origin {
    pub lsn: i64,
    pub origin_name: String,
}

impl Origin {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_ORIGIN);
        buf.put_i64(self.lsn);
        codec::put_cstring(buf, &self.origin_name);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let lsn = buf.get_i64();
        let origin_name = codec::get_cstring(buf).unwrap_or_default();
        Ok(Origin::new(lsn, origin_name))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct Relation {
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: i8,
    pub columns: Vec<RelationColumn>,
}

impl Relation {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_RELATION);
        buf.put_u32(self.relation_id);
        codec::put_cstring(buf, &self.namespace);
        codec::put_cstring(buf, &self.name);
        buf.put_i8(self.replica_identity);
        buf.put_i16(self.columns.len() as i16);
        for col in &self.columns {
            col.encode(buf)?;
        }
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let relation_id = buf.get_u32();
        let namespace = codec::get_cstring(buf).unwrap_or_default();
        let name = codec::get_cstring(buf).unwrap_or_default();
        let replica_identity = buf.get_i8();
        let num_columns = buf.get_i16();
        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            columns.push(RelationColumn::decode(buf)?);
        }
        Ok(Relation::new(
            relation_id,
            namespace,
            name,
            replica_identity,
            columns,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct TypeMessage {
    pub type_id: u32,
    pub namespace: String,
    pub name: String,
}

impl TypeMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_TYPE);
        buf.put_u32(self.type_id);
        codec::put_cstring(buf, &self.namespace);
        codec::put_cstring(buf, &self.name);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let type_id = buf.get_u32();
        let namespace = codec::get_cstring(buf).unwrap_or_default();
        let name = codec::get_cstring(buf).unwrap_or_default();
        Ok(TypeMessage::new(type_id, namespace, name))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct Insert {
    pub relation_id: u32,
    pub new_tuple: TupleData,
}

impl Insert {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_INSERT);
        buf.put_u32(self.relation_id);
        buf.put_u8(UPDATE_NEW_TUPLE);
        self.new_tuple.encode(buf)?;
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let relation_id = buf.get_u32();
        let _new_tuple_type = buf.get_u8();
        let new_tuple = TupleData::decode(buf)?;
        Ok(Insert::new(relation_id, new_tuple))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct Update {
    pub relation_id: u32,
    pub old_tuple_type: Option<u8>,
    pub old_tuple: Option<TupleData>,
    pub new_tuple: TupleData,
}

impl Update {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_UPDATE);
        buf.put_u32(self.relation_id);
        if let Some(ref old_tuple_type) = self.old_tuple_type {
            buf.put_u8(*old_tuple_type);
            if let Some(ref old_tuple) = self.old_tuple {
                old_tuple.encode(buf)?;
            }
        }
        buf.put_u8(UPDATE_NEW_TUPLE);
        self.new_tuple.encode(buf)?;
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let relation_id = buf.get_u32();

        let next_byte = buf[0];
        let (old_tuple_type, old_tuple) = if next_byte == UPDATE_NEW_TUPLE {
            (None, None)
        } else {
            let old_type = buf.get_u8();
            let old = TupleData::decode(buf)?;
            (Some(old_type), Some(old))
        };

        let _new_tuple_type = buf.get_u8();
        let new_tuple = TupleData::decode(buf)?;

        Ok(Update::new(
            relation_id,
            old_tuple_type,
            old_tuple,
            new_tuple,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct Delete {
    pub relation_id: u32,
    pub old_tuple_type: u8,
    pub old_tuple: TupleData,
}

impl Delete {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_DELETE);
        buf.put_u32(self.relation_id);
        buf.put_u8(self.old_tuple_type);
        self.old_tuple.encode(buf)?;
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let relation_id = buf.get_u32();
        let old_tuple_type = buf.get_u8();
        let old_tuple = TupleData::decode(buf)?;
        Ok(Delete::new(relation_id, old_tuple_type, old_tuple))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct Truncate {
    pub relation_count: i32,
    pub options: i8,
    pub relation_ids: Vec<u32>,
}

impl Truncate {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_TRUNCATE);
        buf.put_i32(self.relation_count);
        buf.put_i8(self.options);
        for id in &self.relation_ids {
            buf.put_u32(*id);
        }
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let relation_count = buf.get_i32();
        let options = buf.get_i8();
        let mut relation_ids = Vec::with_capacity(relation_count as usize);
        for _ in 0..relation_count {
            relation_ids.push(buf.get_u32());
        }
        Ok(Truncate::new(relation_count, options, relation_ids))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct LogicalMessage {
    pub flags: i8,
    pub lsn: i64,
    pub prefix: String,
    pub content: Bytes,
}

impl LogicalMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_MESSAGE);
        buf.put_i8(self.flags);
        buf.put_i64(self.lsn);
        codec::put_cstring(buf, &self.prefix);
        buf.put_i32(self.content.len() as i32);
        buf.put_slice(self.content.as_ref());
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let flags = buf.get_i8();
        let lsn = buf.get_i64();
        let prefix = codec::get_cstring(buf).unwrap_or_default();
        let content_len = buf.get_i32() as usize;
        let content = buf.split_to(content_len).freeze();
        Ok(LogicalMessage::new(flags, lsn, prefix, content))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct StreamStart {
    pub transaction_id: i32,
    pub first_segment: bool,
}

impl StreamStart {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_STREAM_START);
        buf.put_i32(self.transaction_id);
        buf.put_u8(if self.first_segment { 1 } else { 0 });
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let transaction_id = buf.get_i32();
        let first_segment = buf.get_u8() == 1;
        Ok(StreamStart::new(transaction_id, first_segment))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct StreamStop;

impl StreamStop {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_STREAM_STOP);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        Ok(StreamStop::new())
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct StreamCommit {
    pub transaction_id: i32,
    pub flags: i8,
    pub commit_lsn: i64,
    pub end_lsn: i64,
    pub commit_timestamp: i64,
}

impl StreamCommit {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_STREAM_COMMIT);
        buf.put_i32(self.transaction_id);
        buf.put_i8(self.flags);
        buf.put_i64(self.commit_lsn);
        buf.put_i64(self.end_lsn);
        buf.put_i64(self.commit_timestamp);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let transaction_id = buf.get_i32();
        let flags = buf.get_i8();
        let commit_lsn = buf.get_i64();
        let end_lsn = buf.get_i64();
        let commit_timestamp = buf.get_i64();
        Ok(StreamCommit::new(
            transaction_id,
            flags,
            commit_lsn,
            end_lsn,
            commit_timestamp,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct StreamAbort {
    pub transaction_id: i32,
    pub subtransaction_id: i32,
}

impl StreamAbort {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_STREAM_ABORT);
        buf.put_i32(self.transaction_id);
        buf.put_i32(self.subtransaction_id);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let transaction_id = buf.get_i32();
        let subtransaction_id = buf.get_i32();
        Ok(StreamAbort::new(transaction_id, subtransaction_id))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct BeginPrepare {
    pub prepare_lsn: i64,
    pub end_lsn: i64,
    pub prepare_timestamp: i64,
    pub transaction_id: i32,
    pub gid: String,
}

impl BeginPrepare {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_BEGIN_PREPARE);
        buf.put_i64(self.prepare_lsn);
        buf.put_i64(self.end_lsn);
        buf.put_i64(self.prepare_timestamp);
        buf.put_i32(self.transaction_id);
        codec::put_cstring(buf, &self.gid);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let prepare_lsn = buf.get_i64();
        let end_lsn = buf.get_i64();
        let prepare_timestamp = buf.get_i64();
        let transaction_id = buf.get_i32();
        let gid = codec::get_cstring(buf).unwrap_or_default();
        Ok(BeginPrepare::new(
            prepare_lsn,
            end_lsn,
            prepare_timestamp,
            transaction_id,
            gid,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct Prepare {
    pub flags: i8,
    pub prepare_lsn: i64,
    pub end_lsn: i64,
    pub prepare_timestamp: i64,
    pub transaction_id: i32,
    pub gid: String,
}

impl Prepare {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_PREPARE);
        buf.put_i8(self.flags);
        buf.put_i64(self.prepare_lsn);
        buf.put_i64(self.end_lsn);
        buf.put_i64(self.prepare_timestamp);
        buf.put_i32(self.transaction_id);
        codec::put_cstring(buf, &self.gid);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let flags = buf.get_i8();
        let prepare_lsn = buf.get_i64();
        let end_lsn = buf.get_i64();
        let prepare_timestamp = buf.get_i64();
        let transaction_id = buf.get_i32();
        let gid = codec::get_cstring(buf).unwrap_or_default();
        Ok(Prepare::new(
            flags,
            prepare_lsn,
            end_lsn,
            prepare_timestamp,
            transaction_id,
            gid,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct CommitPrepared {
    pub flags: i8,
    pub commit_lsn: i64,
    pub end_lsn: i64,
    pub commit_timestamp: i64,
    pub transaction_id: i32,
    pub gid: String,
}

impl CommitPrepared {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_COMMIT_PREPARED);
        buf.put_i8(self.flags);
        buf.put_i64(self.commit_lsn);
        buf.put_i64(self.end_lsn);
        buf.put_i64(self.commit_timestamp);
        buf.put_i32(self.transaction_id);
        codec::put_cstring(buf, &self.gid);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let flags = buf.get_i8();
        let commit_lsn = buf.get_i64();
        let end_lsn = buf.get_i64();
        let commit_timestamp = buf.get_i64();
        let transaction_id = buf.get_i32();
        let gid = codec::get_cstring(buf).unwrap_or_default();
        Ok(CommitPrepared::new(
            flags,
            commit_lsn,
            end_lsn,
            commit_timestamp,
            transaction_id,
            gid,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct RollbackPrepared {
    pub flags: i8,
    pub prepare_lsn: i64,
    pub rollback_lsn: i64,
    pub prepare_timestamp: i64,
    pub rollback_timestamp: i64,
    pub transaction_id: i32,
    pub gid: String,
}

impl RollbackPrepared {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_ROLLBACK_PREPARED);
        buf.put_i8(self.flags);
        buf.put_i64(self.prepare_lsn);
        buf.put_i64(self.rollback_lsn);
        buf.put_i64(self.prepare_timestamp);
        buf.put_i64(self.rollback_timestamp);
        buf.put_i32(self.transaction_id);
        codec::put_cstring(buf, &self.gid);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let flags = buf.get_i8();
        let prepare_lsn = buf.get_i64();
        let rollback_lsn = buf.get_i64();
        let prepare_timestamp = buf.get_i64();
        let rollback_timestamp = buf.get_i64();
        let transaction_id = buf.get_i32();
        let gid = codec::get_cstring(buf).unwrap_or_default();
        Ok(RollbackPrepared::new(
            flags,
            prepare_lsn,
            rollback_lsn,
            prepare_timestamp,
            rollback_timestamp,
            transaction_id,
            gid,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct StreamPrepare {
    pub flags: i8,
    pub prepare_lsn: i64,
    pub end_lsn: i64,
    pub prepare_timestamp: i64,
    pub transaction_id: i32,
    pub gid: String,
}

impl StreamPrepare {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_STREAM_PREPARE);
        buf.put_i8(self.flags);
        buf.put_i64(self.prepare_lsn);
        buf.put_i64(self.end_lsn);
        buf.put_i64(self.prepare_timestamp);
        buf.put_i32(self.transaction_id);
        codec::put_cstring(buf, &self.gid);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let flags = buf.get_i8();
        let prepare_lsn = buf.get_i64();
        let end_lsn = buf.get_i64();
        let prepare_timestamp = buf.get_i64();
        let transaction_id = buf.get_i32();
        let gid = codec::get_cstring(buf).unwrap_or_default();
        Ok(StreamPrepare::new(
            flags,
            prepare_lsn,
            end_lsn,
            prepare_timestamp,
            transaction_id,
            gid,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct XLogData {
    pub start_lsn: i64,
    pub end_lsn: i64,
    pub timestamp: i64,
    pub data: Bytes,
}

impl XLogData {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_XLOG_DATA);
        buf.put_i64(self.start_lsn);
        buf.put_i64(self.end_lsn);
        buf.put_i64(self.timestamp);
        buf.put_slice(self.data.as_ref());
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let start_lsn = buf.get_i64();
        let end_lsn = buf.get_i64();
        let timestamp = buf.get_i64();
        let data = buf.split_to(buf.remaining()).freeze();
        Ok(XLogData::new(start_lsn, end_lsn, timestamp, data))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct PrimaryKeepalive {
    pub end_lsn: i64,
    pub timestamp: i64,
    pub reply_requested: bool,
}

impl PrimaryKeepalive {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_PRIMARY_KEEPALIVE);
        buf.put_i64(self.end_lsn);
        buf.put_i64(self.timestamp);
        buf.put_u8(if self.reply_requested { 1 } else { 0 });
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let end_lsn = buf.get_i64();
        let timestamp = buf.get_i64();
        let reply_requested = buf.get_u8() == 1;
        Ok(PrimaryKeepalive::new(end_lsn, timestamp, reply_requested))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct StandbyStatusUpdate {
    pub write_lsn: i64,
    pub flush_lsn: i64,
    pub apply_lsn: i64,
    pub timestamp: i64,
    pub reply_requested: bool,
}

impl StandbyStatusUpdate {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_STANDBY_STATUS_UPDATE);
        buf.put_i64(self.write_lsn);
        buf.put_i64(self.flush_lsn);
        buf.put_i64(self.apply_lsn);
        buf.put_i64(self.timestamp);
        buf.put_u8(if self.reply_requested { 1 } else { 0 });
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let write_lsn = buf.get_i64();
        let flush_lsn = buf.get_i64();
        let apply_lsn = buf.get_i64();
        let timestamp = buf.get_i64();
        let reply_requested = buf.get_u8() == 1;
        Ok(StandbyStatusUpdate::new(
            write_lsn,
            flush_lsn,
            apply_lsn,
            timestamp,
            reply_requested,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, new)]
pub struct HotStandbyFeedback {
    pub timestamp: i64,
    pub xmin: i32,
    pub xmin_epoch: i32,
    pub catalog_xmin: i32,
    pub catalog_xmin_epoch: i32,
}

impl HotStandbyFeedback {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        buf.put_u8(MSG_TYPE_HOT_STANDBY_FEEDBACK);
        buf.put_i64(self.timestamp);
        buf.put_i32(self.xmin);
        buf.put_i32(self.xmin_epoch);
        buf.put_i32(self.catalog_xmin);
        buf.put_i32(self.catalog_xmin_epoch);
        Ok(())
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Self> {
        let _msg_type = buf.get_u8();
        let timestamp = buf.get_i64();
        let xmin = buf.get_i32();
        let xmin_epoch = buf.get_i32();
        let catalog_xmin = buf.get_i32();
        let catalog_xmin_epoch = buf.get_i32();
        Ok(HotStandbyFeedback::new(
            timestamp,
            xmin,
            xmin_epoch,
            catalog_xmin,
            catalog_xmin_epoch,
        ))
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BackendStreamingReplicationMessage {
    XLogData(XLogData),
    PrimaryKeepalive(PrimaryKeepalive),
}

impl BackendStreamingReplicationMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Self::XLogData(msg) => msg.encode(buf),
            Self::PrimaryKeepalive(msg) => msg.encode(buf),
        }
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Option<Self>> {
        if buf.remaining() < 1 {
            return Ok(None);
        }

        let msg_type = buf[0];
        match msg_type {
            MSG_TYPE_XLOG_DATA => XLogData::decode(buf).map(Self::XLogData).map(Some),
            MSG_TYPE_PRIMARY_KEEPALIVE => PrimaryKeepalive::decode(buf)
                .map(Self::PrimaryKeepalive)
                .map(Some),
            _ => Err(PgWireError::InvalidMessageType(msg_type)),
        }
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FrontendStreamingReplicationMessage {
    StandbyStatusUpdate(StandbyStatusUpdate),
    HotStandbyFeedback(HotStandbyFeedback),
}

impl FrontendStreamingReplicationMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Self::StandbyStatusUpdate(msg) => msg.encode(buf),
            Self::HotStandbyFeedback(msg) => msg.encode(buf),
        }
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Option<Self>> {
        if buf.remaining() < 1 {
            return Ok(None);
        }

        let msg_type = buf[0];
        match msg_type {
            MSG_TYPE_STANDBY_STATUS_UPDATE => StandbyStatusUpdate::decode(buf)
                .map(Self::StandbyStatusUpdate)
                .map(Some),
            MSG_TYPE_HOT_STANDBY_FEEDBACK => HotStandbyFeedback::decode(buf)
                .map(Self::HotStandbyFeedback)
                .map(Some),
            _ => Err(PgWireError::InvalidMessageType(msg_type)),
        }
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BackendLogicalReplicationMessage {
    Begin(Begin),
    Commit(Commit),
    Origin(Origin),
    Relation(Relation),
    Type(TypeMessage),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Truncate(Truncate),
    LogicalMessage(LogicalMessage),
    StreamStart(StreamStart),
    StreamStop(StreamStop),
    StreamCommit(StreamCommit),
    StreamAbort(StreamAbort),
    BeginPrepare(BeginPrepare),
    Prepare(Prepare),
    CommitPrepared(CommitPrepared),
    RollbackPrepared(RollbackPrepared),
    StreamPrepare(StreamPrepare),
}

impl BackendLogicalReplicationMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Self::Begin(msg) => msg.encode(buf),
            Self::Commit(msg) => msg.encode(buf),
            Self::Origin(msg) => msg.encode(buf),
            Self::Relation(msg) => msg.encode(buf),
            Self::Type(msg) => msg.encode(buf),
            Self::Insert(msg) => msg.encode(buf),
            Self::Update(msg) => msg.encode(buf),
            Self::Delete(msg) => msg.encode(buf),
            Self::Truncate(msg) => msg.encode(buf),
            Self::LogicalMessage(msg) => msg.encode(buf),
            Self::StreamStart(msg) => msg.encode(buf),
            Self::StreamStop(msg) => msg.encode(buf),
            Self::StreamCommit(msg) => msg.encode(buf),
            Self::StreamAbort(msg) => msg.encode(buf),
            Self::BeginPrepare(msg) => msg.encode(buf),
            Self::Prepare(msg) => msg.encode(buf),
            Self::CommitPrepared(msg) => msg.encode(buf),
            Self::RollbackPrepared(msg) => msg.encode(buf),
            Self::StreamPrepare(msg) => msg.encode(buf),
        }
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Option<Self>> {
        if buf.remaining() < 1 {
            return Ok(None);
        }

        let msg_type = buf[0];
        match msg_type {
            MSG_TYPE_BEGIN => Begin::decode(buf).map(Self::Begin).map(Some),
            MSG_TYPE_COMMIT => Commit::decode(buf).map(Self::Commit).map(Some),
            MSG_TYPE_ORIGIN => Origin::decode(buf).map(Self::Origin).map(Some),
            MSG_TYPE_RELATION => Relation::decode(buf).map(Self::Relation).map(Some),
            MSG_TYPE_TYPE => TypeMessage::decode(buf).map(Self::Type).map(Some),
            MSG_TYPE_INSERT => Insert::decode(buf).map(Self::Insert).map(Some),
            MSG_TYPE_UPDATE => Update::decode(buf).map(Self::Update).map(Some),
            MSG_TYPE_DELETE => Delete::decode(buf).map(Self::Delete).map(Some),
            MSG_TYPE_TRUNCATE => Truncate::decode(buf).map(Self::Truncate).map(Some),
            MSG_TYPE_MESSAGE => LogicalMessage::decode(buf)
                .map(Self::LogicalMessage)
                .map(Some),
            MSG_TYPE_STREAM_START => StreamStart::decode(buf).map(Self::StreamStart).map(Some),
            MSG_TYPE_STREAM_STOP => StreamStop::decode(buf).map(Self::StreamStop).map(Some),
            MSG_TYPE_STREAM_COMMIT => StreamCommit::decode(buf).map(Self::StreamCommit).map(Some),
            MSG_TYPE_STREAM_ABORT => StreamAbort::decode(buf).map(Self::StreamAbort).map(Some),
            MSG_TYPE_BEGIN_PREPARE => BeginPrepare::decode(buf).map(Self::BeginPrepare).map(Some),
            MSG_TYPE_PREPARE => Prepare::decode(buf).map(Self::Prepare).map(Some),
            MSG_TYPE_COMMIT_PREPARED => CommitPrepared::decode(buf)
                .map(Self::CommitPrepared)
                .map(Some),
            MSG_TYPE_ROLLBACK_PREPARED => RollbackPrepared::decode(buf)
                .map(Self::RollbackPrepared)
                .map(Some),
            MSG_TYPE_STREAM_PREPARE => StreamPrepare::decode(buf)
                .map(Self::StreamPrepare)
                .map(Some),
            _ => Err(PgWireError::InvalidMessageType(msg_type)),
        }
    }
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FrontendLogicalReplicationMessage {
    StandbyStatusUpdate(StandbyStatusUpdate),
}

impl FrontendLogicalReplicationMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> PgWireResult<()> {
        match self {
            Self::StandbyStatusUpdate(msg) => msg.encode(buf),
        }
    }

    pub fn decode(buf: &mut BytesMut) -> PgWireResult<Option<Self>> {
        if buf.remaining() < 1 {
            return Ok(None);
        }

        let msg_type = buf[0];
        match msg_type {
            MSG_TYPE_STANDBY_STATUS_UPDATE => StandbyStatusUpdate::decode(buf)
                .map(Self::StandbyStatusUpdate)
                .map(Some),
            _ => Err(PgWireError::InvalidMessageType(msg_type)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BytesMut;

    macro_rules! backend_streaming_roundtrip {
        ($msg:expr, $variant:ident) => {
            let mut buf = BytesMut::new();
            $msg.encode(&mut buf).unwrap();
            assert!(buf.remaining() > 0);

            let decoded = BackendStreamingReplicationMessage::decode(&mut buf)
                .unwrap()
                .unwrap();
            assert_eq!(buf.remaining(), 0);

            match decoded {
                BackendStreamingReplicationMessage::$variant(m) => assert_eq!($msg, m),
                _ => panic!("wrong message type"),
            }
        };
    }

    macro_rules! frontend_streaming_roundtrip {
        ($msg:expr, $variant:ident) => {
            let mut buf = BytesMut::new();
            $msg.encode(&mut buf).unwrap();
            assert!(buf.remaining() > 0);

            let decoded = FrontendStreamingReplicationMessage::decode(&mut buf)
                .unwrap()
                .unwrap();
            assert_eq!(buf.remaining(), 0);

            match decoded {
                FrontendStreamingReplicationMessage::$variant(m) => assert_eq!($msg, m),
                _ => panic!("wrong message type"),
            }
        };
    }

    macro_rules! backend_logical_roundtrip {
        ($msg:expr, $variant:ident) => {
            let mut buf = BytesMut::new();
            $msg.encode(&mut buf).unwrap();
            assert!(buf.remaining() > 0);

            let decoded = BackendLogicalReplicationMessage::decode(&mut buf)
                .unwrap()
                .unwrap();
            assert_eq!(buf.remaining(), 0);

            match decoded {
                BackendLogicalReplicationMessage::$variant(m) => assert_eq!($msg, m),
                _ => panic!("wrong message type"),
            }
        };
    }

    macro_rules! frontend_logical_roundtrip {
        ($msg:expr, $variant:ident) => {
            let mut buf = BytesMut::new();
            $msg.encode(&mut buf).unwrap();
            assert!(buf.remaining() > 0);

            let decoded = FrontendLogicalReplicationMessage::decode(&mut buf)
                .unwrap()
                .unwrap();
            assert_eq!(buf.remaining(), 0);

            let FrontendLogicalReplicationMessage::$variant(m) = decoded;
            assert_eq!($msg, m);
        };
    }

    #[test]
    fn test_begin() {
        let msg = Begin::new(12345, 67890, 42);
        backend_logical_roundtrip!(msg, Begin);
    }

    #[test]
    fn test_commit() {
        let msg = Commit::new(0, 100, 200, 300);
        backend_logical_roundtrip!(msg, Commit);
    }

    #[test]
    fn test_origin() {
        let msg = Origin::new(555, "origin_server".to_owned());
        backend_logical_roundtrip!(msg, Origin);
    }

    #[test]
    fn test_relation() {
        let msg = Relation::new(
            16384,
            "public".to_owned(),
            "users".to_owned(),
            b'd' as i8,
            vec![
                RelationColumn::new(1, "id".to_owned(), 23, -1),
                RelationColumn::new(0, "name".to_owned(), 25, -1),
            ],
        );
        backend_logical_roundtrip!(msg, Relation);
    }

    #[test]
    fn test_type_message() {
        let msg = TypeMessage::new(12345, "public".to_owned(), "my_type".to_owned());
        backend_logical_roundtrip!(msg, Type);
    }

    #[test]
    fn test_insert() {
        let msg = Insert::new(
            16384,
            TupleData::new(vec![
                TupleDataColumn::Text(Bytes::from_static(b"42")),
                TupleDataColumn::Null,
                TupleDataColumn::Binary(Bytes::from_static(b"\x00\x01\x02\x03")),
            ]),
        );
        backend_logical_roundtrip!(msg, Insert);
    }

    #[test]
    fn test_update_with_old_key() {
        let msg = Update::new(
            16384,
            Some(UPDATE_OLD_TUPLE_KEY),
            Some(TupleData::new(vec![TupleDataColumn::Text(
                Bytes::from_static(b"1"),
            )])),
            TupleData::new(vec![
                TupleDataColumn::Text(Bytes::from_static(b"1")),
                TupleDataColumn::Text(Bytes::from_static(b"new_name")),
            ]),
        );
        backend_logical_roundtrip!(msg, Update);
    }

    #[test]
    fn test_update_without_old() {
        let msg = Update::new(
            16384,
            None,
            None,
            TupleData::new(vec![
                TupleDataColumn::Text(Bytes::from_static(b"1")),
                TupleDataColumn::Text(Bytes::from_static(b"updated")),
            ]),
        );
        backend_logical_roundtrip!(msg, Update);
    }

    #[test]
    fn test_delete() {
        let msg = Delete::new(
            16384,
            DELETE_OLD_TUPLE_KEY,
            TupleData::new(vec![TupleDataColumn::Text(Bytes::from_static(b"42"))]),
        );
        backend_logical_roundtrip!(msg, Delete);
    }

    #[test]
    fn test_truncate() {
        let msg = Truncate::new(2, TRUNCATE_OPTION_CASCADE, vec![16384, 16385]);
        backend_logical_roundtrip!(msg, Truncate);
    }

    #[test]
    fn test_logical_message() {
        let msg = LogicalMessage::new(
            1,
            999,
            "test_prefix".to_owned(),
            Bytes::from_static(b"hello world"),
        );
        backend_logical_roundtrip!(msg, LogicalMessage);
    }

    #[test]
    fn test_stream_start() {
        let msg = StreamStart::new(100, true);
        backend_logical_roundtrip!(msg, StreamStart);
    }

    #[test]
    fn test_stream_stop() {
        let msg = StreamStop::new();
        backend_logical_roundtrip!(msg, StreamStop);
    }

    #[test]
    fn test_stream_commit() {
        let msg = StreamCommit::new(100, 0, 500, 600, 700);
        backend_logical_roundtrip!(msg, StreamCommit);
    }

    #[test]
    fn test_stream_abort() {
        let msg = StreamAbort::new(100, 100);
        backend_logical_roundtrip!(msg, StreamAbort);
    }

    #[test]
    fn test_begin_prepare() {
        let msg = BeginPrepare::new(100, 200, 300, 42, "tx_gid".to_owned());
        backend_logical_roundtrip!(msg, BeginPrepare);
    }

    #[test]
    fn test_prepare() {
        let msg = Prepare::new(0, 100, 200, 300, 42, "tx_gid".to_owned());
        backend_logical_roundtrip!(msg, Prepare);
    }

    #[test]
    fn test_commit_prepared() {
        let msg = CommitPrepared::new(0, 100, 200, 300, 42, "tx_gid".to_owned());
        backend_logical_roundtrip!(msg, CommitPrepared);
    }

    #[test]
    fn test_rollback_prepared() {
        let msg = RollbackPrepared::new(0, 100, 200, 300, 400, 42, "tx_gid".to_owned());
        backend_logical_roundtrip!(msg, RollbackPrepared);
    }

    #[test]
    fn test_stream_prepare() {
        let msg = StreamPrepare::new(0, 100, 200, 300, 42, "tx_gid".to_owned());
        backend_logical_roundtrip!(msg, StreamPrepare);
    }

    #[test]
    fn test_xlog_data() {
        let msg = XLogData::new(
            0x0100000000,
            0x0100000100,
            1234567890,
            Bytes::from_static(b"walogdata"),
        );
        backend_streaming_roundtrip!(msg, XLogData);
    }

    #[test]
    fn test_primary_keepalive() {
        let msg = PrimaryKeepalive::new(0x0100000000, 1234567890, true);
        backend_streaming_roundtrip!(msg, PrimaryKeepalive);
    }

    #[test]
    fn test_standby_status_update_physical() {
        let msg = StandbyStatusUpdate::new(100, 200, 300, 1234567890, false);
        frontend_streaming_roundtrip!(msg, StandbyStatusUpdate);
    }

    #[test]
    fn test_standby_status_update_logical() {
        let msg = StandbyStatusUpdate::new(100, 200, 300, 1234567890, true);
        frontend_logical_roundtrip!(msg, StandbyStatusUpdate);
    }

    #[test]
    fn test_hot_standby_feedback() {
        let msg = HotStandbyFeedback::new(1234567890, 100, 1, 50, 1);
        frontend_streaming_roundtrip!(msg, HotStandbyFeedback);
    }

    #[test]
    fn test_tuple_data_unchanged_toast() {
        let msg = Insert::new(
            16384,
            TupleData::new(vec![
                TupleDataColumn::Text(Bytes::from_static(b"val")),
                TupleDataColumn::UnchangedToast,
            ]),
        );
        backend_logical_roundtrip!(msg, Insert);
    }

    #[test]
    fn test_tuple_data_binary() {
        let msg = Insert::new(
            16384,
            TupleData::new(vec![TupleDataColumn::Binary(Bytes::from_static(
                b"\xff\xfe",
            ))]),
        );
        backend_logical_roundtrip!(msg, Insert);
    }
}

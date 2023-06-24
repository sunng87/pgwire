use async_trait::async_trait;
use futures::Stream;

use crate::{error::PgWireResult, messages::copy::CopyData};

use super::ClientInfo;

/// handler for copy messages
#[async_trait]
pub trait CopyHandler {
    async fn on_copy_in<C, S>(&self, client: &mut C, copy_data_stream: S) -> PgWireResult<()>
    where
        C: ClientInfo,
        S: Stream<Item = CopyData>;

    async fn on_copy_out<C, S>(&self, client: &mut C) -> PgWireResult<S>
    where
        C: ClientInfo,
        S: Stream<Item = CopyData>;

    async fn on_copy_both<C, S1, S2>(
        &self,
        client: &mut C,
        copy_data_stream: S1,
    ) -> PgWireResult<S2>
    where
        C: ClientInfo,
        S1: Stream<Item = CopyData>,
        S2: Stream<Item = CopyData>;
}

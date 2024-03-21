use async_trait::async_trait;
use futures::Stream;

use crate::{error::PgWireResult, messages::copy::CopyData};

use super::ClientInfo;

/// handler for copy messages
#[async_trait]
pub trait CopyHandler {
    async fn on_copy_in<C, S>(&self, _client: &mut C, _copy_data_stream: S) -> PgWireResult<()>
    where
        C: ClientInfo,
        S: Stream<Item = CopyData> + Send,
    {
        Ok(())
    }

    async fn on_copy_out<C>(
        &self,
        _client: &mut C,
    ) -> PgWireResult<Box<dyn Stream<Item = CopyData> + Send>>
    where
        C: ClientInfo,
    {
        let stream: Vec<CopyData> = Vec::new();
        Ok(Box::new(futures::stream::iter(stream)))
    }

    async fn on_copy_both<C, S1>(
        &self,
        _client: &mut C,
        _copy_data_stream: S1,
    ) -> PgWireResult<Box<dyn Stream<Item = CopyData> + Send>>
    where
        C: ClientInfo,
        S1: Stream<Item = CopyData> + Send,
    {
        let stream: Vec<CopyData> = Vec::new();
        Ok(Box::new(futures::stream::iter(stream)))
    }
}

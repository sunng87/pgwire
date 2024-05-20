use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::Stream;
use std::fmt::Debug;

use crate::api::portal::Format;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::copy::{CopyBothResponse, CopyData, CopyInResponse, CopyOutResponse};
use crate::messages::PgWireBackendMessage;

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

pub async fn send_copy_in_response<C>(
    client: &mut C,
    overall_format: i8,
    columns: usize,
    column_formats: Vec<i16>,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let resp = CopyInResponse::new(overall_format, columns as i16, column_formats);
    client
        .send(PgWireBackendMessage::CopyInResponse(resp))
        .await?;
    Ok(())
}

pub async fn send_copy_out_response<C>(
    client: &mut C,
    overall_format: i8,
    columns: usize,
    column_formats: Vec<i16>,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let resp = CopyOutResponse::new(overall_format, columns as i16, column_formats);
    client
        .send(PgWireBackendMessage::CopyOutResponse(resp))
        .await?;
    Ok(())
}

pub async fn send_copy_both_response<C>(
    client: &mut C,
    overall_format: i8,
    columns: usize,
    column_formats: Vec<i16>,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let resp = CopyBothResponse::new(overall_format, columns as i16, column_formats);
    client
        .send(PgWireBackendMessage::CopyBothResponse(resp))
        .await?;
    Ok(())
}

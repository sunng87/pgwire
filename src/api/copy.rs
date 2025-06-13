use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use std::fmt::Debug;

use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::copy::{
    CopyBothResponse, CopyData, CopyDone, CopyFail, CopyInResponse, CopyOutResponse,
};
use crate::messages::PgWireBackendMessage;

use super::results::CopyResponse;
use super::ClientInfo;

/// handler for copy messages
#[async_trait]
pub trait CopyHandler: Send + Sync {
    async fn on_copy_data<C>(&self, _client: &mut C, _copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    async fn on_copy_done<C>(&self, _client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    async fn on_copy_fail<C>(&self, _client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "XX000".to_owned(),
            format!("COPY IN mode terminated by the user: {}", fail.message),
        )))
    }
}

pub async fn send_copy_in_response<C>(client: &mut C, resp: CopyResponse) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let resp = CopyInResponse::new(resp.format, resp.columns as i16, resp.column_formats);
    client
        .send(PgWireBackendMessage::CopyInResponse(resp))
        .await?;
    Ok(())
}

pub async fn send_copy_out_response<C>(client: &mut C, resp: CopyResponse) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let resp = CopyOutResponse::new(resp.format, resp.columns as i16, resp.column_formats);
    client
        .send(PgWireBackendMessage::CopyOutResponse(resp))
        .await?;
    Ok(())
}

pub async fn send_copy_both_response<C>(client: &mut C, resp: CopyResponse) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let resp = CopyBothResponse::new(resp.format, resp.columns as i16, resp.column_formats);
    client
        .send(PgWireBackendMessage::CopyBothResponse(resp))
        .await?;
    Ok(())
}

#[async_trait]
impl CopyHandler for super::NoopHandler {
    async fn on_copy_data<C>(&self, _client: &mut C, _copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }

    async fn on_copy_done<C>(&self, _client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }
}

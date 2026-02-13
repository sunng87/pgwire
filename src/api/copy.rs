use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream::StreamExt;
use std::fmt::Debug;

use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::PgWireBackendMessage;
use crate::messages::copy::{
    CopyBothResponse, CopyData, CopyDone, CopyFail, CopyInResponse, CopyOutResponse,
};

use super::ClientInfo;
use super::results::{CopyResponse, Tag};

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
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let column_formats = resp.column_formats();
    let resp = CopyInResponse::new(resp.format, resp.columns as i16, column_formats);
    client
        .send(PgWireBackendMessage::CopyInResponse(resp))
        .await?;
    Ok(())
}

pub async fn send_copy_out_response<C>(client: &mut C, resp: CopyResponse) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let column_formats = resp.column_formats();
    let CopyResponse {
        format,
        columns,
        mut data_stream,
    } = resp;
    let copy_resp = CopyOutResponse::new(format, columns as i16, column_formats);
    client
        .send(PgWireBackendMessage::CopyOutResponse(copy_resp))
        .await?;

    let mut rows = 0;

    while let Some(copy_data) = data_stream.next().await {
        match copy_data {
            Ok(data) => {
                rows += 1;
                client.feed(PgWireBackendMessage::CopyData(data)).await?;
            }
            Err(e) => {
                let copy_fail = CopyFail::new(format!("{}", e));
                client
                    .send(PgWireBackendMessage::CopyFail(copy_fail))
                    .await?;
                return Err(e);
            }
        }
    }

    let copy_done = CopyDone::new();
    client
        .send(PgWireBackendMessage::CopyDone(copy_done))
        .await?;

    let tag = Tag::new("COPY").with_rows(rows);
    client
        .send(PgWireBackendMessage::CommandComplete(tag.into()))
        .await?;

    Ok(())
}

pub async fn send_copy_both_response<C>(client: &mut C, resp: CopyResponse) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let column_formats = resp.column_formats();
    let CopyResponse {
        format,
        columns,
        mut data_stream,
    } = resp;
    let copy_resp = CopyBothResponse::new(format, columns as i16, column_formats);
    client
        .send(PgWireBackendMessage::CopyBothResponse(copy_resp))
        .await?;

    let mut rows = 0;

    while let Some(copy_data) = data_stream.next().await {
        match copy_data {
            Ok(data) => {
                rows += 1;
                client.feed(PgWireBackendMessage::CopyData(data)).await?;
            }
            Err(e) => {
                let copy_fail = CopyFail::new(format!("{}", e));
                client
                    .send(PgWireBackendMessage::CopyFail(copy_fail))
                    .await?;
                return Err(e);
            }
        }
    }

    let copy_done = CopyDone::new();
    client
        .send(PgWireBackendMessage::CopyDone(copy_done))
        .await?;

    let tag = Tag::new("COPY").with_rows(rows);
    client
        .send(PgWireBackendMessage::CommandComplete(tag.into()))
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
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".to_owned(),
            "08P01".to_owned(),
            "This feature is not implemented.".to_string(),
        ))))
    }

    async fn on_copy_done<C>(&self, _client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".to_owned(),
            "08P01".to_owned(),
            "This feature is not implemented.".to_string(),
        ))))
    }
}

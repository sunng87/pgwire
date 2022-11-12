use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream::StreamExt;

use super::portal::Portal;
use super::results::{into_row_description, Tag};
use super::stmt::Statement;
use super::{ClientInfo, DEFAULT_NAME};
use crate::api::results::{QueryResponse, Response};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::extendedquery::{
    Bind, Close, Describe, Execute, Parse, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL,
    TARGET_TYPE_BYTE_STATEMENT,
};
use crate::messages::response::{EmptyQueryResponse, ReadyForQuery, READY_STATUS_IDLE};
use crate::messages::simplequery::Query;
use crate::messages::PgWireBackendMessage;

/// handler for processing simple query.
#[async_trait]
pub trait SimpleQueryHandler: Send + Sync {
    /// Executed on `Query` request arrived. This is how postgres respond to
    /// simple query. The default implementation calls `do_query` with the
    /// incoming query string.
    async fn on_query<C>(&self, client: &mut C, query: &Query) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.set_state(super::PgWireConnectionState::QueryInProgress);
        let query_string = query.query();
        if query_string.is_empty() {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse))
                .await?;
        } else {
            let resp = self.do_query(client, query.query()).await?;
            for r in resp {
                match r {
                    Response::Query(results) => {
                        send_query_response(client, results, true).await?;
                    }
                    Response::Execution(tag) => {
                        send_execution_response(client, tag).await?;
                    }
                    Response::Error(e) => {
                        client
                            .feed(PgWireBackendMessage::ErrorResponse((*e).into()))
                            .await?;
                    }
                }
            }
        }

        client
            .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                READY_STATUS_IDLE,
            )))
            .await?;
        client.flush().await?;
        client.set_state(super::PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    /// Provide your query implementation using the incoming query string.
    async fn do_query<C>(&self, client: &C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync;
}

#[async_trait]
pub trait ExtendedQueryHandler: Send + Sync {
    async fn on_parse<C>(&self, client: &mut C, message: &Parse) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let stmt = Statement::from(message);
        let id = stmt.id().clone();
        client.stmt_store_mut().put(&id, Arc::new(stmt));

        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: &Bind) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let portal = Portal::try_new(message, client)?;
        let id = portal.name().clone();
        client.portal_store_mut().put(&id, Arc::new(portal));

        Ok(())
    }

    async fn on_execute<C>(&self, client: &mut C, message: &Execute) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let portal_name = message.name().as_ref().map_or(DEFAULT_NAME, String::as_str);
        let store = client.portal_store();
        if let Some(portal) = store.get(portal_name) {
            match self
                .do_query(client, portal.as_ref(), *message.max_rows() as usize)
                .await?
            {
                Response::Query(results) => {
                    send_query_response(client, results, portal.row_description_requested())
                        .await?;
                }
                Response::Execution(tag) => {
                    send_execution_response(client, tag).await?;
                }
                Response::Error(err) => {
                    client
                        .send(PgWireBackendMessage::ErrorResponse((*err).into()))
                        .await?;
                }
            }
            client
                .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    READY_STATUS_IDLE,
                )))
                .await?;

            Ok(())
        } else {
            Err(PgWireError::PortalNotFound(portal_name.to_owned()))
        }
        // TODO: clear/remove portal?
    }

    async fn on_describe<C>(&self, client: &mut C, message: &Describe) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let portal_name = message.name().as_ref().map_or(DEFAULT_NAME, String::as_str);
        if let Some(mut portal) = client.portal_store().get(portal_name) {
            // TODO: check if make_mut works for this
            Arc::make_mut(&mut portal).set_row_description_requested(true);
            client.portal_store_mut().put(portal_name, portal);
            Ok(())
        } else {
            Err(PgWireError::PortalNotFound(portal_name.to_owned()))
        }
    }

    async fn on_sync<C>(&self, client: &mut C, _message: &PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.flush().await?;
        Ok(())
    }

    async fn on_close<C>(&self, client: &mut C, message: &Close) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name().as_ref().map_or(DEFAULT_NAME, String::as_str);
        match message.target_type() {
            TARGET_TYPE_BYTE_STATEMENT => {
                client.stmt_store_mut().del(name);
            }
            TARGET_TYPE_BYTE_PORTAL => {
                client.portal_store_mut().del(name);
            }
            _ => {}
        }
        Ok(())
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync;
}

async fn send_query_response<C>(
    client: &mut C,
    results: QueryResponse,
    row_desc_required: bool,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let QueryResponse {
        row_schema,
        mut data_rows,
    } = results;

    if row_desc_required {
        let row_desc = into_row_description(row_schema);
        client
            .send(PgWireBackendMessage::RowDescription(row_desc))
            .await?;
    }

    let mut rows = 0;
    while let Some(row) = data_rows.next().await {
        let row = row?;
        rows += 1;
        client.send(PgWireBackendMessage::DataRow(row)).await?;
    }

    let tag = Tag::new_for_query(rows);
    client
        .send(PgWireBackendMessage::CommandComplete(tag.into()))
        .await?;

    Ok(())
}

async fn send_execution_response<C>(client: &mut C, tag: Tag) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    client
        .send(PgWireBackendMessage::CommandComplete(tag.into()))
        .await?;

    Ok(())
}

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream::StreamExt;

use super::portal::Portal;
use super::results::{into_row_description, Tag};
use super::stmt::{NoopQueryParser, QueryParser, StoredStatement};
use super::store::{MemPortalStore, PortalStore};
use super::{ClientInfo, DEFAULT_NAME};
use crate::api::results::{DescribeResponse, QueryResponse, Response};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::data::ParameterDescription;
use crate::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Parse, ParseComplete,
    Sync as PgSync, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
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
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
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
                        send_query_response(client, results).await?;
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
    type Statement: Clone + Send + Sync;
    type QueryParser: QueryParser<Statement = Self::Statement>;
    type PortalStore: PortalStore<Statement = Self::Statement>;

    /// Get a reference to associated `PortalStore` implementation
    fn portal_store(&self) -> Arc<Self::PortalStore>;

    /// Get a reference to associated `QueryParser` implementation
    fn query_parser(&self) -> Arc<Self::QueryParser>;

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let stmt = StoredStatement::parse(&message, self.query_parser().as_ref())?;
        self.portal_store().put_statement(Arc::new(stmt));
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;

        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement_name = message.statement_name().as_deref().unwrap_or(DEFAULT_NAME);

        if let Some(statement) = self.portal_store().get_statement(statement_name) {
            let portal = Portal::try_new(&message, statement)?;
            self.portal_store().put_portal(Arc::new(portal));
            client
                .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
                .await?;
            Ok(())
        } else {
            Err(PgWireError::StatementNotFound(statement_name.to_owned()))
        }
    }

    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let portal_name = message.name().as_deref().unwrap_or(DEFAULT_NAME);
        if let Some(portal) = self.portal_store().get_portal(portal_name) {
            match self
                .do_query(client, portal.as_ref(), *message.max_rows() as usize)
                .await?
            {
                Response::Query(results) => {
                    send_query_response(client, results).await?;
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

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name().as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type() {
            TARGET_TYPE_BYTE_STATEMENT => {
                if let Some(stmt) = self.portal_store().get_statement(name) {
                    let describe_response = self.do_describe(client, stmt.as_ref(), true).await?;
                    if let Some(parameter_types) = describe_response.parameters() {
                        // parameter type inference
                        client
                            .send(PgWireBackendMessage::ParameterDescription(
                                ParameterDescription::new(
                                    parameter_types.iter().map(|t| t.oid()).collect(),
                                ),
                            ))
                            .await?;
                    }
                    let row_desc = into_row_description(describe_response.take_fields());
                    client
                        .send(PgWireBackendMessage::RowDescription(row_desc))
                        .await?;
                    client
                        .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                            READY_STATUS_IDLE,
                        )))
                        .await?;
                } else {
                    return Err(PgWireError::StatementNotFound(name.to_owned()));
                }
            }
            TARGET_TYPE_BYTE_PORTAL => {
                if let Some(portal) = self.portal_store().get_portal(name) {
                    let describe_response =
                        self.do_describe(client, portal.statement(), false).await?;
                    let row_schema = describe_response.take_fields();
                    let row_desc = into_row_description(row_schema);
                    client
                        .send(PgWireBackendMessage::RowDescription(row_desc))
                        .await?;
                } else {
                    return Err(PgWireError::PortalNotFound(name.to_owned()));
                }
            }
            _ => return Err(PgWireError::InvalidTargetType(message.target_type())),
        }

        Ok(())
    }

    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.flush().await?;
        Ok(())
    }

    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name().as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type() {
            TARGET_TYPE_BYTE_STATEMENT => {
                self.portal_store().rm_statement(name);
            }
            TARGET_TYPE_BYTE_PORTAL => {
                self.portal_store().rm_portal(name);
            }
            _ => {}
        }
        client
            .send(PgWireBackendMessage::CloseComplete(CloseComplete))
            .await?;
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                READY_STATUS_IDLE,
            )))
            .await?;
        Ok(())
    }

    /// Return resultset metadata without actually execute statement or portal
    async fn do_describe<C>(
        &self,
        client: &mut C,
        statement: &StoredStatement<Self::Statement>,
        inference_parameters: bool,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync;

    /// This is the main implementation for query execution. Context has
    /// been provided:
    ///
    /// - `client`: Information of the client sending the query
    /// - `portal`: Statement and parameters for the query
    /// - `max_rows`: Max requested rows of the query
    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync;
}

async fn send_query_response<C>(client: &mut C, results: QueryResponse) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let QueryResponse {
        row_schema,
        mut data_rows,
    } = results;

    // Simple query has row_schema in query response. For extended query,
    // row_schema is returned as response of `Describe`.
    if let Some(row_schema) = row_schema {
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

/// A placeholder extended query handler. It panics when extended query messages
/// received. This handler is for demo only, never use it in serious
/// application.
#[derive(Debug, Clone)]
pub struct PlaceholderExtendedQueryHandler;

#[async_trait]
impl ExtendedQueryHandler for PlaceholderExtendedQueryHandler {
    type Statement = String;
    type PortalStore = MemPortalStore<Self::Statement>;
    type QueryParser = NoopQueryParser;

    fn portal_store(&self) -> Arc<Self::PortalStore> {
        unimplemented!("Extended Query is not implemented on this server.")
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        unimplemented!("Extended Query is not implemented on this server.")
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        unimplemented!("Extended Query is not implemented on this server.")
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        _statement: &StoredStatement<Self::Statement>,
        _inference_parameters: bool,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        unimplemented!("Extended Query is not implemented on this server.")
    }
}

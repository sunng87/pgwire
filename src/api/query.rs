use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream::StreamExt;

use super::portal::Portal;
use super::results::{into_row_description, Tag};
use super::stmt::{NoopQueryParser, QueryParser, StoredStatement};
use super::store::{EmptyState, MemPortalStore, PortalStore};
use super::{ClientInfo, DEFAULT_NAME};
use crate::api::results::{DescribeResponse, QueryResponse, Response};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::data::{NoData, ParameterDescription};
use crate::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Parse, ParseComplete,
    Sync as PgSync, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use crate::messages::response::{EmptyQueryResponse, ReadyForQuery, READY_STATUS_IDLE};
use crate::messages::simplequery::Query;
use crate::messages::PgWireBackendMessage;

fn is_empty_query(q: &str) -> bool {
    let trimmed_query = q.trim();
    trimmed_query == ";" || trimmed_query.is_empty()
}

/// handler for processing simple query.
#[async_trait]
pub trait SimpleQueryHandler: Send + Sync {
    /// Executed on `Query` request arrived. This is how postgres respond to
    /// simple query. The default implementation calls `do_query` with the
    /// incoming query string.
    ///
    /// This handle checks empty query by default, if the query string is empty
    /// or `;`, it returns `EmptyQueryResponse` and does not call `self.do_query`.
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.set_state(super::PgWireConnectionState::QueryInProgress);
        let query_string = query.query();
        if is_empty_query(query_string) {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse))
                .await?;
        } else {
            let resp = self.do_query(client, query.query()).await?;
            for r in resp {
                match r {
                    Response::EmptyQuery => {
                        client
                            .feed(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse))
                            .await?;
                    }
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
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;
}

#[async_trait]
pub trait ExtendedQueryHandler: Send + Sync {
    type Statement: Clone + Send + Sync;
    type PortalState: Default + Clone + Send + Sync;
    type QueryParser: QueryParser<Statement = Self::Statement> + Send + Sync;
    type PortalStore: PortalStore<Statement = Self::Statement, State = Self::PortalState>;

    /// Get a reference to associated `PortalStore` implementation
    fn portal_store<C>(&self, client: &C) -> Arc<Self::PortalStore>
    where
        C: ClientInfo;

    /// Get a reference to associated `QueryParser` implementation
    fn query_parser(&self) -> Arc<Self::QueryParser>;

    /// Called when client sends `parse` command.
    ///
    /// The default implementation parsed query with `Self::QueryParser` and
    /// stores it in `Self::PortalStore`.
    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let parser = self.query_parser();
        let stmt = StoredStatement::parse(&message, parser).await?;
        self.portal_store(client).put_statement(Arc::new(stmt));
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;

        Ok(())
    }

    /// Called when client sends `bind` command.
    ///
    /// The default implementation associate parameters with previous parsed
    /// statement and stores in `Self::PortalStore` as well.
    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement_name = message.statement_name().as_deref().unwrap_or(DEFAULT_NAME);

        let portal_store = self.portal_store(client);
        if let Some(statement) = portal_store.get_statement(statement_name) {
            let portal = Portal::try_new(&message, statement, Default::default())?;
            portal_store.put_portal(Arc::new(portal));
            client
                .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
                .await?;
            Ok(())
        } else {
            Err(PgWireError::StatementNotFound(statement_name.to_owned()))
        }
    }

    /// Called when client sends `execute` command.
    ///
    /// The default implementation delegates the query to `self::do_query` and
    /// sends response messages according to `Response` from `self::do_query`.
    ///
    /// Note that, different from `SimpleQueryHandler`, this implementation
    /// won't check empty query because it cannot understand parsed
    /// `Self::Statement`.
    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let portal_name = message.name().as_deref().unwrap_or(DEFAULT_NAME);
        if let Some(portal) = self.portal_store(client).get_portal(portal_name) {
            match self
                .do_query(client, portal.as_ref(), *message.max_rows() as usize)
                .await?
            {
                Response::EmptyQuery => {
                    client
                        .feed(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse))
                        .await?;
                }
                Response::Query(results) => {
                    send_query_response(client, results, false).await?;
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

            Ok(())
        } else {
            Err(PgWireError::PortalNotFound(portal_name.to_owned()))
        }
    }

    /// Called when client sends `describe` command.
    ///
    /// The default implementation delegates the call to `self::do_describe`.
    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name().as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type() {
            TARGET_TYPE_BYTE_STATEMENT => {
                if let Some(stmt) = self.portal_store(client).get_statement(name) {
                    let describe_response = self
                        .do_describe(client, StatementOrPortal::Statement(&stmt))
                        .await?;
                    send_describe_response(client, &describe_response, true).await?;
                } else {
                    return Err(PgWireError::StatementNotFound(name.to_owned()));
                }
            }
            TARGET_TYPE_BYTE_PORTAL => {
                if let Some(portal) = self.portal_store(client).get_portal(name) {
                    let describe_response = self
                        .do_describe(client, StatementOrPortal::Portal(&portal))
                        .await?;
                    send_describe_response(client, &describe_response, false).await?;
                } else {
                    return Err(PgWireError::PortalNotFound(name.to_owned()));
                }
            }
            _ => return Err(PgWireError::InvalidTargetType(message.target_type())),
        }

        Ok(())
    }

    /// Called when client sends `sync` command.
    ///
    /// The default implementation flushes client buffer and sends
    /// `READY_FOR_QUERY` response to client
    async fn on_sync<C>(&self, client: &mut C, _message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                READY_STATUS_IDLE,
            )))
            .await?;
        client.flush().await?;
        Ok(())
    }

    /// Called when client sends `close` command.
    ///
    /// The default implementation closes certain statement or portal.
    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name().as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type() {
            TARGET_TYPE_BYTE_STATEMENT => {
                self.portal_store(client).rm_statement(name);
            }
            TARGET_TYPE_BYTE_PORTAL => {
                self.portal_store(client).rm_portal(name);
            }
            _ => {}
        }
        client
            .send(PgWireBackendMessage::CloseComplete(CloseComplete))
            .await?;
        Ok(())
    }

    /// Return resultset metadata without actually executing statement or portal
    async fn do_describe<C>(
        &self,
        client: &mut C,
        target: StatementOrPortal<'_, Self::Statement, Self::PortalState>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    /// This is the main implementation for query execution. Context has
    /// been provided:
    ///
    /// - `client`: Information of the client sending the query
    /// - `portal`: Statement and parameters for the query
    /// - `max_rows`: Max requested rows of the query
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        client: &mut C,
        portal: &'a Portal<Self::Statement, Self::PortalState>,
        max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;
}

/// Helper function to send `QueryResponse` and optional `RowDescription` to client
///
/// For most cases in extended query implementation, `send_describe` is set to
/// false because not all `Execute` comes with `Describe`. The client may have
/// decribed statement/portal before.
pub async fn send_query_response<'a, C>(
    client: &mut C,
    results: QueryResponse<'a>,
    send_describe: bool,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let row_schema = results.row_schema();
    let mut data_rows = results.data_rows();

    // Simple query has row_schema in query response. For extended query,
    // row_schema is returned as response of `Describe`.
    if send_describe {
        let row_desc = into_row_description(&row_schema);
        client
            .send(PgWireBackendMessage::RowDescription(row_desc))
            .await?;
    }

    let mut rows = 0;
    while let Some(row) = data_rows.next().await {
        let row = row?;
        rows += 1;
        client.feed(PgWireBackendMessage::DataRow(row)).await?;
    }

    let tag = Tag::new_for_query(rows);
    client
        .send(PgWireBackendMessage::CommandComplete(tag.into()))
        .await?;

    Ok(())
}

/// Helper function to send response for DMLs.
pub async fn send_execution_response<C>(client: &mut C, tag: Tag) -> PgWireResult<()>
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

/// Helper function to send response for `Describe`.
pub async fn send_describe_response<C>(
    client: &mut C,
    describe_response: &DescribeResponse,
    include_parameters: bool,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    if describe_response.is_no_data() {
        client.send(PgWireBackendMessage::NoData(NoData)).await?;
    } else {
        if include_parameters {
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
        }

        let row_desc = into_row_description(describe_response.fields());
        client
            .send(PgWireBackendMessage::RowDescription(row_desc))
            .await?;
    }

    Ok(())
}

/// An enum holds borrowed statement or portal
#[derive(Debug)]
pub enum StatementOrPortal<'a, S, State: Default + Clone + Send> {
    Statement(&'a StoredStatement<S>),
    Portal(&'a Portal<S, State>),
}

/// A placeholder extended query handler. It panics when extended query messages
/// received. This handler is for demo only, never use it in serious
/// application.
#[derive(Debug, Clone)]
pub struct PlaceholderExtendedQueryHandler;

#[async_trait]
impl ExtendedQueryHandler for PlaceholderExtendedQueryHandler {
    type Statement = String;
    type PortalState = EmptyState;
    type PortalStore = MemPortalStore<Self::Statement>;
    type QueryParser = NoopQueryParser;

    fn portal_store<C>(&self, _client: &C) -> Arc<Self::PortalStore> {
        unimplemented!("Extended Query is not implemented on this server.")
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        unimplemented!("Extended Query is not implemented on this server.")
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        _portal: &'a Portal<Self::Statement, Self::PortalState>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        unimplemented!("Extended Query is not implemented on this server.")
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        _statement: StatementOrPortal<'_, Self::Statement, Self::PortalState>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        unimplemented!("Extended Query is not implemented on this server.")
    }
}

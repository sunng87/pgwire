use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream::StreamExt;

use super::portal::Portal;
use super::results::{into_row_description, Tag};
use super::stmt::{NoopQueryParser, QueryParser, StoredStatement};
use super::store::PortalStore;
use super::{copy, ClientInfo, ClientPortalStore, DEFAULT_NAME};
use crate::api::results::{
    DescribePortalResponse, DescribeResponse, DescribeStatementResponse, QueryResponse, Response,
};
use crate::api::PgWireConnectionState;
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::data::{NoData, ParameterDescription};
use crate::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Flush, Parse, ParseComplete,
    Sync as PgSync, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use crate::messages::response::{
    EmptyQueryResponse, NoticeResponse, ReadyForQuery, TransactionStatus,
};
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
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Make sure client is ready for query
        // We will still let query to execute when running in transaction error
        // state because we have no knowledge about whether to query is to
        // terminate the transaction. But developer who implementing transaction
        // should respect the transaction state.
        if !matches!(client.state(), super::PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        let mut transaction_status = client.transaction_status();

        client.set_state(super::PgWireConnectionState::QueryInProgress);
        let query_string = query.query;

        if is_empty_query(&query_string) {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse))
                .await?;
        } else {
            let resp = self.do_query(client, &query_string).await?;
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
                    Response::TransactionStart(tag) => {
                        send_execution_response(client, tag).await?;
                        transaction_status = transaction_status.to_in_transaction_state();
                    }
                    Response::TransactionEnd(tag) => {
                        send_execution_response(client, tag).await?;
                        transaction_status = transaction_status.to_idle_state();
                    }
                    Response::Error(e) => {
                        client
                            .feed(PgWireBackendMessage::ErrorResponse((*e).into()))
                            .await?;
                        transaction_status = transaction_status.to_error_state();
                    }
                    Response::CopyIn(result) => {
                        copy::send_copy_in_response(client, result).await?;
                        client.set_state(PgWireConnectionState::CopyInProgress(false));
                    }
                    Response::CopyOut(result) => {
                        copy::send_copy_out_response(client, result).await?;
                        client.set_state(PgWireConnectionState::CopyInProgress(false));
                    }
                    Response::CopyBoth(result) => {
                        copy::send_copy_both_response(client, result).await?;
                        client.set_state(PgWireConnectionState::CopyInProgress(false));
                    }
                }
            }
        }

        if !matches!(client.state(), PgWireConnectionState::CopyInProgress(_)) {
            // If the client state to `CopyInProgress` it means that a COPY FROM
            // STDIN / TO STDOUT is now in progress. In this case, we don't want
            // to send a `ReadyForQuery` message or reset the connection state
            // back to `ReadyForQuery`. This is the responsibility of of the
            // `on_copy_done` / `on_copy_fail`.
            client.set_state(super::PgWireConnectionState::ReadyForQuery);
            client.set_transaction_status(transaction_status);
            send_ready_for_query(client, transaction_status).await?;
        };

        Ok(())
    }

    /// Provide your query implementation using the incoming query string.
    async fn do_query<'a, C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;
}

#[async_trait]
pub trait ExtendedQueryHandler: Send + Sync {
    type Statement: Clone + Send + Sync;
    type QueryParser: QueryParser<Statement = Self::Statement> + Send + Sync;

    /// Get a reference to associated `QueryParser` implementation
    fn query_parser(&self) -> Arc<Self::QueryParser>;

    /// Called when client sends `parse` command.
    ///
    /// The default implementation parsed query with `Self::QueryParser` and
    /// stores it in `Self::PortalStore`.
    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let parser = self.query_parser();
        let stmt = StoredStatement::parse(client, &message, parser).await?;
        client.portal_store().put_statement(Arc::new(stmt));
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
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement_name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);

        if let Some(statement) = client.portal_store().get_statement(statement_name) {
            let portal = Portal::try_new(&message, statement)?;
            client.portal_store().put_portal(Arc::new(portal));
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
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // make sure client is ready for query
        if !matches!(client.state(), super::PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        let mut transaction_status = client.transaction_status();

        client.set_state(super::PgWireConnectionState::QueryInProgress);

        let portal_name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        if let Some(portal) = client.portal_store().get_portal(portal_name) {
            match self
                .do_query(client, portal.as_ref(), message.max_rows as usize)
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
                Response::TransactionStart(tag) => {
                    send_execution_response(client, tag).await?;
                    transaction_status = transaction_status.to_in_transaction_state();
                }
                Response::TransactionEnd(tag) => {
                    send_execution_response(client, tag).await?;
                    transaction_status = transaction_status.to_idle_state();
                }

                Response::Error(err) => {
                    client
                        .send(PgWireBackendMessage::ErrorResponse((*err).into()))
                        .await?;
                    transaction_status = transaction_status.to_error_state();
                }
                Response::CopyIn(result) => {
                    client.set_state(PgWireConnectionState::CopyInProgress(true));
                    copy::send_copy_in_response(client, result).await?;
                }
                Response::CopyOut(result) => {
                    client.set_state(PgWireConnectionState::CopyInProgress(true));
                    copy::send_copy_out_response(client, result).await?;
                }
                Response::CopyBoth(result) => {
                    client.set_state(PgWireConnectionState::CopyInProgress(true));
                    copy::send_copy_both_response(client, result).await?;
                }
            }

            if !matches!(client.state(), PgWireConnectionState::CopyInProgress(_)) {
                client.set_state(super::PgWireConnectionState::ReadyForQuery);
                client.set_transaction_status(transaction_status);
            };

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
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                if let Some(stmt) = client.portal_store().get_statement(name) {
                    let describe_response = self.do_describe_statement(client, &stmt).await?;
                    send_describe_response(client, &describe_response).await?;
                } else {
                    return Err(PgWireError::StatementNotFound(name.to_owned()));
                }
            }
            TARGET_TYPE_BYTE_PORTAL => {
                if let Some(portal) = client.portal_store().get_portal(name) {
                    let describe_response = self.do_describe_portal(client, &portal).await?;
                    send_describe_response(client, &describe_response).await?;
                } else {
                    return Err(PgWireError::PortalNotFound(name.to_owned()));
                }
            }
            _ => return Err(PgWireError::InvalidTargetType(message.target_type)),
        }

        Ok(())
    }

    /// Called when client sends `flush` command.
    ///
    /// The default implementation flushes client buffer
    async fn on_flush<C>(&self, client: &mut C, _message: Flush) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.flush().await?;
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
                client.transaction_status(),
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
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => {
                client.portal_store().rm_statement(name);
            }
            TARGET_TYPE_BYTE_PORTAL => {
                client.portal_store().rm_portal(name);
            }
            _ => {}
        }
        client
            .send(PgWireBackendMessage::CloseComplete(CloseComplete))
            .await?;
        Ok(())
    }

    /// Return resultset metadata without actually executing statement
    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    /// Return resultset metadata without actually executing portal
    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    /// This is the main implementation for query execution. Context has
    /// been provided:
    ///
    /// - `client`: Information of the client sending the query
    /// - `portal`: Statement and parameters for the query
    /// - `max_rows`: Max requested rows of the query
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;
}

/// Helper function to send `QueryResponse` and optional `RowDescription` to client
///
/// For most cases in extended query implementation, `send_describe` is set to
/// false because not all `Execute` comes with `Describe`. The client may have
/// decribed statement/portal before.
pub async fn send_query_response<C>(
    client: &mut C,
    results: QueryResponse<'_>,
    send_describe: bool,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let command_tag = results.command_tag().to_owned();
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

    let tag = Tag::new(&command_tag).with_rows(rows);
    client
        .send(PgWireBackendMessage::CommandComplete(tag.into()))
        .await?;

    Ok(())
}

/// Helper function to send a ReadyForQuery response.
pub async fn send_ready_for_query<C>(
    client: &mut C,
    transaction_status: TransactionStatus,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let message = ReadyForQuery::new(transaction_status);
    client
        .send(PgWireBackendMessage::ReadyForQuery(message))
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
pub async fn send_describe_response<C, DR>(
    client: &mut C,
    describe_response: &DR,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    DR: DescribeResponse,
{
    if let Some(parameter_types) = describe_response.parameters() {
        // parameter type inference
        client
            .send(PgWireBackendMessage::ParameterDescription(
                ParameterDescription::new(parameter_types.iter().map(|t| t.oid()).collect()),
            ))
            .await?;
    }
    if describe_response.is_no_data() {
        client.send(PgWireBackendMessage::NoData(NoData)).await?;
    } else {
        let row_desc = into_row_description(describe_response.fields());
        client
            .send(PgWireBackendMessage::RowDescription(row_desc))
            .await?;
    }

    Ok(())
}

#[async_trait]
impl ExtendedQueryHandler for super::NoopHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                ErrorInfo::new(
                    "NOTICE".to_owned(),
                    "01000".to_owned(),
                    "This is a demo handler for extended query.".to_string(),
                ),
            )))
            .await?;
        Ok(Response::Execution(Tag::new("OK")))
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribeStatementResponse::no_data())
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribePortalResponse::no_data())
    }
}

#[async_trait]
impl SimpleQueryHandler for super::NoopHandler {
    async fn do_query<'a, C>(&self, client: &mut C, _query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                ErrorInfo::new(
                    "NOTICE".to_owned(),
                    "01000".to_owned(),
                    "This is a demo handler for simple query.".to_string(),
                ),
            )))
            .await?;
        Ok(vec![Response::Execution(Tag::new("OK"))])
    }
}

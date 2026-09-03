use std::cmp::max;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use futures::channel::oneshot;
use futures::future::{Either, select};
use futures::sink::{Sink, SinkExt};
use futures::stream::StreamExt;

use super::portal::Portal;
use super::results::{Tag, into_row_description};
use super::stmt::{NoopQueryParser, QueryParser, StoredStatement};
use super::store::{PortalEntry, PortalStore, StatementEntry};
use super::{ClientInfo, ClientPortalStore, ConnectionHandle, DEFAULT_NAME, copy};
use crate::api::PgWireConnectionState;
use crate::api::Type;
use crate::api::portal::PortalExecutionState;
use crate::api::results::{
    DescribePortalResponse, DescribeResponse, DescribeStatementResponse, QueryResponse, Response,
};
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::PgWireBackendMessage;
use crate::messages::data::{NoData, ParameterDescription};
use crate::messages::extendedquery::{
    Bind, BindComplete, Close, CloseComplete, Describe, Execute, Flush, Parse, ParseComplete,
    PortalSuspended, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT,
};
use crate::messages::response::{EmptyQueryResponse, ReadyForQuery, TransactionStatus};
use crate::messages::simplequery::Query;

pub(crate) fn is_empty_query(q: &str) -> bool {
    // A query string that contains only semicolons and whitespace parses to no
    // statements, which PostgreSQL treats as an empty query and answers with
    // `EmptyQueryResponse` instead of dispatching to the executor. This covers
    // `""`, `" "`, `";"`, `";;"`, `";;;"`, `"; ;"`, etc. — matching the
    // behavior of PostgreSQL's simple query protocol, where consecutive or
    // stray semicolons do not constitute a real statement.
    q.chars().all(|c| c == ';' || c.is_whitespace())
}

async fn get_cancel_receiver<C>(client: &mut C) -> Option<oneshot::Receiver<()>>
where
    C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
{
    let handle = client.session_extensions().get::<Arc<ConnectionHandle>>()?;
    Some(handle.start_query().await)
}

/// handler for processing simple query.
#[async_trait]
pub trait SimpleQueryHandler: Send + Sync {
    /// Executed on `Query` request arrived. This is how postgres respond to
    /// simple query. The default implementation calls `do_query` with the
    /// incoming query string.
    ///
    /// This handle checks empty query by default, if the query string is empty
    /// or contains only semicolons and whitespace (e.g. `;`, `;;`, `;;;`), it
    /// returns `EmptyQueryResponse` and does not call `self.do_query`.
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self._on_query(client, query).await
    }

    /// This is the default implementation of `on_query`. If you want to
    /// override `on_query` with your own pre/post processing logic, you can
    /// call this function.
    async fn _on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
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
            let cancel_rx = get_cancel_receiver(client).await;
            let resp = if let Some(cancel_rx) = cancel_rx {
                match select(self.do_query(client, &query_string), cancel_rx).await {
                    Either::Left((result, _)) => result,
                    Either::Right(_) => Err(PgWireError::QueryCanceled),
                }
            } else {
                self.do_query(client, &query_string).await
            }?;
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
    ///
    /// When implementing PREPARE/EXECUTE statements, handlers can downcast
    /// `C::PortalStore` to access the specific statement type stored by
    /// ExtendedQueryHandler. This enables sharing prepared statements between
    /// simple and extended query protocols.
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;
}

/// Handler for extended query protocol (parse/bind/describe/execute/close).
#[async_trait]
pub trait ExtendedQueryHandler: Send + Sync {
    type Statement: Clone + Send + Sync + 'static;
    type QueryParser: QueryParser<Statement = Self::Statement> + Send + Sync;

    /// Get a reference to associated `QueryParser` implementation
    fn query_parser(&self) -> Arc<Self::QueryParser>;

    /// Called when client sends `parse` command.
    ///
    /// The default implementation parses the query with
    /// `Self::QueryParser` and stores it in `Self::PortalStore`. Empty
    /// queries are stored as empty statements instead, like PostgreSQL:
    /// they bind, describe and execute as empty queries.
    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message
            .name
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_owned());

        let parser = self.query_parser();
        match StoredStatement::parse(client, &message, parser).await? {
            Some(stmt) => client.portal_store().put_statement(Arc::new(stmt)),
            None => client.portal_store().put_empty_statement(&name),
        }
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;

        Ok(())
    }

    /// Called when client sends `bind` command.
    ///
    /// The default implementation associates parameters with a previously
    /// parsed statement and stores the result in `Self::PortalStore` as well.
    /// Binding an empty statement stores an empty portal, with zero
    /// parameters, like PostgreSQL.
    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement_name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
        let portal_name = message.portal_name.as_deref().unwrap_or(DEFAULT_NAME);

        match client.portal_store().get_statement(statement_name) {
            Some(StatementEntry::Statement(statement)) => {
                let portal = Portal::try_new(&message, statement)?;
                client.portal_store().put_portal(Arc::new(portal));
            }
            Some(StatementEntry::Empty) => {
                if !message.parameters.is_empty() {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "08P01".to_owned(),
                        format!(
                            "bind message supplies {} parameters, but prepared statement {:?} requires 0",
                            message.parameters.len(),
                            statement_name
                        ),
                    ))));
                }
                client.portal_store().put_empty_portal(portal_name);
            }
            None => return Err(PgWireError::StatementNotFound(statement_name.to_owned())),
        }

        client
            .send(PgWireBackendMessage::BindComplete(BindComplete::new()))
            .await?;
        Ok(())
    }

    /// Called when client sends `execute` command.
    ///
    /// The default implementation delegates the query to `self::do_query` and
    /// sends response messages according to `Response` from `self::do_query`.
    /// Empty portals answer `EmptyQueryResponse` and never reach `do_query`.
    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self._on_execute(client, message).await
    }

    /// The default implementation of `on_execute`.
    ///
    /// If write your own `on_execute` for pre/post query processing, you can
    /// reference this implementation by calling `self._on_execute(...)`.
    async fn _on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
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
        let max_rows = message.max_rows as usize;

        let portal = match client.portal_store().get_portal(portal_name) {
            Some(PortalEntry::Portal(portal)) => portal,
            Some(PortalEntry::Empty) => {
                // never reaches do_query; stays valid for repeated Execute
                client
                    .feed(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse))
                    .await?;
                client.set_state(super::PgWireConnectionState::ReadyForQuery);
                return Ok(());
            }
            None => return Err(PgWireError::PortalNotFound(portal_name.to_owned())),
        };
        // Execute query if the portal hasn't been started yet
        let needs_fetch = if matches!(
            portal.state().lock().await.deref(),
            PortalExecutionState::Initial
        ) {
            let cancel_rx = get_cancel_receiver(client).await;
            let resp = if let Some(cancel_rx) = cancel_rx {
                match select(self.do_query(client, portal.as_ref(), max_rows), cancel_rx).await {
                    Either::Left((result, _)) => result,
                    Either::Right(_) => Err(PgWireError::QueryCanceled),
                }
            } else {
                self.do_query(client, portal.as_ref(), max_rows).await
            }?;

            match resp {
                Response::Query(results) => {
                    portal.start(results).await;
                    true
                }
                Response::EmptyQuery => {
                    client
                        .feed(PgWireBackendMessage::EmptyQueryResponse(EmptyQueryResponse))
                        .await?;
                    false
                }
                Response::Execution(tag) => {
                    send_execution_response(client, tag).await?;
                    false
                }
                Response::TransactionStart(tag) => {
                    send_execution_response(client, tag).await?;
                    transaction_status = transaction_status.to_in_transaction_state();
                    false
                }
                Response::TransactionEnd(tag) => {
                    send_execution_response(client, tag).await?;
                    transaction_status = transaction_status.to_idle_state();

                    // remove unnamed portal when transaction ends
                    client.portal_store().rm_portal(DEFAULT_NAME);

                    false
                }
                Response::Error(err) => {
                    client
                        .send(PgWireBackendMessage::ErrorResponse((*err).into()))
                        .await?;
                    transaction_status = transaction_status.to_error_state();
                    false
                }
                Response::CopyIn(result) => {
                    client.set_state(PgWireConnectionState::CopyInProgress(true));
                    copy::send_copy_in_response(client, result).await?;
                    false
                }
                Response::CopyOut(result) => {
                    copy::send_copy_out_response(client, result).await?;
                    false
                }
                Response::CopyBoth(result) => {
                    client.set_state(PgWireConnectionState::CopyInProgress(true));
                    copy::send_copy_both_response(client, result).await?;
                    false
                }
            }
        } else {
            // Suspended or Finished — fetch remaining rows
            true
        };

        // Fetch rows from the portal and send to client
        if needs_fetch {
            let fetch_result = portal.fetch(max_rows).await?;
            let mut response = fetch_result.response;
            let command_tag = response.command_tag().to_owned();
            let mut row_count = 0;
            while let Some(row) = response.data_rows().next().await {
                client.feed(PgWireBackendMessage::DataRow(row?)).await?;
                row_count += 1;
            }
            if fetch_result.suspended {
                client
                    .send(PgWireBackendMessage::PortalSuspended(PortalSuspended))
                    .await?;
            } else {
                let tag = Tag::new(&command_tag).with_rows(row_count);
                client
                    .send(PgWireBackendMessage::CommandComplete(tag.into()))
                    .await?;
            }
        }

        if !matches!(client.state(), PgWireConnectionState::CopyInProgress(_)) {
            client.set_state(super::PgWireConnectionState::ReadyForQuery);
            client.set_transaction_status(transaction_status);
        };

        Ok(())
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
        self._on_describe(client, message).await
    }

    /// The default implementation of `on_describe`
    ///
    /// If you are writing pre/post processing for describe, you can reference
    /// this implementation by `self._on_describe(...)`
    async fn _on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_STATEMENT => match client.portal_store().get_statement(name) {
                Some(StatementEntry::Statement(stmt)) => {
                    let describe_response = self.do_describe_statement(client, &stmt).await?;
                    send_describe_response(client, &describe_response).await?;
                }
                Some(StatementEntry::Empty) => {
                    let describe_response = DescribeStatementResponse::no_data();
                    send_describe_response(client, &describe_response).await?;
                }
                None => return Err(PgWireError::StatementNotFound(name.to_owned())),
            },
            TARGET_TYPE_BYTE_PORTAL => match client.portal_store().get_portal(name) {
                Some(PortalEntry::Portal(portal)) => {
                    let describe_response = self.do_describe_portal(client, &portal).await?;
                    send_describe_response(client, &describe_response).await?;
                }
                Some(PortalEntry::Empty) => {
                    let describe_response = DescribePortalResponse::no_data();
                    send_describe_response(client, &describe_response).await?;
                }
                None => return Err(PgWireError::PortalNotFound(name.to_owned())),
            },
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
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.portal_store().rm_portal(DEFAULT_NAME);

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
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let stmt = &target.statement;
        let query_parser = self.query_parser();

        let server_param_types = query_parser.get_parameter_types(stmt)?;
        let result_schema = query_parser.get_result_schema(stmt, None)?;

        // use client given types, and fallback to server types if it's not available
        let param_types = (0usize..max(target.parameter_types.len(), server_param_types.len()))
            .map(|idx| {
                target
                    .parameter_types
                    .get(idx)
                    .cloned()
                    .and_then(|f| f)
                    .or_else(|| server_param_types.get(idx).cloned())
                    .unwrap_or(Type::UNKNOWN)
            })
            .collect::<Vec<Type>>();

        Ok(DescribeStatementResponse::new(param_types, result_schema))
    }

    /// Return resultset metadata without actually executing portal
    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let stmt = &target.statement.statement;
        let query_parser = self.query_parser();

        let result_schema =
            query_parser.get_result_schema(stmt, Some(&target.result_column_format))?;
        Ok(DescribePortalResponse::new(result_schema))
    }

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
    results: QueryResponse,
    send_describe: bool,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let QueryResponse {
        command_tag,
        row_schema,
        mut data_rows,
    } = results;

    // Simple query has row_schema in query response. For extended query,
    // row_schema is returned as response of `Describe`.
    //
    // Use `feed` rather than `send` so the whole response coalesces into the one
    // terminal flush the connection loop already performs (`send_ready_for_query`
    // for simple queries, `on_sync`/`on_flush` for extended). With TCP_NODELAY on,
    // each `send` flush is its own `sendto`/segment.
    if send_describe {
        let row_desc = into_row_description(&row_schema);
        client
            .feed(PgWireBackendMessage::RowDescription(row_desc))
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
        .feed(PgWireBackendMessage::CommandComplete(tag.into()))
        .await?;

    Ok(())
}

/// Sends up to `max_rows` from the query response, returning `true` if more rows remain.
pub async fn send_partial_query_response<C>(
    client: &mut C,
    results: &mut QueryResponse,
    max_rows: usize,
) -> PgWireResult<bool>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let command_tag = results.command_tag().to_string();
    let data_rows = results.data_rows();

    let mut rows = 0;
    let mut suspended = true;
    while max_rows == 0 || rows < max_rows {
        if let Some(row) = data_rows.next().await {
            let row = row?;
            client.feed(PgWireBackendMessage::DataRow(row)).await?;
            rows += 1;
        } else {
            suspended = false;
            break;
        }
    }

    if suspended {
        client
            .send(PgWireBackendMessage::PortalSuspended(PortalSuspended))
            .await?;
    } else {
        let tag = Tag::new(&command_tag).with_rows(rows);
        client
            .send(PgWireBackendMessage::CommandComplete(tag.into()))
            .await?;
    }

    Ok(suspended)
}

/// Helper function to send a ReadyForQuery response.
pub async fn send_ready_for_query<C>(
    client: &mut C,
    transaction_status: TransactionStatus,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
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
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    // Use `feed` rather than `send` so the CommandComplete coalesces with the
    // trailing ReadyForQuery into one socket write (see `send_query_response`).
    client
        .feed(PgWireBackendMessage::CommandComplete(tag.into()))
        .await?;

    Ok(())
}

/// Helper function to send response for `Describe`.
pub async fn send_describe_response<C, DR>(
    client: &mut C,
    describe_response: &DR,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
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

    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
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
    async fn do_query<C>(&self, _client: &mut C, _query: &str) -> PgWireResult<Vec<Response>>
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

#[cfg(test)]
mod tests {
    use super::is_empty_query;

    #[test]
    fn empty_and_whitespace_are_empty() {
        assert!(is_empty_query(""));
        assert!(is_empty_query(" "));
        assert!(is_empty_query("\t"));
        assert!(is_empty_query("\n\r\t \n"));
    }

    #[test]
    fn single_semicolon_is_empty() {
        assert!(is_empty_query(";"));
        // whitespace around the semicolon must not change the verdict
        assert!(is_empty_query(" ; "));
        assert!(is_empty_query("\n;\n"));
    }

    #[test]
    fn multiple_semicolons_are_empty() {
        // Reported in https://github.com/GreptimeTeam/greptimedb/issues/8855:
        // PostgreSQL treats `;;;` (and any sequence of only semicolons and
        // whitespace) as an empty query, answering with EmptyQueryResponse.
        assert!(is_empty_query(";;"));
        assert!(is_empty_query(";;;"));
        assert!(is_empty_query(";;;;;;"));
        assert!(is_empty_query("; ; ;"));
        assert!(is_empty_query("\n;\t;\r; "));
    }

    #[test]
    fn real_queries_are_not_empty() {
        assert!(!is_empty_query("select 1"));
        assert!(!is_empty_query("select 1;"));
        assert!(!is_empty_query("select 1;;;"));
        assert!(!is_empty_query("select 1; select 2;"));
        // a string literal containing only a semicolon is a real query
        assert!(!is_empty_query("';'"));
    }
}

/// Extended-query empty statement handling, mirroring PostgreSQL 18
/// message sequences.
#[cfg(test)]
mod extended_empty_query_tests {
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::Sink;

    use super::*;
    use crate::api::results::Tag;
    use crate::api::{DefaultClient, PgWireConnectionState};
    use crate::messages::response::TransactionStatus;

    /// A client test-double recording backend messages instead of encoding
    /// them.
    struct TestClient {
        inner: DefaultClient<String>,
        sent: Mutex<Vec<PgWireBackendMessage>>,
    }

    impl TestClient {
        fn new() -> Self {
            let mut inner = DefaultClient::new(SocketAddr::from(([127, 0, 0, 1], 5432)), false);
            inner.set_state(PgWireConnectionState::ReadyForQuery);
            TestClient {
                inner,
                sent: Mutex::new(Vec::new()),
            }
        }

        /// Short names of all backend messages sent so far.
        fn sent(&self) -> Vec<&'static str> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|m| match m {
                    PgWireBackendMessage::ParseComplete(_) => "ParseComplete",
                    PgWireBackendMessage::BindComplete(_) => "BindComplete",
                    PgWireBackendMessage::CloseComplete(_) => "CloseComplete",
                    PgWireBackendMessage::EmptyQueryResponse(_) => "EmptyQueryResponse",
                    PgWireBackendMessage::ParameterDescription(_) => "ParameterDescription",
                    PgWireBackendMessage::NoData(_) => "NoData",
                    PgWireBackendMessage::CommandComplete(_) => "CommandComplete",
                    PgWireBackendMessage::ReadyForQuery(_) => "ReadyForQuery",
                    _ => "other",
                })
                .collect()
        }

        /// Number of parameter types in the `ParameterDescription` at
        /// `idx` among the sent messages.
        fn parameter_description_len(&self, idx: usize) -> usize {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .filter_map(|m| match m {
                    PgWireBackendMessage::ParameterDescription(p) => Some(p.types.len()),
                    _ => None,
                })
                .nth(idx)
                .unwrap()
        }
    }

    impl ClientInfo for TestClient {
        fn socket_addr(&self) -> SocketAddr {
            self.inner.socket_addr()
        }

        fn is_secure(&self) -> bool {
            self.inner.is_secure()
        }

        fn protocol_version(&self) -> crate::messages::ProtocolVersion {
            self.inner.protocol_version()
        }

        fn set_protocol_version(&mut self, version: crate::messages::ProtocolVersion) {
            self.inner.set_protocol_version(version)
        }

        fn pid_and_secret_key(&self) -> (i32, crate::messages::startup::SecretKey) {
            self.inner.pid_and_secret_key()
        }

        fn set_pid_and_secret_key(
            &mut self,
            pid: i32,
            secret_key: crate::messages::startup::SecretKey,
        ) {
            self.inner.set_pid_and_secret_key(pid, secret_key)
        }

        fn state(&self) -> PgWireConnectionState {
            self.inner.state()
        }

        fn set_state(&mut self, new_state: PgWireConnectionState) {
            self.inner.set_state(new_state)
        }

        fn transaction_status(&self) -> TransactionStatus {
            self.inner.transaction_status()
        }

        fn set_transaction_status(&mut self, new_status: TransactionStatus) {
            self.inner.set_transaction_status(new_status)
        }

        fn metadata(&self) -> &std::collections::HashMap<String, String> {
            self.inner.metadata()
        }

        fn metadata_mut(&mut self) -> &mut std::collections::HashMap<String, String> {
            self.inner.metadata_mut()
        }

        fn session_extensions(&self) -> &crate::api::SessionExtensions {
            self.inner.session_extensions()
        }

        #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
        fn sni_server_name(&self) -> Option<&str> {
            self.inner.sni_server_name()
        }

        #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
        fn client_certificates<'a>(&self) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
            self.inner.client_certificates()
        }
    }

    impl ClientPortalStore for TestClient {
        type PortalStore = crate::api::store::MemPortalStore<String>;

        fn portal_store(&self) -> &Self::PortalStore {
            self.inner.portal_store()
        }
    }

    impl Sink<PgWireBackendMessage> for TestClient {
        type Error = PgWireError;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: PgWireBackendMessage) -> Result<(), Self::Error> {
            self.sent.lock().unwrap().push(item);
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// A parser that fails on syntactically empty queries (they must never
    /// reach a parser) and reports comment-only queries as empty.
    #[derive(Default)]
    struct RecordingParser {
        calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl QueryParser for RecordingParser {
        type Statement = String;

        async fn parse_sql<C>(
            &self,
            _client: &C,
            sql: &str,
            _types: &[Option<Type>],
        ) -> PgWireResult<Option<Self::Statement>>
        where
            C: ClientInfo + Unpin + Send + Sync,
        {
            assert!(
                !is_empty_query(sql),
                "parser must never be called for an empty query, got {sql:?}"
            );
            self.calls.lock().unwrap().push(sql.to_owned());
            if sql.starts_with("--") {
                Ok(None)
            } else {
                Ok(Some(sql.to_owned()))
            }
        }

        fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
            Ok(vec![])
        }

        fn get_result_schema(
            &self,
            _stmt: &Self::Statement,
            _column_format: Option<&crate::api::portal::Format>,
        ) -> PgWireResult<Vec<crate::api::results::FieldInfo>> {
            Ok(vec![])
        }
    }

    struct TestHandler {
        parser: Arc<RecordingParser>,
    }

    impl TestHandler {
        fn new() -> Self {
            TestHandler {
                parser: Arc::new(RecordingParser::default()),
            }
        }
    }

    #[async_trait]
    impl ExtendedQueryHandler for TestHandler {
        type Statement = String;
        type QueryParser = RecordingParser;

        fn query_parser(&self) -> Arc<Self::QueryParser> {
            self.parser.clone()
        }

        async fn do_query<C>(
            &self,
            _client: &mut C,
            _portal: &Portal<Self::Statement>,
            _max_rows: usize,
        ) -> PgWireResult<Response>
        where
            C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
            C::PortalStore: PortalStore<Statement = Self::Statement>,
            C::Error: Debug,
            PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
        {
            Ok(Response::Execution(Tag::new("OK")))
        }
    }

    fn parse(name: Option<&str>, query: &str) -> Parse {
        Parse {
            name: name.map(str::to_owned),
            query: query.to_owned(),
            type_oids: vec![],
        }
    }

    fn bind(portal: Option<&str>, statement: Option<&str>) -> Bind {
        Bind {
            portal_name: portal.map(str::to_owned),
            statement_name: statement.map(str::to_owned),
            parameter_format_codes: vec![],
            parameters: vec![],
            result_column_format_codes: vec![],
        }
    }

    fn describe(target_type: u8, name: Option<&str>) -> Describe {
        Describe {
            target_type,
            name: name.map(str::to_owned),
        }
    }

    fn close(target_type: u8, name: Option<&str>) -> Close {
        Close {
            target_type,
            name: name.map(str::to_owned),
        }
    }

    /// Parse/Bind/Describe/Execute/Sync of an empty query behaves exactly
    /// like PostgreSQL.
    #[tokio::test]
    async fn empty_query_extended_protocol_sequence() {
        let handler = TestHandler::new();
        let mut client = TestClient::new();

        handler
            .on_parse(&mut client, parse(None, ""))
            .await
            .unwrap();
        assert_eq!(client.sent(), ["ParseComplete"]);

        handler
            ._on_describe(&mut client, describe(TARGET_TYPE_BYTE_STATEMENT, None))
            .await
            .unwrap();
        assert_eq!(client.sent()[1..], ["ParameterDescription", "NoData"]);
        assert_eq!(client.parameter_description_len(0), 0);

        handler
            .on_bind(&mut client, bind(None, None))
            .await
            .unwrap();
        assert_eq!(client.sent()[3..], ["BindComplete"]);

        handler
            ._on_describe(&mut client, describe(TARGET_TYPE_BYTE_PORTAL, None))
            .await
            .unwrap();
        assert_eq!(client.sent()[4..], ["NoData"]);

        // empty portals stay valid across repeated Execute
        for _ in 0..2 {
            handler
                ._on_execute(
                    &mut client,
                    Execute {
                        name: None,
                        max_rows: 0,
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(
            client.sent()[5..],
            ["EmptyQueryResponse", "EmptyQueryResponse"]
        );
        assert!(matches!(
            client.state(),
            PgWireConnectionState::ReadyForQuery
        ));

        handler.on_sync(&mut client, PgSync).await.unwrap();
        assert_eq!(client.sent()[7..], ["ReadyForQuery"]);

        // Sync removed the unnamed empty portal
        assert!(matches!(
            handler
                ._on_execute(
                    &mut client,
                    Execute {
                        name: None,
                        max_rows: 0
                    },
                )
                .await,
            Err(PgWireError::PortalNotFound(_))
        ));

        assert!(handler.parser.calls.lock().unwrap().is_empty());
    }

    /// A query the parser reports as empty (`None`) is stored and executed
    /// as an empty query.
    #[tokio::test]
    async fn parser_reported_empty_query_executes_as_empty() {
        let handler = TestHandler::new();
        let mut client = TestClient::new();

        handler
            .on_parse(&mut client, parse(Some("c"), "-- comment only"))
            .await
            .unwrap();
        assert_eq!(
            handler.parser.calls.lock().unwrap().as_slice(),
            ["-- comment only"]
        );

        handler
            ._on_describe(&mut client, describe(TARGET_TYPE_BYTE_STATEMENT, Some("c")))
            .await
            .unwrap();
        assert_eq!(client.sent()[1..], ["ParameterDescription", "NoData"]);

        handler
            .on_bind(&mut client, bind(Some("p"), Some("c")))
            .await
            .unwrap();
        handler
            ._on_execute(
                &mut client,
                Execute {
                    name: Some("p".to_owned()),
                    max_rows: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(client.sent()[3..], ["BindComplete", "EmptyQueryResponse"]);
    }

    /// Semicolon-only and whitespace-only queries are empty in the extended
    /// protocol as well.
    #[tokio::test]
    async fn semicolon_only_queries_are_empty() {
        let handler = TestHandler::new();
        let mut client = TestClient::new();

        handler
            .on_parse(&mut client, parse(Some("s1"), ";"))
            .await
            .unwrap();
        handler
            .on_parse(&mut client, parse(Some("s2"), " \n;\t"))
            .await
            .unwrap();
        assert_eq!(client.sent(), ["ParseComplete", "ParseComplete"]);

        handler
            .on_bind(&mut client, bind(Some("p1"), Some("s1")))
            .await
            .unwrap();
        handler
            ._on_execute(
                &mut client,
                Execute {
                    name: Some("p1".to_owned()),
                    max_rows: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(client.sent()[2..], ["BindComplete", "EmptyQueryResponse"]);

        assert!(handler.parser.calls.lock().unwrap().is_empty());
    }

    /// A string literal containing only a semicolon is a real query and must
    /// reach the parser.
    #[tokio::test]
    async fn string_literal_semicolon_reaches_parser() {
        let handler = TestHandler::new();
        let mut client = TestClient::new();

        handler
            .on_parse(&mut client, parse(None, "';'"))
            .await
            .unwrap();
        assert_eq!(client.sent(), ["ParseComplete"]);
        assert_eq!(handler.parser.calls.lock().unwrap().as_slice(), ["';'"]);

        handler
            .on_bind(&mut client, bind(None, None))
            .await
            .unwrap();
        handler
            ._on_execute(
                &mut client,
                Execute {
                    name: None,
                    max_rows: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(client.sent()[1..], ["BindComplete", "CommandComplete"]);
    }

    /// An empty Parse replaces a previously stored statement of the same
    /// name, like PostgreSQL.
    #[tokio::test]
    async fn empty_parse_replaces_stored_statement() {
        let handler = TestHandler::new();
        let mut client = TestClient::new();

        handler
            .on_parse(&mut client, parse(None, "select 1"))
            .await
            .unwrap();
        handler
            .on_parse(&mut client, parse(None, ""))
            .await
            .unwrap();
        assert_eq!(client.sent(), ["ParseComplete", "ParseComplete"]);

        // describes the empty statement, not the previously parsed `select 1`
        handler
            ._on_describe(&mut client, describe(TARGET_TYPE_BYTE_STATEMENT, None))
            .await
            .unwrap();
        assert_eq!(client.sent()[2..], ["ParameterDescription", "NoData"]);

        handler
            .on_bind(&mut client, bind(None, None))
            .await
            .unwrap();
        handler
            ._on_execute(
                &mut client,
                Execute {
                    name: None,
                    max_rows: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(client.sent()[4..], ["BindComplete", "EmptyQueryResponse"]);
    }

    /// Binding an empty statement to a portal name replaces a previously
    /// bound real portal of the same name.
    #[tokio::test]
    async fn empty_bind_replaces_stored_portal() {
        let handler = TestHandler::new();
        let mut client = TestClient::new();

        handler
            .on_parse(&mut client, parse(None, "select 1"))
            .await
            .unwrap();
        handler
            .on_bind(&mut client, bind(Some("p"), None))
            .await
            .unwrap();
        handler
            ._on_execute(
                &mut client,
                Execute {
                    name: Some("p".to_owned()),
                    max_rows: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(client.sent()[1..], ["BindComplete", "CommandComplete"]);

        // re-parse the unnamed statement as empty, re-bind the same portal
        handler
            .on_parse(&mut client, parse(None, ""))
            .await
            .unwrap();
        handler
            .on_bind(&mut client, bind(Some("p"), None))
            .await
            .unwrap();
        handler
            ._on_execute(
                &mut client,
                Execute {
                    name: Some("p".to_owned()),
                    max_rows: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(client.sent()[4..], ["BindComplete", "EmptyQueryResponse"]);
    }

    /// Bind on a statement that was never parsed is an error, and binding
    /// parameters to an empty statement is a protocol violation (PostgreSQL
    /// answers 08P01).
    #[tokio::test]
    async fn bind_errors() {
        let handler = TestHandler::new();
        let mut client = TestClient::new();

        assert!(matches!(
            handler
                .on_bind(&mut client, bind(Some("p"), Some("missing")))
                .await,
            Err(PgWireError::StatementNotFound(_))
        ));

        handler
            .on_parse(&mut client, parse(Some("e"), ""))
            .await
            .unwrap();
        let mut with_param = bind(Some("p"), Some("e"));
        with_param.parameters.push(Some(Bytes::from_static(b"1")));
        match handler.on_bind(&mut client, with_param).await {
            Err(PgWireError::UserError(info)) => {
                assert_eq!(info.code, "08P01");
                assert!(info.message.contains("requires 0"));
            }
            other => panic!("expected 08P01 user error, got {other:?}"),
        }
    }

    /// `Close` removes empty statements and empty portals like real ones.
    #[tokio::test]
    async fn close_removes_empty_statement_and_portal() {
        let handler = TestHandler::new();
        let mut client = TestClient::new();

        handler
            .on_parse(&mut client, parse(Some("e"), ""))
            .await
            .unwrap();
        handler
            .on_close(&mut client, close(TARGET_TYPE_BYTE_STATEMENT, Some("e")))
            .await
            .unwrap();
        assert_eq!(client.sent(), ["ParseComplete", "CloseComplete"]);
        assert!(matches!(
            handler
                .on_bind(&mut client, bind(Some("p"), Some("e")))
                .await,
            Err(PgWireError::StatementNotFound(_))
        ));

        handler
            .on_parse(&mut client, parse(Some("e2"), ""))
            .await
            .unwrap();
        handler
            .on_bind(&mut client, bind(Some("p2"), Some("e2")))
            .await
            .unwrap();
        handler
            .on_close(&mut client, close(TARGET_TYPE_BYTE_PORTAL, Some("p2")))
            .await
            .unwrap();
        assert!(matches!(
            handler
                ._on_execute(
                    &mut client,
                    Execute {
                        name: Some("p2".to_owned()),
                        max_rows: 0,
                    },
                )
                .await,
            Err(PgWireError::PortalNotFound(_))
        ));
    }

    /// After a failed Bind the empty statement stays prepared, matching
    /// PostgreSQL.
    #[tokio::test]
    async fn marker_survives_failed_bind() {
        let handler = TestHandler::new();
        let mut client = TestClient::new();

        handler
            .on_parse(&mut client, parse(Some("e"), ""))
            .await
            .unwrap();
        let mut bad = bind(Some("p"), Some("e"));
        bad.parameters.push(Some(Bytes::from_static(b"1")));
        assert!(handler.on_bind(&mut client, bad).await.is_err());

        handler
            .on_bind(&mut client, bind(Some("p"), Some("e")))
            .await
            .unwrap();
        handler
            ._on_execute(
                &mut client,
                Execute {
                    name: Some("p".to_owned()),
                    max_rows: 0,
                },
            )
            .await
            .unwrap();
        assert_eq!(client.sent()[1..], ["BindComplete", "EmptyQueryResponse"]);
    }
}

use std::str::FromStr;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};
use postgres_types::{Oid, Type};

use crate::api::results::{FieldInfo, Tag};
use crate::error::{ErrorInfo, PgWireClientError, PgWireClientResult, PgWireError};
use crate::messages::data::{DataRow, ParameterDescription, RowDescription};
use crate::messages::extendedquery::{
    Bind, Close, Describe, Execute, Flush, Parse, Sync, TARGET_TYPE_BYTE_PORTAL,
    TARGET_TYPE_BYTE_STATEMENT,
};
use crate::messages::response::{CommandComplete, EmptyQueryResponse, ReadyForQuery};
use crate::messages::simplequery::Query;
use crate::messages::startup::ParameterStatus;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::result::DataRowsReader;
use super::{ClientInfo, ReadyState};

/// Response from a prepare (Parse) operation.
#[derive(Debug, Clone)]
pub struct PrepareResponse {
    pub name: Option<String>,
    pub param_types: Vec<Type>,
}

/// Response from a describe operation, containing parameter and result field metadata.
#[derive(Debug, Default)]
pub struct DescribeResponse {
    pub param_types: Vec<Type>,
    pub fields: Vec<FieldInfo>,
}

/// Result of an execute operation, either completed or suspended.
#[derive(Debug)]
pub enum ExecuteResult<T> {
    Complete(T),
    Suspended(T),
}

/// Target of a describe or close operation (statement or portal).
#[derive(Debug, Clone, Copy)]
pub enum DescribeTarget<'a> {
    Statement(Option<&'a str>),
    Portal(Option<&'a str>),
}

/// Handler trait for the simple query subprotocol.
#[async_trait]
pub trait SimpleQueryHandler: Send {
    type QueryResponse;

    /// Send a simple query to the server.
    async fn simple_query<C>(&mut self, client: &mut C, query: &str) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Handle a single backend message during simple query execution.
    async fn on_message<C>(
        &mut self,
        client: &mut C,
        message: PgWireBackendMessage,
    ) -> PgWireClientResult<ReadyState<Vec<Self::QueryResponse>>>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        match message {
            PgWireBackendMessage::RowDescription(row_description) => {
                self.on_row_description(client, row_description).await?;
            }
            PgWireBackendMessage::DataRow(data_row) => {
                self.on_data_row(client, data_row).await?;
            }
            PgWireBackendMessage::CommandComplete(command_complete) => {
                self.on_command_complete(client, command_complete).await?;
            }
            PgWireBackendMessage::EmptyQueryResponse(empty_query) => {
                self.on_empty_query(client, empty_query).await?;
            }
            PgWireBackendMessage::ParameterStatus(parameter_status) => {
                self.on_parameter_status(client, parameter_status).await?;
            }
            PgWireBackendMessage::ReadyForQuery(ready_for_query) => {
                let response = self.on_ready_for_query(client, ready_for_query).await?;
                return Ok(ReadyState::Ready(response));
            }
            PgWireBackendMessage::ErrorResponse(error) => {
                let error_info = ErrorInfo::from(error);
                return Err(error_info.into());
            }
            PgWireBackendMessage::NoticeResponse(_) => {}
            _ => return Err(PgWireClientError::UnexpectedMessage(Box::new(message))),
        }

        Ok(ReadyState::Pending)
    }

    /// Called when a `RowDescription` message is received.
    async fn on_row_description<C>(
        &mut self,
        client: &mut C,
        message: RowDescription,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Called when a `DataRow` message is received.
    async fn on_data_row<C>(&mut self, client: &mut C, message: DataRow) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Called when a `CommandComplete` message is received.
    async fn on_command_complete<C>(
        &mut self,
        client: &mut C,
        message: CommandComplete,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Called when an `EmptyQueryResponse` message is received.
    async fn on_empty_query<C>(
        &mut self,
        client: &mut C,
        message: EmptyQueryResponse,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Called when a `ParameterStatus` message is received during query
    /// execution.
    ///
    /// Servers report parameter changes this way, for example after a `SET`
    /// statement. The default implementation updates the client's cached
    /// server parameters via [`ClientInfo::set_server_parameter`].
    async fn on_parameter_status<C>(
        &mut self,
        client: &mut C,
        message: ParameterStatus,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        client.set_server_parameter(message.name, message.value);
        Ok(())
    }

    /// Called when a `ReadyForQuery` message is received.
    async fn on_ready_for_query<C>(
        &mut self,
        client: &mut C,
        message: ReadyForQuery,
    ) -> PgWireClientResult<Vec<Self::QueryResponse>>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;
}

/// Handler trait for the extended query subprotocol.
#[async_trait]
pub trait ExtendedQueryHandler: Send {
    type QueryResponse;

    /// Send a Parse message to the server.
    async fn parse<C>(&mut self, client: &mut C, query: Parse) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Send a Bind message to the server.
    async fn bind<C>(&mut self, client: &mut C, bind: Bind) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Send an Execute message to the server.
    async fn execute<C>(&mut self, client: &mut C, execute: Execute) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Send a Describe message to the server.
    async fn describe<C>(&mut self, client: &mut C, describe: Describe) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Send a Close message to the server.
    async fn close<C>(&mut self, client: &mut C, close: Close) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Send a Sync message to the server.
    async fn sync<C>(&mut self, client: &mut C, sync: Sync) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Send a Flush message to the server.
    async fn flush<C>(&mut self, client: &mut C, flush: Flush) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Called when a `ParameterDescription` message is received.
    async fn on_parameter_description(
        &mut self,
        msg: ParameterDescription,
    ) -> PgWireClientResult<Vec<Type>>;

    /// Called when a `RowDescription` message is received.
    async fn on_row_description(
        &mut self,
        msg: RowDescription,
    ) -> PgWireClientResult<Vec<FieldInfo>>;

    /// Called when a `DataRow` message is received.
    async fn on_data_row(&mut self, msg: DataRow) -> PgWireClientResult<Self::QueryResponse>;

    /// Called when a `CommandComplete` message is received.
    async fn on_command_complete(&mut self, msg: CommandComplete) -> PgWireClientResult<Tag>;

    /// Called when a `PortalSuspended` message is received.
    async fn on_portal_suspended(&mut self) -> PgWireClientResult<()>;
}

/// State tracker for extended query operations.
#[derive(Debug)]
pub struct ExtendedQueryState {}

/// Response from a simple or extended query execution.
#[derive(Debug)]
pub enum Response {
    EmptyQuery,
    Query((Tag, Vec<FieldInfo>, Vec<DataRow>)),
    Execution(Tag),
}

impl Response {
    /// Convert this response into a `DataRowsReader` for row-by-row access.
    pub fn into_data_rows_reader(self) -> DataRowsReader {
        if let Response::Query((_, fields, rows)) = self {
            DataRowsReader::new(fields, rows)
        } else {
            DataRowsReader::empty()
        }
    }
}

impl FromStr for Tag {
    type Err = PgWireClientError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let segs = s.split_whitespace().collect::<Vec<&str>>();
        // Command tags have the shape `COMMAND`, `COMMAND ROWS` or
        // `COMMAND OID ROWS` (INSERT). Some commands contain spaces
        // themselves (`CREATE TABLE`, `ALTER TABLE`, ...), so trailing
        // segments are only treated as counts when they are numeric.
        if segs.len() >= 2
            && let Ok(rows) = segs[segs.len() - 1].parse::<usize>()
        {
            if segs.len() == 2 {
                return Ok(Tag::new(segs[0]).with_rows(rows));
            } else if segs.len() == 3
                && let Ok(oid) = segs[1].parse::<Oid>()
            {
                return Ok(Tag::new(segs[0]).with_oid(oid).with_rows(rows));
            }
        }
        Ok(Tag::new(s))
    }
}

struct QueryResponseBuffer {
    row_schema: Vec<FieldInfo>,
    data_rows: Vec<DataRow>,
}

/// Default handler that collects simple query results into `Response` values.
#[derive(Default, new)]
pub struct DefaultSimpleQueryHandler {
    #[new(default)]
    current_buffer: Option<QueryResponseBuffer>,
    #[new(default)]
    responses: Vec<Response>,
}

#[async_trait]
impl SimpleQueryHandler for DefaultSimpleQueryHandler {
    type QueryResponse = Response;

    async fn simple_query<C>(&mut self, client: &mut C, query: &str) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        let query = Query::new(query.to_owned());
        client.send(PgWireFrontendMessage::Query(query)).await?;
        Ok(())
    }

    async fn on_row_description<C>(
        &mut self,
        _client: &mut C,
        message: RowDescription,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        let fields = message.fields.into_iter().map(|f| f.into()).collect();
        let buffer = QueryResponseBuffer {
            row_schema: fields,
            data_rows: Vec::new(),
        };
        self.current_buffer = Some(buffer);
        Ok(())
    }

    async fn on_data_row<C>(&mut self, _client: &mut C, message: DataRow) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        if let Some(ref mut current_buffer) = self.current_buffer {
            current_buffer.data_rows.push(message);
            Ok(())
        } else {
            Err(PgWireClientError::UnexpectedMessage(Box::new(
                PgWireBackendMessage::DataRow(message),
            )))
        }
    }

    async fn on_command_complete<C>(
        &mut self,
        _client: &mut C,
        message: CommandComplete,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        if self.current_buffer.is_some() {
            let current_buffer = std::mem::take(&mut self.current_buffer);
            let current_buffer = current_buffer.unwrap();
            self.responses.push(Response::Query((
                message.tag.parse::<Tag>()?,
                current_buffer.row_schema,
                current_buffer.data_rows,
            )));
        } else {
            let tag = message.tag.parse::<Tag>()?;
            self.responses.push(Response::Execution(tag));
        }

        Ok(())
    }

    async fn on_empty_query<C>(
        &mut self,
        _client: &mut C,
        _message: EmptyQueryResponse,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        self.responses.push(Response::EmptyQuery);
        Ok(())
    }

    async fn on_ready_for_query<C>(
        &mut self,
        _client: &mut C,
        _message: ReadyForQuery,
    ) -> PgWireClientResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        let responses = std::mem::take(&mut self.responses);
        Ok(responses)
    }
}

/// Client for executing extended query protocol operations.
pub struct ExtendedQueryClient<'a, C, H> {
    client: &'a mut C,
    handler: &'a mut H,
}

impl<'a, C, H> ExtendedQueryClient<'a, C, H>
where
    C: ClientInfo
        + Sink<PgWireFrontendMessage, Error = PgWireError>
        + Stream<Item = Result<PgWireBackendMessage, PgWireError>>
        + Unpin
        + Send,
    H: ExtendedQueryHandler,
{
    /// Create a new extended query client.
    pub fn new(client: &'a mut C, handler: &'a mut H) -> Self {
        Self { client, handler }
    }

    /// Discard messages until `ReadyForQuery` without sending anything.
    ///
    /// Used on error paths where the current extended-query cycle has
    /// already been terminated with `Sync`: the server sends a trailing
    /// `ReadyForQuery` (and nothing else responds to the already-sent
    /// `Sync`), so draining is enough — sending another `Sync` would make
    /// the server emit an extra `ReadyForQuery` and desynchronize the next
    /// query.
    async fn drain_to_ready(&mut self) {
        while let Some(message_result) = self.client.next().await {
            match message_result {
                Ok(PgWireBackendMessage::ReadyForQuery(_)) => break,
                Ok(PgWireBackendMessage::ParameterStatus(parameter_status)) => {
                    self.client
                        .set_server_parameter(parameter_status.name, parameter_status.value);
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
    }

    /// Send `Sync` and discard messages until `ReadyForQuery`.
    ///
    /// This closes the extended-query cycle. It is used after a query
    /// completes, and to recover a clean connection state after an
    /// `ErrorResponse` when the cycle is still open (only `Flush` has been
    /// sent): after an error the backend discards messages until it
    /// receives `Sync`.
    async fn finish(&mut self) -> PgWireClientResult<()> {
        self.handler.sync(self.client, Sync::new()).await?;
        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::ReadyForQuery(_) => return Ok(()),
                PgWireBackendMessage::ParameterStatus(parameter_status) => {
                    self.client
                        .set_server_parameter(parameter_status.name, parameter_status.value);
                }
                _ => {}
            }
        }
        Err(PgWireClientError::UnexpectedEOF)
    }

    /// Prepare a statement for later execution.
    ///
    /// The statement is parsed and immediately described, so the returned
    /// [`PrepareResponse`] carries the parameter types the server inferred
    /// (or confirmed).
    pub async fn prepare(
        &mut self,
        name: Option<&str>,
        query: &str,
        param_types: &[Oid],
    ) -> PgWireClientResult<PrepareResponse> {
        let parse = Parse::new(
            name.map(|n| n.to_owned()),
            query.to_owned(),
            param_types.to_vec(),
        );
        // ParameterDescription and RowDescription are only sent in response
        // to a Describe message, so one has to follow Parse for the server
        // to report the statement's parameter and result types.
        let describe = Describe::new(TARGET_TYPE_BYTE_STATEMENT, name.map(|n| n.to_owned()));
        self.handler.parse(self.client, parse).await?;
        self.handler.describe(self.client, describe).await?;
        self.handler.sync(self.client, Sync::new()).await?;

        let mut response = PrepareResponse {
            name: name.map(|n| n.to_owned()),
            param_types: Vec::new(),
        };

        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::ParseComplete(_) => {}
                PgWireBackendMessage::ParameterDescription(param_desc) => {
                    response.param_types =
                        self.handler.on_parameter_description(param_desc).await?;
                }
                PgWireBackendMessage::RowDescription(row_desc) => {
                    // result metadata of the statement; `describe` returns it
                    let _ = self.handler.on_row_description(row_desc).await?;
                }
                PgWireBackendMessage::NoData(_) => {}
                PgWireBackendMessage::ReadyForQuery(_) => {
                    return Ok(response);
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    // Sync has already been sent above; drain only
                    self.drain_to_ready().await;
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                PgWireBackendMessage::ParameterStatus(parameter_status) => {
                    self.client
                        .set_server_parameter(parameter_status.name, parameter_status.value);
                }
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    /// Bind parameters to a prepared statement, creating a portal.
    ///
    /// The extended-query cycle is kept open (the messages are flushed with
    /// `Flush` instead of terminated with `Sync`): portals are destroyed
    /// when the cycle ends, so a later [`Self::execute`] or
    /// [`Self::describe`] on the portal requires the cycle to still be open.
    pub async fn bind(
        &mut self,
        portal: Option<&str>,
        statement: Option<&str>,
        params: Vec<Option<Bytes>>,
        result_formats: Vec<i16>,
    ) -> PgWireClientResult<()> {
        let bind = Bind::new(
            portal.map(|p| p.to_owned()),
            statement.map(|s| s.to_owned()),
            vec![],
            params,
            result_formats,
        );
        self.handler.bind(self.client, bind).await?;
        self.handler.flush(self.client, Flush::new()).await?;

        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::BindComplete(_) => {
                    return Ok(());
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    self.finish().await?;
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                PgWireBackendMessage::ParameterStatus(parameter_status) => {
                    self.client
                        .set_server_parameter(parameter_status.name, parameter_status.value);
                }
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    /// Execute a portal and return the result rows.
    ///
    /// On [`ExecuteResult::Complete`] the extended-query cycle is closed
    /// with `Sync`. On [`ExecuteResult::Suspended`] the cycle is left open
    /// so that the portal can be executed again; callers must eventually
    /// run the portal to completion (or [`Self::close`] it) before using
    /// other APIs on the connection.
    pub async fn execute(
        &mut self,
        portal: Option<&str>,
        max_rows: i32,
    ) -> PgWireClientResult<ExecuteResult<Vec<H::QueryResponse>>> {
        let execute = Execute::new(portal.map(|p| p.to_owned()), max_rows);
        self.handler.execute(self.client, execute).await?;
        self.handler.flush(self.client, Flush::new()).await?;

        let mut rows = Vec::new();

        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::DataRow(data_row) => {
                    let row = self.handler.on_data_row(data_row).await?;
                    rows.push(row);
                }
                PgWireBackendMessage::CommandComplete(command_complete) => {
                    self.handler.on_command_complete(command_complete).await?;
                    self.finish().await?;
                    return Ok(ExecuteResult::Complete(rows));
                }
                PgWireBackendMessage::PortalSuspended(_) => {
                    self.handler.on_portal_suspended().await?;
                    // keep the extended-query cycle open: a Sync here would
                    // destroy the suspended portal
                    return Ok(ExecuteResult::Suspended(rows));
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    self.finish().await?;
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                PgWireBackendMessage::ParameterStatus(parameter_status) => {
                    self.client
                        .set_server_parameter(parameter_status.name, parameter_status.value);
                }
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    /// Describe a prepared statement or portal.
    ///
    /// Statements persist across extended-query cycles, so describing a
    /// statement is its own (terminated) cycle. Portals only exist inside
    /// the open cycle created by [`Self::bind`], so describing a portal uses
    /// `Flush` and leaves that cycle open.
    pub async fn describe(
        &mut self,
        target: DescribeTarget<'_>,
    ) -> PgWireClientResult<DescribeResponse> {
        let (target_type, name) = match target {
            DescribeTarget::Statement(name) => (TARGET_TYPE_BYTE_STATEMENT, name),
            DescribeTarget::Portal(name) => (TARGET_TYPE_BYTE_PORTAL, name),
        };
        let describe = Describe::new(target_type, name.map(|n| n.to_owned()));
        self.handler.describe(self.client, describe).await?;

        let is_statement = matches!(target, DescribeTarget::Statement(_));
        match target {
            DescribeTarget::Statement(_) => {
                self.handler.sync(self.client, Sync::new()).await?;
            }
            DescribeTarget::Portal(_) => {
                self.handler.flush(self.client, Flush::new()).await?;
            }
        }

        let mut response = DescribeResponse::default();
        // for portals the cycle stays open, so the loop ends on the metadata
        // message itself; for statements it ends on ReadyForQuery
        let mut done = false;

        while !done {
            let message_result = self
                .client
                .next()
                .await
                .ok_or(PgWireClientError::UnexpectedEOF)?;
            let message = message_result?;
            match message {
                PgWireBackendMessage::ParameterDescription(param_desc) => {
                    response.param_types =
                        self.handler.on_parameter_description(param_desc).await?;
                }
                PgWireBackendMessage::RowDescription(row_desc) => {
                    response.fields = self.handler.on_row_description(row_desc).await?;
                    if matches!(target, DescribeTarget::Portal(_)) {
                        done = true;
                    }
                }
                PgWireBackendMessage::NoData(_) => {
                    if matches!(target, DescribeTarget::Portal(_)) {
                        done = true;
                    }
                }
                PgWireBackendMessage::ReadyForQuery(_) => {
                    done = true;
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    if is_statement {
                        // Sync has already been sent; drain only
                        self.drain_to_ready().await;
                    } else {
                        // the cycle is still open; close it
                        self.finish().await?;
                    }
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                PgWireBackendMessage::ParameterStatus(parameter_status) => {
                    self.client
                        .set_server_parameter(parameter_status.name, parameter_status.value);
                }
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Ok(response)
    }

    /// Close a prepared statement or portal.
    pub async fn close(&mut self, target: DescribeTarget<'_>) -> PgWireClientResult<()> {
        let (target_type, name) = match target {
            DescribeTarget::Statement(name) => (TARGET_TYPE_BYTE_STATEMENT, name),
            DescribeTarget::Portal(name) => (TARGET_TYPE_BYTE_PORTAL, name),
        };
        let close = Close::new(target_type, name.map(|n| n.to_owned()));
        self.handler.close(self.client, close).await?;
        self.handler.sync(self.client, Sync::new()).await?;

        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::CloseComplete(_) => {}
                PgWireBackendMessage::ReadyForQuery(_) => {
                    return Ok(());
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    // Sync has already been sent; drain only
                    self.drain_to_ready().await;
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                PgWireBackendMessage::ParameterStatus(parameter_status) => {
                    self.client
                        .set_server_parameter(parameter_status.name, parameter_status.value);
                }
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    /// Execute a one-shot extended query (parse, bind, execute in a single
    /// extended-query cycle).
    pub async fn query(
        &mut self,
        sql: &str,
        param_types: &[Oid],
        params: Vec<Option<Bytes>>,
    ) -> PgWireClientResult<Vec<H::QueryResponse>> {
        let parse = Parse::new(None, sql.to_owned(), param_types.to_vec());
        let bind = Bind::new(None, None, vec![], params, vec![]);
        let execute = Execute::new(None, 0);

        self.handler.parse(self.client, parse).await?;
        self.handler.bind(self.client, bind).await?;
        self.handler.execute(self.client, execute).await?;
        self.handler.sync(self.client, Sync::new()).await?;

        let mut rows = Vec::new();

        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::ParseComplete(_) => {}
                PgWireBackendMessage::BindComplete(_) => {}
                PgWireBackendMessage::DataRow(data_row) => {
                    let row = self.handler.on_data_row(data_row).await?;
                    rows.push(row);
                }
                PgWireBackendMessage::CommandComplete(command_complete) => {
                    self.handler.on_command_complete(command_complete).await?;
                }
                PgWireBackendMessage::EmptyQueryResponse(_) => {}
                PgWireBackendMessage::ErrorResponse(error) => {
                    // Sync has already been sent; drain only
                    self.drain_to_ready().await;
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                PgWireBackendMessage::ParameterStatus(parameter_status) => {
                    self.client
                        .set_server_parameter(parameter_status.name, parameter_status.value);
                }
                PgWireBackendMessage::ReadyForQuery(_) => {
                    return Ok(rows);
                }
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }
}

/// Default handler that forwards extended query messages and collects results.
#[derive(Default, new)]
pub struct DefaultExtendedQueryHandler {
    #[new(default)]
    current_row: Option<DataRow>,
    #[new(default)]
    current_fields: Vec<FieldInfo>,
}

#[async_trait]
impl ExtendedQueryHandler for DefaultExtendedQueryHandler {
    type QueryResponse = DataRow;

    async fn parse<C>(&mut self, client: &mut C, query: Parse) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        client.send(PgWireFrontendMessage::Parse(query)).await?;
        Ok(())
    }

    async fn bind<C>(&mut self, client: &mut C, bind: Bind) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        client.send(PgWireFrontendMessage::Bind(bind)).await?;
        Ok(())
    }

    async fn execute<C>(&mut self, client: &mut C, execute: Execute) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        client.send(PgWireFrontendMessage::Execute(execute)).await?;
        Ok(())
    }

    async fn describe<C>(&mut self, client: &mut C, describe: Describe) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        client
            .send(PgWireFrontendMessage::Describe(describe))
            .await?;
        Ok(())
    }

    async fn close<C>(&mut self, client: &mut C, close: Close) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        client.send(PgWireFrontendMessage::Close(close)).await?;
        Ok(())
    }

    async fn sync<C>(&mut self, client: &mut C, _sync: Sync) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        client.send(PgWireFrontendMessage::Sync(_sync)).await?;
        Ok(())
    }

    async fn flush<C>(&mut self, client: &mut C, _flush: Flush) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        client.send(PgWireFrontendMessage::Flush(_flush)).await?;
        Ok(())
    }

    async fn on_parameter_description(
        &mut self,
        msg: ParameterDescription,
    ) -> PgWireClientResult<Vec<Type>> {
        Ok(msg.types.into_iter().filter_map(Type::from_oid).collect())
    }

    async fn on_row_description(
        &mut self,
        msg: RowDescription,
    ) -> PgWireClientResult<Vec<FieldInfo>> {
        self.current_fields = msg.fields.into_iter().map(|f| f.into()).collect();
        Ok(self.current_fields.clone())
    }

    async fn on_data_row(&mut self, msg: DataRow) -> PgWireClientResult<DataRow> {
        self.current_row = Some(msg.clone());
        Ok(msg)
    }

    async fn on_command_complete(&mut self, _msg: CommandComplete) -> PgWireClientResult<Tag> {
        Ok(_msg.tag.parse::<Tag>()?)
    }

    async fn on_portal_suspended(&mut self) -> PgWireClientResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag_from_str() {
        // plain commands without counts
        assert_eq!("BEGIN".parse::<Tag>().unwrap(), Tag::new("BEGIN"));
        // commands whose name contains a space must not be parsed as counts
        assert_eq!(
            "CREATE TABLE".parse::<Tag>().unwrap(),
            Tag::new("CREATE TABLE")
        );
        assert_eq!("DROP TABLE".parse::<Tag>().unwrap(), Tag::new("DROP TABLE"));
        // command with a row count
        assert_eq!(
            "SELECT 5".parse::<Tag>().unwrap(),
            Tag::new("SELECT").with_rows(5)
        );
        assert_eq!(
            "UPDATE 2".parse::<Tag>().unwrap(),
            Tag::new("UPDATE").with_rows(2)
        );
        // INSERT reports `oid rows`, in that order
        assert_eq!(
            "INSERT 0 2".parse::<Tag>().unwrap(),
            Tag::new("INSERT").with_oid(0).with_rows(2)
        );
    }

    #[test]
    fn test_tag_round_trip_with_command_complete() {
        for tag in [
            Tag::new("BEGIN"),
            Tag::new("CREATE TABLE"),
            Tag::new("SELECT").with_rows(1),
            Tag::new("INSERT").with_oid(0).with_rows(2),
        ] {
            let command_complete: CommandComplete = tag.clone().into();
            assert_eq!(command_complete.tag.parse::<Tag>().unwrap(), tag);
        }
    }
}

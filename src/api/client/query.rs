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
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::result::DataRowsReader;
use super::{ClientInfo, ReadyState};

#[derive(Debug, Clone)]
pub struct PrepareResponse {
    pub name: Option<String>,
    pub param_types: Vec<Type>,
}

#[derive(Debug, Default)]
pub struct DescribeResponse {
    pub param_types: Vec<Type>,
    pub fields: Vec<FieldInfo>,
}

#[derive(Debug)]
pub enum ExecuteResult<T> {
    Complete(T),
    Suspended(T),
}

#[derive(Debug, Clone, Copy)]
pub enum DescribeTarget<'a> {
    Statement(Option<&'a str>),
    Portal(Option<&'a str>),
}

#[async_trait]
pub trait SimpleQueryHandler: Send {
    type QueryResponse;

    async fn simple_query<C>(&mut self, client: &mut C, query: &str) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

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

    async fn on_row_description<C>(
        &mut self,
        client: &mut C,
        message: RowDescription,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_data_row<C>(&mut self, client: &mut C, message: DataRow) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_command_complete<C>(
        &mut self,
        client: &mut C,
        message: CommandComplete,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_empty_query<C>(
        &mut self,
        client: &mut C,
        message: EmptyQueryResponse,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_ready_for_query<C>(
        &mut self,
        client: &mut C,
        message: ReadyForQuery,
    ) -> PgWireClientResult<Vec<Self::QueryResponse>>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;
}

#[async_trait]
pub trait ExtendedQueryHandler: Send {
    type QueryResponse;

    async fn parse<C>(&mut self, client: &mut C, query: Parse) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn bind<C>(&mut self, client: &mut C, bind: Bind) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn execute<C>(&mut self, client: &mut C, execute: Execute) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn describe<C>(&mut self, client: &mut C, describe: Describe) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn close<C>(&mut self, client: &mut C, close: Close) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn sync<C>(&mut self, client: &mut C, sync: Sync) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn flush<C>(&mut self, client: &mut C, flush: Flush) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    async fn on_parameter_description(
        &mut self,
        msg: ParameterDescription,
    ) -> PgWireClientResult<Vec<Type>>;

    async fn on_row_description(
        &mut self,
        msg: RowDescription,
    ) -> PgWireClientResult<Vec<FieldInfo>>;

    async fn on_data_row(&mut self, msg: DataRow) -> PgWireClientResult<Self::QueryResponse>;

    async fn on_command_complete(&mut self, msg: CommandComplete) -> PgWireClientResult<Tag>;

    async fn on_portal_suspended(&mut self) -> PgWireClientResult<()>;
}

#[derive(Debug)]
pub struct ExtendedQueryState {}

#[derive(Debug)]
pub enum Response {
    EmptyQuery,
    Query((Tag, Vec<FieldInfo>, Vec<DataRow>)),
    Execution(Tag),
}

impl Response {
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
        if segs.len() == 2 {
            let rows = segs[1]
                .parse::<usize>()
                .map_err(|e| PgWireClientError::InvalidTag(Box::new(e)))?;
            Ok(Tag::new(segs[0]).with_rows(rows))
        } else if segs.len() == 3 {
            let rows = segs[1]
                .parse::<usize>()
                .map_err(|e| PgWireClientError::InvalidTag(Box::new(e)))?;
            let oid = segs[2]
                .parse::<Oid>()
                .map_err(|e| PgWireClientError::InvalidTag(Box::new(e)))?;
            Ok(Tag::new(segs[0]).with_rows(rows).with_oid(oid))
        } else {
            Ok(Tag::new(s))
        }
    }
}

struct QueryResponseBuffer {
    row_schema: Vec<FieldInfo>,
    data_rows: Vec<DataRow>,
}

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
    pub fn new(client: &'a mut C, handler: &'a mut H) -> Self {
        Self { client, handler }
    }

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
        self.handler.parse(self.client, parse).await?;
        self.handler.sync(self.client, Sync::new()).await?;

        let mut param_type_result = Vec::new();
        let mut response = PrepareResponse {
            name: name.map(|n| n.to_owned()),
            param_types: Vec::new(),
        };

        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::ParseComplete(_) => {}
                PgWireBackendMessage::ParameterDescription(param_desc) => {
                    param_type_result = self.handler.on_parameter_description(param_desc).await?;
                }
                PgWireBackendMessage::RowDescription(row_desc) => {
                    let _ = self.handler.on_row_description(row_desc).await?;
                }
                PgWireBackendMessage::NoData(_) => {}
                PgWireBackendMessage::ReadyForQuery(_) => {
                    response.param_types = param_type_result;
                    return Ok(response);
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

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
        self.handler.sync(self.client, Sync::new()).await?;

        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::BindComplete(_) => {}
                PgWireBackendMessage::ReadyForQuery(_) => {
                    return Ok(());
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    pub async fn execute(
        &mut self,
        portal: Option<&str>,
        max_rows: i32,
    ) -> PgWireClientResult<ExecuteResult<Vec<H::QueryResponse>>> {
        let execute = Execute::new(portal.map(|p| p.to_owned()), max_rows);
        self.handler.execute(self.client, execute).await?;
        self.handler.sync(self.client, Sync::new()).await?;

        let mut rows = Vec::new();
        let mut is_suspended = false;

        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::DataRow(data_row) => {
                    let row = self.handler.on_data_row(data_row).await?;
                    rows.push(row);
                }
                PgWireBackendMessage::CommandComplete(command_complete) => {
                    self.handler.on_command_complete(command_complete).await?;
                }
                PgWireBackendMessage::PortalSuspended(_) => {
                    self.handler.on_portal_suspended().await?;
                    is_suspended = true;
                }
                PgWireBackendMessage::ReadyForQuery(_) => {
                    if is_suspended {
                        return Ok(ExecuteResult::Suspended(rows));
                    } else {
                        return Ok(ExecuteResult::Complete(rows));
                    }
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

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
        self.handler.sync(self.client, Sync::new()).await?;

        let mut response = DescribeResponse::default();

        while let Some(message_result) = self.client.next().await {
            let message = message_result?;
            match message {
                PgWireBackendMessage::ParameterDescription(param_desc) => {
                    response.param_types =
                        self.handler.on_parameter_description(param_desc).await?;
                }
                PgWireBackendMessage::RowDescription(row_desc) => {
                    response.fields = self.handler.on_row_description(row_desc).await?;
                }
                PgWireBackendMessage::NoData(_) => {}
                PgWireBackendMessage::ReadyForQuery(_) => {
                    return Ok(response);
                }
                PgWireBackendMessage::ErrorResponse(error) => {
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

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
                    return Err(ErrorInfo::from(error).into());
                }
                PgWireBackendMessage::NoticeResponse(_) => {}
                _ => {
                    return Err(PgWireClientError::UnexpectedMessage(Box::new(message)));
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    pub async fn query(
        &mut self,
        sql: &str,
        param_types: &[Oid],
        params: Vec<Option<Bytes>>,
    ) -> PgWireClientResult<Vec<H::QueryResponse>> {
        self.prepare(None, sql, param_types).await?;
        self.bind(None, None, params, vec![]).await?;
        let result = self.execute(None, 0).await?;

        match result {
            ExecuteResult::Complete(rows) => Ok(rows),
            ExecuteResult::Suspended(rows) => {
                self.close(DescribeTarget::Portal(None)).await?;
                Ok(rows)
            }
        }
    }
}

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

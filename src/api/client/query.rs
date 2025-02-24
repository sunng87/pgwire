use std::str::FromStr;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use postgres_types::Oid;

use crate::api::results::{FieldInfo, Tag};
use crate::error::{ErrorInfo, PgWireClientError, PgWireClientResult};
use crate::messages::data::{DataRow, RowDescription};
use crate::messages::response::{CommandComplete, EmptyQueryResponse, ReadyForQuery};
use crate::messages::simplequery::Query;
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::result::DataRowsReader;
use super::{ClientInfo, ReadyState};

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

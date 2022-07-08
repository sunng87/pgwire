use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream;

use super::ClientInfo;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::data::{DataRow, RowDescription};
use crate::messages::response::{CommandComplete, ErrorResponse, ReadyForQuery, READY_STATUS_IDLE};
use crate::messages::PgWireMessage;

/// handler for processing simple query.
#[async_trait]
pub trait SimpleQueryHandler: Send + Sync {
    ///
    async fn on_query<C>(&self, client: &mut C, query: &PgWireMessage) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireMessage>>::Error>,
    {
        if let PgWireMessage::Query(query) = query {
            client.set_state(super::PgWireConnectionState::QueryInProgress);
            let resp = self.do_query(client, query.query()).await?;
            match resp {
                QueryResponse::Data(row_description, data_rows, status) => {
                    let mut msgs = Vec::new();
                    msgs.push(PgWireMessage::RowDescription(row_description));
                    msgs.extend(data_rows.into_iter().map(|row| PgWireMessage::DataRow(row)));
                    msgs.push(PgWireMessage::CommandComplete(status));
                    msgs.push(PgWireMessage::ReadyForQuery(ReadyForQuery::new(
                        READY_STATUS_IDLE,
                    )));

                    let mut stream = stream::iter(msgs.into_iter().map(Ok));
                    client.send_all(&mut stream).await?;
                }
                QueryResponse::Empty(status) => {
                    client.feed(PgWireMessage::CommandComplete(status)).await?;
                    client
                        .feed(PgWireMessage::ReadyForQuery(ReadyForQuery::new(
                            READY_STATUS_IDLE,
                        )))
                        .await?;
                    client.flush().await?;
                }
                QueryResponse::Error(e) => {
                    client.feed(PgWireMessage::ErrorResponse(e)).await?;
                    client
                        .feed(PgWireMessage::ReadyForQuery(ReadyForQuery::new(
                            READY_STATUS_IDLE,
                        )))
                        .await?;
                    client.flush().await?;
                }
            }
        }

        client.set_state(super::PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    ///
    async fn do_query<C>(&self, client: &C, query: &str) -> PgWireResult<QueryResponse>
    where
        C: ClientInfo + Unpin + Send + Sync;
}

/// Query response types:
///
/// * Data: the response contains data rows,
/// * Empty: the response has no data, like update/delete/insert
/// * Error: an error response
pub enum QueryResponse {
    Data(RowDescription, Vec<DataRow>, CommandComplete),
    Empty(CommandComplete),
    Error(ErrorResponse),
}

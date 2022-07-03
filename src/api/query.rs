use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream;

use super::ClientInfo;
use crate::messages::data::{DataRow, RowDescription};
use crate::messages::response::{CommandComplete, ErrorResponse, ReadyForQuery, READY_STATUS_IDLE};
use crate::messages::PgWireMessage;

/// handler for processing simple query.
#[async_trait]
pub trait SimpleQueryHandler: Sync {
    ///
    async fn on_query<C>(&self, client: &mut C, query: &PgWireMessage) -> Result<(), std::io::Error>
    where
        C: ClientInfo + Sink<PgWireMessage> + Unpin + Send + Sync,
        C::Error: Debug,
    {
        if let PgWireMessage::Query(query) = query {
            client.set_state(super::PgWireConnectionState::QueryInProgress);
            let resp = self.do_query(client, query.query()).await?;
            match resp {
                QueryResponse::Data(row_description, data_rows, status) => {
                    // TODO: combine all these response into one `send_all`
                    client
                        .send(PgWireMessage::RowDescription(row_description))
                        .await
                        .unwrap();
                    client
                        .send_all(&mut stream::iter(
                            data_rows
                                .into_iter()
                                .map(|row| Ok(PgWireMessage::DataRow(row))),
                        ))
                        .await
                        .unwrap();
                    client
                        .send(PgWireMessage::CommandComplete(status))
                        .await
                        .unwrap();
                    client
                        .send(PgWireMessage::ReadyForQuery(ReadyForQuery::new(
                            READY_STATUS_IDLE,
                        )))
                        .await
                        .unwrap();
                }
                QueryResponse::Empty(status) => {
                    client
                        .send(PgWireMessage::CommandComplete(status))
                        .await
                        .unwrap();
                    client
                        .send(PgWireMessage::ReadyForQuery(ReadyForQuery::new(
                            READY_STATUS_IDLE,
                        )))
                        .await
                        .unwrap();
                }
                QueryResponse::Error(e) => {
                    client.send(PgWireMessage::ErrorResponse(e)).await.unwrap();
                    client
                        .send(PgWireMessage::ReadyForQuery(ReadyForQuery::new(
                            READY_STATUS_IDLE,
                        )))
                        .await
                        .unwrap();
                }
            }
        }

        client.set_state(super::PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    ///
    async fn do_query<C>(&self, client: &C, query: &str) -> Result<QueryResponse, std::io::Error>
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

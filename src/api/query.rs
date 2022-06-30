use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::Sink;

use super::ClientInfo;
use crate::messages::PgWireMessage;

/// handler for processing simple query.
#[async_trait]
pub trait SimpleQueryHandler {
    ///
    async fn on_query<C>(
        &self,
        client: &mut C,
        query: &PgWireMessage,
    ) -> Result<(), std::io::Error>
    where
        C: ClientInfo + Sink<PgWireMessage> + Unpin + Send,
        C::Error: Debug;

    ///
    async fn do_query<C>(&self, client: &C, query: &str) -> Result<QueryResponse, std::io::Error>
    where
        C: ClientInfo + Unpin + Send;
}

pub enum QueryResponse {
    Data(), //TODO: row definition and data row
    Empty,
    Error,
}

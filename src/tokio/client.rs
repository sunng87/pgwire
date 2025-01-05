use std::io::{Error as IOError, ErrorKind};
use std::sync::Arc;

use bytes::Buf;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use tokio_rustls::TlsConnector;
use tokio_rustls::TlsStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::api::client::config::Host;
use crate::api::client::Config;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

#[non_exhaustive]
#[derive(Debug)]
pub struct PgWireMessageClientCodec;

impl Decoder for PgWireMessageClientCodec {
    type Item = PgWireBackendMessage;
    type Error = PgWireError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        PgWireBackendMessage::decode(src)
    }
}

impl Encoder<PgWireFrontendMessage> for PgWireMessageClientCodec {
    type Error = PgWireError;

    fn encode(
        &mut self,
        item: PgWireFrontendMessage,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        item.encode(dst).map_err(Into::into)
    }
}

pub struct PgWireClient<S> {
    transport: SplitSink<Framed<S, PgWireMessageClientCodec>, PgWireFrontendMessage>,
}

impl PgWireClient<TcpStream> {
    pub fn process_socket(socket: TcpStream) -> Arc<Self> {
        let socket = Framed::new(socket, PgWireMessageClientCodec);
        let (sender, mut receiver) = socket.split();
        let client = Arc::new(PgWireClient { transport: sender });
        let handle_client = client.clone();

        let handle = async move {
            while let Some(msg) = receiver.next().await {
                if let Ok(msg) = msg {
                    if let Err(e) = handle_client.process_message(msg).await {
                        if let Err(_e) = handle_client.process_error(e).await {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        };

        tokio::spawn(handle);

        client
    }

    async fn process_message(&self, message: PgWireBackendMessage) -> PgWireResult<()> {
        todo!();
        Ok(())
    }

    async fn process_error(&self, error: PgWireError) -> Result<(), IOError> {
        todo!()
    }
}

#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
impl PgWireClient<TlsStream<TcpStream>> {
    pub fn from_socket(socket: TcpStream, tls_connector: TlsConnector) -> Self {
        let socket = Framed::new(socket, PgWireMessageClientCodec);
        todo!()
    }
}

fn get_addr(config: &Config) -> Result<String, IOError> {
    if config.get_hostaddrs().len() > 0 {
        return Ok(format!(
            "{}:{}",
            config.get_hostaddrs()[0].to_string(),
            config.get_ports().get(0).cloned().unwrap_or(5432u16)
        ));
    }

    if config.get_hosts().len() > 0 {
        match &config.get_hosts()[0] {
            Host::Tcp(host) => {
                return Ok(format!(
                    "{}:{}",
                    host,
                    config.get_ports().get(0).cloned().unwrap_or(5432u16)
                ))
            }
            _ => {
                return Err(IOError::new(ErrorKind::InvalidData, "Invalid host"));
            }
        }
    }

    Err(IOError::new(ErrorKind::InvalidData, "Invalid host"))
}

pub async fn connect(config: Config) -> Result<TcpStream, IOError> {
    TcpStream::connect(get_addr(&config)?).await
}

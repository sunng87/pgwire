use std::fs::File;
use std::io::{BufReader, Error as IOError};
use std::sync::Arc;

use pgwire::api::client::ClientInfo;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::DefaultSimpleQueryHandler;
use pgwire::tokio::client::PgWireClient;
use rustls_pki_types::CertificateDer;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

fn setup_tls() -> Result<Arc<ClientConfig>, IOError> {
    let mut roots = RootCertStore::empty();

    let cert = rustls_pemfile::certs(&mut BufReader::new(File::open("examples/ssl/server.crt")?))
        .collect::<Result<Vec<CertificateDer>, _>>()?;

    roots.add_parsable_certificates(cert);

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(Arc::new(config))
}

#[tokio::main]
pub async fn main() {
    let config = Arc::new(
        "host=127.0.0.1 port=5433 user=pgwire dbname=demo password=pencil sslmode=prefer"
            .parse()
            .unwrap(),
    );
    let startup_handler = DefaultStartupHandler::new();
    let mut client = PgWireClient::connect(
        config,
        startup_handler,
        Some(TlsConnector::from(setup_tls().unwrap())),
    )
    .await
    .unwrap();

    println!("{:?}", client.server_parameters());

    let simple_query_handler = DefaultSimpleQueryHandler::new();
    let result = client
        .simple_query(simple_query_handler, "SELECT 1")
        .await
        .unwrap()
        .remove(0);

    let mut reader = result.into_data_rows_reader();
    while let Some(mut row) = reader.next_row() {
        let value = row.next_value::<i8>();
        println!("{:?}", value);
    }
}

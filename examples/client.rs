use std::sync::Arc;

use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::ClientInfo;
use pgwire::tokio::client::PgWireClient;

#[tokio::main]
pub async fn main() {
    let config = Arc::new(
        "host=127.0.0.1 port=5432 user=pgwire dbname=demo password=pencil"
            .parse()
            .unwrap(),
    );
    let handler = DefaultStartupHandler::new();
    let client = PgWireClient::connect(config, handler, None).await.unwrap();

    println!("{:?}", client.server_parameters());
}

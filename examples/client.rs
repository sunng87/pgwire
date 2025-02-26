use std::sync::Arc;

use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::query::DefaultSimpleQueryHandler;
use pgwire::api::client::ClientInfo;
use pgwire::tokio::client::PgWireClient;

#[tokio::main]
pub async fn main() {
    let config = Arc::new(
        "host=127.0.0.1 port=5432 user=pgwire dbname=demo password=pencil"
            .parse()
            .unwrap(),
    );
    let startup_handler = DefaultStartupHandler::new();
    let mut client = PgWireClient::connect(config, startup_handler, None)
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
    loop {
        if let Some(mut row) = reader.next_row() {
            let value = row.next_value::<i8>();
            println!("{:?}", value);
        } else {
            break;
        };
    }
}

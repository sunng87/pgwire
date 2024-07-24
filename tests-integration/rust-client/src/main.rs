use std::time::SystemTime;

use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::{Client, SimpleQueryMessage};
use postgres_openssl::MakeTlsConnector;

fn main() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    postgres_openssl::set_postgresql_alpn(&mut builder).unwrap();
    let connector = MakeTlsConnector::new(builder.build());
    let mut client = Client::connect(
        "host=localhost port=5432 user=postgres password=pencil dbname=localdb keepalives=0 sslmode=require sslnegotiation=direct",
        connector,
    )
    .unwrap();

    let results = client.simple_query("SELECT * FROM testtable").unwrap();
    for row in results {
        if let SimpleQueryMessage::Row(row) = row {
            println!("{:?}", row.get(0));
            println!("{:?}", row.get(1));
            println!("{:?}", row.get(2));
        }
    }

    for row in client
        .query("SELECT * FROM testtable WHERE id = ?", &[&1])
        .unwrap()
    {
        println!("{:?}", row.get::<usize, Option<i32>>(0));
        println!("{:?}", row.get::<usize, Option<String>>(1));
        println!("{:?}", row.get::<usize, Option<SystemTime>>(2));
    }

    client
        .simple_query("INSERT INTO testtable VALUES (1)")
        .unwrap();

    client.close().unwrap();
}

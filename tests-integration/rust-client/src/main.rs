use std::time::SystemTime;

use postgres::{Client, NoTls, SimpleQueryMessage};

fn main() {
    let mut client = Client::connect(
        "host=localhost port=5432 user=postgres password=pencil dbname=localdb keepalives=0",
        NoTls,
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

    client.close().unwrap();
}

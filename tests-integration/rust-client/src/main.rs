use std::time::SystemTime;

use postgres::{Client, NoTls};

fn main() {
    let mut client = Client::connect(
        "host=localhost port=5432 user=postgres password=pencil dbname=localdb",
        NoTls,
    )
    .unwrap();

    for row in client.query("SELECT * FROM testtable", &[]).unwrap() {
        println!("{:?}", row.get::<usize, Option<i32>>(0));
        println!("{:?}", row.get::<usize, Option<String>>(1));
        println!("{:?}", row.get::<usize, Option<SystemTime>>(2));
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

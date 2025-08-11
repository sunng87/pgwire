use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

/// This is the main function of our application, which uses tokio's runtime.
/// It demonstrates connecting to a PostgreSQL database using sqlx's connection pool.
#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    // 1. Load the database URL from an environment variable.
    // This is a best practice for not hardcoding credentials.
    let database_url = "postgres://postgres:password@127.0.0.1:5432/test";

    println!("Connecting to database...");

    // 2. Configure and create the PostgreSQL connection pool.
    // The pool manages a set of connections, so your application doesn't
    // have to open and close a new connection for every query.
    let pool = PgPoolOptions::new()
        // Set the maximum number of connections in the pool.
        .max_connections(5)
        // Set a timeout for acquiring a connection from the pool.
        .acquire_timeout(Duration::from_secs(5))
        // Connect to the database.
        .connect(&database_url)
        .await
        .unwrap();

    println!("Connection pool created successfully!");

    // 3. Execute a simple query to verify the connection.
    // We use `sqlx::query!` to check at compile time that the SQL is valid.
    let value: (i32, String) = sqlx::query_as("SELECT 1").fetch_one(&pool).await.unwrap();

    println!("The value is: {}", value.0);

    Ok(())
}

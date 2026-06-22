//! ADBC (Arrow Database Connectivity) example for pgwire
//!
//! This example demonstrates how to create a PostgreSQL-compatible server using
//! pgwire with any ADBC-compatible database backend.
//!
//! # What is ADBC?
//!
//! ADBC (Arrow Database Connectivity) is a set of APIs and libraries for
//! Arrow-native database access. It provides a standard interface similar to
//! ODBC/JDBC but designed specifically for columnar data using Apache Arrow.
//!
//! Key benefits of ADBC:
//! - **Zero-copy data transfer**: Data flows as Arrow RecordBatches
//! - **Database-agnostic**: Same API works with any ADBC-compatible database
//! - **High performance**: Eliminates row-by-row serialization overhead
//!
//! # Architecture
//!
//! ```text
//! PostgreSQL Client (psql, etc.)
//!         │
//!         ▼ (PostgreSQL Wire Protocol)
//!     pgwire Server
//!         │
//!         ▼ (ADBC API via adbc_core)
//!     Any ADBC Driver (loaded dynamically)
//!         │
//!         ▼ (Arrow RecordBatches)
//!     Any Database (SQLite, PostgreSQL, DuckDB, etc.)
//! ```
//!
//! # ADBC Drivers
//!
//! This example uses the `adbc_core` driver manager to load ADBC drivers
//! dynamically. You can use any ADBC driver:
//!
//! - SQLite: `libadbc_driver_sqlite.so`
//! - PostgreSQL: `libadbc_driver_postgresql.so`
//! - DuckDB: `libadbc_driver_duckdb.so`
//! - Snowflake: `libadbc_driver_snowflake.so`
//! - Flight SQL: `libadbc_driver_flightsql.so`
//!
//! Install drivers via conda-forge:
//! ```bash
//! conda install -c conda-forge libadbc-driver-sqlite
//! ```
//!
//! # Running the server
//!
//! ```bash
//! # With SQLite driver (specify full path to driver library)
//! # Uses the driver's default database, which is typically IN-MEMORY (ephemeral)
//! cargo run --example adbc -- --driver /path/to/libadbc_driver_sqlite.so
//!
//! # With database URI (persistent on-disk file)
//! cargo run --example adbc -- --driver /path/to/libadbc_driver_sqlite.so --uri "file:mydb.sqlite"
//!
//! # With driver name (searches system paths)
//! cargo run --example adbc -- --driver-name adbc_driver_sqlite
//! ```
//!
//! # Testing with psql
//!
//! ```bash
//! psql -h 127.0.0.1 -p 5433 -U any_user
//! # Password: pencil
//! ```

use std::env;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use adbc_core::driver_manager::ManagedDriver;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Database, Driver, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, RecordBatch};
use arrow_array::RecordBatchReader;
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};
use async_trait::async_trait;
use futures::{stream, Sink, Stream};
use tokio::net::TcpListener;

use pgwire::api::auth::md5pass::{hash_md5_password, Md5PasswordAuthStartupHandler};
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

// ============================================================================
// Arrow to PostgreSQL Conversion
// ============================================================================

fn arrow_type_to_pg_type(arrow_type: &ArrowDataType) -> Type {
    match arrow_type {
        ArrowDataType::Boolean => Type::BOOL,
        ArrowDataType::Int8 | ArrowDataType::Int16 => Type::INT2,
        ArrowDataType::Int32 => Type::INT4,
        ArrowDataType::Int64 => Type::INT8,
        ArrowDataType::UInt8 | ArrowDataType::UInt16 => Type::INT2,
        ArrowDataType::UInt32 => Type::INT4,
        ArrowDataType::UInt64 => Type::INT8,
        ArrowDataType::Float16 | ArrowDataType::Float32 => Type::FLOAT4,
        ArrowDataType::Float64 => Type::FLOAT8,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Type::VARCHAR,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Type::BYTEA,
        ArrowDataType::Date32 | ArrowDataType::Date64 => Type::DATE,
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => Type::TIME,
        ArrowDataType::Timestamp(_, _) => Type::TIMESTAMP,
        _ => Type::VARCHAR,
    }
}

fn schema_to_field_info(schema: &ArrowSchema, format: &Format) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            FieldInfo::new(
                field.name().clone(),
                None,
                None,
                arrow_type_to_pg_type(field.data_type()),
                format.format_for(idx),
            )
        })
        .collect()
}

// ============================================================================
// pgwire Backend using ADBC Driver Manager
// ============================================================================

/// pgwire backend that uses ADBC driver manager for database connectivity
pub struct AdbcBackend {
    database: Arc<Mutex<ManagedDatabase>>,
    query_parser: Arc<NoopQueryParser>,
}

type ManagedDatabase = <ManagedDriver as Driver>::DatabaseType;

impl AdbcBackend {
    /// Create a new ADBC backend with the given managed database
    pub fn new(database: ManagedDatabase) -> Self {
        AdbcBackend {
            database: Arc::new(Mutex::new(database)),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }

    /// Execute a query using ADBC
    fn execute_query(
        &self,
        sql: &str,
    ) -> Result<QueryResult, Box<dyn std::error::Error + Send + Sync>> {
        let mut db = self.database.lock().unwrap();
        let mut conn = db.new_connection()?;
        let mut stmt = conn.new_statement()?;
        stmt.set_sql_query(sql)?;

        let reader = stmt.execute()?;
        let schema = reader.schema();

        // Collect all batches
        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result?);
        }

        Ok(QueryResult { schema, batches })
    }

    /// Execute an update using ADBC
    fn execute_update(&self, sql: &str) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let mut db = self.database.lock().unwrap();
        let mut conn = db.new_connection()?;
        let mut stmt = conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        let update_result = stmt.execute_update()?;
        Ok(update_result.unwrap_or(0))
    }
}

struct QueryResult {
    schema: Arc<ArrowSchema>,
    batches: Vec<RecordBatch>,
}

fn arrow_batches_to_rows(
    result: QueryResult,
    schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut results = Vec::new();

    for batch in result.batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut encoder = DataRowEncoder::new(schema.clone());

            for col_idx in 0..num_cols {
                let column = batch.column(col_idx);

                if column.is_null(row_idx) {
                    encoder.encode_field(&None::<i32>).unwrap();
                    continue;
                }

                match column.data_type() {
                    ArrowDataType::Boolean => {
                        let arr = column.as_boolean();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Int8 => {
                        let arr = column.as_primitive::<Int8Type>();
                        encoder.encode_field(&(arr.value(row_idx) as i16)).unwrap();
                    }
                    ArrowDataType::Int16 => {
                        let arr = column.as_primitive::<Int16Type>();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Int32 => {
                        let arr = column.as_primitive::<Int32Type>();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Int64 => {
                        let arr = column.as_primitive::<Int64Type>();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::UInt8 => {
                        let arr = column.as_primitive::<UInt8Type>();
                        encoder.encode_field(&(arr.value(row_idx) as i16)).unwrap();
                    }
                    ArrowDataType::UInt16 => {
                        let arr = column.as_primitive::<UInt16Type>();
                        encoder.encode_field(&(arr.value(row_idx) as i32)).unwrap();
                    }
                    ArrowDataType::UInt32 => {
                        let arr = column.as_primitive::<UInt32Type>();
                        encoder.encode_field(&(arr.value(row_idx) as i64)).unwrap();
                    }
                    ArrowDataType::UInt64 => {
                        let arr = column.as_primitive::<UInt64Type>();
                        encoder.encode_field(&(arr.value(row_idx) as i64)).unwrap();
                    }
                    ArrowDataType::Float32 => {
                        let arr = column.as_primitive::<Float32Type>();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Float64 => {
                        let arr = column.as_primitive::<Float64Type>();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Utf8 => {
                        let arr = column.as_string::<i32>();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::LargeUtf8 => {
                        let arr = column.as_string::<i64>();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    _ => {
                        // Fallback: convert to string representation
                        let s = format!("{:?}", column.slice(row_idx, 1));
                        encoder.encode_field(&s).unwrap();
                    }
                }
            }

            results.push(Ok(encoder.take_row()));
        }
    }

    stream::iter(results)
}

// ============================================================================
// Query Handlers
// ============================================================================

#[async_trait]
impl SimpleQueryHandler for AdbcBackend {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("ADBC Query: {}", query);

        let query_upper = query.trim().to_uppercase();

        if query_upper.starts_with("SELECT") || query_upper.starts_with("SHOW") {
            let result = self.execute_query(query).map_err(PgWireError::ApiError)?;

            let field_info = schema_to_field_info(&result.schema, &Format::UnifiedText);
            let schema = Arc::new(field_info);

            let rows = arrow_batches_to_rows(result, schema.clone());
            Ok(vec![Response::Query(QueryResponse::new(schema, rows))])
        } else {
            let affected = self.execute_update(query).map_err(PgWireError::ApiError)?;
            Ok(vec![Response::Execution(
                Tag::new("OK").with_rows(affected as usize),
            )])
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for AdbcBackend {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        let query_upper = query.trim().to_uppercase();

        if query_upper.starts_with("SELECT") || query_upper.starts_with("SHOW") {
            let result = self.execute_query(query).map_err(PgWireError::ApiError)?;

            let field_info = schema_to_field_info(&result.schema, &portal.result_column_format);
            let schema = Arc::new(field_info);

            let rows = arrow_batches_to_rows(result, schema.clone());
            Ok(Response::Query(QueryResponse::new(schema, rows)))
        } else {
            let affected = self.execute_update(query).map_err(PgWireError::ApiError)?;
            Ok(Response::Execution(
                Tag::new("OK").with_rows(affected as usize),
            ))
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let param_types: Vec<Type> = stmt
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::UNKNOWN))
            .collect();

        // Try to get schema by executing with LIMIT 0
        let fields = match self.execute_query(&format!(
            "SELECT * FROM ({}) LIMIT 0",
            stmt.statement
        )) {
            Ok(result) => schema_to_field_info(&result.schema, &Format::UnifiedBinary),
            Err(_) => vec![],
        };

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = match self.execute_query(&format!(
            "SELECT * FROM ({}) LIMIT 0",
            portal.statement.statement
        )) {
            Ok(result) => schema_to_field_info(&result.schema, &portal.result_column_format),
            Err(_) => vec![],
        };

        Ok(DescribePortalResponse::new(fields))
    }
}

// ============================================================================
// Authentication
// ============================================================================

#[derive(Debug)]
struct AdbcAuthSource;

#[async_trait]
impl AuthSource for AdbcAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        println!("Login attempt from user: {:?}", login_info.user());
        let salt = vec![0, 0, 0, 0];
        let password = "pencil";
        let hash_password =
            hash_md5_password(login_info.user().as_ref().unwrap(), password, salt.as_ref());
        Ok(Password::new(Some(salt), hash_password.as_bytes().to_vec()))
    }
}

// ============================================================================
// Server Factory
// ============================================================================

struct AdbcBackendFactory {
    handler: Arc<AdbcBackend>,
}

impl PgWireServerHandlers for AdbcBackendFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let mut params = DefaultServerParameterProvider::default();
        params.server_version = "pgwire ADBC Server".to_owned();

        Arc::new(Md5PasswordAuthStartupHandler::new(
            Arc::new(AdbcAuthSource),
            Arc::new(params),
        ))
    }
}

// ============================================================================
// CLI Argument Parsing
// ============================================================================

struct Config {
    driver_path: Option<String>,
    driver_name: Option<String>,
    entry_point: Option<String>,
    uri: Option<String>,
    port: u16,
}

fn parse_args() -> Config {
    let args: Vec<String> = env::args().collect();
    let mut config = Config {
        driver_path: None,
        driver_name: None,
        entry_point: None,
        uri: None,
        port: 5433,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--driver" | "-d" => {
                if i + 1 < args.len() {
                    config.driver_path = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--driver-name" | "-n" => {
                if i + 1 < args.len() {
                    config.driver_name = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--entry-point" | "-e" => {
                if i + 1 < args.len() {
                    config.entry_point = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--uri" | "-u" => {
                if i + 1 < args.len() {
                    config.uri = Some(args[i + 1].clone());
                    i += 1;
                }
            }
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    config.port = args[i + 1].parse().unwrap_or(5433);
                    i += 1;
                }
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    config
}

fn print_usage() {
    println!(
        r#"
pgwire ADBC Server - PostgreSQL-compatible server using any ADBC driver

USAGE:
    cargo run --example adbc -- [OPTIONS]

OPTIONS:
    -d, --driver <PATH>       Path to ADBC driver shared library
                              Example: /path/to/libadbc_driver_sqlite.so

    -n, --driver-name <NAME>  Driver name (without lib prefix and extension)
                              Example: adbc_driver_sqlite

    -e, --entry-point <NAME>  Driver entry point function name
                              Example: SqliteDriverInit

    -u, --uri <URI>           Database URI
                              Example: file:mydb.sqlite

    -p, --port <PORT>         Server port (default: 5433)

    -h, --help                Print this help message

EXAMPLES:
    # Using SQLite driver with full path
    cargo run --example adbc -- --driver /opt/conda/lib/libadbc_driver_sqlite.so

    # Using driver name (searches system paths)
    cargo run --example adbc -- --driver-name adbc_driver_sqlite

    # With database URI
    cargo run --example adbc -- --driver /path/to/driver.so --uri "file:test.db"

INSTALLING DRIVERS:
    # Via conda-forge
    conda install -c conda-forge libadbc-driver-sqlite
    conda install -c conda-forge libadbc-driver-postgresql

    # Driver libraries will be in $CONDA_PREFIX/lib/
"#
    );
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
pub async fn main() {
    let config = parse_args();

    // Load the ADBC driver
    let mut driver = if let Some(path) = &config.driver_path {
        let entry_point = config.entry_point.as_ref().map(|s| s.as_bytes().to_vec());
        ManagedDriver::load_dynamic_from_filename(path, entry_point.as_deref(), AdbcVersion::V110)
            .unwrap_or_else(|e| panic!("Failed to load driver from {}: {}", path, e))
    } else if let Some(name) = &config.driver_name {
        let entry_point = config.entry_point.as_ref().map(|s| s.as_bytes().to_vec());
        ManagedDriver::load_dynamic_from_name(name, entry_point.as_deref(), AdbcVersion::V110)
            .unwrap_or_else(|e| panic!("Failed to load driver {}: {}", name, e))
    } else {
        eprintln!("Error: No driver specified. Use --driver or --driver-name.");
        eprintln!("Run with --help for usage information.");
        std::process::exit(1);
    };

    // Create database with optional URI
    let database = if let Some(uri) = &config.uri {
        let opts = [(OptionDatabase::Uri, uri.as_str().into())];
        driver
            .new_database_with_opts(opts)
            .expect("Failed to create database with URI")
    } else {
        driver.new_database().expect("Failed to create database")
    };

    // Create the pgwire backend with our ADBC database
    let backend = AdbcBackend::new(database);
    let factory = Arc::new(AdbcBackendFactory {
        handler: Arc::new(backend),
    });

    let server_addr = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(&server_addr).await.unwrap();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║              pgwire + ADBC Example Server                    ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ Listening on: {:44}║", server_addr);
    if let Some(path) = &config.driver_path {
        println!("║ Driver: {:50}║", path);
    } else if let Some(name) = &config.driver_name {
        println!("║ Driver: {:50}║", name);
    }
    if let Some(uri) = &config.uri {
        println!("║ URI: {:53}║", uri);
    }
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!(
        "║ Connect: psql -h 127.0.0.1 -p {} -U any_user {:15}║",
        config.port, ""
    );
    println!("║ Password: pencil                                             ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ Architecture:                                                ║");
    println!("║                                                              ║");
    println!("║   PostgreSQL Client (psql)                                   ║");
    println!("║          │                                                   ║");
    println!("║          ▼ PostgreSQL Wire Protocol                          ║");
    println!("║      pgwire Server                                           ║");
    println!("║          │                                                   ║");
    println!("║          ▼ ADBC API (adbc_core driver_manager)               ║");
    println!("║      ADBC Driver (loaded dynamically)                        ║");
    println!("║          │                                                   ║");
    println!("║          ▼ Arrow RecordBatches                               ║");
    println!("║      Database                                                ║");
    println!("║                                                              ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();
        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}

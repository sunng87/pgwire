//! DuckDB example using ADBC (Arrow Database Connectivity) pattern
//!
//! This example demonstrates how to create a PostgreSQL-compatible server using
//! pgwire that connects to DuckDB via ADBC (Arrow Database Connectivity) traits.
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
//!         ▼ (ADBC API - Driver/Database/Connection/Statement)
//!    DuckDB ADBC Driver
//!         │
//!         ▼ (Arrow RecordBatches)
//!       DuckDB
//! ```
//!
//! # About This Example
//!
//! This example implements ADBC traits locally because native Rust ADBC drivers
//! for DuckDB are not yet available on crates.io. The official ADBC project
//! provides drivers in C/C++ that can be loaded via `adbc_driver_manager`.
//!
//! **In production**, you would use the `adbc_driver_manager` crate to load
//! official ADBC drivers dynamically:
//!
//! ```ignore
//! use adbc_driver_manager::ManagedDriver;
//!
//! // Load the DuckDB ADBC driver (requires libduckdb.so on your system)
//! let driver = ManagedDriver::load_dynamic_from_name(
//!     "adbc_driver_duckdb",
//!     Some("duckdb_adbc_init"),
//!     AdbcVersion::V110
//! )?;
//!
//! // From here, the API is the same regardless of which database you use!
//! let database = driver.new_database()?;
//! let connection = database.new_connection()?;
//! let statement = connection.new_statement()?;
//! ```
//!
//! The beauty of ADBC is that once you have a driver, switching databases
//! only requires changing the driver initialization - all other code stays
//! the same.
//!
//! # ADBC Trait Hierarchy
//!
//! The four core ADBC traits that form the driver interface:
//!
//! 1. **`AdbcDriver`** - Entry point for creating database connections
//! 2. **`AdbcDatabase`** - Represents a database instance
//! 3. **`AdbcConnection`** - Represents an active database connection
//! 4. **`AdbcStatement`** - Represents a prepared or ad-hoc SQL statement
//!
//! # Data Flow
//!
//! 1. psql sends SQL query via PostgreSQL wire protocol
//! 2. pgwire parses the protocol and calls the query handler
//! 3. Query handler uses ADBC API to execute the query
//! 4. Arrow RecordBatches are returned (zero-copy from database)
//! 5. Results are converted to pgwire DataRows and sent back to client
//!
//! # Running the server
//!
//! ```bash
//! cargo run --example duckdb
//! ```
//!
//! # Testing with psql
//!
//! ```bash
//! psql -h 127.0.0.1 -p 5433 -U any_user
//! # Password: pencil
//! ```
//!
//! # Accessing data with DuckDB CLI
//!
//! The database is persisted to `/tmp/pgwire_duckdb.db`. To access the data
//! with the DuckDB CLI:
//!
//! 1. **Stop the pgwire server first** (Ctrl+C) - DuckDB only allows one writer
//! 2. Open the database with DuckDB CLI:
//!
//! ```bash
//! duckdb /tmp/pgwire_duckdb.db
//! ```
//!
//! 3. Query your tables:
//!
//! ```sql
//! SHOW TABLES;
//! SELECT * FROM test_table;
//! ```

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
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
// ADBC Driver Implementation for DuckDB
//
// This implements the ADBC pattern to provide a standardized Arrow-native
// interface to DuckDB.
// ============================================================================

/// ADBC Error type
#[derive(Debug)]
pub struct AdbcError {
    message: String,
}

impl std::fmt::Display for AdbcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ADBC Error: {}", self.message)
    }
}

impl std::error::Error for AdbcError {}

impl From<duckdb::Error> for AdbcError {
    fn from(e: duckdb::Error) -> Self {
        AdbcError {
            message: e.to_string(),
        }
    }
}

pub type AdbcResult<T> = Result<T, AdbcError>;

/// ADBC Driver trait - entry point for creating database connections
pub trait AdbcDriver: Send + Sync {
    type Database: AdbcDatabase;

    /// Create a new database handle
    fn new_database(&self) -> AdbcResult<Self::Database>;

    /// Create a new database handle with options
    fn new_database_with_opts(&self, opts: HashMap<String, String>) -> AdbcResult<Self::Database>;
}

/// ADBC Database trait - represents a database instance
pub trait AdbcDatabase: Send + Sync {
    type Connection: AdbcConnection;

    /// Create a new connection to the database
    fn new_connection(&self) -> AdbcResult<Self::Connection>;
}

/// ADBC Connection trait - represents an active database connection
pub trait AdbcConnection: Send + Sync {
    type Statement: AdbcStatement;

    /// Create a new statement on this connection
    fn new_statement(&self) -> AdbcResult<Self::Statement>;
}

/// ADBC Statement trait - represents a prepared or ad-hoc SQL statement
pub trait AdbcStatement: Send + Sync {
    /// Set the SQL query to execute
    fn set_sql_query(&mut self, query: &str) -> AdbcResult<()>;

    /// Execute the query and return Arrow RecordBatches
    fn execute_query(&mut self) -> AdbcResult<Vec<RecordBatch>>;

    /// Execute a statement that doesn't return results (DDL, DML)
    fn execute_update(&mut self) -> AdbcResult<i64>;

    /// Get the schema of the result set (without executing)
    fn get_result_schema(&self) -> AdbcResult<Option<Arc<ArrowSchema>>>;
}

// ============================================================================
// DuckDB ADBC Implementation
// ============================================================================

/// DuckDB ADBC Driver
pub struct DuckDbDriver;

impl DuckDbDriver {
    pub fn new() -> Self {
        DuckDbDriver
    }
}

impl AdbcDriver for DuckDbDriver {
    type Database = DuckDbDatabase;

    fn new_database(&self) -> AdbcResult<DuckDbDatabase> {
        self.new_database_with_opts(HashMap::new())
    }

    fn new_database_with_opts(&self, opts: HashMap<String, String>) -> AdbcResult<DuckDbDatabase> {
        let path = opts.get("path").map(|s| s.as_str()).unwrap_or(":memory:");
        let conn = if path == ":memory:" {
            duckdb::Connection::open_in_memory()?
        } else {
            duckdb::Connection::open(path)?
        };
        Ok(DuckDbDatabase {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

/// DuckDB ADBC Database
pub struct DuckDbDatabase {
    conn: Arc<Mutex<duckdb::Connection>>,
}

impl AdbcDatabase for DuckDbDatabase {
    type Connection = DuckDbConnection;

    fn new_connection(&self) -> AdbcResult<DuckDbConnection> {
        Ok(DuckDbConnection {
            conn: self.conn.clone(),
        })
    }
}

/// DuckDB ADBC Connection
pub struct DuckDbConnection {
    conn: Arc<Mutex<duckdb::Connection>>,
}

impl AdbcConnection for DuckDbConnection {
    type Statement = DuckDbStatement;

    fn new_statement(&self) -> AdbcResult<DuckDbStatement> {
        Ok(DuckDbStatement {
            conn: self.conn.clone(),
            query: None,
        })
    }
}

/// DuckDB ADBC Statement
pub struct DuckDbStatement {
    conn: Arc<Mutex<duckdb::Connection>>,
    query: Option<String>,
}

impl AdbcStatement for DuckDbStatement {
    fn set_sql_query(&mut self, query: &str) -> AdbcResult<()> {
        self.query = Some(query.to_string());
        Ok(())
    }

    fn execute_query(&mut self) -> AdbcResult<Vec<RecordBatch>> {
        let query = self.query.as_ref().ok_or_else(|| AdbcError {
            message: "No query set".to_string(),
        })?;

        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(query)?;

        // Use DuckDB's native Arrow support
        let batches: Vec<RecordBatch> = stmt
            .query_arrow([])
            .map_err(|e| AdbcError {
                message: e.to_string(),
            })?
            .collect();

        Ok(batches)
    }

    fn execute_update(&mut self) -> AdbcResult<i64> {
        let query = self.query.as_ref().ok_or_else(|| AdbcError {
            message: "No query set".to_string(),
        })?;

        let conn = self.conn.lock().unwrap();
        let affected = conn.execute(query, [])? as i64;

        // Force checkpoint to ensure data is persisted to disk
        let _ = conn.execute("CHECKPOINT", []);

        Ok(affected)
    }

    fn get_result_schema(&self) -> AdbcResult<Option<Arc<ArrowSchema>>> {
        let query = match &self.query {
            Some(q) => q,
            None => return Ok(None),
        };

        // Execute with LIMIT 0 to get schema
        let conn = self.conn.lock().unwrap();
        let wrapped_query = format!("SELECT * FROM ({}) LIMIT 0", query);
        let mut stmt = conn.prepare(&wrapped_query)?;
        let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

        Ok(batches.first().map(|b| b.schema()))
    }
}

// ============================================================================
// pgwire Backend using ADBC
// ============================================================================

/// pgwire backend that uses ADBC to connect to DuckDB
pub struct AdbcBackend {
    database: DuckDbDatabase,
    query_parser: Arc<NoopQueryParser>,
}

impl AdbcBackend {
    fn new() -> Self {
        // Use a file-based database so it can be accessed by DuckDB CLI
        let db_path = "/tmp/pgwire_duckdb.db";
        println!("Opening DuckDB database at: {}", db_path);

        let mut opts = HashMap::new();
        opts.insert("path".to_string(), db_path.to_string());

        let driver = DuckDbDriver::new();
        let database = driver.new_database_with_opts(opts).expect("Failed to create database");

        // Verify file was created
        if std::path::Path::new(db_path).exists() {
            println!("Database file created successfully");
        } else {
            println!("WARNING: Database file was NOT created!");
        }

        // Initialize with sample data
        {
            let conn = database.new_connection().expect("Failed to create connection");
            let mut stmt = conn.new_statement().expect("Failed to create statement");

            stmt.set_sql_query(
                r"
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    value DOUBLE,
                    active BOOLEAN DEFAULT true
                );
                INSERT INTO test_table (id, name, value, active)
                SELECT 1, 'Alice', 100.5, true WHERE NOT EXISTS (SELECT 1 FROM test_table WHERE id=1);
                INSERT INTO test_table (id, name, value, active)
                SELECT 2, 'Bob', 200.75, false WHERE NOT EXISTS (SELECT 1 FROM test_table WHERE id=2);
                INSERT INTO test_table (id, name, value, active)
                SELECT 3, 'Charlie', 300.25, true WHERE NOT EXISTS (SELECT 1 FROM test_table WHERE id=3);
                ",
            )
            .unwrap();
            stmt.execute_update().unwrap();
        }

        AdbcBackend {
            database,
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }

    /// Execute a query using ADBC and return Arrow RecordBatches
    fn execute_query_adbc(&self, sql: &str) -> AdbcResult<Vec<RecordBatch>> {
        let conn = self.database.new_connection()?;
        let mut stmt = conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.execute_query()
    }

    /// Execute an update using ADBC
    fn execute_update_adbc(&self, sql: &str) -> AdbcResult<i64> {
        let conn = self.database.new_connection()?;
        let mut stmt = conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.execute_update()
    }
}

// ============================================================================
// Authentication
// ============================================================================

#[derive(Debug)]
struct DuckDbAuthSource;

#[async_trait]
impl AuthSource for DuckDbAuthSource {
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

fn arrow_batches_to_rows(
    batches: Vec<RecordBatch>,
    schema: Arc<Vec<FieldInfo>>,
) -> impl Stream<Item = PgWireResult<DataRow>> {
    let mut results = Vec::new();

    for batch in batches {
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
                        let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Int16 => {
                        let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Int32 => {
                        let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Int64 => {
                        let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Float32 => {
                        let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Float64 => {
                        let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::Utf8 => {
                        let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    ArrowDataType::LargeUtf8 => {
                        let arr = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                        encoder.encode_field(&arr.value(row_idx)).unwrap();
                    }
                    _ => {
                        let s = format!("{:?}", column.slice(row_idx, 1));
                        encoder.encode_field(&s).unwrap();
                    }
                }
            }

            results.push(encoder.finish());
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
            // Use ADBC to execute the query
            let batches = self
                .execute_query_adbc(query)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            if batches.is_empty() {
                let schema = Arc::new(vec![]);
                let rows = stream::iter(vec![]);
                return Ok(vec![Response::Query(QueryResponse::new(schema, rows))]);
            }

            let arrow_schema = batches[0].schema();
            let field_info = schema_to_field_info(&arrow_schema, &Format::UnifiedText);
            let schema = Arc::new(field_info);

            let rows = arrow_batches_to_rows(batches, schema.clone());
            Ok(vec![Response::Query(QueryResponse::new(schema, rows))])
        } else {
            // Use ADBC to execute the update
            let affected = self
                .execute_update_adbc(query)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
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
            let batches = self
                .execute_query_adbc(query)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            if batches.is_empty() {
                let schema = Arc::new(vec![]);
                let rows = stream::iter(vec![]);
                return Ok(Response::Query(QueryResponse::new(schema, rows)));
            }

            let arrow_schema = batches[0].schema();
            let field_info = schema_to_field_info(&arrow_schema, &portal.result_column_format);
            let schema = Arc::new(field_info);

            let rows = arrow_batches_to_rows(batches, schema.clone());
            Ok(Response::Query(QueryResponse::new(schema, rows)))
        } else {
            let affected = self
                .execute_update_adbc(query)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
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

        // Use ADBC to get the schema
        let conn = self.database.new_connection().map_err(|e| {
            PgWireError::ApiError(Box::new(e))
        })?;
        let mut adbc_stmt = conn.new_statement().map_err(|e| {
            PgWireError::ApiError(Box::new(e))
        })?;
        adbc_stmt.set_sql_query(&stmt.statement).map_err(|e| {
            PgWireError::ApiError(Box::new(e))
        })?;

        let fields = match adbc_stmt.get_result_schema() {
            Ok(Some(schema)) => schema_to_field_info(&schema, &Format::UnifiedBinary),
            _ => vec![],
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
        let conn = self.database.new_connection().map_err(|e| {
            PgWireError::ApiError(Box::new(e))
        })?;
        let mut stmt = conn.new_statement().map_err(|e| {
            PgWireError::ApiError(Box::new(e))
        })?;
        stmt.set_sql_query(&portal.statement.statement).map_err(|e| {
            PgWireError::ApiError(Box::new(e))
        })?;

        let fields = match stmt.get_result_schema() {
            Ok(Some(schema)) => schema_to_field_info(&schema, &portal.result_column_format),
            _ => vec![],
        };

        Ok(DescribePortalResponse::new(fields))
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
        params.server_version = "DuckDB via ADBC (pgwire)".to_owned();

        Arc::new(Md5PasswordAuthStartupHandler::new(
            Arc::new(DuckDbAuthSource),
            Arc::new(params),
        ))
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
pub async fn main() {
    let factory = Arc::new(AdbcBackendFactory {
        handler: Arc::new(AdbcBackend::new()),
    });

    let server_addr = "127.0.0.1:5433";
    let listener = TcpListener::bind(server_addr).await.unwrap();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         pgwire + ADBC + DuckDB Example Server                ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ Listening on: {}                               ║", server_addr);
    println!("║ Connect: psql -h 127.0.0.1 -p 5433 -U any_user               ║");
    println!("║ Password: pencil                                             ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ Architecture:                                                ║");
    println!("║                                                              ║");
    println!("║   PostgreSQL Client (psql)                                   ║");
    println!("║          │                                                   ║");
    println!("║          ▼ PostgreSQL Wire Protocol                          ║");
    println!("║      pgwire Server                                           ║");
    println!("║          │                                                   ║");
    println!("║          ▼ ADBC API (Driver/Database/Connection/Statement)   ║");
    println!("║     DuckDB ADBC Driver                                       ║");
    println!("║          │                                                   ║");
    println!("║          ▼ Arrow RecordBatches                               ║");
    println!("║        DuckDB (/tmp/pgwire_duckdb.db)                         ║");
    println!("║                                                              ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ To access data with DuckDB CLI:                              ║");
    println!("║   1. Stop this server (Ctrl+C)                               ║");
    println!("║   2. Run: duckdb /tmp/pgwire_duckdb.db                        ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ Try: SELECT * FROM test_table;                               ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();
        tokio::spawn(async move {
            process_socket(incoming_socket.0, None, factory_ref).await
        });
    }
}

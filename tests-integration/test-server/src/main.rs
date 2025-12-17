use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use futures::{stream, StreamExt};
use rust_decimal::Decimal;
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use pgwire::api::auth::sasl::scram::{gen_salted_password, ScramAuth};
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeResponse, DescribeStatementResponse, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

const ITERATIONS: usize = 4096;
#[derive(Debug)]
struct DummyAuthSource;

#[async_trait]
impl AuthSource for DummyAuthSource {
    async fn get_password(&self, login_info: &LoginInfo) -> PgWireResult<Password> {
        println!("login info: {:?}", login_info);

        let password = "pencil";
        let salt = vec![0, 20, 40, 80];

        let hash_password = gen_salted_password(password, salt.as_ref(), ITERATIONS);
        Ok(Password::new(Some(salt), hash_password))
    }
}

#[derive(Default)]
struct DummyDatabase {
    query_parser: Arc<NoopQueryParser>,
}

impl DummyDatabase {
    fn schema(&self, format: &Format) -> Vec<FieldInfo> {
        let f1 = FieldInfo::new("id".into(), None, None, Type::INT4, format.format_for(0));
        let f2 = FieldInfo::new(
            "name".into(),
            None,
            None,
            Type::VARCHAR,
            format.format_for(1),
        );
        let f3 = FieldInfo::new(
            "ts".into(),
            None,
            None,
            Type::TIMESTAMP,
            format.format_for(2),
        );
        let f4 = FieldInfo::new(
            "signed".into(),
            None,
            None,
            Type::BOOL,
            format.format_for(3),
        );
        let f5 = FieldInfo::new("data".into(), None, None, Type::BYTEA, format.format_for(4));
        vec![f1, f2, f3, f4, f5]
    }
}

#[async_trait]
impl SimpleQueryHandler for DummyDatabase {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("simple query: {:?}", query);
        if query.starts_with("SELECT") {
            let schema = Arc::new(self.schema(&Format::UnifiedText));
            let schema_ref = schema.clone();
            let data = vec![
                (
                    Some(0),
                    Some("Tom"),
                    Some("2023-02-01 22:27:25.042674"),
                    Some(true),
                    Some("tomcat".as_bytes()),
                ),
                (
                    Some(1),
                    Some("Jerry"),
                    Some("2023-02-01 22:27:42.165585"),
                    Some(false),
                    Some("".as_bytes()),
                ),
                (Some(2), None, None, None, None),
            ];
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            let data_row_stream = stream::iter(data).map(move |r| {
                encoder.encode_field(&r.0)?;
                encoder.encode_field(&r.1)?;
                encoder.encode_field(&r.2)?;
                encoder.encode_field(&r.3)?;
                encoder.encode_field(&r.4)?;

                Ok(encoder.take_row())
            });

            Ok(vec![Response::Query(QueryResponse::new(
                schema,
                Box::pin(data_row_stream),
            ))])
        } else {
            Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for DummyDatabase {
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
        println!("extended query: {:?}", query);
        if query.starts_with("SELECT") {
            // try to parse all parameters
            for idx in 0..portal.parameter_len() {
                let param_type = portal
                    .statement
                    .parameter_types
                    .get(idx)
                    .and_then(|f| f.clone());

                match &param_type {
                    Some(Type::INT8) => {
                        let _ = portal.parameter::<i64>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::INT4) => {
                        let _ = portal.parameter::<i32>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::INT2) => {
                        let _ = portal.parameter::<i16>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::FLOAT4) => {
                        let _ = portal.parameter::<f32>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::FLOAT8) => {
                        let _ = portal.parameter::<f64>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::TEXT) | Some(Type::VARCHAR) => {
                        let _ = portal.parameter::<String>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::TIMESTAMP) => {
                        let _ =
                            portal.parameter::<NaiveDateTime>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::TIMESTAMPTZ) => {
                        let _ = portal.parameter::<DateTime<FixedOffset>>(
                            idx,
                            param_type.as_ref().unwrap(),
                        )?;
                    }
                    Some(Type::DATE) => {
                        let _ = portal.parameter::<NaiveDate>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::NUMERIC) => {
                        let _ = portal.parameter::<Decimal>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::BYTEA) => {
                        let _ = portal.parameter::<Vec<u8>>(idx, param_type.as_ref().unwrap())?;
                    }
                    Some(Type::BOOL) => {
                        let _ = portal.parameter::<bool>(idx, param_type.as_ref().unwrap())?;
                    }
                    _ => {
                        // JDBC doesn't provide type information for some cases,
                        // we try to parse them as TIMESTAMP or DATE accordingly

                        if let Err(_) = portal.parameter::<NaiveDateTime>(idx, &Type::TIMESTAMP) {
                            let _ = portal.parameter::<NaiveDate>(idx, &Type::DATE);
                        }
                    }
                }
            }

            let data = vec![
                (
                    Some(0),
                    Some("Tom"),
                    Some(SystemTime::now()),
                    Some(true),
                    Some("tomcat".as_bytes()),
                ),
                (
                    Some(1),
                    Some("Jerry"),
                    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(86400 * 5000)),
                    Some(false),
                    Some("".as_bytes()),
                ),
                (Some(2), None, None, None, None),
            ];
            let schema = Arc::new(self.schema(&portal.result_column_format));
            let schema_ref = schema.clone();
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            let data_row_stream = stream::iter(data).map(move |r| {
                encoder.encode_field(&r.0)?;
                encoder.encode_field(&r.1)?;
                encoder.encode_field(&r.2)?;
                encoder.encode_field(&r.3)?;
                encoder.encode_field(&r.4)?;

                Ok(encoder.take_row())
            });

            Ok(Response::Query(QueryResponse::new(
                schema,
                Box::pin(data_row_stream),
            )))
        } else {
            Ok(Response::Execution(Tag::new("OK").with_rows(1)))
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
        println!("describe: {:?}", stmt);
        let param_types = vec![Type::INT4];
        let schema = self.schema(&Format::UnifiedText);
        Ok(DescribeStatementResponse::new(param_types, schema))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("describe: {:?}", portal);
        if portal.statement.statement.starts_with("SELECT") {
            let schema = self.schema(&portal.result_column_format);
            Ok(DescribePortalResponse::new(schema))
        } else {
            Ok(DescribePortalResponse::no_data())
        }
    }
}

struct DummyDatabaseFactory(Arc<DummyDatabase>);

impl PgWireServerHandlers for DummyDatabaseFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.0.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.0.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let mut scram = ScramAuth::new(Arc::new(DummyAuthSource));
        scram.set_iterations(ITERATIONS);

        let authenticator =
            SASLAuthStartupHandler::new(Arc::new(DefaultServerParameterProvider::default()))
                .with_scram(scram);

        Arc::new(authenticator)
    }
}

fn setup_tls() -> Result<TlsAcceptor, IOError> {
    let cert = certs(&mut BufReader::new(File::open(
        "../../examples/ssl/server.crt",
    )?))
    .collect::<Result<Vec<CertificateDer>, IOError>>()?;

    let key = pkcs8_private_keys(&mut BufReader::new(File::open(
        "../../examples/ssl/server.key",
    )?))
    .map(|key| key.map(PrivateKeyDer::from))
    .collect::<Result<Vec<PrivateKeyDer>, IOError>>()?
    .remove(0);

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert, key)
        .map_err(|err| IOError::new(ErrorKind::InvalidInput, err))?;

    config.alpn_protocols = vec![b"postgresql".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[tokio::main]
pub async fn main() {
    let factory = Arc::new(DummyDatabaseFactory(Arc::new(DummyDatabase::default())));

    let server_addr = "127.0.0.1:5432";
    let tls_acceptor = setup_tls().unwrap();
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let tls_acceptor_ref = tls_acceptor.clone();
        let factory_ref = factory.clone();

        tokio::spawn(async move {
            process_socket(incoming_socket.0, Some(tls_acceptor_ref), factory_ref).await
        });
    }
}

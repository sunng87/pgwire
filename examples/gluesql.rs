use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use tokio::net::TcpListener;

use gluesql::prelude::*;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::tokio::process_socket;

pub struct GluesqlProcessor {
    glue: Arc<Mutex<Glue<MemoryStorage>>>,
}

#[async_trait]
impl SimpleQueryHandler for GluesqlProcessor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("{:?}", query);
        let mut glue = self.glue.lock().unwrap();
        futures::executor::block_on(glue.execute(query))
            .map_err(|err| PgWireError::ApiError(Box::new(err)))
            .and_then(|payloads| {
                payloads
                    .iter()
                    .map(|payload| match payload {
                        Payload::Select { labels, rows } => {
                            let fields = labels
                                .iter()
                                .map(|label| {
                                    FieldInfo::new(
                                        label.into(),
                                        None,
                                        None,
                                        Type::UNKNOWN,
                                        FieldFormat::Text,
                                    )
                                })
                                .collect::<Vec<_>>();
                            let fields = Arc::new(fields);

                            let mut results = Vec::with_capacity(rows.len());
                            for row in rows {
                                let mut encoder = DataRowEncoder::new(fields.clone());
                                for field in row.iter() {
                                    match field {
                                        Value::Bool(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::BOOL,
                                                FieldFormat::Text,
                                            )?,
                                        Value::I8(v) => encoder.encode_field_with_type_and_format(
                                            v,
                                            &Type::CHAR,
                                            FieldFormat::Text,
                                        )?,
                                        Value::I16(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::INT2,
                                                FieldFormat::Text,
                                            )?,
                                        Value::I32(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::INT4,
                                                FieldFormat::Text,
                                            )?,
                                        Value::I64(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::INT8,
                                                FieldFormat::Text,
                                            )?,
                                        Value::U8(v) => encoder.encode_field_with_type_and_format(
                                            &(*v as i8),
                                            &Type::CHAR,
                                            FieldFormat::Text,
                                        )?,
                                        Value::F64(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::FLOAT8,
                                                FieldFormat::Text,
                                            )?,
                                        Value::Str(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::VARCHAR,
                                                FieldFormat::Text,
                                            )?,
                                        Value::Bytea(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::BYTEA,
                                                FieldFormat::Text,
                                            )?,
                                        Value::Date(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::DATE,
                                                FieldFormat::Text,
                                            )?,
                                        Value::Time(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::TIME,
                                                FieldFormat::Text,
                                            )?,
                                        Value::Timestamp(v) => encoder
                                            .encode_field_with_type_and_format(
                                                v,
                                                &Type::TIMESTAMP,
                                                FieldFormat::Text,
                                            )?,
                                        _ => unimplemented!(),
                                    }
                                }
                                results.push(encoder.finish());
                            }

                            Ok(Response::Query(QueryResponse::new(
                                fields,
                                stream::iter(results.into_iter()),
                            )))
                        }
                        Payload::Insert(rows) => Ok(Response::Execution(
                            Tag::new("INSERT").with_oid(0).with_rows(*rows),
                        )),
                        Payload::Delete(rows) => {
                            Ok(Response::Execution(Tag::new("DELETE").with_rows(*rows)))
                        }
                        Payload::Update(rows) => {
                            Ok(Response::Execution(Tag::new("UPDATE").with_rows(*rows)))
                        }
                        Payload::Create => Ok(Response::Execution(Tag::new("CREATE TABLE"))),
                        Payload::AlterTable => Ok(Response::Execution(Tag::new("ALTER TABLE"))),
                        Payload::DropTable => Ok(Response::Execution(Tag::new("DROP TABLE"))),
                        Payload::CreateIndex => Ok(Response::Execution(Tag::new("CREATE INDEX"))),
                        Payload::DropIndex => Ok(Response::Execution(Tag::new("DROP INDEX"))),
                        _ => {
                            unimplemented!()
                        }
                    })
                    .collect::<Result<Vec<Response>, PgWireError>>()
            })
    }
}

#[tokio::main]
pub async fn main() {
    let gluesql = GluesqlProcessor {
        glue: Arc::new(Mutex::new(Glue::new(MemoryStorage::default()))),
    };

    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(gluesql)));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));
    let noop_copy_handler = Arc::new(NoopCopyHandler);

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        let copy_handler_ref = noop_copy_handler.clone();

        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
                copy_handler_ref,
            )
            .await
        });
    }
}

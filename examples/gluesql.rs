use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use tokio::net::TcpListener;

use gluesql::prelude::*;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{query_response, DataRowEncoder, FieldFormat, FieldInfo, Response, Tag};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::FORMAT_CODE_TEXT;
use pgwire::tokio::process_socket;

pub struct GluesqlProcessor {
    glue: Arc<Mutex<Glue<MemoryStorage>>>,
}

#[async_trait]
impl SimpleQueryHandler for GluesqlProcessor {
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("{:?}", query);
        let mut glue = self.glue.lock().unwrap();
        glue.execute(query)
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
                            let nfields = fields.len();

                            let mut results = Vec::with_capacity(rows.len());
                            for row in rows {
                                let mut encoder = DataRowEncoder::new(nfields);
                                for field in row.iter() {
                                    match field {
                                        Value::Bool(v) => encoder.encode_field(
                                            v,
                                            &Type::BOOL,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::I8(v) => encoder.encode_field(
                                            v,
                                            &Type::CHAR,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::I16(v) => encoder.encode_field(
                                            v,
                                            &Type::INT2,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::I32(v) => encoder.encode_field(
                                            v,
                                            &Type::INT4,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::I64(v) => encoder.encode_field(
                                            v,
                                            &Type::INT8,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::U8(v) => encoder.encode_field(
                                            &(*v as i8),
                                            &Type::CHAR,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::F64(v) => encoder.encode_field(
                                            v,
                                            &Type::FLOAT8,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::Str(v) => encoder.encode_field(
                                            v,
                                            &Type::VARCHAR,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::Bytea(v) => encoder.encode_field(
                                            v,
                                            &Type::BYTEA,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::Date(v) => encoder.encode_field(
                                            v,
                                            &Type::DATE,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::Time(v) => encoder.encode_field(
                                            v,
                                            &Type::TIME,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        Value::Timestamp(v) => encoder.encode_field(
                                            v,
                                            &Type::TIMESTAMP,
                                            FORMAT_CODE_TEXT,
                                        )?,
                                        _ => unimplemented!(),
                                    }
                                }
                                results.push(encoder.finish());
                            }

                            Ok(Response::Query(query_response(
                                Some(fields),
                                stream::iter(results.into_iter()),
                            )))
                        }
                        Payload::Insert(rows) => Ok(Response::Execution(Tag::new_for_execution(
                            "Insert",
                            Some(*rows),
                        ))),
                        Payload::Delete(rows) => Ok(Response::Execution(Tag::new_for_execution(
                            "Delete",
                            Some(*rows),
                        ))),
                        Payload::Update(rows) => Ok(Response::Execution(Tag::new_for_execution(
                            "Update",
                            Some(*rows),
                        ))),
                        Payload::Create => {
                            Ok(Response::Execution(Tag::new_for_execution("Create", None)))
                        }
                        Payload::AlterTable => Ok(Response::Execution(Tag::new_for_execution(
                            "Alter Table",
                            None,
                        ))),
                        Payload::DropTable => Ok(Response::Execution(Tag::new_for_execution(
                            "Drop Table",
                            None,
                        ))),
                        Payload::CreateIndex => Ok(Response::Execution(Tag::new_for_execution(
                            "Create Index",
                            None,
                        ))),
                        Payload::DropIndex => Ok(Response::Execution(Tag::new_for_execution(
                            "Drop Index",
                            None,
                        ))),
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

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
        });
    }
}

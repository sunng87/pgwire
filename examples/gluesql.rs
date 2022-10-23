use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::net::TcpListener;

use gluesql::prelude::*;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldInfo, Response, Tag, TextQueryResponseBuilder};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::tokio::process_socket;

pub struct GluesqlProcessor {
    // TODO: mutex
    glue: Arc<Mutex<Glue<MemoryStorage>>>,
}

#[async_trait]
impl SimpleQueryHandler for GluesqlProcessor {
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("{:?}", query);
        let mut glue = self.glue.lock().unwrap();
        let result = glue.execute(query);

        match result {
            Ok(payloads) => Ok(payloads
                .iter()
                .map(|payload| match payload {
                    Payload::Select { labels, rows } => {
                        let fields = labels
                            .iter()
                            .map(|label| FieldInfo::new(label.into(), None, None, Type::UNKNOWN))
                            .collect();
                        let mut result_builder = TextQueryResponseBuilder::new(fields);

                        for row in rows {
                            for field in row.iter() {
                                match field {
                                    Value::Bool(v) => {
                                        result_builder.append_field(Some(*v)).unwrap()
                                    }
                                    Value::I8(v) => result_builder.append_field(Some(*v)).unwrap(),
                                    Value::I16(v) => result_builder.append_field(Some(*v)).unwrap(),
                                    Value::I32(v) => result_builder.append_field(Some(*v)).unwrap(),
                                    Value::I64(v) => result_builder.append_field(Some(*v)).unwrap(),
                                    Value::I128(v) => {
                                        result_builder.append_field(Some(*v)).unwrap()
                                    }
                                    Value::U8(v) => result_builder.append_field(Some(*v)).unwrap(),
                                    Value::F64(v) => result_builder.append_field(Some(*v)).unwrap(),
                                    Value::Str(v) => result_builder.append_field(Some(v)).unwrap(),
                                    Value::Bytea(v) => {
                                        result_builder.append_field(Some(hex::encode(v))).unwrap()
                                    }
                                    Value::Date(v) => result_builder.append_field(Some(v)).unwrap(),
                                    Value::Time(v) => result_builder.append_field(Some(v)).unwrap(),
                                    Value::Timestamp(v) => {
                                        result_builder.append_field(Some(v)).unwrap()
                                    }
                                    _ => unimplemented!(),
                                }
                            }
                            result_builder.finish_row();
                        }

                        Response::Query(result_builder.build())
                    }
                    Payload::Insert(rows) => {
                        Response::Execution(Tag::new_for_execution("Insert", Some(*rows)))
                    }
                    Payload::Delete(rows) => {
                        Response::Execution(Tag::new_for_execution("Delete", Some(*rows)))
                    }
                    Payload::Update(rows) => {
                        Response::Execution(Tag::new_for_execution("Update", Some(*rows)))
                    }
                    Payload::Create => Response::Execution(Tag::new_for_execution("Create", None)),
                    Payload::AlterTable => {
                        Response::Execution(Tag::new_for_execution("Alter Table", None))
                    }
                    Payload::DropTable => {
                        Response::Execution(Tag::new_for_execution("Drop Table", None))
                    }
                    Payload::CreateIndex => {
                        Response::Execution(Tag::new_for_execution("Create Index", None))
                    }
                    Payload::DropIndex => {
                        Response::Execution(Tag::new_for_execution("Drop Index", None))
                    }
                    _ => {
                        unimplemented!()
                    }
                })
                .collect()),
            Err(err) => Err(PgWireError::ApiError(Box::new(err))),
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for GluesqlProcessor {
    async fn do_query<C>(&self, _client: &mut C, _portal: &Portal) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        todo!()
    }
}

#[tokio::main]
pub async fn main() {
    let gluesql = GluesqlProcessor {
        glue: Arc::new(Mutex::new(Glue::new(MemoryStorage::default()))),
    };

    let processor = Arc::new(gluesql);
    let authenticator = Arc::new(NoopStartupHandler);

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let authenticator_ref = authenticator.clone();
        let processor_ref = processor.clone();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                None,
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
            )
            .await
        });
    }
}

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, PrimitiveArray};
use datafusion::arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt32Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use futures::{stream, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    query_response, DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;

pub struct DfSessionService {
    session_context: Arc<Mutex<SessionContext>>,
}

impl DfSessionService {
    pub fn new() -> DfSessionService {
        DfSessionService {
            session_context: Arc::new(Mutex::new(SessionContext::new())),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DfSessionService {
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!("{:?}", query);
        if query.starts_with("LOAD") {
            let commands = query.split(" ").collect::<Vec<&str>>();
            let table_name = commands[1];
            let csv_path = commands[2];
            let ctx = self.session_context.lock().await;
            ctx.register_csv(table_name, csv_path, CsvReadOptions::new())
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            Ok(vec![Response::Execution(Tag::new_for_execution(
                "OK",
                Some(1),
            ))])
        } else if query.starts_with("SELECT") {
            let ctx = self.session_context.lock().await;
            let df = ctx
                .sql(query)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let resp = encode_dataframe(df).await?;
            Ok(vec![Response::Query(resp)])
        } else {
            Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "Datafusion is a readonly execution engine.".to_owned(),
            )))])
        }
    }
}

fn into_pg_type(df_type: &DataType) -> PgWireResult<Type> {
    Ok(match df_type {
        DataType::Null => Type::UNKNOWN,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::CHAR,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 => Type::CHAR,
        DataType::UInt16 => Type::INT2,
        DataType::UInt32 => Type::INT4,
        DataType::UInt64 => Type::INT8,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Binary => Type::BYTEA,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::VARCHAR,
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {df_type}"),
            ))));
        }
    })
}

async fn encode_dataframe<'a>(df: DataFrame) -> PgWireResult<QueryResponse<'a>> {
    let schema = df.schema();
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let pg_type = into_pg_type(f.data_type())?;
            Ok(FieldInfo::new(
                f.name().into(),
                None,
                None,
                pg_type,
                FieldFormat::Text,
            ))
        })
        .collect::<PgWireResult<Vec<FieldInfo>>>()?;

    let recordbatch_stream = df
        .execute_stream()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let fields2 = fields.clone();

    let pg_row_stream = recordbatch_stream
        .map(move |rb: datafusion::error::Result<RecordBatch>| {
            let rb = rb.unwrap();
            let rows = rb.num_rows();
            let cols = rb.num_columns();
            let mut results = Vec::with_capacity(rows);

            for row in 0..rows {
                let mut encoder = DataRowEncoder::new(cols);
                for col in 0..cols {
                    let array = rb.column(col);
                    let f = &fields2[col];
                    if array.is_null(row) {
                        encoder
                            .encode_field(&None::<i8>, f.datatype(), *f.format())
                            .unwrap();
                    } else {
                        encode_value(&mut encoder, array, row, f.datatype(), *f.format()).unwrap();
                    }
                }
                results.push(encoder.finish());
            }

            stream::iter(results)
        })
        .flatten();

    Ok(query_response(Some(fields.clone()), pg_row_stream))
}

fn get_bool_value(arr: &Arc<dyn Array>, idx: usize) -> bool {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(idx)
}

macro_rules! get_primitive_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> $pt {
            arr.as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .value(idx)
        }
    };
}

get_primitive_value!(get_i8_value, Int8Type, i8);
get_primitive_value!(get_i16_value, Int16Type, i16);
get_primitive_value!(get_i32_value, Int32Type, i32);
get_primitive_value!(get_i64_value, Int64Type, i64);
// get_primitive_value!(get_u8_value, UInt8Type, u8);
// get_primitive_value!(get_u16_value, UInt16Type, u16);
get_primitive_value!(get_u32_value, UInt32Type, u32);
// get_primitive_value!(get_u64_value, UInt64Type, u64);
get_primitive_value!(get_f32_value, Float32Type, f32);
get_primitive_value!(get_f64_value, Float64Type, f64);

fn encode_value(
    encoder: &mut DataRowEncoder,
    arr: &Arc<dyn Array>,
    idx: usize,
    pg_type: &Type,
    format: FieldFormat,
) -> PgWireResult<()> {
    match arr.data_type() {
        DataType::Boolean => encoder.encode_field(&get_bool_value(arr, idx), pg_type, format)?,
        DataType::Int8 => encoder.encode_field(&get_i8_value(arr, idx), pg_type, format)?,
        DataType::Int16 => encoder.encode_field(&get_i16_value(arr, idx), pg_type, format)?,
        DataType::Int32 => encoder.encode_field(&get_i32_value(arr, idx), pg_type, format)?,
        DataType::Int64 => encoder.encode_field(&get_i64_value(arr, idx), pg_type, format)?,
        DataType::UInt32 => encoder.encode_field(&get_u32_value(arr, idx), pg_type, format)?,
        DataType::Float32 => encoder.encode_field(&get_f32_value(arr, idx), pg_type, format)?,
        DataType::Float64 => encoder.encode_field(&get_f64_value(arr, idx), pg_type, format)?,
        _ => unimplemented!(),
    }
    Ok(())
}

#[tokio::main]
pub async fn main() {
    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(DfSessionService::new())));
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

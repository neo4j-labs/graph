use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::{
    datatypes::{DataType, Field, Schema},
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer,
    utils::flight_data_from_arrow_batch, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use futures::{Stream, StreamExt};
use graph::prelude::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tonic::{transport::Server, Request, Response, Status, Streaming};

type GraphCatalog = Arc<Mutex<HashMap<String, DirectedCsrGraph<usize>>>>;
type PropertyStore = Arc<Mutex<HashMap<PropertyId, (Arc<Schema>, Vec<RecordBatch>)>>>;

pub struct FlightServiceImpl {
    // Stores created graphs
    graph_catalog: GraphCatalog,
    // Stores algorithm resuts
    property_store: PropertyStore,
}

impl FlightServiceImpl {
    pub fn new() -> Self {
        Self {
            graph_catalog: Arc::new(Mutex::new(HashMap::new())),
            property_store: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FlightServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

/// Used to chunk data into record batches
const CHUNK_SIZE: usize = 10_000;

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let property_id = request.into_inner().try_into()?;

        let property_store = self.property_store.lock().unwrap();
        let (schema, record_batches) = property_store
            .get(&property_id)
            .ok_or_else(|| Status::not_found(format!("PropertyId not found: {property_id:?}")))?;

        let ipc_write_options = IpcWriteOptions::default();
        // Record batches are pre-computed and are immediately available.
        // Imho, there is no need to implement lazy batch computation.
        let record_batches = record_batches
            .iter()
            .map(|batch| flight_data_from_arrow_batch(batch, &ipc_write_options).1)
            .map(Ok)
            .collect::<Vec<_>>();

        let schema_ipc = SchemaAsIpc::new(schema, &ipc_write_options);
        let schema_flight_data = FlightData::from(schema_ipc);

        let schema_stream = futures::stream::once(async move { Ok(schema_flight_data) });
        let batches_stream = futures::stream::iter(record_batches);

        Ok(Response::new(Box::pin(schema_stream.chain(batches_stream))))
    }

    // TODO: return more info about possible actions
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::once(async {
            Ok(ActionType {
                r#type: String::from("create"),
                description: String::from("creates an in-memory graph"),
            })
        }))))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        let action: GraphAction = action.try_into()?;

        match action {
            GraphAction::Create(config) => {
                let CreateConfig {
                    graph_name,
                    file_format,
                    path,
                } = config;

                let graph = tokio::task::spawn_blocking(move || {
                    let builder = GraphBuilder::new().csr_layout(CsrLayout::Sorted);
                    match file_format {
                        FileFormat::EdgeList => builder
                            .file_format(EdgeListInput::default())
                            .path(path)
                            .build::<DirectedCsrGraph<usize>>(),
                        FileFormat::Graph500 => builder
                            .file_format(Graph500Input::default())
                            .path(path)
                            .build::<DirectedCsrGraph<usize>>(),
                    }
                    .unwrap()
                })
                .await
                .unwrap();

                let result = CreateActionResult {
                    node_count: graph.node_count(),
                    edge_count: graph.edge_count(),
                };
                self.graph_catalog.lock().unwrap().insert(graph_name, graph);

                let result = serde_json::to_vec(&result).map_err(from_json_error)?;
                let result = arrow_flight::Result { body: result };

                Ok(Response::new(Box::pin(futures::stream::once(async {
                    Ok(result)
                }))))
            }
            GraphAction::Compute(config) => {
                let ComputeConfig {
                    graph_name,
                    algorithm: algo_name,
                    property_key: mutate_property,
                } = config;

                let catalog = self.graph_catalog.clone();

                match algo_name {
                    Algorithm::PageRank(config) => {
                        let catalog_key = graph_name.clone();

                        let (ranks, iterations, error) = tokio::task::spawn_blocking(move || {
                            let catalog = catalog.lock().unwrap();
                            let graph = catalog.get(catalog_key.as_str()).unwrap();
                            graph::page_rank::page_rank(graph, config)
                        })
                        .await
                        .unwrap();

                        let property_id = PropertyId::new(graph_name, mutate_property);
                        let record_batches = to_f32_record_batches(ranks, "page_rank").await;

                        self.property_store
                            .lock()
                            .unwrap()
                            .insert(property_id.clone(), record_batches);

                        let result = HashMap::from([
                            ("iterations".to_string(), Value::Integer(iterations)),
                            ("error".to_string(), Value::Float(error)),
                        ]);

                        let result = AlgorithmActionResult {
                            property_id,
                            result,
                        };

                        let result = serde_json::to_vec(&result).map_err(from_json_error)?;

                        Ok(Response::new(Box::pin(futures::stream::once(async {
                            Ok(arrow_flight::Result { body: result })
                        }))))
                    }
                }
            }
        }
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

// TODO: macro for supported types
async fn to_f32_record_batches(
    data: Vec<f32>,
    field_name: impl AsRef<str>,
) -> (Arc<Schema>, Vec<RecordBatch>) {
    let field = Field::new(field_name.as_ref(), DataType::Float32, false);
    let schema = Schema::new(vec![field]);
    let schema = Arc::new(schema);

    let record_batches = data
        .into_iter()
        .chunks(CHUNK_SIZE)
        .into_iter()
        .map(|chunk| {
            let chunk = chunk.collect::<Vec<_>>();
            let chunk = arrow::array::Float32Array::from(chunk);
            RecordBatch::try_new(schema.clone(), vec![Arc::new(chunk)]).unwrap()
        })
        .collect::<Vec<_>>();

    (schema, record_batches)
}

#[derive(Hash, Eq, PartialEq, Serialize, Deserialize, Debug, Clone)]
struct PropertyId {
    graph_name: String,
    property_key: String,
}

impl PropertyId {
    fn new(graph_name: String, property_key: String) -> Self {
        Self {
            graph_name,
            property_key,
        }
    }
}

impl TryFrom<Ticket> for PropertyId {
    type Error = Status;

    fn try_from(ticket: Ticket) -> Result<Self, Self::Error> {
        serde_json::from_slice::<PropertyId>(ticket.ticket.as_slice()).map_err(from_json_error)
    }
}

enum GraphAction {
    Create(CreateConfig),
    Compute(ComputeConfig),
}

impl TryFrom<Action> for GraphAction {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        let action_type = action.r#type.as_str();
        match action_type {
            "create" => {
                let create_action = action.try_into()?;
                Ok(GraphAction::Create(create_action))
            }
            "compute" => {
                let compute_action = action.try_into()?;
                Ok(GraphAction::Compute(compute_action))
            }
            _ => Err(Status::invalid_argument(format!(
                "Unknown action type: {action_type}"
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum FileFormat {
    EdgeList,
    Graph500,
}

#[derive(Serialize, Deserialize, Debug)]
struct CreateConfig {
    graph_name: String,
    file_format: FileFormat,
    path: String,
}

impl TryFrom<Action> for CreateConfig {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<CreateConfig>(action.body.as_slice()).map_err(from_json_error)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CreateActionResult {
    node_count: usize,
    edge_count: usize,
}

#[derive(Serialize, Deserialize, Debug)]
enum Algorithm {
    PageRank(PageRankConfig),
}

#[derive(Serialize, Deserialize, Debug)]
struct ComputeConfig {
    graph_name: String,
    algorithm: Algorithm,
    property_key: String,
}

impl TryFrom<Action> for ComputeConfig {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<ComputeConfig>(action.body.as_slice()).map_err(from_json_error)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct AlgorithmActionResult {
    property_id: PropertyId,
    result: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, Debug)]
enum Value {
    Float(f64),
    Integer(usize),
    Boolean(bool),
    String(String),
}

fn from_json_error(error: serde_json::Error) -> Status {
    Status::internal(format!("JsonError: {error:?}"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = FlightServiceImpl::new();

    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

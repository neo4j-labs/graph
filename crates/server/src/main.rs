use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::utils::flight_data_from_arrow_batch;
use arrow_flight::SchemaAsIpc;
use graph::prelude::*;
use itertools::Itertools;

use serde::{Deserialize, Serialize};

use futures::Stream;
use futures::StreamExt;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

pub struct FlightServiceImpl {
    // Stores created graphs
    catalog: Arc<Mutex<HashMap<String, DirectedCsrGraph<usize>>>>,
    // Stores algorithm resuts
    node_properties: Arc<Mutex<HashMap<PropertyId, Vec<f32>>>>,
}

impl FlightServiceImpl {
    pub fn new() -> Self {
        Self {
            catalog: Arc::new(Mutex::new(HashMap::new())),
            node_properties: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

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

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let property_id = request.into_inner().try_into()?;

        let ipc_write_options = IpcWriteOptions::default();

        let mut properties = self.node_properties.lock().unwrap();
        let data = properties.remove(&property_id).unwrap();

        let field = Field::new("page_rank", DataType::Float32, false);
        let schema = Schema::new(vec![field]);
        let schema = Arc::new(schema);

        let batches = data
            .into_iter()
            .chunks(10_000)
            .into_iter()
            .map(|chunk| {
                let chunk = chunk.collect::<Vec<_>>();
                let chunk = arrow::array::Float32Array::from(chunk);
                let batch =
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(chunk)]).unwrap();
                let (_, flight_data) = flight_data_from_arrow_batch(&batch, &ipc_write_options);
                flight_data
            })
            .map(Ok)
            .collect::<Vec<_>>();

        let schema_ipc = SchemaAsIpc::new(&schema, &ipc_write_options);
        let schema_flight_data = FlightData::from(schema_ipc);

        let schema_stream = futures::stream::once(async move { Ok(schema_flight_data) });
        let batches_stream = futures::stream::iter(batches);

        let result = schema_stream.chain(batches_stream);

        Ok(Response::new(Box::pin(result)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_ref() {
            "create" => {
                let CreateAction { graph_name, path } = action.try_into()?;

                let graph = tokio::task::spawn_blocking(move || {
                    GraphBuilder::new()
                        .csr_layout(CsrLayout::Sorted)
                        .file_format(EdgeListInput::default())
                        .path(path)
                        .build::<DirectedCsrGraph<usize>>()
                        .unwrap()
                })
                .await
                .unwrap();

                let result = CreateActionResult {
                    node_count: graph.node_count(),
                    edge_count: graph.edge_count(),
                };
                self.catalog.lock().unwrap().insert(graph_name, graph);

                let result = serde_json::to_vec(&result).map_err(from_json_error)?;
                let result = arrow_flight::Result { body: result };

                Ok(Response::new(Box::pin(futures::stream::once(async {
                    Ok(result)
                }))))
            }
            "algo" => {
                let AlgorithmAction {
                    graph_name,
                    algo_name,
                    mutate_property,
                } = action.try_into()?;

                let catalog = self.catalog.clone();

                match algo_name {
                    Algo::PageRank => {
                        let catalog_key = graph_name.clone();

                        let (ranks, iterations, error) = tokio::task::spawn_blocking(move || {
                            let catalog = catalog.lock().unwrap();
                            let graph = catalog.get(catalog_key.as_str()).unwrap();
                            graph::page_rank::page_rank(graph, 20, 1E-4)
                        })
                        .await
                        .unwrap();

                        let property_id = PropertyId::new(graph_name, mutate_property);

                        self.node_properties
                            .lock()
                            .unwrap()
                            .insert(property_id.clone(), ranks);

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
            _ => Err(Status::unimplemented(
                "Action {action.r#type} not supported",
            )),
        }
    }

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

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
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

#[derive(Serialize, Deserialize, Debug)]
struct CreateAction {
    graph_name: String,
    path: String,
}

impl TryFrom<Action> for CreateAction {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<CreateAction>(action.body.as_slice()).map_err(from_json_error)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CreateActionResult {
    node_count: usize,
    edge_count: usize,
}

#[derive(Serialize, Deserialize, Debug)]
enum Algo {
    PageRank,
}

#[derive(Serialize, Deserialize, Debug)]
struct AlgorithmAction {
    graph_name: String,
    algo_name: Algo,
    mutate_property: String,
}

impl TryFrom<Action> for AlgorithmAction {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<AlgorithmAction>(action.body.as_slice()).map_err(from_json_error)
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

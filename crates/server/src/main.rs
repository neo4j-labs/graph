use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::SchemaAsIpc;
use graph::prelude::*;

use serde::{Deserialize, Serialize};

use futures::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

// #[derive(Clone)]
pub struct FlightServiceImpl {
    catalog: Mutex<HashMap<String, DirectedCsrGraph<usize>>>,
    node_properties: Mutex<HashMap<PropertyId, Vec<f32>>>,
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

impl FlightServiceImpl {
    pub fn new() -> Self {
        Self {
            catalog: Mutex::new(HashMap::new()),
            node_properties: Mutex::new(HashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

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
        let ticket = request.into_inner();
        let property_id = serde_json::from_slice::<PropertyId>(ticket.ticket.as_slice()).unwrap();

        println!("{property_id:?}");

        let mut properties = self.node_properties.lock().unwrap();
        let data = properties.remove(&property_id).unwrap();
        let data = arrow::array::Float32Array::from(data);

        let field = Field::new("page_rank", DataType::Float32, false);
        let schema = Schema::new(vec![field]);
        let schema = Arc::new(schema);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(data)]).unwrap();

        let (_, flight_data) =
            arrow_flight::utils::flight_data_from_arrow_batch(&batch, &IpcWriteOptions::default());

        let schema_ipc = SchemaAsIpc {
            pair: (&schema, &IpcWriteOptions::default()),
        };

        let schema_flight_data = FlightData::from(schema_ipc);

        let s = futures::stream::iter(vec![Ok(schema_flight_data), Ok(flight_data)]);

        Ok(Response::new(Box::pin(s)))
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
        let action = request.get_ref();

        match action.r#type.as_ref() {
            "create" => {
                let CreateAction { graph_name, path } =
                    serde_json::from_slice::<CreateAction>(action.body.as_slice()).unwrap();

                let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
                    .csr_layout(CsrLayout::Sorted)
                    .file_format(EdgeListInput::default())
                    .path(path)
                    .build()
                    .unwrap();

                let node_count = graph.node_count();
                let edge_count = graph.edge_count();

                self.catalog.lock().unwrap().insert(graph_name, graph);

                let result = CreateActionResult {
                    node_count,
                    edge_count,
                };

                let result = serde_json::to_vec(&result).unwrap();

                Ok(Response::new(Box::pin(futures::stream::once(async {
                    Ok(arrow_flight::Result { body: result })
                }))))
            }
            "algo" => {
                let AlgorithmAction {
                    graph_name,
                    algo_name,
                    mutate_property,
                } = serde_json::from_slice::<AlgorithmAction>(action.body.as_slice()).unwrap();

                let catalog = self.catalog.lock().unwrap();

                let graph = catalog.get(graph_name.as_str()).unwrap();

                match algo_name {
                    Algo::PageRank => {
                        let (ranks, iterations, error) =
                            graph::page_rank::page_rank(graph, 20, 1E-4);
                        let property_id = PropertyId::new(graph_name.clone(), mutate_property);

                        self.node_properties
                            .lock()
                            .unwrap()
                            .insert(property_id.clone(), ranks);

                        let mut result = HashMap::new();
                        result.insert(String::from("iterations"), Value::Integer(iterations));
                        result.insert(String::from("error"), Value::Float(error));

                        let result = AlgorithmActionResult {
                            property_id,
                            result,
                        };

                        let result = serde_json::to_vec(&result).unwrap();

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

#[derive(Serialize, Deserialize, Debug)]
struct CreateAction {
    graph_name: String,
    path: String,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = FlightServiceImpl::new();

    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

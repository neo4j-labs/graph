use crate::actions::*;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::datatypes::Int64Type;
use arrow::error::ArrowError;
use arrow::{
    datatypes::{DataType, Field, Schema},
    ipc::writer::IpcWriteOptions,
    record_batch::RecordBatch,
};
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{
    flight_service_server::FlightService, utils::flight_data_from_arrow_batch, Action, ActionType,
    Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt};
use graph::prelude::*;
use itertools::Itertools;
use log::info;
use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status, Streaming};

type GraphCatalog = Arc<Mutex<HashMap<String, DirectedCsrGraph<usize>>>>;
type PropertyStore = Arc<Mutex<HashMap<PropertyId, (Arc<Schema>, Vec<RecordBatch>)>>>;

#[derive(Hash, Eq, PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct PropertyId {
    pub graph_name: String,
    pub property_key: String,
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

// Used to chunk data into record batches
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

        info!("Received GET request for ticket: {property_id:?}");

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

        info!(
            "Streaming {} record batches to the client",
            record_batches.len()
        );

        let schema_ipc = SchemaAsIpc::new(schema, &ipc_write_options);
        let schema_flight_data = FlightData::from(schema_ipc);

        let schema_stream = futures::stream::once(async move { Ok(schema_flight_data) });
        let batches_stream = futures::stream::iter(record_batches);

        Ok(Response::new(Box::pin(schema_stream.chain(batches_stream))))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut request = request.into_inner();
        let mut schema_flight_data = request.next().await.unwrap()?;

        let CreateGraphCommand {
            graph_name,
            edge_count,
            csr_layout,
        } = if let Some(descriptor) = schema_flight_data.flight_descriptor.take() {
            let command = descriptor.try_into();
            info!("Received PUT request with command: {command:?}");
            command
        } else {
            Err(Status::invalid_argument("Missing flight descriptor"))
        }?;

        let schema = Arc::new(Schema::try_from(&schema_flight_data).map_err(from_arrow_err)?);
        info!("Reading graph from schema = {schema:?}");

        // all the remaining stream messages should be dictionary and record batches
        let mut edge_list = Vec::with_capacity(edge_count as usize);
        let dictionaries_by_field = vec![None; schema.fields().len()];
        while let Some(flight_data) = request.message().await? {
            let batch =
                flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_field)
                    .unwrap();

            let source_ids = arrow::array::as_primitive_array::<Int64Type>(batch.column(0));
            let target_ids = arrow::array::as_primitive_array::<Int64Type>(batch.column(1));

            let mut batch = source_ids
                .iter()
                .zip(target_ids.iter())
                .map(|(s, t)| (s.unwrap() as usize, t.unwrap() as usize))
                .collect::<Vec<_>>();

            edge_list.append(&mut batch);
        }

        let graph: DirectedCsrGraph<usize> = tokio::task::spawn_blocking(move || {
            GraphBuilder::new()
                .csr_layout(csr_layout)
                .edges(edge_list)
                .build()
        })
        .await
        .unwrap();

        info!(
            "Created graph '{graph_name}' with {} nodes and {} edges",
            graph.node_count(),
            graph.edge_count()
        );

        let result = CreateActionResult {
            node_count: graph.node_count(),
            edge_count: graph.edge_count(),
        };

        self.graph_catalog.lock().unwrap().insert(graph_name, graph);

        let result = serde_json::to_vec(&result).map_err(from_json_error)?;
        let result = arrow_flight::PutResult {
            app_metadata: result,
        };

        Ok(Response::new(Box::pin(futures::stream::once(async {
            Ok(result)
        }))))
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

        let action: FlightAction = action.try_into()?;

        match action {
            FlightAction::Create(config) => {
                info!("Creating graph using config: {config:?}");

                let CreateGraphFromFileConfig {
                    graph_name,
                    file_format,
                    path,
                    csr_layout,
                } = config;

                let graph = tokio::task::spawn_blocking(move || {
                    let builder = GraphBuilder::new().csr_layout(csr_layout);
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
            FlightAction::Compute(config) => {
                let ComputeConfig {
                    graph_name,
                    algorithm,
                    property_key,
                } = config;

                let catalog = self.graph_catalog.clone();

                match algorithm {
                    Algorithm::PageRank(config) => {
                        info!(
                            "Computing page rank on graph '{graph_name}' using config: {config:?}"
                        );
                        let catalog_key = graph_name.clone();

                        let (ranks, iterations, error) = tokio::task::spawn_blocking(move || {
                            let catalog = catalog.lock().unwrap();
                            let graph = catalog.get(catalog_key.as_str()).unwrap();
                            graph::page_rank::page_rank(graph, config)
                        })
                        .await
                        .unwrap();

                        let property_id = PropertyId::new(graph_name, property_key);
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

fn from_arrow_err(e: ArrowError) -> Status {
    Status::internal(format!("ArrowError: {:?}", e))
}

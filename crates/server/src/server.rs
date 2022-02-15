use crate::actions::*;
use crate::catalog::*;

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Int64Type;
use arrow::error::ArrowError;
use arrow::{datatypes::Schema, ipc::writer::IpcWriteOptions};
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{
    flight_service_server::FlightService, utils::flight_data_from_arrow_batch, Action, ActionType,
    Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::{Stream, StreamExt};
use graph::page_rank::PageRankConfig;
use log::error;
use log::info;
use parking_lot::RwLock;
use tonic::{Request, Response, Status, Streaming};

// Used to chunk data into record batches
pub const CHUNK_SIZE: usize = 10_000;

pub struct FlightServiceImpl {
    // Stores created graphs
    graph_catalog: Arc<RwLock<GraphCatalog>>,
    // Stores algorithm resuts
    property_store: Arc<RwLock<PropertyStore>>,
}

impl FlightServiceImpl {
    pub fn new() -> Self {
        Self {
            graph_catalog: Arc::new(RwLock::new(GraphCatalog::new())),
            property_store: Arc::new(RwLock::new(PropertyStore::new())),
        }
    }
}

impl Default for FlightServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;
type FlightResult<T> = Result<T, Status>;

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(&self, request: Request<Ticket>) -> FlightResult<Response<Self::DoGetStream>> {
        let property_id = request.into_inner().try_into()?;

        info!("Received GET request for ticket: {property_id:?}");

        let property_store = self.property_store.read();
        let property_entry = property_store.get(&property_id)?;

        let ipc_write_options = IpcWriteOptions::default();
        // Record batches are pre-computed and are immediately available.
        // Imho, there is no need to implement lazy batch computation.
        let record_batches = property_entry
            .batches
            .iter()
            .map(|batch| flight_data_from_arrow_batch(batch, &ipc_write_options).1)
            .map(Ok)
            .collect::<Vec<_>>();

        info!(
            "Streaming {} record batches to the client",
            record_batches.len()
        );

        let schema_ipc = SchemaAsIpc::new(&property_entry.schema, &ipc_write_options);
        let schema_flight_data = FlightData::from(schema_ipc);

        let batches = std::iter::once(Ok(schema_flight_data)).chain(record_batches);

        Ok(Response::new(Box::pin(futures::stream::iter(batches))))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> FlightResult<Response<Self::DoPutStream>> {
        let mut request = request.into_inner();
        let mut schema_flight_data = request.next().await.unwrap()?;

        let CreateGraphCommand {
            graph_name,
            edge_count,
            csr_layout,
            orientation,
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
        let start = Instant::now();
        let dicts = vec![None; schema.fields().len()];
        let mut edge_list = Vec::with_capacity(edge_count as usize);
        while let Some(flight_data) = request.message().await? {
            let batch = flight_data_to_arrow_batch(&flight_data, schema.clone(), &dicts)
                .map_err(from_arrow_err)?;

            let source_ids = arrow::array::as_primitive_array::<Int64Type>(batch.column(0));
            let target_ids = arrow::array::as_primitive_array::<Int64Type>(batch.column(1));

            let batch = source_ids
                .iter()
                .zip(target_ids.iter())
                .map(|(s, t)| (s.unwrap() as usize, t.unwrap() as usize));

            edge_list.extend(batch);
        }

        let graph = tokio::task::spawn_blocking(move || {
            GraphType::from_edge_list(edge_list, orientation, csr_layout)
        })
        .await
        .unwrap();

        let result = CreateActionResult::new(
            graph.node_count(),
            graph.edge_count(),
            start.elapsed().as_millis(),
        );

        info!("Created graph '{graph_name}': {result:?}");

        self.graph_catalog.write().insert(graph_name, graph);

        let result = serde_json::to_vec(&result).map_err(from_json_error)?;
        let result = arrow_flight::PutResult {
            app_metadata: result,
        };

        Ok(Response::new(Box::pin(futures::stream::once(async {
            Ok(result)
        }))))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> FlightResult<Response<Self::ListActionsStream>> {
        let actions = futures::stream::iter(FlightAction::action_types().into_iter().map(Ok));
        Ok(Response::new(Box::pin(actions)))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> FlightResult<Response<Self::DoActionStream>> {
        let action = request.into_inner();
        let action: FlightAction = action.try_into()?;

        let result = match action {
            FlightAction::Create(config) => {
                create_graph(config, Arc::clone(&self.graph_catalog)).await?
            }
            FlightAction::Relabel(config) => {
                relabel_graph(config, Arc::clone(&self.graph_catalog)).await?
            }
            FlightAction::Compute(config) => {
                let ComputeConfig {
                    graph_name,
                    algorithm,
                    property_key,
                } = config;

                match algorithm {
                    Algorithm::PageRank(config) => {
                        compute_page_rank(
                            config,
                            Arc::clone(&self.graph_catalog),
                            Arc::clone(&self.property_store),
                            graph_name,
                            property_key,
                        )
                        .await?
                    }
                    Algorithm::TriangleCount => {
                        compute_triangle_count(Arc::clone(&self.graph_catalog), graph_name).await?
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(futures::stream::once(async {
            Ok(result)
        }))))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> FlightResult<Response<Self::HandshakeStream>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> FlightResult<Response<Self::ListFlightsStream>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> FlightResult<Response<FlightInfo>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> FlightResult<Response<SchemaResult>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> FlightResult<Response<Self::DoExchangeStream>> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

async fn create_graph(
    config: CreateGraphFromFileConfig,
    graph_catalog: Arc<RwLock<GraphCatalog>>,
) -> FlightResult<arrow_flight::Result> {
    info!("Creating graph using config: {config:?}");

    let CreateGraphFromFileConfig {
        graph_name,
        file_format,
        path,
        csr_layout,
        orientation,
    } = config;

    let start = Instant::now();
    let graph = tokio::task::spawn_blocking(move || {
        GraphType::from_file(path, file_format, orientation, csr_layout)
    })
    .await
    .unwrap()?;

    let result = CreateActionResult::new(
        graph.node_count(),
        graph.edge_count(),
        start.elapsed().as_millis(),
    );
    info!("Created graph '{graph_name}': {result:?}");

    graph_catalog.write().insert(graph_name, graph);

    let result = serde_json::to_vec(&result).map_err(from_json_error)?;
    Ok(arrow_flight::Result { body: result })
}

async fn relabel_graph(
    config: RelabelConfig,
    graph_catalog: Arc<RwLock<GraphCatalog>>,
) -> FlightResult<arrow_flight::Result> {
    info!("Relabelling graph using config: {config:?}");
    let RelabelConfig { graph_name } = config;

    let result = tokio::task::spawn_blocking(move || {
        let mut catalog = graph_catalog.write();
        let graph = catalog.get_mut(graph_name)?;
        if let GraphType::Undirected(graph) = graph {
            use graph::prelude::RelabelByDegreeOp;
            let start = Instant::now();
            graph.to_degree_ordered();
            Ok(RelabelActionResult {
                relabel_millis: start.elapsed().as_millis(),
            })
        } else {
            Err(Status::invalid_argument(
                "Relabelling directed graphs is not supported.",
            ))
        }
    })
    .await
    .unwrap()?;

    let result = serde_json::to_vec(&result).map_err(from_json_error)?;
    Ok(arrow_flight::Result { body: result })
}

async fn compute_page_rank(
    config: PageRankConfig,
    graph_catalog: Arc<RwLock<GraphCatalog>>,
    property_store: Arc<RwLock<PropertyStore>>,
    graph_name: String,
    property_key: String,
) -> FlightResult<arrow_flight::Result> {
    info!("Computing page rank on graph '{graph_name}' using config: {config:?}");

    let catalog_key = graph_name.clone();

    let (ranks, result) = tokio::task::spawn_blocking(move || {
        let catalog = graph_catalog.read();

        if let GraphType::Directed(graph) = catalog.get(catalog_key).unwrap() {
            let start = Instant::now();
            let (ranks, iterations, error) = graph::page_rank::page_rank(graph, config);
            let result = PageRankResult {
                iterations: iterations as u64,
                error,
                compute_millis: start.elapsed().as_millis(),
            };
            Ok((ranks, result))
        } else {
            error!("Attempted running page rank on undirected graph");
            Err(Status::invalid_argument(
                "Page Rank requires a directed graph",
            ))
        }
    })
    .await
    .unwrap()?;

    let property_id = PropertyId::new(graph_name, property_key);
    let record_batches = crate::catalog::to_f32_record_batches(ranks, "page_rank").await;

    property_store
        .write()
        .insert(property_id.clone(), record_batches);

    let result = MutateResult::new(property_id, result);

    let result = serde_json::to_vec(&result).map_err(from_json_error)?;
    Ok(arrow_flight::Result { body: result })
}

async fn compute_triangle_count(
    graph_catalog: Arc<RwLock<GraphCatalog>>,
    graph_name: String,
) -> FlightResult<arrow_flight::Result> {
    info!("Computing global triangle count on graph '{graph_name}'");
    let graph_name = graph_name.clone();

    let result = tokio::task::spawn_blocking(move || {
        let catalog = graph_catalog.read();
        if let GraphType::Undirected(graph) = catalog.get(graph_name).unwrap() {
            let start = Instant::now();
            let tc = graph::triangle_count::global_triangle_count(graph);
            let res = TriangleCountResult {
                triangle_count: tc,
                compute_millis: start.elapsed().as_millis(),
            };
            Ok(res)
        } else {
            error!("Attempted running triangle count on directed graph");
            Err(Status::invalid_argument(
                "Triangle count requires an undirected graph",
            ))
        }
    })
    .await
    .unwrap()?;

    let result = serde_json::to_vec(&result).map_err(from_json_error)?;
    Ok(arrow_flight::Result { body: result })
}

fn from_arrow_err(e: ArrowError) -> Status {
    Status::internal(format!("ArrowError: {:?}", e))
}

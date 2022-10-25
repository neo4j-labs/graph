use std::{collections::HashMap, marker::PhantomData, path::Path, sync::Arc};

use arrow::{
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use arrow_flight::Ticket;
use graph::prelude::*;
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::actions::{from_json_error, FileFormat, GraphInfo, Orientation};

pub enum GraphType {
    Directed(DirectedCsrGraph<u64>),
    Undirected(UndirectedCsrGraph<u64>),
    DirectedWeighted(DirectedCsrGraph<u64, (), f32>),
    UndirectedWeighted(UndirectedCsrGraph<u64, (), f32>),
}

impl std::fmt::Display for GraphType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match &self {
                GraphType::Directed(_) => "directed",
                GraphType::Undirected(_) => "undirected",
                GraphType::DirectedWeighted(_) => "directed+weighted",
                GraphType::UndirectedWeighted(_) => "undirected+weighted",
            }
        )
    }
}

impl GraphType {
    pub fn from_edge_list(
        edge_list: Vec<(u64, u64)>,
        orientation: Orientation,
        csr_layout: CsrLayout,
    ) -> Self {
        let builder = GraphBuilder::new().csr_layout(csr_layout).edges(edge_list);

        match orientation {
            Orientation::Directed => GraphType::Directed(builder.build()),
            Orientation::Undirected => GraphType::Undirected(builder.build()),
        }
    }

    #[allow(dead_code)]
    pub fn from_edge_list_with_weights(
        edge_list: Vec<(u64, u64, f32)>,
        orientation: Orientation,
        csr_layout: CsrLayout,
    ) -> Self {
        let builder = GraphBuilder::new()
            .csr_layout(csr_layout)
            .edges_with_values(edge_list);

        match orientation {
            Orientation::Directed => GraphType::DirectedWeighted(builder.build()),
            Orientation::Undirected => GraphType::UndirectedWeighted(builder.build()),
        }
    }

    pub fn from_file<P: AsRef<Path>>(
        path: P,
        format: FileFormat,
        orientation: Orientation,
        csr_layout: CsrLayout,
    ) -> Result<Self, Status> {
        let builder = GraphBuilder::new().csr_layout(csr_layout);
        match (orientation, format) {
            (Orientation::Directed, FileFormat::EdgeList) => {
                let graph = builder
                    .file_format(EdgeListInput::default())
                    .path(path)
                    .build()
                    .map_err(from_graph_error)?;
                Ok(GraphType::Directed(graph))
            }
            (Orientation::Undirected, FileFormat::EdgeList) => {
                let graph = builder
                    .file_format(EdgeListInput::default())
                    .path(path)
                    .build()
                    .map_err(from_graph_error)?;
                Ok(GraphType::Undirected(graph))
            }
            (Orientation::Directed, FileFormat::EdgeListWeighted) => {
                let graph = builder
                    .file_format(EdgeListInput::default())
                    .path(path)
                    .build()
                    .map_err(from_graph_error)?;
                Ok(GraphType::DirectedWeighted(graph))
            }
            (Orientation::Undirected, FileFormat::EdgeListWeighted) => {
                let graph = builder
                    .file_format(EdgeListInput::default())
                    .path(path)
                    .build()
                    .map_err(from_graph_error)?;
                Ok(GraphType::UndirectedWeighted(graph))
            }
            (Orientation::Directed, FileFormat::Graph500) => {
                let graph = builder
                    .file_format(Graph500Input::default())
                    .path(path)
                    .build()
                    .map_err(from_graph_error)?;
                Ok(GraphType::Directed(graph))
            }
            (Orientation::Undirected, FileFormat::Graph500) => {
                let graph = builder
                    .file_format(Graph500Input::default())
                    .path(path)
                    .build()
                    .map_err(from_graph_error)?;
                Ok(GraphType::Undirected(graph))
            }
        }
    }

    pub fn node_count(&self) -> u64 {
        match self {
            GraphType::Directed(g) => g.node_count(),
            GraphType::Undirected(g) => g.node_count(),
            GraphType::DirectedWeighted(g) => g.node_count(),
            GraphType::UndirectedWeighted(g) => g.node_count(),
        }
    }

    pub fn edge_count(&self) -> u64 {
        match self {
            GraphType::Directed(g) => g.edge_count(),
            GraphType::Undirected(g) => g.edge_count(),
            GraphType::DirectedWeighted(g) => g.edge_count(),
            GraphType::UndirectedWeighted(g) => g.edge_count(),
        }
    }
}

fn from_graph_error(error: graph::prelude::Error) -> Status {
    Status::internal(format!("GraphError: {error:?}"))
}

pub struct GraphCatalog {
    graphs: HashMap<String, GraphType>,
}

impl GraphCatalog {
    pub fn new() -> Self {
        Self {
            graphs: HashMap::new(),
        }
    }

    pub fn get<K: AsRef<str>>(&self, graph_name: K) -> Result<&GraphType, Status> {
        self.graphs
            .get(graph_name.as_ref())
            .ok_or_else(|| Status::not_found("Graph with name '{graph_name}' not found"))
    }

    pub fn get_mut<K: AsRef<str>>(&mut self, graph_name: K) -> Result<&mut GraphType, Status> {
        self.graphs
            .get_mut(graph_name.as_ref())
            .ok_or_else(|| Status::not_found("Graph with name '{graph_name}' not found"))
    }

    pub fn insert(&mut self, graph_name: String, graph: GraphType) {
        self.graphs.insert(graph_name, graph);
    }

    pub fn list(&self) -> Vec<GraphInfo> {
        self.graphs
            .iter()
            .map(|(graph_name, graph_type)| {
                let graph_name = graph_name.clone();
                let (node_count, edge_count) = match graph_type {
                    GraphType::Directed(g) => (g.node_count(), g.edge_count()),
                    GraphType::Undirected(g) => (g.node_count(), g.edge_count()),
                    GraphType::DirectedWeighted(g) => (g.node_count(), g.edge_count()),
                    GraphType::UndirectedWeighted(g) => (g.node_count(), g.edge_count()),
                };
                GraphInfo::new(graph_name, graph_type.to_string(), node_count, edge_count)
            })
            .collect::<Vec<_>>()
    }

    pub fn remove<K: AsRef<str>>(&mut self, graph_name: K) -> Result<GraphInfo, Status> {
        self.graphs.remove(graph_name.as_ref()).map_or_else(
            || {
                Err(Status::not_found(
                    "Graph with name '{graph_name}' not found",
                ))
            },
            |g| {
                Ok(GraphInfo::new(
                    graph_name.as_ref().to_string(),
                    g.to_string(),
                    g.node_count(),
                    g.edge_count(),
                ))
            },
        )
    }
}

#[derive(Hash, Eq, PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct PropertyId {
    pub graph_name: String,
    pub property_key: String,
}

impl PropertyId {
    pub fn new(graph_name: String, property_key: String) -> Self {
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

pub struct PropertyEntry {
    pub schema: Arc<Schema>,
    pub batches: Vec<RecordBatch>,
}

impl PropertyEntry {
    pub fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

pub struct PropertyStore {
    properties: HashMap<PropertyId, PropertyEntry>,
}

impl PropertyStore {
    pub fn new() -> Self {
        Self {
            properties: HashMap::new(),
        }
    }

    pub fn get(&self, property_id: &PropertyId) -> Result<&PropertyEntry, Status> {
        self.properties
            .get(property_id)
            .ok_or_else(|| Status::not_found(format!("Property Id not found: {property_id:?}")))
    }

    pub fn insert(&mut self, property_id: PropertyId, entry: PropertyEntry) {
        self.properties.insert(property_id, entry);
    }
}

pub async fn to_record_batches<T: arrow::datatypes::ArrowPrimitiveType>(
    data: &[T::Native],
    field_name: impl AsRef<str>,
    _phantom: PhantomData<T>,
) -> PropertyEntry {
    let field = Field::new(field_name.as_ref(), T::DATA_TYPE, false);
    let schema = Schema::new(vec![field]);
    let schema = Arc::new(schema);

    let batches = data
        .chunks(crate::server::CHUNK_SIZE)
        .map(|chunk| {
            let chunk = arrow::array::PrimitiveArray::<T>::from_iter_values(chunk.to_vec());
            RecordBatch::try_new(schema.clone(), vec![Arc::new(chunk)]).unwrap()
        })
        .collect::<Vec<_>>();

    PropertyEntry::new(schema, batches)
}

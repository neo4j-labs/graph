use std::{collections::HashMap, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_flight::Ticket;
use graph::prelude::*;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::actions::from_json_error;

pub enum GraphType {
    Directed(DirectedCsrGraph<usize>),
    Undirected(UndirectedCsrGraph<usize>),
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

    pub fn insert(&mut self, graph_name: String, graph: GraphType) {
        self.graphs.insert(graph_name, graph);
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

// TODO: macro for supported types
pub async fn to_f32_record_batches(data: Vec<f32>, field_name: impl AsRef<str>) -> PropertyEntry {
    let field = Field::new(field_name.as_ref(), DataType::Float32, false);
    let schema = Schema::new(vec![field]);
    let schema = Arc::new(schema);

    let batches = data
        .into_iter()
        .chunks(crate::server::CHUNK_SIZE)
        .into_iter()
        .map(|chunk| {
            let chunk = chunk.collect::<Vec<_>>();
            let chunk = arrow::array::Float32Array::from(chunk);
            RecordBatch::try_new(schema.clone(), vec![Arc::new(chunk)]).unwrap()
        })
        .collect::<Vec<_>>();

    PropertyEntry::new(schema, batches)
}

use arrow_flight::{flight_descriptor::DescriptorType, Action, ActionType, FlightDescriptor};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::catalog::PropertyId;
use graph::prelude::*;

pub enum FlightAction {
    Create(CreateGraphFromFileConfig),
    Compute(ComputeConfig),
    Relabel(RelabelConfig),
}

impl FlightAction {
    pub fn action_types() -> [ActionType; 3] {
        [
            ActionType {
                r#type: "create".into(),
                description: "Create an in-memory graph.".into(),
            },
            ActionType {
                r#type: "compute".into(),
                description: "Compute a graph algorithm on an in-memory graph.".into(),
            },
            ActionType {
                r#type: "relabel".into(),
                description: "Relabel an in-memory graph".into(),
            },
        ]
    }
}

impl TryFrom<Action> for FlightAction {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        let action_type = action.r#type.as_str();
        match action_type {
            "create" => {
                let create_action = action.try_into()?;
                Ok(FlightAction::Create(create_action))
            }
            "compute" => {
                let compute_action = action.try_into()?;
                Ok(FlightAction::Compute(compute_action))
            }
            "relabel" => {
                let relabel_action = action.try_into()?;
                Ok(FlightAction::Relabel(relabel_action))
            }
            _ => Err(Status::invalid_argument(format!(
                "Unknown action type: {action_type}"
            ))),
        }
    }
}

#[derive(Deserialize, Debug)]
pub enum FileFormat {
    EdgeList,
    Graph500,
}

#[derive(Deserialize, Debug)]
#[serde(remote = "CsrLayout")]
pub enum CsrLayoutRef {
    Sorted,
    Unsorted,
    Deduplicated,
}

#[derive(Deserialize, Debug)]
pub enum Orientation {
    Directed,
    Undirected,
}

impl Default for Orientation {
    fn default() -> Self {
        Self::Directed
    }
}

#[derive(Deserialize, Debug)]
pub struct CreateGraphFromFileConfig {
    pub graph_name: String,
    pub file_format: FileFormat,
    pub path: String,
    #[serde(with = "CsrLayoutRef")]
    #[serde(default)]
    pub csr_layout: CsrLayout,
    #[serde(default)]
    pub orientation: Orientation,
}

impl TryFrom<Action> for CreateGraphFromFileConfig {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<CreateGraphFromFileConfig>(action.body.as_slice())
            .map_err(from_json_error)
    }
}

#[derive(Deserialize, Debug)]
pub struct CreateGraphCommand {
    pub graph_name: String,
    pub edge_count: i64,
    #[serde(with = "CsrLayoutRef")]
    #[serde(default)]
    pub csr_layout: CsrLayout,
    #[serde(default)]
    pub orientation: Orientation,
}

impl TryFrom<FlightDescriptor> for CreateGraphCommand {
    type Error = Status;

    fn try_from(descriptor: FlightDescriptor) -> Result<Self, Self::Error> {
        match DescriptorType::from_i32(descriptor.r#type) {
            None => Err(Status::invalid_argument(format!(
                "unsupported descriptor type: {}",
                descriptor.r#type
            ))),
            Some(DescriptorType::Cmd) => {
                serde_json::from_slice::<Self>(&descriptor.cmd).map_err(from_json_error)
            }
            Some(descriptor_type) => Err(Status::invalid_argument(format!(
                "Expected command, got {descriptor_type:?}"
            ))),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct CreateActionResult {
    node_count: usize,
    edge_count: usize,
    create_millis: u128,
}

impl CreateActionResult {
    pub fn new(node_count: usize, edge_count: usize, create_millis: u128) -> Self {
        Self {
            node_count,
            edge_count,
            create_millis,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct RelabelConfig {
    pub graph_name: String,
}

impl TryFrom<Action> for RelabelConfig {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<Self>(action.body.as_slice()).map_err(from_json_error)
    }
}

#[derive(Serialize, Debug)]
pub struct RelabelActionResult {
    pub relabel_millis: u128,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Algorithm {
    PageRank(PageRankConfig),
    TriangleCount,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ComputeConfig {
    pub graph_name: String,
    pub algorithm: Algorithm,
    pub property_key: String,
}

impl TryFrom<Action> for ComputeConfig {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<ComputeConfig>(action.body.as_slice()).map_err(from_json_error)
    }
}

#[derive(Serialize, Debug)]
pub struct PageRankResult {
    pub iterations: u64,
    pub error: f64,
    pub compute_millis: u128,
}

#[derive(Serialize, Debug)]
pub struct TriangleCountResult {
    pub triangle_count: u64,
    pub compute_millis: u128,
}

#[derive(Serialize, Debug)]
pub struct MutateResult<T> {
    property_id: PropertyId,
    algo_result: T,
}

impl<T> MutateResult<T> {
    pub fn new(property_id: PropertyId, algo_result: T) -> Self {
        Self {
            property_id,
            algo_result,
        }
    }
}

pub fn from_json_error(error: serde_json::Error) -> Status {
    Status::internal(format!("JsonError: {error:?}"))
}

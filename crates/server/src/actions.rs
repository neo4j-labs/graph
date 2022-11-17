use arrow_flight::{flight_descriptor::DescriptorType, Action, ActionType, FlightDescriptor};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::{catalog::PropertyId, server::FlightResult};
use graph::prelude::*;

#[derive(Debug)]
pub enum FlightAction {
    Create(CreateGraphFromFileConfig),
    List,
    Remove(RemoveGraphConfig),
    Compute(ComputeConfig),
    ToRelabeled(ToRelabeledConfig),
    ToUndirected(ToUndirectedConfig),
}

impl FlightAction {
    pub fn action_types() -> [ActionType; 6] {
        [
            ActionType {
                r#type: "create".into(),
                description: "Create a new graph.".into(),
            },
            ActionType {
                r#type: "list".into(),
                description: "List all graphs.".into(),
            },
            ActionType {
                r#type: "remove".into(),
                description: "Remove a graph.".into(),
            },
            ActionType {
                r#type: "compute".into(),
                description: "Compute a graph algorithm on a graph.".into(),
            },
            ActionType {
                r#type: "to_relabeled".into(),
                description: "Relabels the node ids of a graph in degree-descending order".into(),
            },
            ActionType {
                r#type: "to_undirected".into(),
                description: "Converts a directed graph to an undirected graph".into(),
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
            "list" => Ok(FlightAction::List),
            "remove" => {
                let remove_action = action.try_into()?;
                Ok(FlightAction::Remove(remove_action))
            }
            "compute" => {
                let compute_action = action.try_into()?;
                Ok(FlightAction::Compute(compute_action))
            }
            "to_relabeled" => {
                let relabel_action = action.try_into()?;
                Ok(FlightAction::ToRelabeled(relabel_action))
            }
            "to_undirected" => {
                let to_undirected_action = action.try_into()?;
                Ok(FlightAction::ToUndirected(to_undirected_action))
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
    EdgeListWeighted,
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
    node_count: u64,
    edge_count: u64,
    create_millis: u128,
}

impl CreateActionResult {
    pub fn new(node_count: u64, edge_count: u64, create_millis: u128) -> Self {
        Self {
            node_count,
            edge_count,
            create_millis,
        }
    }
}

#[derive(Serialize, Debug)]
pub struct ListActionResult {
    graph_infos: Vec<GraphInfo>,
}

impl ListActionResult {
    pub fn new(graph_infos: Vec<GraphInfo>) -> Self {
        Self { graph_infos }
    }
}

#[derive(Serialize, Debug)]
pub struct GraphInfo {
    graph_name: String,
    graph_type: String,
    node_count: u64,
    edge_count: u64,
}

impl GraphInfo {
    pub fn new(graph_name: String, graph_type: String, node_count: u64, edge_count: u64) -> Self {
        Self {
            graph_name,
            graph_type,
            node_count,
            edge_count,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct RemoveGraphConfig {
    pub graph_name: String,
}

impl TryFrom<Action> for RemoveGraphConfig {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<Self>(action.body.as_slice()).map_err(from_json_error)
    }
}

#[derive(Deserialize, Debug)]
pub struct ToRelabeledConfig {
    pub graph_name: String,
}

impl TryFrom<Action> for ToRelabeledConfig {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<Self>(action.body.as_slice()).map_err(from_json_error)
    }
}

#[derive(Serialize, Debug)]
pub struct ToRelabeledResult {
    pub to_relabeled_millis: u128,
}

#[derive(Deserialize, Debug)]
pub struct ToUndirectedConfig {
    pub graph_name: String,
    #[serde(with = "CsrLayoutRef")]
    #[serde(default)]
    pub csr_layout: CsrLayout,
}

impl TryFrom<Action> for ToUndirectedConfig {
    type Error = Status;

    fn try_from(action: Action) -> Result<Self, Self::Error> {
        serde_json::from_slice::<Self>(action.body.as_slice()).map_err(from_json_error)
    }
}

#[derive(Serialize, Debug)]
pub struct ToUndirectedResult {
    pub to_undirected_millis: u128,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Algorithm {
    PageRank(PageRankConfig),
    TriangleCount,
    Sssp(DeltaSteppingConfig),
    Wcc(WccConfig),
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
pub struct SsspResult {
    pub compute_millis: u128,
}

#[derive(Serialize, Debug)]
pub struct WccResult {
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

pub fn into_flight_result<T: serde::Serialize>(result: T) -> FlightResult<arrow_flight::Result> {
    let result = serde_json::to_vec(&result).map_err(from_json_error)?;
    Ok(arrow_flight::Result { body: result })
}

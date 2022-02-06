use std::collections::HashMap;

use arrow_flight::{flight_descriptor::DescriptorType, Action, FlightDescriptor};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::catalog::PropertyId;
use graph::prelude::*;

pub enum FlightAction {
    Create(CreateGraphFromFileConfig),
    Compute(ComputeConfig),
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
            _ => Err(Status::invalid_argument(format!(
                "Unknown action type: {action_type}"
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FileFormat {
    EdgeList,
    Graph500,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(remote = "CsrLayout")]
pub enum CsrLayoutRef {
    Sorted,
    Unsorted,
    Deduplicated,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Orientation {
    Directed,
    Undirected,
}

impl Default for Orientation {
    fn default() -> Self {
        Self::Directed
    }
}

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateGraphCommand {
    pub graph_name: String,
    pub edge_count: i64,
    #[serde(with = "CsrLayoutRef")]
    #[serde(default)]
    pub csr_layout: CsrLayout,
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

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateActionResult {
    pub node_count: usize,
    pub edge_count: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Algorithm {
    PageRank(PageRankConfig),
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

#[derive(Serialize, Deserialize, Debug)]
pub struct AlgorithmActionResult {
    pub property_id: PropertyId,
    pub result: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Value {
    Float(f64),
    Integer(usize),
    Boolean(bool),
    String(String),
}

pub fn from_json_error(error: serde_json::Error) -> Status {
    Status::internal(format!("JsonError: {error:?}"))
}

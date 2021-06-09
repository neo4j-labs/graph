use std::{collections::HashMap, path::Path};

use crate::InputCapabilities;

pub struct EdgeListInput;

impl InputCapabilities for EdgeListInput {
    type GraphInput = EdgeList;
}

struct Edge(usize, usize);

pub struct EdgeList(Box<[Edge]>);

impl From<&Path> for EdgeList {
    fn from(_: &Path) -> Self {
        todo!()
    }
}

pub struct DotGraphInput;

impl InputCapabilities for DotGraphInput {
    type GraphInput = DotGraph;
}

pub struct DotGraph {
    node_count: usize,
    relationship_count: usize,
    labels: Vec<usize>,
    offsets: Vec<usize>,
    neighbors: Vec<usize>,
    max_degree: usize,
    max_label: usize,
    label_frequency: HashMap<usize, usize>,
}

impl From<&Path> for DotGraph {
    fn from(_: &Path) -> Self {
        todo!()
    }
}

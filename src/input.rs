use std::{collections::HashMap, ops::Deref, path::Path};

use crate::InputCapabilities;

pub struct EdgeListInput;

impl InputCapabilities for EdgeListInput {
    type GraphInput = EdgeList;
}

pub type Edge = (usize, usize);

pub struct EdgeList(Box<[Edge]>);

impl AsRef<[Edge]> for EdgeList {
    fn as_ref(&self) -> &[Edge] {
        &self.0
    }
}

impl Deref for EdgeList {
    type Target = [Edge];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

impl EdgeList {
    pub fn new(edges: Vec<Edge>) -> Self {
        Self(edges.into_boxed_slice())
    }

    pub(crate) fn max_node_id(&self) -> usize {
        self.0
            .iter()
            .map(|(s, t)| usize::max(*s, *t))
            .reduce(usize::max)
            .unwrap_or_default()
    }

    pub(crate) fn degrees(&self, node_count: usize, direction: Direction) -> Vec<usize> {
        let mut degrees = vec![0_usize; node_count];

        match direction {
            Direction::Outgoing => self.iter().for_each(|(s, _)| degrees[*s] += 1),
            Direction::Incoming => self.iter().for_each(|(_, t)| degrees[*t] += 1),
            Direction::Undirected => self.iter().for_each(|(s, t)| {
                degrees[*s] += 1;
                degrees[*t] += 1;
            }),
        }

        degrees
    }
}

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

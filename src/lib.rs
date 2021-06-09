#![allow(dead_code)]
pub mod graph;
pub mod input;
pub mod read;

use std::{collections::HashMap, path::Path};

use input::EdgeList;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {}

pub trait Graph {
    fn node_count(&self) -> usize;

    fn edge_count(&self) -> usize;
}

pub trait UndirectedGraph: Graph {
    fn degree(&self, node: usize) -> usize;

    fn neighbors(&self, node: usize) -> &[usize];
}

pub trait DirectedGraph: Graph {
    fn out_degree(&self, node: usize) -> usize;

    fn out_neighbors(&self, node: usize) -> &[usize];

    fn in_degree(&self, node: usize) -> usize;

    fn in_neighbors(&self, node: usize) -> &[usize];
}

pub trait NodeLabeledGraph: Graph {
    fn label(&self, node: usize) -> usize;

    fn nodes_by_label(&self, label: usize) -> &[usize];

    fn label_count(&self) -> usize;

    fn max_label(&self) -> usize;

    fn max_label_frequency(&self) -> usize;

    fn neighbor_label_frequency(&self, node: usize) -> &HashMap<usize, usize>;
}

pub trait InputCapabilities {
    type GraphInput;
}

pub fn create_graph<G: From<EdgeList>>(edge_list: EdgeList) -> G {
    G::from(edge_list)
}

pub fn read_graph<G, F, P>(path: P, _fmt: F) -> Result<G, Error>
where
    P: AsRef<Path>,
    F: InputCapabilities,
    for<'a> F::GraphInput: From<&'a Path>,
    G: From<F::GraphInput>,
{
    Ok(G::from(F::GraphInput::from(path.as_ref())))
}

#[cfg(test)]
mod tests {
    use crate::{
        graph::{DirectedCSRGraph, NodeLabeledCSRGraph, UndirectedCSRGraph},
        input::{DotGraphInput, EdgeListInput},
    };

    use super::*;

    #[test]
    fn read_graph_test() {
        let _g0: DirectedCSRGraph = read_graph("graph", EdgeListInput).unwrap();
        let _g1: UndirectedCSRGraph = read_graph("graph", EdgeListInput).unwrap();
        let _g2: NodeLabeledCSRGraph<DirectedCSRGraph> =
            read_graph("graph", DotGraphInput).unwrap();
        let _g3: NodeLabeledCSRGraph<UndirectedCSRGraph> =
            read_graph("graph", DotGraphInput).unwrap();
    }
}

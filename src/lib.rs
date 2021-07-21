#![allow(dead_code)]
pub mod graph;
pub mod index;
pub mod input;

use crate::index::Idx;
use input::EdgeList;
use std::convert::TryFrom;
use std::{collections::HashMap, path::Path};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("error while loading graph")]
    LoadGraph {
        #[from]
        source: std::io::Error,
    },
}

pub trait Graph<Node: Idx> {
    fn node_count(&self) -> Node;

    fn edge_count(&self) -> Node;
}

pub trait UndirectedGraph<Node: Idx>: Graph<Node> {
    fn degree(&self, node: Node) -> Node;

    fn neighbors(&self, node: Node) -> &[Node];
}

pub trait DirectedGraph<Node: Idx>: Graph<Node> {
    fn out_degree(&self, node: Node) -> Node;

    fn out_neighbors(&self, node: Node) -> &[Node];

    fn in_degree(&self, node: Node) -> Node;

    fn in_neighbors(&self, node: Node) -> &[Node];
}

pub trait NodeLabeledGraph<Node: Idx>: Graph<Node> {
    fn label(&self, node: Node) -> Node;

    fn nodes_by_label(&self, label: Node) -> &[Node];

    fn label_count(&self) -> Node;

    fn max_label(&self) -> Node;

    fn max_label_frequency(&self) -> Node;

    fn neighbor_label_frequency(&self, node: Node) -> &HashMap<Node, Node>;
}

pub trait InputCapabilities<Node: Idx> {
    type GraphInput;
}

pub fn create_graph<Node: Idx, G: From<EdgeList<Node>>>(edge_list: EdgeList<Node>) -> G {
    G::from(edge_list)
}

pub fn read_graph<G, F, P, N>(
    path: P,
    _fmt: F,
) -> Result<G, <F::GraphInput as TryFrom<input::MyPath<P>>>::Error>
where
    P: AsRef<Path>,
    N: Idx,
    F: InputCapabilities<N>,
    F::GraphInput: TryFrom<input::MyPath<P>>,
    G: From<F::GraphInput>,
{
    Ok(G::from(F::GraphInput::try_from(input::MyPath(path))?))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        graph::{DirectedCSRGraph, NodeLabeledCSRGraph, UndirectedCSRGraph},
        input::{DotGraphInput, EdgeListInput},
    };

    use super::*;

    #[test]
    fn read_graph_test() {
        fn inner_test() -> Result<(), Error> {
            let _g0: DirectedCSRGraph<usize> = read_graph("graph", EdgeListInput::default())?;
            let _g0: DirectedCSRGraph<_> = read_graph("graph", EdgeListInput::<usize>::default())?;

            let _g1: UndirectedCSRGraph<usize> = read_graph("graph", EdgeListInput::default())?;
            let _g2: NodeLabeledCSRGraph<DirectedCSRGraph<usize>> =
                read_graph("graph", DotGraphInput::default())?;
            let _g3: NodeLabeledCSRGraph<UndirectedCSRGraph<usize>> =
                read_graph("graph", DotGraphInput::default())?;

            Ok(())
        }

        assert!(inner_test().is_err())
    }

    #[test]
    fn directed_graph_from_edge_list() {
        let edge_list = EdgeList::new(vec![(0, 1), (0, 2)]);

        let g: DirectedCSRGraph<usize> = create_graph(edge_list);

        assert_eq!(g.node_count(), 3);
        assert_eq!(g.edge_count(), 2);

        assert_eq!(g.out_degree(0), 2);
        assert_eq!(g.out_degree(1), 0);
        assert_eq!(g.out_degree(2), 0);

        assert_eq!(g.in_degree(0), 0);
        assert_eq!(g.in_degree(1), 1);
        assert_eq!(g.in_degree(2), 1);

        assert_eq!(g.out_neighbors(0), &[1, 2]);
        assert_eq!(g.out_neighbors(1), &[]);
        assert_eq!(g.out_neighbors(2), &[]);

        assert_eq!(g.in_neighbors(0), &[]);
        assert_eq!(g.in_neighbors(1), &[0]);
        assert_eq!(g.in_neighbors(2), &[0]);
    }

    #[test]
    fn undirected_graph_from_edge_list() {
        let edge_list = EdgeList::new(vec![(0, 1), (0, 2)]);

        let g: UndirectedCSRGraph<usize> = create_graph(edge_list);

        assert_eq!(g.node_count(), 3);
        assert_eq!(g.edge_count(), 2);

        assert_eq!(g.degree(0), 2);
        assert_eq!(g.degree(1), 1);
        assert_eq!(g.degree(2), 1);

        assert_eq!(g.neighbors(0), &[1, 2]);
        assert_eq!(g.neighbors(1), &[0]);
        assert_eq!(g.neighbors(2), &[0]);
    }

    #[test]
    fn directed_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        let g: DirectedCSRGraph<usize> = read_graph(path, EdgeListInput::default()).unwrap();

        assert_eq!(g.node_count(), 3);
        assert_eq!(g.edge_count(), 2);

        assert_eq!(g.out_degree(0), 2);
        assert_eq!(g.out_degree(1), 0);
        assert_eq!(g.out_degree(2), 0);

        assert_eq!(g.in_degree(0), 0);
        assert_eq!(g.in_degree(1), 1);
        assert_eq!(g.in_degree(2), 1);

        assert_eq!(g.out_neighbors(0), &[1, 2]);
        assert_eq!(g.out_neighbors(1), &[]);
        assert_eq!(g.out_neighbors(2), &[]);

        assert_eq!(g.in_neighbors(0), &[]);
        assert_eq!(g.in_neighbors(1), &[0]);
        assert_eq!(g.in_neighbors(2), &[0]);
    }

    #[test]
    fn undirected_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        let g: UndirectedCSRGraph<usize> = read_graph(path, EdgeListInput::default()).unwrap();

        assert_eq!(g.node_count(), 3);
        assert_eq!(g.edge_count(), 2);

        assert_eq!(g.degree(0), 2);
        assert_eq!(g.degree(1), 1);
        assert_eq!(g.degree(2), 1);

        assert_eq!(g.neighbors(0), &[1, 2]);
        assert_eq!(g.neighbors(1), &[0]);
        assert_eq!(g.neighbors(2), &[0]);
    }
}

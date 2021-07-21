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
    fn should_compile_test() {
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
    fn directed_usize_graph_from_edge_list() {
        let edges = vec![(0, 1), (0, 2)];
        assert_directed_graph::<usize>(create_graph(EdgeList::new(edges)));
    }

    #[test]
    fn directed_u32_graph_from_edge_list() {
        let edges = vec![(0, 1), (0, 2)];
        assert_directed_graph::<u32>(create_graph(EdgeList::new(edges)));
    }

    #[test]
    fn undirected_usize_graph_from_edge_list() {
        let edge_list = EdgeList::new(vec![(0, 1), (0, 2)]);
        assert_undirected_graph::<usize>(create_graph(edge_list));
    }

    #[test]
    fn undirected_u32_graph_from_edge_list() {
        let edge_list = EdgeList::new(vec![(0, 1), (0, 2)]);
        assert_undirected_graph::<u32>(create_graph(edge_list));
    }

    #[test]
    fn directed_usize_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        assert_directed_graph::<usize>(read_graph(path, EdgeListInput::default()).unwrap());
    }

    #[test]
    fn directed_u32_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        assert_directed_graph::<u32>(read_graph(path, EdgeListInput::default()).unwrap());
    }

    #[test]
    fn undirected_usize_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        assert_undirected_graph::<usize>(read_graph(path, EdgeListInput::default()).unwrap());
    }

    #[test]
    fn undirected_u32_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        assert_undirected_graph::<u32>(read_graph(path, EdgeListInput::default()).unwrap());
    }

    fn assert_directed_graph<Node: Idx>(g: DirectedCSRGraph<Node>) {
        assert_eq!(g.node_count(), Node::new(3));
        assert_eq!(g.edge_count(), Node::new(2));

        assert_eq!(g.out_degree(Node::new(0)), Node::new(2));
        assert_eq!(g.out_degree(Node::new(1)), Node::new(0));
        assert_eq!(g.out_degree(Node::new(2)), Node::new(0));

        assert_eq!(g.in_degree(Node::new(0)), Node::new(0));
        assert_eq!(g.in_degree(Node::new(1)), Node::new(1));
        assert_eq!(g.in_degree(Node::new(2)), Node::new(1));

        assert_eq!(g.out_neighbors(Node::new(0)), &[Node::new(1), Node::new(2)]);
        assert_eq!(g.out_neighbors(Node::new(1)), &[]);
        assert_eq!(g.out_neighbors(Node::new(2)), &[]);

        assert_eq!(g.in_neighbors(Node::new(0)), &[]);
        assert_eq!(g.in_neighbors(Node::new(1)), &[Node::new(0)]);
        assert_eq!(g.in_neighbors(Node::new(2)), &[Node::new(0)]);
    }

    fn assert_undirected_graph<Node: Idx>(g: UndirectedCSRGraph<Node>) {
        assert_eq!(g.node_count(), Node::new(3));
        assert_eq!(g.edge_count(), Node::new(2));

        assert_eq!(g.degree(Node::new(0)), Node::new(2));
        assert_eq!(g.degree(Node::new(1)), Node::new(1));
        assert_eq!(g.degree(Node::new(2)), Node::new(1));

        assert_eq!(g.neighbors(Node::new(0)), &[Node::new(1), Node::new(2)]);
        assert_eq!(g.neighbors(Node::new(1)), &[Node::new(0)]);
        assert_eq!(g.neighbors(Node::new(2)), &[Node::new(0)]);
    }
}

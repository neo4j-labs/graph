#![feature(slice_partition_dedup)]
#![feature(vec_spare_capacity)]
#![feature(maybe_uninit_write_slice)]
#![feature(maybe_uninit_slice)]
#![allow(dead_code)]
pub mod builder;
pub mod graph;
pub mod graph_ops;
pub mod index;
pub mod input;
pub mod prelude;

use crate::index::Idx;
use std::{collections::HashMap, ops::Range};

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

pub trait UndirectedGraphOps<Node: Idx>: UndirectedGraph<Node> {
    fn degree_partition(&self, concurrency: Node) -> Vec<Range<Node>>;
}

pub trait DirectedGraphOps<Node: Idx>: DirectedGraph<Node> {
    fn out_degree_partition(&self, concurrency: Node) -> Vec<Range<Node>>;

    fn in_degree_partition(&self, concurrency: Node) -> Vec<Range<Node>>;
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        builder::GraphBuilder,
        graph::{CSROption, DirectedCSRGraph, NodeLabeledCSRGraph, UndirectedCSRGraph},
        input::{DotGraphInput, EdgeListInput},
    };

    use super::*;

    #[test]
    fn should_compile_test() {
        fn inner_test() -> Result<(), Error> {
            let _g: DirectedCSRGraph<usize> = GraphBuilder::new()
                .file_format(EdgeListInput::default())
                .path("graph")
                .build()?;

            let _g: DirectedCSRGraph<_> = GraphBuilder::new()
                .file_format(EdgeListInput::<usize>::default())
                .path("graph")
                .build()?;

            let _g: UndirectedCSRGraph<usize> = GraphBuilder::new()
                .file_format(EdgeListInput::default())
                .path("graph")
                .build()?;

            let _g: NodeLabeledCSRGraph<DirectedCSRGraph<usize>> = GraphBuilder::new()
                .file_format(DotGraphInput::default())
                .path("graph")
                .build()?;

            let _g: NodeLabeledCSRGraph<UndirectedCSRGraph<usize>> = GraphBuilder::new()
                .file_format(DotGraphInput::default())
                .path("graph")
                .build()?;

            Ok(())
        }

        assert!(inner_test().is_err())
    }

    #[test]
    fn directed_usize_graph_from_edge_list() {
        assert_directed_graph::<usize>(GraphBuilder::new().edges([(0, 1), (0, 2)]).build());
    }

    #[test]
    fn directed_u32_graph_from_edge_list() {
        assert_directed_graph::<u32>(GraphBuilder::new().edges([(0, 1), (0, 2)]).build());
    }

    #[test]
    fn undirected_usize_graph_from_edge_list() {
        assert_undirected_graph::<usize>(GraphBuilder::new().edges([(0, 1), (0, 2)]).build());
    }

    #[test]
    fn undirected_u32_graph_from_edge_list() {
        assert_undirected_graph::<u32>(GraphBuilder::new().edges([(0, 1), (0, 2)]).build());
    }

    #[test]
    fn directed_usize_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        let graph = GraphBuilder::new()
            .csr_option(CSROption::Sorted)
            .file_format(EdgeListInput::default())
            .path(path)
            .build()
            .expect("loading failed");

        assert_directed_graph::<usize>(graph);
    }

    #[test]
    fn directed_u32_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        let graph = GraphBuilder::new()
            .csr_option(CSROption::Sorted)
            .file_format(EdgeListInput::default())
            .path(path)
            .build()
            .expect("loading failed");

        assert_directed_graph::<u32>(graph);
    }

    #[test]
    fn undirected_usize_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        let graph = GraphBuilder::new()
            .csr_option(CSROption::Sorted)
            .file_format(EdgeListInput::default())
            .path(path)
            .build()
            .expect("loading failed");

        assert_undirected_graph::<usize>(graph);
    }

    #[test]
    fn undirected_u32_graph_from_edge_list_file() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.el"]
            .iter()
            .collect::<PathBuf>();

        let graph = GraphBuilder::new()
            .csr_option(CSROption::Sorted)
            .file_format(EdgeListInput::default())
            .path(path)
            .build()
            .expect("loading failed");

        assert_undirected_graph::<u32>(graph);
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

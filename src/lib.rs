#![feature(slice_partition_dedup)]
#![feature(vec_spare_capacity)]
#![feature(maybe_uninit_write_slice)]
#![feature(maybe_uninit_slice)]
#![feature(step_trait)]
#![allow(dead_code)]

//! A library that can be used as a building block for high-performant graph
//! algorithms.
//!
//! Graph provides implementations for directed and undirected graphs. Graphs
//! can be created programatically or read from custom input formats in a
//! type-safe way. The library uses [rayon](https://github.com/rayon-rs/rayon)
//! to parallelize all steps during graph creation.
//!
//! The implementation uses a Compressed-Sparse-Row (CSR) data structure which
//! is tailored for fast and concurrent access to the graph topology.
//!
//! **Note**: The development is mainly driven by
//! [Neo4j](https://github.com/neo4j/neo4j) developers. However, the library is
//! __not__ an official product of Neo4j.
//!
//! # What is a graph?
//!
//! A graph consists of nodes and edges where edges connect exactly two nodes. A
//! graph can be either directed, i.e., an edge has a source and a target node
//! or undirected where there is no such distinction.
//!
//! In a directed graph, each node `u` has outgoing and incoming neighbors. An
//! outgoing neighbor of node `u` is any node `v` for which an edge `(u, v)`
//! exists. An incoming neighbor of node `u` is any node `v` for which an edge
//! `(v, u)` exists.
//!
//! In an undirected graph there is no distinction between source and target
//! node. A neighbor of node `u` is any node `v` for which either an edge `(u,
//! v)` or `(v, u)` exists.
//!
//! # How to use graph?
//!
//! The library provides a builder that can be used to construct a graph from a
//! given list of edges.
//!
//! For example, to create a directed graph that uses `usize` as node
//! identifier, one can use the builder like so:
//!
//! ```
//! use graph::prelude::*;
//!
//! let graph: DirectedCSRGraph<usize> = GraphBuilder::new()
//!     .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 3)])
//!     .build();
//!
//! assert_eq!(graph.node_count(), 4);
//! assert_eq!(graph.edge_count(), 5);
//!
//! assert_eq!(graph.out_degree(1), 2);
//! assert_eq!(graph.in_degree(1), 1);
//!
//! assert_eq!(graph.out_neighbors(1), &[2, 3]);
//! assert_eq!(graph.in_neighbors(1), &[0]);
//! ```
//!
//! To build an undirected graph using `u32` as node identifer, we only need to
//! change the expected types:
//!
//! ```
//! use graph::prelude::*;
//!
//! let graph: UndirectedCSRGraph<u32> = GraphBuilder::new()
//!     .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 3)])
//!     .build();
//!
//! assert_eq!(graph.node_count(), 4);
//! assert_eq!(graph.edge_count(), 5);
//!
//! assert_eq!(graph.degree(1), 3);
//!
//! assert_eq!(graph.neighbors(1), &[0, 2, 3]);
//! ```
//!
//! It is also possible to create a graph from a specific input format. In the
//! following example we use the `EdgeListInput` which is an input format where
//! each line of a file contains an edge of the graph.
//!
//! ```
//! use std::path::PathBuf;
//!
//! use graph::prelude::*;
//!
//! let path = [env!("CARGO_MANIFEST_DIR"), "resources", "example.el"]
//!     .iter()
//!     .collect::<PathBuf>();
//!
//! let graph: DirectedCSRGraph<usize> = GraphBuilder::new()
//!     .csr_option(CSROption::Sorted)
//!     .file_format(EdgeListInput::default())
//!     .path(path)
//!     .build()
//!     .expect("loading failed");
//!
//! assert_eq!(graph.node_count(), 4);
//! assert_eq!(graph.edge_count(), 5);
//!
//! assert_eq!(graph.out_degree(1), 2);
//! assert_eq!(graph.in_degree(1), 1);
//!
//! assert_eq!(graph.out_neighbors(1), &[2, 3]);
//! assert_eq!(graph.in_neighbors(1), &[0]);
//! ```
//!
//! # Examples?
//!
//! Check the [TriangleCount](./examples/triangle_count.rs) and
//! [PageRank](./examples/page_rank.rs) implementations  to see how the library
//! is used to implement high-performant graph algorithms.

pub mod builder;
pub mod graph;
pub mod graph_ops;
pub mod index;
pub mod input;
pub mod prelude;

use crate::index::Idx;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("error while loading graph")]
    LoadGraph {
        #[from]
        source: std::io::Error,
    },
    #[error("invalid partitioning")]
    InvalidPartitioning,
    #[error("number of node values must be the same as node count")]
    InvalidNodeValues,
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

#[repr(transparent)]
pub struct SharedMut<T>(*mut T);
unsafe impl<T: Send> Send for SharedMut<T> {}
unsafe impl<T: Sync> Sync for SharedMut<T> {}

impl<T> SharedMut<T> {
    pub fn new(ptr: *mut T) -> Self {
        SharedMut(ptr)
    }

    delegate::delegate! {
        to self.0 {
            /// # Safety
            ///
            /// Ensure that `count` does not exceed the capacity of the Vec.
            pub unsafe fn add(&self, count: usize) -> *mut T;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        builder::GraphBuilder,
        graph::csr::{CSROption, DirectedCSRGraph, UndirectedCSRGraph},
        input::edgelist::EdgeListInput,
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

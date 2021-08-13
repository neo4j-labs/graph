#![feature(slice_partition_dedup)]
#![feature(vec_spare_capacity)]
#![feature(maybe_uninit_write_slice)]
#![feature(maybe_uninit_slice)]
#![feature(step_trait)]
#![feature(new_uninit)]
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
//! let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
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
//! let graph: UndirectedCsrGraph<u32> = GraphBuilder::new()
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
//! let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
//!     .csr_layout(CsrLayout::Sorted)
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

use std::convert::Infallible;

use crate::index::Idx;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("error while loading graph")]
    IoError {
        #[from]
        source: std::io::Error,
    },
    #[error("invalid partitioning")]
    InvalidPartitioning,
    #[error("number of node values must be the same as node count")]
    InvalidNodeValues,
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

/// A graph is a tuple `(N, E)`, where `N` is a set of nodes and `E` a set of
/// edges. Each edge connects exactly two nodes.
///
/// `Graph` is parameterized over the node index type `Node` which is used to
/// uniquely identify a node. An edge is a tuple of node identifiers.
pub trait Graph<Node: Idx> {
    /// Returns the number of nodes in the graph.
    fn node_count(&self) -> Node;

    /// Returns the number of edges in the graph.
    fn edge_count(&self) -> Node;
}

/// A graph where the order within an edge tuple is unimportant.
///
/// The edge `(42, 1337)` is equivalent to the edge `(1337, 42)`.
pub trait UndirectedGraph<Node: Idx>: Graph<Node> {
    /// Returns the number of edges connected to the given node.
    fn degree(&self, node: Node) -> Node;

    /// Returns a slice of all nodes connected to the given node.
    fn neighbors(&self, node: Node) -> &[Node];
}

/// A graph where the order within an edge tuple is important.
///
/// An edge tuple `e = (u, v)` has a source node `u` and a target node `v`. From
/// the perspective of `u`, the edge `e` is an **outgoing** edge. From the
/// perspective of node `v`, the edge `e` is an **incoming** edge. The edges
/// `(u, v)` and `(v, u)` are not considered equivalent.
pub trait DirectedGraph<Node: Idx>: Graph<Node> {
    /// Returns the number of edges where the given node is a source node.
    fn out_degree(&self, node: Node) -> Node;

    /// Returns a slice of all nodes which are connected in outgoing direction
    /// to the given node, i.e., the given node is the source node of the
    /// connecting edge.
    fn out_neighbors(&self, node: Node) -> &[Node];

    /// Returns the number of edges where the given node is a target node.
    fn in_degree(&self, node: Node) -> Node;

    /// Returns a slice of all nodes which are connected in incoming direction
    /// to the given node, i.e., the given node is the target node of the
    /// connecting edge.
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
        graph::csr::{CsrLayout, DirectedCsrGraph, UndirectedCsrGraph},
        input::{binary::BinaryInput, edgelist::EdgeListInput},
    };

    use super::*;

    #[test]
    fn should_compile_test() {
        fn inner_test() -> Result<(), Error> {
            let _g: DirectedCsrGraph<usize> = GraphBuilder::new()
                .file_format(EdgeListInput::default())
                .path("graph")
                .build()?;

            let _g: DirectedCsrGraph<_> = GraphBuilder::new()
                .file_format(EdgeListInput::<usize>::default())
                .path("graph")
                .build()?;

            let _g: UndirectedCsrGraph<usize> = GraphBuilder::new()
                .file_format(EdgeListInput::default())
                .path("graph")
                .build()?;

            let _g: DirectedCsrGraph<usize> = GraphBuilder::new()
                .file_format(BinaryInput::<usize>::default())
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
            .csr_layout(CsrLayout::Sorted)
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
            .csr_layout(CsrLayout::Sorted)
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
            .csr_layout(CsrLayout::Sorted)
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
            .csr_layout(CsrLayout::Sorted)
            .file_format(EdgeListInput::default())
            .path(path)
            .build()
            .expect("loading failed");

        assert_undirected_graph::<u32>(graph);
    }

    fn assert_directed_graph<Node: Idx>(g: DirectedCsrGraph<Node>) {
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

    fn assert_undirected_graph<Node: Idx>(g: UndirectedCsrGraph<Node>) {
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

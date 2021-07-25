#![allow(dead_code)]
pub mod graph;
pub mod index;
pub mod input;

use crate::graph::CSROption;
use crate::index::Idx;
use input::EdgeList;
use std::convert::TryFrom;
use std::marker::PhantomData;
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

pub struct GraphBuilder {
    csr_option: CSROption,
}

impl Default for GraphBuilder {
    fn default() -> Self {
        GraphBuilder::new()
    }
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            csr_option: CSROption::default(),
        }
    }

    pub fn csr_option(mut self, csr_option: CSROption) -> Self {
        self.csr_option = csr_option;
        self
    }

    pub fn edges<Node, Edges>(self, edges: Edges) -> GraphFromEdgesBuilder<Node, Edges>
    where
        Node: Idx,
        Edges: IntoIterator<Item = (Node, Node)>,
    {
        GraphFromEdgesBuilder(edges, self.csr_option)
    }

    pub fn file_format<F, P, N>(self, format: F) -> GraphFromFormatBuilder<F, P, N>
    where
        P: AsRef<Path>,
        N: Idx,
        F: InputCapabilities<N>,
        F::GraphInput: TryFrom<input::MyPath<P>>,
    {
        GraphFromFormatBuilder {
            csr_option: self.csr_option,
            format,
            path: None,
            _idx: PhantomData,
        }
    }
}

pub struct GraphFromEdgesBuilder<Node, Edges>(Edges, CSROption)
where
    Node: Idx,
    Edges: IntoIterator<Item = (Node, Node)>;

impl<Node, Edges> GraphFromEdgesBuilder<Node, Edges>
where
    Node: Idx,
    Edges: IntoIterator<Item = (Node, Node)>,
{
    pub fn build<G>(self) -> G
    where
        G: From<(EdgeList<Node>, CSROption)>,
    {
        G::from((EdgeList::new(self.0.into_iter().collect()), self.1))
    }
}

pub struct GraphFromFormatBuilder<F, P, N>
where
    P: AsRef<Path>,
    N: Idx,
    F: InputCapabilities<N>,
    F::GraphInput: TryFrom<input::MyPath<P>>,
{
    csr_option: CSROption,
    format: F,
    path: Option<P>,
    _idx: PhantomData<N>,
}

impl<F, P, N> GraphFromFormatBuilder<F, P, N>
where
    P: AsRef<Path>,
    N: Idx,
    F: InputCapabilities<N>,
    F::GraphInput: TryFrom<input::MyPath<P>>,
{
    pub fn path(self, path: P) -> GraphFromPathBuilder<F, P, N> {
        GraphFromPathBuilder {
            csr_option: self.csr_option,
            format: self.format,
            path,
            _idx: PhantomData,
        }
    }
}

pub struct GraphFromPathBuilder<F, P, N>
where
    P: AsRef<Path>,
    N: Idx,
    F: InputCapabilities<N>,
    F::GraphInput: TryFrom<input::MyPath<P>>,
{
    csr_option: CSROption,
    format: F,
    path: P,
    _idx: PhantomData<N>,
}

impl<F, P, N> GraphFromPathBuilder<F, P, N>
where
    P: AsRef<Path>,
    N: Idx,
    F: InputCapabilities<N>,
    F::GraphInput: TryFrom<input::MyPath<P>>,
{
    pub fn build<G>(self) -> Result<G, <F::GraphInput as TryFrom<input::MyPath<P>>>::Error>
    where
        G: From<(F::GraphInput, CSROption)>,
    {
        let graph_input: F::GraphInput = F::GraphInput::try_from(input::MyPath(self.path))?;
        Ok(G::from((graph_input, self.csr_option)))
    }
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

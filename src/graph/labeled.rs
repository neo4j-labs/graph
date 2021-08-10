use std::collections::HashMap;

use crate::input::edgelist::EdgeList;
use crate::{index::Idx, input::dotgraph::DotGraph, DirectedGraph, Graph, UndirectedGraph};

use super::csr::CSROption;

pub trait NodeLabeledGraph<Node: Idx>: Graph<Node> {
    fn label(&self, node: Node) -> Node;

    fn nodes_by_label(&self, label: Node) -> &[Node];

    fn label_count(&self) -> Node;

    fn max_label(&self) -> Node;

    fn max_label_frequency(&self) -> Node;

    fn neighbor_label_frequency(&self, node: Node) -> &HashMap<Node, Node>;
}

pub struct NodeLabeledCSRGraph<G> {
    graph: G,
    label_index: Box<[usize]>,
    label_index_offsets: Box<[usize]>,
    max_degree: usize,
    max_label: usize,
    max_label_frequency: usize,
    label_frequency: HashMap<usize, usize>,
    neighbor_label_frequencies: Option<Box<[HashMap<usize, usize>]>>,
}

impl<Node: Idx, G: Graph<Node>> Graph<Node> for NodeLabeledCSRGraph<G> {
    #[inline]
    fn node_count(&self) -> Node {
        self.graph.node_count()
    }

    #[inline]
    fn edge_count(&self) -> Node {
        self.graph.edge_count()
    }
}

impl<Node: Idx, G: DirectedGraph<Node>> DirectedGraph<Node> for NodeLabeledCSRGraph<G> {
    fn out_degree(&self, node: Node) -> Node {
        self.graph.out_degree(node)
    }

    fn out_neighbors(&self, node: Node) -> &[Node] {
        self.graph.out_neighbors(node)
    }

    fn in_degree(&self, node: Node) -> Node {
        self.graph.in_degree(node)
    }

    fn in_neighbors(&self, node: Node) -> &[Node] {
        self.graph.in_neighbors(node)
    }
}

impl<Node: Idx, G: UndirectedGraph<Node>> UndirectedGraph<Node> for NodeLabeledCSRGraph<G> {
    fn degree(&self, node: Node) -> Node {
        self.graph.degree(node)
    }

    fn neighbors(&self, node: Node) -> &[Node] {
        self.graph.neighbors(node)
    }
}

impl<Node: Idx, G: From<(EdgeList<Node>, CSROption)>> From<(DotGraph<Node>, CSROption)>
    for NodeLabeledCSRGraph<G>
{
    fn from(_: (DotGraph<Node>, CSROption)) -> Self {
        todo!()
    }
}

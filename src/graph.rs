use std::collections::HashMap;

use crate::{
    input::{DotGraph, EdgeList},
    DirectedGraph, Graph, UndirectedGraph,
};

struct CSR {
    offsets: Box<[usize]>,
    targets: Box<[usize]>,
}

impl CSR {
    #[inline]
    fn degree(&self, node: usize) -> usize {
        self.offsets[node + 1] - self.offsets[node]
    }

    #[inline]
    fn neighbors(&self, node: usize) -> &[usize] {
        let from = self.offsets[node];
        let to = self.offsets[node + 1];
        &self.targets[from..to]
    }
}

pub struct DirectedCSRGraph {
    node_count: usize,
    edge_count: usize,
    out_edges: CSR,
    in_edges: CSR,
}

impl Graph for DirectedCSRGraph {
    fn node_count(&self) -> usize {
        self.node_count
    }

    fn edge_count(&self) -> usize {
        self.edge_count
    }
}

impl DirectedGraph for DirectedCSRGraph {
    fn out_degree(&self, node: usize) -> usize {
        self.out_edges.degree(node)
    }

    fn out_neighbors(&self, node: usize) -> &[usize] {
        self.out_edges.neighbors(node)
    }

    fn in_degree(&self, node: usize) -> usize {
        self.in_edges.degree(node)
    }

    fn in_neighbors(&self, node: usize) -> &[usize] {
        self.in_edges.neighbors(node)
    }
}

impl From<EdgeList> for DirectedCSRGraph {
    fn from(_: EdgeList) -> Self {
        todo!()
    }
}

pub struct UndirectedCSRGraph {
    node_count: usize,
    edge_count: usize,
    edges: CSR,
}

impl Graph for UndirectedCSRGraph {
    fn node_count(&self) -> usize {
        self.node_count
    }

    fn edge_count(&self) -> usize {
        self.edge_count
    }
}

impl UndirectedGraph for UndirectedCSRGraph {
    fn degree(&self, node: usize) -> usize {
        self.edges.degree(node)
    }

    fn neighbors(&self, node: usize) -> &[usize] {
        self.edges.neighbors(node)
    }
}

impl From<EdgeList> for UndirectedCSRGraph {
    fn from(_: EdgeList) -> Self {
        todo!()
    }
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

impl<G: Graph> Graph for NodeLabeledCSRGraph<G> {
    #[inline]
    fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    #[inline]
    fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }
}

impl<G: DirectedGraph> DirectedGraph for NodeLabeledCSRGraph<G> {
    fn out_degree(&self, node: usize) -> usize {
        self.graph.out_degree(node)
    }

    fn out_neighbors(&self, node: usize) -> &[usize] {
        self.graph.out_neighbors(node)
    }

    fn in_degree(&self, node: usize) -> usize {
        self.graph.in_degree(node)
    }

    fn in_neighbors(&self, node: usize) -> &[usize] {
        self.graph.in_neighbors(node)
    }
}

impl<G: UndirectedGraph> UndirectedGraph for NodeLabeledCSRGraph<G> {
    fn degree(&self, node: usize) -> usize {
        self.graph.degree(node)
    }

    fn neighbors(&self, node: usize) -> &[usize] {
        self.graph.neighbors(node)
    }
}

impl<G: From<EdgeList>> From<DotGraph> for NodeLabeledCSRGraph<G> {
    fn from(_: DotGraph) -> Self {
        todo!()
    }
}

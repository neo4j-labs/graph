use std::hash::Hash;

use fxhash::FxHashMap;

use crate::input::dotgraph::DotGraph;
use crate::input::edgelist::EdgeList;
use crate::NodeLabeledGraph;
use crate::{index::Idx, DirectedGraph, Graph, UndirectedGraph};

use super::csr::{Csr, CsrLayout, DirectedCsrGraph, UndirectedCsrGraph};

pub type DirectedNodeLabeledCsrGraph<Node, Label> =
    NodeLabeledCsrGraph<DirectedCsrGraph<Node>, Node, Label>;
pub type UndirectedNodeLabeledCsrGraph<Node, Label> =
    NodeLabeledCsrGraph<UndirectedCsrGraph<Node>, Node, Label>;

pub struct NodeLabeledCsrGraph<G, Node: Idx, Label: Idx> {
    graph: G,
    label_count: Label,
    labels: Box<[Label]>,
    label_index: Csr<Label, Node>,
    max_degree: Node,
    max_label: Label,
    max_label_frequency: usize,
    label_frequency: FxHashMap<Label, usize>,
}

impl<G, Node, Label> Graph<Node> for NodeLabeledCsrGraph<G, Node, Label>
where
    Node: Idx,
    Label: Idx,
    G: Graph<Node>,
{
    delegate::delegate! {
        to self.graph {
            fn node_count(&self) -> Node;

            fn edge_count(&self) -> Node;
        }
    }
}

impl<G, Node, Label> DirectedGraph<Node> for NodeLabeledCsrGraph<G, Node, Label>
where
    Node: Idx,
    Label: Idx,
    G: DirectedGraph<Node>,
{
    delegate::delegate! {
        to self.graph {
            fn out_degree(&self, node: Node) -> Node;

            fn out_neighbors(&self, node: Node) -> &[Node];

            fn in_degree(&self, node: Node) -> Node;

            fn in_neighbors(&self, node: Node) -> &[Node] ;
        }
    }
}

impl<G, Node, Label> UndirectedGraph<Node> for NodeLabeledCsrGraph<G, Node, Label>
where
    Node: Idx,
    Label: Idx,
    G: UndirectedGraph<Node>,
{
    delegate::delegate! {
        to self.graph {
            fn degree(&self, node: Node) -> Node;

            fn neighbors(&self, node: Node) -> &[Node];
        }
    }
}

impl<G, Node, Label> NodeLabeledGraph<Node, Label> for NodeLabeledCsrGraph<G, Node, Label>
where
    Node: Idx,
    Label: Idx + Hash,
    G: Graph<Node>,
{
    fn label(&self, node: Node) -> Label {
        self.labels[node.index()]
    }

    fn nodes_by_label(&self, label: Label) -> &[Node] {
        self.label_index.targets(label)
    }

    fn label_count(&self) -> Label {
        self.label_count
    }

    fn label_frequency(&self, label: Label) -> usize {
        self.label_frequency
            .get(&label)
            .cloned()
            .unwrap_or_default()
    }

    fn max_label(&self) -> Label {
        self.max_label
    }

    fn max_label_frequency(&self) -> usize {
        self.max_label_frequency
    }
}

impl<G, Node, Label> NodeLabeledCsrGraph<G, Node, Label>
where
    Node: Idx,
    Label: Idx + Hash,
    G: Graph<Node>,
{
    pub fn max_degree(&self) -> Node {
        self.max_degree
    }
}

impl<G, Node, Label> From<(DotGraph<Node, Label>, CsrLayout)>
    for NodeLabeledCsrGraph<G, Node, Label>
where
    Node: Idx,
    Label: Idx + Hash,
    G: From<(EdgeList<Node>, CsrLayout)>,
{
    fn from((dot_graph, csr_layout): (DotGraph<Node, Label>, CsrLayout)) -> Self {
        let label_index = dot_graph.label_index();
        let label_count = dot_graph.label_count();
        let max_label_frequency = dot_graph.max_label_frequency();

        let DotGraph {
            label_frequencies: label_frequency,
            edge_list,
            labels,
            max_degree,
            max_label,
        } = dot_graph;

        let graph = G::from((edge_list, csr_layout));

        NodeLabeledCsrGraph {
            graph,
            label_count,
            labels: labels.into_boxed_slice(),
            label_index,
            max_degree,
            max_label,
            max_label_frequency,
            label_frequency,
        }
    }
}

impl<Node: Idx, Label: Idx + Hash> From<(&gdl::Graph, CsrLayout)>
    for DirectedNodeLabeledCsrGraph<Node, Label>
{
    fn from((gdl_graph, csr_layout): (&gdl::Graph, CsrLayout)) -> Self {
        DirectedNodeLabeledCsrGraph::from((DotGraph::<Node, Label>::from(gdl_graph), csr_layout))
    }
}

impl<Node: Idx, Label: Idx + Hash> From<(gdl::Graph, CsrLayout)>
    for DirectedNodeLabeledCsrGraph<Node, Label>
{
    fn from((gdl_graph, csr_layout): (gdl::Graph, CsrLayout)) -> Self {
        DirectedNodeLabeledCsrGraph::from((DotGraph::<Node, Label>::from(&gdl_graph), csr_layout))
    }
}

impl<Node: Idx, Label: Idx + Hash> From<(&gdl::Graph, CsrLayout)>
    for UndirectedNodeLabeledCsrGraph<Node, Label>
{
    fn from((gdl_graph, csr_layout): (&gdl::Graph, CsrLayout)) -> Self {
        UndirectedNodeLabeledCsrGraph::from((DotGraph::<Node, Label>::from(gdl_graph), csr_layout))
    }
}

impl<Node: Idx, Label: Idx + Hash> From<(gdl::Graph, CsrLayout)>
    for UndirectedNodeLabeledCsrGraph<Node, Label>
{
    fn from((gdl_graph, csr_layout): (gdl::Graph, CsrLayout)) -> Self {
        UndirectedNodeLabeledCsrGraph::from((DotGraph::<Node, Label>::from(&gdl_graph), csr_layout))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{builder::GraphBuilder, input::dotgraph::DotGraphInput, Error};

    use super::*;

    #[test]
    fn should_compile_test() {
        fn inner_test() -> Result<(), Error> {
            let _g: DirectedNodeLabeledCsrGraph<usize, usize> = GraphBuilder::new()
                .file_format(DotGraphInput::default())
                .path("graph")
                .build()?;

            let _g: UndirectedNodeLabeledCsrGraph<usize, usize> = GraphBuilder::new()
                .file_format(DotGraphInput::default())
                .path("graph")
                .build()?;

            Ok(())
        }

        assert!(inner_test().is_err())
    }

    #[test]
    fn directed_from_file_test() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.graph"]
            .iter()
            .collect::<PathBuf>();

        let g: DirectedNodeLabeledCsrGraph<usize, usize> = GraphBuilder::new()
            .file_format(DotGraphInput::default())
            .path(path)
            .build()
            .unwrap();

        assert_eq!(g.node_count(), 5);
        assert_eq!(g.edge_count(), 6);
        assert_eq!(g.label_count(), 3);
        assert_eq!(g.max_label(), 2);
        assert_eq!(g.max_label_frequency(), 2);
        assert_eq!(g.max_degree, 3);

        assert_eq!(g.label(0), 0);
        assert_eq!(g.label(1), 1);
        assert_eq!(g.label(2), 2);
        assert_eq!(g.label(3), 1);
        assert_eq!(g.label(4), 2);

        assert_eq!(g.nodes_by_label(0), &[0]);
        assert_eq!(g.nodes_by_label(1), &[1, 3]);
        assert_eq!(g.nodes_by_label(2), &[2, 4]);

        assert_eq!(g.label_frequency(0), 1);
        assert_eq!(g.label_frequency(1), 2);
        assert_eq!(g.label_frequency(2), 2);

        assert_eq!(g.out_neighbors(0), &[1, 2]);
        assert_eq!(g.out_neighbors(1), &[2, 3]);
        assert_eq!(g.out_neighbors(2), &[4]);
        assert_eq!(g.out_neighbors(3), &[4]);
        assert_eq!(g.out_neighbors(4), &[]);

        assert_eq!(g.in_neighbors(0), &[]);
        assert_eq!(g.in_neighbors(1), &[0]);
        assert_eq!(g.in_neighbors(2), &[0, 1]);
        assert_eq!(g.in_neighbors(3), &[1]);
        assert_eq!(g.in_neighbors(4), &[2, 3]);
    }

    #[test]
    fn undirected_from_file_test() {
        let path = [env!("CARGO_MANIFEST_DIR"), "resources", "test.graph"]
            .iter()
            .collect::<PathBuf>();

        let g: UndirectedNodeLabeledCsrGraph<usize, usize> = GraphBuilder::new()
            .file_format(DotGraphInput::default())
            .path(path)
            .build()
            .unwrap();

        assert_eq!(g.node_count(), 5);
        assert_eq!(g.edge_count(), 6);
        assert_eq!(g.label_count(), 3);
        assert_eq!(g.max_label(), 2);
        assert_eq!(g.max_label_frequency(), 2);
        assert_eq!(g.max_degree, 3);

        assert_eq!(g.label(0), 0);
        assert_eq!(g.label(1), 1);
        assert_eq!(g.label(2), 2);
        assert_eq!(g.label(3), 1);
        assert_eq!(g.label(4), 2);

        assert_eq!(g.nodes_by_label(0), &[0]);
        assert_eq!(g.nodes_by_label(1), &[1, 3]);
        assert_eq!(g.nodes_by_label(2), &[2, 4]);

        assert_eq!(g.label_frequency(0), 1);
        assert_eq!(g.label_frequency(1), 2);
        assert_eq!(g.label_frequency(2), 2);

        assert_eq!(g.neighbors(0), &[1, 2]);
        assert_eq!(g.neighbors(1), &[0, 2, 3]);
        assert_eq!(g.neighbors(2), &[0, 1, 4]);
        assert_eq!(g.neighbors(3), &[1, 4]);
        assert_eq!(g.neighbors(4), &[2, 3]);
    }

    #[test]
    fn directed_from_gdl_test() {
        let graph: DirectedNodeLabeledCsrGraph<usize, usize> = GraphBuilder::new()
            .gdl_str::<usize, _>("(:L0)-->(:L1)-->(:L2)-->(:L0)")
            .build()
            .unwrap();

        assert_eq!(graph.node_count(), 4);
        assert_eq!(graph.edge_count(), 3);

        assert_eq!(graph.label(0), 0);
        assert_eq!(graph.label(1), 1);
        assert_eq!(graph.label(2), 2);
        assert_eq!(graph.label(3), 0);
    }

    #[test]
    fn undirected_from_gdl_test() {
        let graph: UndirectedNodeLabeledCsrGraph<usize, usize> = GraphBuilder::new()
            .gdl_str::<usize, _>("(:L0)-->(:L1)-->(:L2)-->(:L0)")
            .build()
            .unwrap();

        assert_eq!(graph.node_count(), 4);
        assert_eq!(graph.edge_count(), 3);

        assert_eq!(graph.label(0), 0);
        assert_eq!(graph.label(1), 1);
        assert_eq!(graph.label(2), 2);
        assert_eq!(graph.label(3), 0);
    }
}

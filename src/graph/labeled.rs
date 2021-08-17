use std::collections::HashMap;

use crate::input::dotgraph::DotGraph;
use crate::input::EdgeList;
use crate::{index::Idx, DirectedGraph, Graph, UndirectedGraph};

use super::csr::{Csr, CsrLayout, DirectedCsrGraph, UndirectedCsrGraph};

pub type DirectedNodeLabeledCsrGraph<Node, Label> =
    NodeLabeledCsrGraph<DirectedCsrGraph<Node>, Label>;
pub type UndirectedNodeLabeledCsrGraph<Node, Label> =
    NodeLabeledCsrGraph<UndirectedCsrGraph<Node>, Label>;

pub trait NodeLabeledGraph<Node: Idx, Label: Idx>: Graph<Node> {
    fn label(&self, node: Node) -> Label;

    fn nodes_by_label(&self, label: Label) -> &[Node];

    fn label_count(&self) -> usize;

    fn max_label(&self) -> Label;

    fn max_label_frequency(&self) -> usize;

    fn neighbor_label_frequency(&self, node: Node) -> &HashMap<Label, usize>;
}

pub struct NodeLabeledCsrGraph<G, Label: Idx> {
    graph: G,
    labels: Csr<Label>,
    max_degree: usize,
    max_label: Label,
    max_label_frequency: usize,
    label_frequency: HashMap<Label, usize>,
    neighbor_label_frequencies: Option<Box<[HashMap<Label, usize>]>>,
}

impl<Node, Label, G> Graph<Node> for NodeLabeledCsrGraph<G, Label>
where
    Node: Idx,
    Label: Idx,
    G: Graph<Node>,
{
    #[inline]
    fn node_count(&self) -> Node {
        self.graph.node_count()
    }

    #[inline]
    fn edge_count(&self) -> Node {
        self.graph.edge_count()
    }
}

impl<Node, Label, G> DirectedGraph<Node> for NodeLabeledCsrGraph<G, Label>
where
    Node: Idx,
    Label: Idx,
    G: DirectedGraph<Node>,
{
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

impl<Node, Label, G> UndirectedGraph<Node> for NodeLabeledCsrGraph<G, Label>
where
    Node: Idx,
    Label: Idx,
    G: UndirectedGraph<Node>,
{
    fn degree(&self, node: Node) -> Node {
        self.graph.degree(node)
    }

    fn neighbors(&self, node: Node) -> &[Node] {
        self.graph.neighbors(node)
    }
}

impl<Node, Label, G> From<(DotGraph<Node, Label>, CsrLayout)> for NodeLabeledCsrGraph<G, Label>
where
    Node: Idx,
    Label: Idx,
    G: From<(EdgeList<Node>, CsrLayout)>,
{
    fn from(_: (DotGraph<Node, Label>, CsrLayout)) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
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
}

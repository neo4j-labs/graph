use std::collections::HashMap;

use crate::input::dotgraph::DotGraph;
use crate::input::EdgeList;
use crate::{index::Idx, DirectedGraph, Graph, UndirectedGraph};

use super::csr::{Csr, CsrLayout, DirectedCsrGraph, UndirectedCsrGraph};

pub type DirectedNodeLabeledCsrGraph<Node, Label> =
    NodeLabeledCsrGraph<DirectedCsrGraph<Node>, Node, Label>;
pub type UndirectedNodeLabeledCsrGraph<Node, Label> =
    NodeLabeledCsrGraph<UndirectedCsrGraph<Node>, Node, Label>;

pub trait NodeLabeledGraph<Node, Label>: Graph<Node>
where
    Node: Idx,
    Label: Idx,
{
    fn label(&self, node: Node) -> Label;

    fn nodes_by_label(&self, label: Label) -> &[Node];

    fn label_count(&self) -> usize;

    fn max_label(&self) -> Label;

    fn max_label_frequency(&self) -> usize;

    fn neighbor_label_frequency(&self, node: Node) -> &HashMap<Label, usize>;
}

pub struct NodeLabeledCsrGraph<G, Node: Idx, Label: Idx> {
    graph: G,
    label_count: usize,
    labels: Box<[Label]>,
    label_index: Csr<Label, Node>,
    max_degree: usize,
    max_label: Label,
    max_label_frequency: usize,
    label_frequency: HashMap<Label, usize>,
    neighbor_label_frequencies: Option<Box<[HashMap<Label, usize>]>>,
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
    Label: Idx,
    G: Graph<Node>,
{
    fn label(&self, node: Node) -> Label {
        self.labels[node.index()]
    }

    fn nodes_by_label(&self, label: Label) -> &[Node] {
        self.label_index.targets(label)
    }

    fn label_count(&self) -> usize {
        self.label_count
    }

    fn max_label(&self) -> Label {
        self.max_label
    }

    fn max_label_frequency(&self) -> usize {
        self.max_label_frequency
    }

    fn neighbor_label_frequency(&self, node: Node) -> &HashMap<Label, usize> {
        if let Some(nlfs) = &self.neighbor_label_frequencies {
            &nlfs[node.index()]
        } else {
            panic!("Neighbor label frequencies have not been loaded.")
        }
    }
}

impl<G, Node, Label> From<(DotGraph<Node, Label>, CsrLayout)>
    for NodeLabeledCsrGraph<G, Node, Label>
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
    fn from_file_test() {
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
        assert_eq!(g.label_count(), 5);
        assert_eq!(g.max_label(), 2);
        assert_eq!(g.max_degree, 3);
    }
}

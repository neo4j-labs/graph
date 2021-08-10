use std::collections::HashMap;

#[cfg(feature = "dotgraph")]
use crate::input::dotgraph::DotGraph;
use crate::{index::Idx, DirectedGraph, Graph, UndirectedGraph};

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

impl<Node, G> DirectedGraph<Node> for NodeLabeledCSRGraph<G>
where
    Node: Idx,
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

impl<Node, G> UndirectedGraph<Node> for NodeLabeledCSRGraph<G>
where
    Node: Idx,
    G: UndirectedGraph<Node>,
{
    fn degree(&self, node: Node) -> Node {
        self.graph.degree(node)
    }

    fn neighbors(&self, node: Node) -> &[Node] {
        self.graph.neighbors(node)
    }
}

#[cfg(feature = "dotgraph")]
impl<Node: Idx, G: From<(EdgeList<Node>, CSROption)>> From<(DotGraph<Node>, CSROption)>
    for NodeLabeledCSRGraph<G>
{
    fn from(_: (DotGraph<Node>, CSROption)) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "dotgraph")]
    #[test]
    fn should_compile_test() {
        fn inner_test() -> Result<(), Error> {
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
}

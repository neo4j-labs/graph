use crate::index::{AtomicIdx, Idx};

use rayon::prelude::*;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::Ordering::AcqRel,
};

pub mod binary;
pub mod dotgraph;
pub mod edgelist;

pub struct InputPath<P>(pub(crate) P);

pub trait InputCapabilities<Node: Idx> {
    type GraphInput;
}

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

pub struct EdgeList<Node: Idx>(Box<[(Node, Node)]>);

impl<Node: Idx> AsRef<[(Node, Node)]> for EdgeList<Node> {
    fn as_ref(&self) -> &[(Node, Node)] {
        &self.0
    }
}

impl<Node: Idx> Deref for EdgeList<Node> {
    type Target = [(Node, Node)];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<Node: Idx> DerefMut for EdgeList<Node> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<Node: Idx> EdgeList<Node> {
    pub fn new(edges: Vec<(Node, Node)>) -> Self {
        Self(edges.into_boxed_slice())
    }

    pub(crate) fn max_node_id(&self) -> Node {
        self.par_iter()
            .map(|(s, t)| Node::max(*s, *t))
            .reduce(Node::zero, Node::max)
    }

    pub(crate) fn degrees(&self, node_count: Node, direction: Direction) -> Vec<Node::Atomic> {
        let mut degrees = Vec::with_capacity(node_count.index());
        degrees.resize_with(node_count.index(), Node::Atomic::zero);

        if matches!(direction, Direction::Outgoing | Direction::Undirected) {
            self.par_iter().for_each(|(s, _)| {
                degrees[s.index()].get_and_increment(AcqRel);
            });
        }

        if matches!(direction, Direction::Incoming | Direction::Undirected) {
            self.par_iter().for_each(|(_, t)| {
                degrees[t.index()].get_and_increment(AcqRel);
            });
        }

        degrees
    }
}

impl<Node: Idx> From<&gdl::Graph> for EdgeList<Node> {
    fn from(gdl_graph: &gdl::Graph) -> Self {
        let edges = gdl_graph
            .relationships()
            .into_iter()
            .map(|r| {
                let source = gdl_graph.get_node(r.source()).unwrap().id();
                let target = gdl_graph.get_node(r.target()).unwrap().id();

                (Node::new(source), Node::new(target))
            })
            .collect::<Vec<_>>();

        EdgeList::new(edges)
    }
}

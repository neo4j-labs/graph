use crate::index::{AtomicIdx, Idx};

use rayon::prelude::*;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::Ordering::SeqCst,
};

#[cfg(feature = "dotgraph")]
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
        self.0
            .par_iter()
            .map(|(s, t)| Node::max(*s, *t))
            .reduce(Node::zero, Node::max)
    }

    pub(crate) fn degrees(&self, node_count: Node, direction: Direction) -> Vec<Node::Atomic> {
        let mut degrees = Vec::with_capacity(node_count.index());
        degrees.resize_with(node_count.index(), Node::Atomic::zero);

        match direction {
            Direction::Outgoing => self.par_iter().for_each(|(s, _)| {
                degrees[s.index()].get_and_increment(SeqCst);
            }),
            Direction::Incoming => self.par_iter().for_each(|(_, t)| {
                degrees[t.index()].get_and_increment(SeqCst);
            }),
            Direction::Undirected => self.par_iter().for_each(|(s, t)| {
                degrees[s.index()].get_and_increment(SeqCst);
                degrees[t.index()].get_and_increment(SeqCst);
            }),
        }

        degrees
    }
}

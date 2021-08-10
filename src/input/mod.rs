use crate::index::Idx;

#[cfg(feature = "dotgraph")]
pub mod dotgraph;
pub mod edgelist;

pub struct MyPath<P>(pub(crate) P);

pub trait InputCapabilities<Node: Idx> {
    type GraphInput;
}

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

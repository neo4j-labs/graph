use crate::index::Idx;

pub mod binary;
pub mod dotgraph;
pub mod edgelist;

pub use binary::BinaryInput;
pub use dotgraph::DotGraph;
pub use dotgraph::DotGraphInput;
pub use edgelist::EdgeList;
pub use edgelist::EdgeListInput;

pub struct InputPath<P>(pub(crate) P);

pub trait InputCapabilities<NI: Idx> {
    type GraphInput;
}

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

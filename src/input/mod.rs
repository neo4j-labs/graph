pub mod dotgraph;
pub mod edgelist;

pub use edgelist::EdgeList;
pub use edgelist::EdgeListInput;

pub use dotgraph::DotGraph;
pub use dotgraph::DotGraphInput;
pub struct MyPath<P>(pub(crate) P);

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

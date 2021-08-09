pub mod dotgraph;
pub mod edgelist;

pub struct MyPath<P>(pub(crate) P);

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

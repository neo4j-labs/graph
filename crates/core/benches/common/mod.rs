#![allow(dead_code)]

pub mod gen;

#[derive(Clone, Copy)]
pub(crate) struct Input {
    pub name: &'static str,
    pub node_count: usize,
    pub edge_count: usize,
}

pub(crate) const SMALL: Input = Input {
    name: "small",
    node_count: 1_000,
    edge_count: 10_000,
};

pub(crate) const MEDIUM: Input = Input {
    name: "medium",
    node_count: 10_000,
    edge_count: 100_000,
};

pub(crate) const LARGE: Input = Input {
    name: "large",
    node_count: 100_000,
    edge_count: 1_000_000,
};

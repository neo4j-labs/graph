#![allow(dead_code)]

use graph_builder::prelude::Idx;
use rand::prelude::*;


#[derive(Clone, Copy)]
pub struct Input {
    pub name: &'static str,
    pub node_count: usize,
    pub edge_count: usize,
}

pub const SMALL: Input = Input {
    name: "small",
    node_count: 1_000,
    edge_count: 10_000,
};

pub const MEDIUM: Input = Input {
    name: "medium",
    node_count: 10_000,
    edge_count: 100_000,
};

pub const LARGE: Input = Input {
    name: "large",
    node_count: 100_000,
    edge_count: 1_000_000,
};

pub fn uniform_edge_list<NI, EV, F>(
    node_count: usize,
    edge_count: usize,
    edge_value: F,
) -> Vec<(NI, NI, EV)>
where
    NI: Idx,
    F: Fn(NI, NI) -> EV,
{
    let mut rng = StdRng::seed_from_u64(42);

    (0..edge_count)
        .map(|_| {
            let source = NI::new(rng.gen_range(0..node_count));
            let target = NI::new(rng.gen_range(0..node_count));

            (source, target, edge_value(source, target))
        })
        .collect::<Vec<_>>()
}

pub fn node_values<NV, F>(node_count: usize, node_value: F) -> Vec<NV>
where
    F: Fn(usize, &mut StdRng) -> NV,
{
    let mut rng = StdRng::seed_from_u64(42);

    (0..node_count)
        .map(|n| node_value(n, &mut rng))
        .collect::<Vec<_>>()
}

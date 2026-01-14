use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use graph_builder::index::Idx;

pub fn uniform_edge_list<NI>(
    node_count: usize,
    edge_count: usize,
) -> Vec<(NI, NI)>
where
    NI: Idx,
{
    let mut rng = StdRng::seed_from_u64(42);

    (0..edge_count)
        .map(|_| {
            let source = NI::new(rng.random_range(0..node_count));
            let target = NI::new(rng.random_range(0..node_count));

            (source, target)
        })
        .collect::<Vec<_>>()
}
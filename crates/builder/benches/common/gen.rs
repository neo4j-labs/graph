use graph_builder::prelude::Idx;
use rand::prelude::*;

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

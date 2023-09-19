use criterion::*;
use graph_builder::graph::csr::Csr;

use graph_builder::topology_from_edge_list;

use bench_util::*;
use graph_builder::graph::adj_list::AdjacencyList;

fn csr_from_edge_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("csr_from_edge_list");
    group.sampling_mode(SamplingMode::Flat);
    topology_from_edge_list::<Csr<usize, usize, ()>>(group);
}

fn adjacency_list_from_edge_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("adjacency_list_from_edge_list");
    group.sampling_mode(SamplingMode::Flat);
    topology_from_edge_list::<AdjacencyList<usize, ()>>(group);
}

criterion_group!(benches, csr_from_edge_list, adjacency_list_from_edge_list);
criterion_main!(benches);

use std::time::Duration;

use bench_util::*;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use graph::prelude::*;
use rand::prelude::*;

fn msbfs(c: &mut Criterion) {
    let graph500_scale = 22;
    let seed = 42;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut group = c.benchmark_group("msbfs");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(200))
        .sampling_mode(SamplingMode::Flat);

    let edge_list_file = create_graph_500(graph500_scale).unwrap();
    let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
        .file_format(EdgeListInput::default())
        .path(edge_list_file)
        .build()
        .unwrap();

    let node_count = graph.node_count().index();
    let bit_width = u64::BITS as usize;

    // Pick nodes at random and let's :fingers-crossed: that
    // they are no isolated components.
    let mut sources = Vec::with_capacity(bit_width);
    sources.resize_with(bit_width, || rng.gen_range(0..node_count));

    group.bench_function("base", |b| {
        b.iter(|| {
            black_box(graph::msbfs::msbfs(&graph, &sources, |sources, t, d| {
                for s in sources {
                    black_box((s, t, d));
                }
            }))
        })
    });
    group.bench_function("anp", |b| {
        b.iter(|| {
            black_box(graph::msbfs::msbfs_anp(
                &graph,
                &sources,
                |sources, t, d| {
                    for s in sources {
                        black_box((s, t, d));
                    }
                },
            ))
        })
    });

    group.finish();
}

criterion_group!(benches, msbfs);
criterion_main!(benches);

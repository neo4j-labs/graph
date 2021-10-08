use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use graph::prelude::{Direction, EdgeList, Idx};
use rand::prelude::*;

#[derive(Clone, Copy)]
struct Input {
    name: &'static str,
    node_count: usize,
    edge_count: usize,
}

const SMALL: Input = Input {
    name: "small",
    node_count: 1_000,
    edge_count: 10_000,
};

const MEDIUM: Input = Input {
    name: "medium",
    node_count: 10_000,
    edge_count: 100_000,
};

const LARGE: Input = Input {
    name: "large",
    node_count: 100_000,
    edge_count: 1_000_000,
};

fn max_node_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("max_node_id");
    group.sampling_mode(SamplingMode::Flat);

    group.bench_with_input(
        BenchmarkId::from_parameter(SMALL.name),
        &SMALL,
        |b, &input| bench_max_node_id(b, input),
    );
    group.bench_with_input(
        BenchmarkId::from_parameter(MEDIUM.name),
        &MEDIUM,
        |b, &input| bench_max_node_id(b, input),
    );
    group.bench_with_input(
        BenchmarkId::from_parameter(LARGE.name),
        &LARGE,
        |b, &input| bench_max_node_id(b, input),
    );

    group.finish();
}

fn bench_max_node_id(b: &mut criterion::Bencher, input: Input) {
    let input: Vec<(usize, usize, ())> = gen_input(input.node_count, input.edge_count, |_, _| ());
    b.iter_batched(
        || EdgeList::new(input.clone()),
        |edge_list| black_box(edge_list.max_node_id()),
        criterion::BatchSize::SmallInput,
    )
}

fn degrees(c: &mut Criterion) {
    let mut group = c.benchmark_group("degrees");
    group.sampling_mode(SamplingMode::Flat);

    for direction in [
        Direction::Outgoing,
        Direction::Incoming,
        Direction::Undirected,
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_{:?}", SMALL.name, direction)),
            &(SMALL, direction),
            |b, &(input, direction)| bench_degrees(b, input, direction),
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_{:?}", MEDIUM.name, direction)),
            &(MEDIUM, direction),
            |b, &(input, direction)| bench_degrees(b, input, direction),
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_{:?}", LARGE.name, direction)),
            &(LARGE, direction),
            |b, &(input, direction)| bench_degrees(b, input, direction),
        );
    }

    group.finish();
}

fn bench_degrees(b: &mut criterion::Bencher, input: Input, direction: Direction) {
    let edges: Vec<(usize, usize, ())> = gen_input(input.node_count, input.edge_count, |_, _| ());
    b.iter_batched(
        || EdgeList::new(edges.clone()),
        |edge_list| black_box(edge_list.degrees(input.node_count, direction)),
        criterion::BatchSize::SmallInput,
    )
}

criterion_group!(benches, max_node_id, degrees);
criterion_main!(benches);

fn gen_input<NI, EV, F>(node_count: usize, edge_count: usize, edge_value: F) -> Vec<(NI, NI, EV)>
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

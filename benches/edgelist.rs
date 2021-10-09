use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use graph::prelude::{Direction, EdgeList};

mod common;

use common::gen::uniform_edge_list;
use common::*;

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

fn bench_max_node_id(
    b: &mut criterion::Bencher,
    Input {
        name: _,
        node_count,
        edge_count,
    }: Input,
) {
    let input: Vec<(usize, usize, ())> = uniform_edge_list(node_count, edge_count, |_, _| ());
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

fn bench_degrees(
    b: &mut criterion::Bencher,
    Input {
        name: _,
        node_count,
        edge_count,
    }: Input,
    direction: Direction,
) {
    let edges: Vec<(usize, usize, ())> = uniform_edge_list(node_count, edge_count, |_, _| ());
    b.iter_batched(
        || EdgeList::new(edges.clone()),
        |edge_list| black_box(edge_list.degrees(node_count, direction)),
        criterion::BatchSize::SmallInput,
    )
}

criterion_group!(benches, max_node_id, degrees);
criterion_main!(benches);

use graph::prelude::*;

use log::info;

use std::time::Instant;

use super::*;

pub(crate) fn sssp(args: CommonArgs, config: DeltaSteppingConfig) -> Result<()> {
    let CommonArgs {
        path,
        format: _,
        use_32_bit,
        runs,
        warmup_runs,
    } = args;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    if use_32_bit {
        run::<u32>(path, runs, warmup_runs, config)
    } else {
        run::<usize>(path, runs, warmup_runs, config)
    }
}

fn run<NI: Idx>(
    path: PathBuf,
    runs: usize,
    warmup_runs: usize,
    config: DeltaSteppingConfig,
) -> Result<()> {
    let graph: DirectedCsrGraph<NI, (), f32> = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(EdgeListInput::default())
        .path(path)
        .build()?;

    for run in 1..=warmup_runs {
        let start = Instant::now();
        delta_stepping(&graph, config);
        let took = start.elapsed();

        info!(
            "Warm-up run {} of {} finished in {:.6?}",
            run, warmup_runs, took,
        );
    }

    let mut durations = vec![];

    for run in 1..runs {
        let start = Instant::now();
        delta_stepping(&graph, config);
        let took = start.elapsed();
        durations.push(took);

        info!("Run {} of {} finished in {:.6?}", run, runs, took,);
    }

    let total = durations
        .into_iter()
        .reduce(|a, b| a + b)
        .unwrap_or_default();

    info!("Average runtime: {:?}", total / runs as u32);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CsrLayout;
    use crate::GraphBuilder;

    #[test]
    fn test_dijkstra() {
        let gdl = "(a:A)
                        (b:B)
                        (c:C)
                        (d:D)
                        (e:E)
                        (f:F)
                        (a)-[{cost:  4.0 }]->(b)
                        (a)-[{cost:  2.0 }]->(c)
                        (b)-[{cost:  5.0 }]->(c)
                        (b)-[{cost: 10.0 }]->(d)
                        (c)-[{cost:  3.0 }]->(e)
                        (d)-[{cost: 11.0 }]->(f)
                        (e)-[{cost:  4.0 }]->(d)";

        let graph: DirectedCsrGraph<usize, (), f32> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        let actual: Vec<f32> = dijkstra(&graph, 0);
        let expected: Vec<f32> = vec![0.0, 4.0, 2.0, 9.0, 5.0, 20.0];

        assert_eq!(actual, expected);
    }

    fn dijkstra<NI: Idx>(graph: &DirectedCsrGraph<NI, (), f32>, start_node: usize) -> Vec<f32> {
        const INF: f32 = f32::MAX;
        use float_ord::FloatOrd;
        use std::cmp::Reverse;
        use std::time::Instant;

        let start = Instant::now();

        let node_count = graph.node_count().index();

        let mut distances = Vec::with_capacity(node_count);
        distances.resize_with(node_count, || FloatOrd(INF));
        distances[start_node.index()] = FloatOrd(0.0);

        let mut queue = std::collections::BinaryHeap::new();
        queue.push(Reverse((FloatOrd(0.0), NI::new(start_node))));

        while let Some(Reverse((cost, node))) = queue.pop() {
            if cost == distances[node.index()] {
                for Target { target, value } in graph.out_neighbors_with_values(node) {
                    let new_distance = cost.0 + value;
                    if new_distance < distances[target.index()].0 {
                        let new_distance = FloatOrd(new_distance);
                        distances[target.index()] = new_distance;
                        queue.push(Reverse((new_distance, *target)));
                    }
                }
            }
        }

        info!("Computed Dijkstra in {:?}", start.elapsed());

        distances.into_iter().map(|d| d.0).collect()
    }
}

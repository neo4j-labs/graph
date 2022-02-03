use graph::prelude::*;

use float_ord::FloatOrd;
use log::info;
use rayon::prelude::*;

use std::{cmp::Reverse, path::Path, path::PathBuf, sync::atomic::Ordering, time::Instant};

const INF: f32 = f32::MAX;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli::AppArgs {
        path,
        use_32_bit,
        validate,
        runs,
        start_node,
        delta,
    } = cli::create()?;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    if validate {
        if use_32_bit {
            validate_result::<u32>(&path, start_node as u32, delta)
        } else {
            validate_result::<usize>(&path, start_node, delta)
        }
    } else if use_32_bit {
        run::<u32>(path, runs, start_node as u32, delta)
    } else {
        run::<usize>(path, runs, start_node, delta)
    }
}

fn run<NI: Idx>(
    path: PathBuf,
    runs: usize,
    start_node: NI,
    delta: f32,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph: DirectedCsrGraph<NI, (), f32> = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(EdgeListInput::default())
        .path(path)
        .build()?;

    let config = DeltaSteppingConfig::new(start_node, delta);

    for _ in 0..runs {
        delta_stepping(&graph, config);
    }

    Ok(())
}

fn validate_result<NI: Idx>(
    path: &Path,
    start_node: NI,
    delta: f32,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph: DirectedCsrGraph<NI, (), f32> = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(EdgeListInput::default())
        .path(path)
        .build()?;

    let config = DeltaSteppingConfig::new(start_node, delta);

    let par_result = delta_stepping(&graph, config);

    let reached_nodes: usize = par_result
        .par_iter()
        .filter_map(|distance| {
            if distance.load(Ordering::Acquire) == INF {
                None
            } else {
                Some(1)
            }
        })
        .sum();

    info!(
        "SSSP reached {} nodes ({:.2}%)",
        reached_nodes,
        reached_nodes as f64 / graph.node_count().index() as f64 * 100.0
    );

    let seq_result = dijkstra(&graph, start_node);

    assert_eq!(par_result.len(), seq_result.len());

    par_result
        .into_iter()
        .enumerate()
        .zip(seq_result.into_iter())
        .filter_map(|((node, actual), expected)| {
            let actual = actual.load(Ordering::Acquire);
            if actual != expected {
                Some((node, actual, expected))
            } else {
                None
            }
        })
        .for_each(|(node, actual, expected)| {
            println!(
                "mismatch for node {}, actual = {}, expected = {}",
                node, actual, expected
            );
        });

    Ok(())
}

fn dijkstra<NI: Idx>(graph: &DirectedCsrGraph<NI, (), f32>, start_node: NI) -> Vec<f32> {
    let start = Instant::now();

    let node_count = graph.node_count().index();

    let mut distances = Vec::with_capacity(node_count);
    distances.resize_with(node_count, || FloatOrd(INF));
    distances[start_node.index()] = FloatOrd(0.0);

    let mut queue = std::collections::BinaryHeap::new();
    queue.push(Reverse((FloatOrd(0.0), start_node)));

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

mod cli {
    use pico_args::Arguments;
    use std::{convert::Infallible, ffi::OsStr, path::PathBuf};

    #[derive(Debug)]
    pub(crate) struct AppArgs {
        pub(crate) path: std::path::PathBuf,
        pub(crate) runs: usize,
        pub(crate) use_32_bit: bool,
        pub(crate) validate: bool,
        pub(crate) start_node: usize,
        pub(crate) delta: f32,
    }

    pub(crate) fn create() -> Result<AppArgs, Box<dyn std::error::Error>> {
        let mut pargs = Arguments::from_env();

        fn as_path_buf(arg: &OsStr) -> Result<PathBuf, Infallible> {
            Ok(arg.into())
        }

        let args = AppArgs {
            path: pargs.value_from_os_str(["-p", "--path"], as_path_buf)?,
            runs: pargs.opt_value_from_str(["-r", "--runs"])?.unwrap_or(1),
            use_32_bit: pargs.contains("--use-32-bit"),
            validate: pargs.contains("--validate"),
            start_node: pargs.value_from_str("--start-node")?,
            delta: pargs.value_from_str("--delta")?,
        };

        Ok(args)
    }
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
}

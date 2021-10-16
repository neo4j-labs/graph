use atomic_float::AtomicF32;
use float_ord::FloatOrd;
use graph::prelude::*;
use log::info;

use rayon::prelude::*;
use std::{
    cmp::Reverse,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

const INF: f32 = f32::MAX;
const NO_BIN: usize = usize::MAX;
const BIN_SIZE_THRESHOLD: usize = 1000;

const BATCH_SIZE: usize = 64;

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
    } else {
        if use_32_bit {
            run::<u32>(path, runs, start_node as u32, delta)
        } else {
            run::<usize>(path, runs, start_node, delta)
        }
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

    for _ in 0..runs {
        delta_stepping(&graph, start_node, delta);
    }

    Ok(())
}

fn delta_stepping<NI: Idx>(
    graph: &DirectedCsrGraph<NI, (), f32>,
    start_node: NI,
    delta: f32,
) -> Vec<AtomicF32> {
    let start = Instant::now();

    let node_count = graph.node_count().index();
    let thread_count = rayon::current_num_threads();

    let mut distance: Vec<AtomicF32> = Vec::with_capacity(node_count);
    distance.resize_with(node_count, || AtomicF32::new(INF));
    distance[start_node.index()].store(0.0, Ordering::Release);

    let mut frontier = vec![NI::zero(); graph.edge_count().index()];
    frontier[0] = start_node;
    let frontier_idx = AtomicUsize::new(0);
    let mut frontier_len = 1;

    let mut local_bins = Vec::with_capacity(thread_count);
    local_bins.resize_with(thread_count, ThreadLocalBins::<NI>::new);

    let mut curr_bin = 0;

    while curr_bin != NO_BIN {
        frontier_idx.store(0, Ordering::Relaxed);

        let next_bin = local_bins
            .par_iter_mut()
            .map(|local_bins| {
                process_shared_bin(
                    local_bins,
                    curr_bin,
                    graph,
                    &frontier_idx,
                    frontier_len,
                    &frontier,
                    &distance,
                    delta,
                )
            })
            .map(|local_bins| process_local_bins(local_bins, curr_bin, graph, &distance, delta))
            .map(|local_bins| min_non_empty_bin(local_bins, curr_bin))
            .min_by(|x, y| x.cmp(y))
            .unwrap_or(NO_BIN);

        // copy next local bins into shared global bin
        frontier_len = frontier_slices(&mut frontier, &local_bins, next_bin)
            .par_iter_mut()
            .zip(local_bins.par_iter_mut())
            .filter(|(_, local_bins)| local_bins.contains(next_bin))
            .map(|(slice, local_bins)| {
                slice.copy_from_slice(local_bins.slice(next_bin));
                local_bins.clear(next_bin);
                slice.len()
            })
            .sum();

        curr_bin = next_bin;
    }

    info!("Computed SSSP in {:?}", start.elapsed());

    distance
}

fn process_shared_bin<'bins, NI: Idx>(
    bins: &'bins mut ThreadLocalBins<NI>,
    curr_bin: usize,
    graph: &DirectedCsrGraph<NI, (), f32>,
    frontier_idx: &AtomicUsize,
    frontier_len: usize,
    frontier: &[NI],
    distance: &[AtomicF32],
    delta: f32,
) -> &'bins mut ThreadLocalBins<NI> {
    loop {
        let offset = frontier_idx.fetch_add(BATCH_SIZE, Ordering::AcqRel);

        if offset >= frontier_len {
            break;
        }

        let limit = usize::min(offset + BATCH_SIZE, frontier_len);

        for node in frontier[offset..limit].iter() {
            if distance[node.index()].load(Ordering::Acquire) >= delta * curr_bin as f32 {
                relax_edges(graph, &distance, bins, *node, delta);
            }
        }
    }
    bins
}

fn process_local_bins<'bins, NI: Idx>(
    bins: &'bins mut ThreadLocalBins<NI>,
    curr_bin: usize,
    graph: &DirectedCsrGraph<NI, (), f32>,
    distance: &[AtomicF32],
    delta: f32,
) -> &'bins mut ThreadLocalBins<NI> {
    while curr_bin < bins.len()
        && !bins.is_empty(curr_bin)
        && bins.bin_len(curr_bin) < BIN_SIZE_THRESHOLD
    {
        let current_bin_copy = bins.clone(curr_bin);
        bins.clear(curr_bin);

        for node in current_bin_copy {
            relax_edges(graph, &distance, bins, node, delta);
        }
    }
    bins
}

fn min_non_empty_bin<NI: Idx>(local_bins: &mut ThreadLocalBins<NI>, curr_bin: usize) -> usize {
    let mut next_local_bin = NO_BIN;
    for bin in curr_bin..local_bins.len() {
        if !local_bins.is_empty(bin) {
            next_local_bin = bin;
            break;
        }
    }
    next_local_bin
}

fn relax_edges<NI: Idx>(
    graph: &DirectedCsrGraph<NI, (), f32>,
    distances: &[AtomicF32],
    local_bins: &mut ThreadLocalBins<NI>,
    node: NI,
    delta: f32,
) {
    for Target { target, value } in graph.out_neighbors_with_values(node) {
        let mut old_distance = distances[target.index()].load(Ordering::Acquire);
        let new_distance = distances[node.index()].load(Ordering::Acquire) + value;

        while new_distance < old_distance {
            match distances[target.index()].compare_exchange_weak(
                old_distance,
                new_distance,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let dest_bin = (new_distance / delta) as usize;
                    if dest_bin >= local_bins.len() {
                        local_bins.resize(dest_bin + 1);
                    }
                    local_bins.push(dest_bin, *target);
                    break;
                }
                // CAX failed -> retry with new min distance
                Err(min_distance) => old_distance = min_distance,
            }
        }
    }
}

fn frontier_slices<'a, NI: Idx>(
    frontier: &'a mut [NI],
    bins: &[ThreadLocalBins<NI>],
    next_bin: usize,
) -> Vec<&'a mut [NI]> {
    let mut slices = Vec::with_capacity(bins.len());
    let mut tail = frontier;

    for local_bins in bins.iter() {
        if local_bins.contains(next_bin) {
            let (head, remainder) = tail.split_at_mut(local_bins.bin_len(next_bin));
            slices.push(head);
            tail = remainder;
        } else {
            slices.push(&mut []);
        }
    }

    slices
}

fn validate_result<NI: Idx>(
    path: &PathBuf,
    start_node: NI,
    delta: f32,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph: DirectedCsrGraph<NI, (), f32> = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(EdgeListInput::default())
        .path(path)
        .build()?;

    let par_result = delta_stepping(&graph, start_node, delta);

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

#[derive(Debug)]
struct ThreadLocalBins<T> {
    bins: Vec<Vec<T>>,
}

impl<T> ThreadLocalBins<T>
where
    T: Clone,
{
    fn new() -> Self {
        Self { bins: vec![vec![]] }
    }

    fn contains(&self, bin: usize) -> bool {
        self.len() > bin
    }

    fn len(&self) -> usize {
        self.bins.len()
    }

    fn bin_len(&self, bin: usize) -> usize {
        self.bins[bin].len()
    }

    fn is_empty(&self, bin: usize) -> bool {
        self.bins[bin].is_empty()
    }

    fn clone(&self, bin: usize) -> Vec<T> {
        self.bins[bin].clone()
    }

    fn clear(&mut self, bin: usize) {
        self.bins[bin].clear();
    }

    fn slice(&self, bin: usize) -> &[T] {
        &self.bins[bin]
    }

    fn resize(&mut self, new_len: usize) {
        self.bins.resize_with(new_len, || vec![])
    }

    fn push(&mut self, bin: usize, val: T) {
        self.bins[bin].push(val);
    }
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
    fn test_sssp() {
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

        let actual: Vec<f32> = delta_stepping(&graph, 0, 3.0)
            .into_iter()
            .map(|d| d.load(Ordering::Relaxed))
            .collect();
        let expected: Vec<f32> = vec![0.0, 4.0, 2.0, 9.0, 5.0, 20.0];

        assert_eq!(actual, expected);
    }

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

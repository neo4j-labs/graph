use atomic_float::AtomicF64;
use log::{debug, info};

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use std::{path::PathBuf, sync::atomic::Ordering, time::Instant};

use graph::{prelude::*, SharedMut};

use graph::index::AtomicIdx;

const CHUNK_SIZE: usize = 16384;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli::AppArgs {
        path,
        use_32_bit,
        runs,
        max_iterations,
        tolerance,
    } = cli::create()?;

    info!(
        "Reading graph ({} bit) from: {:?}",
        if use_32_bit { "32" } else { "64" },
        path
    );

    if use_32_bit {
        run::<u32>(path, runs, max_iterations, tolerance)
    } else {
        run::<usize>(path, runs, max_iterations, tolerance)
    }
}

fn run<Node: Idx>(
    path: PathBuf,
    runs: usize,
    max_iterations: usize,
    tolerance: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let graph: DirectedCsrGraph<Node> = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .file_format(EdgeListInput::default())
        .path(path)
        .build()
        .unwrap();

    for run in 1..=runs {
        let start = Instant::now();
        let (_, ran_iterations, error) = page_rank(&graph, max_iterations, tolerance);
        info!(
            "Run {} of {} finished in {:.6?} (ran_iterations = {}, error = {:.6})",
            run,
            runs,
            start.elapsed(),
            ran_iterations,
            error
        );
    }

    Ok(())
}

fn page_rank<Node: Idx>(
    graph: &DirectedCsrGraph<Node>,
    max_iterations: usize,
    tolerance: f64,
) -> (Vec<f32>, usize, f64) {
    let damping_factor = 0.85_f32;
    let node_count = graph.node_count().index();
    let init_score = 1_f32 / node_count as f32;
    let base_score = (1.0_f32 - damping_factor) / node_count as f32;

    let mut out_scores = Vec::with_capacity(node_count);

    (0..node_count)
        .into_par_iter()
        .map(Node::new)
        .map(|node| init_score / graph.out_degree(node).index() as f32)
        .collect_into_vec(&mut out_scores);

    let mut scores = vec![init_score; node_count];

    let scores_ptr = SharedMut::new(scores.as_mut_ptr());
    let out_scores_ptr = SharedMut::new(out_scores.as_mut_ptr());

    let mut iteration = 0;

    loop {
        let start = Instant::now();
        let error = page_rank_iteration(
            &graph,
            base_score,
            damping_factor,
            &out_scores_ptr,
            &scores_ptr,
        );

        debug!(
            "Finished iteration {} with an error of {:.6} in {:?}",
            iteration,
            error,
            start.elapsed()
        );

        iteration += 1;

        if error < tolerance || iteration == max_iterations {
            return (scores, iteration, error);
        }
    }
}

fn page_rank_iteration<Node: Idx>(
    graph: &DirectedCsrGraph<Node>,
    base_score: f32,
    damping_factor: f32,
    out_scores: &SharedMut<f32>,
    scores: &SharedMut<f32>,
) -> f64 {
    let next_chunk = Node::zero().atomic();
    let total_error = AtomicF64::new(0_f64);

    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let mut error = 0_f64;

                loop {
                    let start = next_chunk.fetch_add(Node::new(CHUNK_SIZE), Ordering::AcqRel);
                    if start >= graph.node_count() {
                        break;
                    }

                    let end = (start + Node::new(CHUNK_SIZE)).min(graph.node_count());

                    for u in start..end {
                        let incoming_total = graph
                            .in_neighbors(u)
                            .iter()
                            .map(|v| unsafe { out_scores.add(v.index()).read() })
                            .sum::<f32>();

                        let old_score = unsafe { scores.add(u.index()).read() };
                        let new_score = base_score + damping_factor * incoming_total;

                        unsafe { scores.add(u.index()).write(new_score) };
                        let diff = (new_score - old_score) as f64;
                        error += f64::abs(diff);

                        unsafe {
                            out_scores
                                .add(u.index())
                                .write(new_score / graph.out_degree(u).index() as f32)
                        }
                    }
                }
                total_error.fetch_add(error, Ordering::SeqCst);
            });
        }
    });

    total_error.load(Ordering::SeqCst)
}

mod cli {
    use pico_args::Arguments;
    use std::{convert::Infallible, ffi::OsStr, path::PathBuf};

    #[derive(Debug)]
    pub(crate) struct AppArgs {
        pub(crate) path: std::path::PathBuf,
        pub(crate) runs: usize,
        pub(crate) use_32_bit: bool,
        pub(crate) max_iterations: usize,
        pub(crate) tolerance: f64,
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
            max_iterations: pargs
                .opt_value_from_str(["-i", "--max-iterations"])?
                .unwrap_or(20),
            tolerance: pargs
                .opt_value_from_str(["-t", "--tolerance"])?
                .unwrap_or(1E-4),
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
    fn test_pr_two_components() {
        let edges = vec![(0, 1), (0, 2), (1, 2), (3, 4), (3, 5), (4, 5)];

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges(edges)
            .build();

        let actual = page_rank(&graph, 20, 1E-4)
            .0
            .into_iter()
            .collect::<Vec<_>>();

        let expected: Vec<f32> = vec![
            0.024999997,
            0.035624996,
            0.06590624,
            0.024999997,
            0.035624996,
            0.06590624,
        ];

        assert_eq!(actual, expected);
    }
}

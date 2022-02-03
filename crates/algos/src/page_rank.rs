use crate::prelude::*;

use atomic_float::AtomicF64;
use graph_builder::SharedMut;
use log::info;
use rayon::prelude::*;

use std::sync::atomic::Ordering;
use std::time::Instant;

const CHUNK_SIZE: usize = 16384;

#[derive(Copy, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PageRankConfig {
    // The maximum number of page rank iterations.
    pub max_iterations: usize,
    // If the sum of page rank deltas per iteration is
    // below the tolerance value, the computation stop.
    pub tolerance: f64,
    // Imagining a random surfer clicking links, the
    // damping factor defines the probability if the
    // surfer will continue at any step.
    pub damping_factor: f32,
}

impl Default for PageRankConfig {
    fn default() -> Self {
        Self {
            max_iterations: 20,
            tolerance: 1E-4,
            damping_factor: 0.85,
        }
    }
}

impl PageRankConfig {
    pub fn new(max_iterations: usize, tolerance: f64, damping_factor: f32) -> Self {
        Self {
            max_iterations,
            tolerance,
            damping_factor,
        }
    }
}

pub fn page_rank<NI: Idx>(
    graph: &DirectedCsrGraph<NI>,
    config: PageRankConfig,
) -> (Vec<f32>, usize, f64) {
    let PageRankConfig {
        max_iterations,
        tolerance,
        damping_factor,
    } = config;

    let node_count = graph.node_count().index();
    let init_score = 1_f32 / node_count as f32;
    let base_score = (1.0_f32 - damping_factor) / node_count as f32;

    let mut out_scores = Vec::with_capacity(node_count);

    (0..node_count)
        .into_par_iter()
        .map(NI::new)
        .map(|node| init_score / graph.out_degree(node).index() as f32)
        .collect_into_vec(&mut out_scores);

    let mut scores = vec![init_score; node_count];

    let scores_ptr = SharedMut::new(scores.as_mut_ptr());
    let out_scores_ptr = SharedMut::new(out_scores.as_mut_ptr());

    let mut iteration = 0;

    loop {
        let start = Instant::now();
        let error = page_rank_iteration(
            graph,
            base_score,
            damping_factor,
            &out_scores_ptr,
            &scores_ptr,
        );

        info!(
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

fn page_rank_iteration<NI: Idx>(
    graph: &DirectedCsrGraph<NI>,
    base_score: f32,
    damping_factor: f32,
    out_scores: &SharedMut<f32>,
    scores: &SharedMut<f32>,
) -> f64 {
    let next_chunk = NI::zero().atomic();
    let total_error = AtomicF64::new(0_f64);

    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let mut error = 0_f64;

                loop {
                    let start = next_chunk.fetch_add(NI::new(CHUNK_SIZE), Ordering::AcqRel);
                    if start >= graph.node_count() {
                        break;
                    }

                    let end = (start + NI::new(CHUNK_SIZE)).min(graph.node_count());

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{CsrLayout, GraphBuilder};

    #[test]
    fn test_pr_two_components() {
        let gdl = "(a)-->()-->()<--(a),(b)-->()-->()<--(b)";

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        let actual = page_rank(&graph, PageRankConfig::default())
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

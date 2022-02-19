use std::{
    collections::HashMap,
    hash::Hash,
    sync::{atomic::Ordering, Arc},
};

use crate::{dss::DisjointSetStruct, prelude::*};
use rayon::prelude::*;

// Number of nodes to be processed in batch by a single thread.
const CHUNK_SIZE: usize = 64;
// The number of relationships of each node to sample during subgraph sampling.
const NEIGHBOR_ROUNDS: usize = 2;
// The number of samples from the DSS to find the largest component.
const SAMPLING_SIZE: usize = 1024;

pub fn wcc_par_iter<NI: Idx>(graph: &DirectedCsrGraph<NI>) -> DisjointSetStruct<NI> {
    let node_count = graph.node_count().index();
    let dss = Arc::new(DisjointSetStruct::new(node_count));

    (0..node_count).into_par_iter().map(NI::new).for_each(|u| {
        graph.out_neighbors(u).iter().for_each(|v| dss.union(u, *v));
    });

    Arc::try_unwrap(dss).ok().unwrap()
}

pub fn wcc_rayon_chunks<NI: Idx>(graph: &DirectedCsrGraph<NI>) -> DisjointSetStruct<NI> {
    let node_count = graph.node_count().index();
    let dss = Arc::new(DisjointSetStruct::new(node_count));

    (0..node_count)
        .into_par_iter()
        .chunks(CHUNK_SIZE)
        .for_each(|chunk| {
            for u in chunk {
                let u = NI::new(u);
                graph.out_neighbors(u).iter().for_each(|v| dss.union(u, *v));
            }
        });

    Arc::try_unwrap(dss).ok().unwrap()
}

pub fn wcc_manual_chunks<NI: Idx>(graph: &DirectedCsrGraph<NI>) -> DisjointSetStruct<NI> {
    let node_count = graph.node_count().index();
    let dss = Arc::new(DisjointSetStruct::new(node_count));

    let next_chunk = NI::zero().atomic();

    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let start = next_chunk.fetch_add(NI::new(CHUNK_SIZE), Ordering::AcqRel);
                if start >= graph.node_count() {
                    return;
                }

                let end = (start + NI::new(CHUNK_SIZE)).min(graph.node_count());

                for u in start..end {
                    for v in graph.out_neighbors(u) {
                        dss.union(u, *v);
                    }
                }
            });
        }
    });

    Arc::try_unwrap(dss).ok().unwrap()
}

pub fn wcc_std_threads<NI: Idx>(graph: &DirectedCsrGraph<NI>) -> DisjointSetStruct<NI> {
    let next_chunk = NI::zero().atomic();
    let dss = Arc::new(DisjointSetStruct::new(graph.node_count().index()));

    easy_parallel::Parallel::new()
        .each(0..num_cpus::get(), |_| {
            let start = next_chunk.fetch_add(NI::new(CHUNK_SIZE), Ordering::AcqRel);
            if start >= graph.node_count() {
                return;
            }

            let end = (start + NI::new(CHUNK_SIZE)).min(graph.node_count());

            for u in start..end {
                for v in graph.out_neighbors(u) {
                    dss.union(u, *v);
                }
            }
        })
        .run();

    Arc::try_unwrap(dss).ok().unwrap()
}

pub fn wcc<NI: Idx + Hash>(graph: &DirectedCsrGraph<NI>) -> DisjointSetStruct<NI> {
    let dss = Arc::new(DisjointSetStruct::new(graph.node_count().index()));

    sample_subgraph(graph, Arc::clone(&dss));
    let largest_component = find_largest_component(Arc::clone(&dss));
    link_remaining(graph, Arc::clone(&dss), largest_component);

    Arc::try_unwrap(dss).ok().unwrap()
}

// Sample a subgraph by looking at the first `NEIGHBOR_ROUNDS` many targets of each node.
fn sample_subgraph<NI: Idx>(graph: &DirectedCsrGraph<NI>, dss: Arc<DisjointSetStruct<NI>>) {
    let next_chunk = NI::zero().atomic();

    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let start = next_chunk.fetch_add(NI::new(CHUNK_SIZE), Ordering::AcqRel);
                if start >= graph.node_count() {
                    return;
                }

                let end = (start + NI::new(CHUNK_SIZE)).min(graph.node_count());

                for u in start..end {
                    let upper_bound = usize::min(graph.out_degree(u).index(), NEIGHBOR_ROUNDS);

                    for v in &graph.out_neighbors(u)[..upper_bound] {
                        dss.union(u, *v);
                    }
                }
            })
        }
    })
}

// Find the largest component after running wcc on the sampled graph.
fn find_largest_component<NI: Idx + Hash>(dss: Arc<DisjointSetStruct<NI>>) -> NI {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut sample_counts = HashMap::<NI, usize>::new();

    for _ in 0..SAMPLING_SIZE {
        let component = dss.find(NI::new(rng.gen_range(0..dss.len())));
        let count = sample_counts.entry(component).or_insert(0);
        *count += 1;
    }

    let (most_frequent, _) = sample_counts
        .iter()
        .max_by(|(_, v1), (_, v2)| v1.cmp(v2))
        .unwrap();

    *most_frequent
}

// Process the remaining edges while skipping nodes that are in the largest component.
fn link_remaining<NI: Idx>(
    graph: &DirectedCsrGraph<NI>,
    dss: Arc<DisjointSetStruct<NI>>,
    skip_component: NI,
) {
    let next_chunk = NI::zero().atomic();

    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let start = next_chunk.fetch_add(NI::new(CHUNK_SIZE), Ordering::AcqRel);
                if start >= graph.node_count() {
                    return;
                }

                let end = (start + NI::new(CHUNK_SIZE)).min(graph.node_count());

                for u in start..end {
                    if dss.find(u) == skip_component {
                        continue;
                    }

                    if graph.out_degree(u).index() > NEIGHBOR_ROUNDS {
                        for v in &graph.out_neighbors(u)[NEIGHBOR_ROUNDS..] {
                            dss.union(u, *v);
                        }
                    }

                    for v in graph.in_neighbors(u) {
                        dss.union(u, *v);
                    }
                }
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_components() {
        let graph: DirectedCsrGraph<usize> =
            GraphBuilder::new().edges(vec![(0, 1), (2, 3)]).build();

        let dss = wcc(&graph);

        assert_eq!(dss.find(0), dss.find(1));
        assert_eq!(dss.find(2), dss.find(3));
        assert_ne!(dss.find(1), dss.find(2));
    }
}

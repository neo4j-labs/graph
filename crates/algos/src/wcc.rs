use log::info;
use std::{collections::HashMap, hash::Hash, time::Instant};

use crate::{dss::DisjointSetStruct, prelude::*};
use rayon::prelude::*;

#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WccConfig {
    // Number of nodes to be processed in batch by a single thread.
    pub chunk_size: usize,
    // Number of relationships of each node to sample during subgraph linking.
    pub neighbor_rounds: usize,
    // Number of samples to draw from the DSS to find the largest component.
    pub sampling_size: usize,
}

impl Default for WccConfig {
    fn default() -> Self {
        Self {
            chunk_size: 16384,
            neighbor_rounds: 2,
            sampling_size: 1024,
        }
    }
}

impl WccConfig {
    pub fn new(chunk_size: usize, neighbor_rounds: usize, sampling_size: usize) -> Self {
        Self {
            chunk_size,
            neighbor_rounds,
            sampling_size,
        }
    }
}

pub fn wcc_baseline<NI: Idx>(
    graph: &DirectedCsrGraph<NI>,
    config: WccConfig,
) -> DisjointSetStruct<NI> {
    let node_count = graph.node_count().index();
    let dss = DisjointSetStruct::new(node_count);

    (0..node_count)
        .into_par_iter()
        .chunks(config.chunk_size)
        .for_each(|chunk| {
            for u in chunk {
                let u = NI::new(u);
                graph.out_neighbors(u).iter().for_each(|v| dss.union(u, *v));
            }
        });

    dss
}

pub fn wcc_afforest_dss<NI: Idx + Hash>(
    graph: &DirectedCsrGraph<NI>,
    config: WccConfig,
) -> DisjointSetStruct<NI> {
    let start = Instant::now();
    let dss = DisjointSetStruct::new(graph.node_count().index());
    info!("DSS creation took {:?}", start.elapsed());

    let start = Instant::now();
    sample_subgraph(graph, &dss, config);
    info!("Link subgraph took {:?}", start.elapsed());

    let start = Instant::now();
    let largest_component = find_largest_component(&dss, config);
    info!("Get component took {:?}", start.elapsed());

    let start = Instant::now();
    link_remaining(graph, &dss, largest_component, config);
    info!("Link remaining took {:?}", start.elapsed());

    dss
}

// Sample a subgraph by looking at the first `NEIGHBOR_ROUNDS` many targets of each node.
fn sample_subgraph<NI: Idx>(
    graph: &DirectedCsrGraph<NI>,
    dss: &DisjointSetStruct<NI>,
    config: WccConfig,
) {
    (0..graph.node_count().index())
        .into_par_iter()
        .chunks(config.chunk_size)
        .for_each(|chunk| {
            for u in chunk {
                let u = NI::new(u);
                let limit = usize::min(graph.out_degree(u).index(), config.neighbor_rounds);

                for v in &graph.out_neighbors(u)[..limit] {
                    dss.union(u, *v);
                }
            }
        });
}

// Find the largest component after running wcc on the sampled graph.
fn find_largest_component<NI: Idx + Hash>(dss: &DisjointSetStruct<NI>, config: WccConfig) -> NI {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut sample_counts = HashMap::<NI, usize>::new();

    for _ in 0..config.sampling_size {
        let component = dss.find(NI::new(rng.gen_range(0..dss.len())));
        let count = sample_counts.entry(component).or_insert(0);
        *count += 1;
    }

    let (most_frequent, size) = sample_counts
        .iter()
        .max_by(|(_, v1), (_, v2)| v1.cmp(v2))
        .unwrap();

    info!(
        "Largest intermediate component {most_frequent:?} containing approx. {}% of the graph.",
        (*size as f32 / config.sampling_size as f32 * 100.0) as usize
    );

    *most_frequent
}

// Process the remaining edges while skipping nodes that are in the largest component.
fn link_remaining<NI: Idx>(
    graph: &DirectedCsrGraph<NI>,
    dss: &DisjointSetStruct<NI>,
    skip_component: NI,
    config: WccConfig,
) {
    (0..graph.node_count().index())
        .into_par_iter()
        .chunks(config.chunk_size)
        .for_each(|chunk| {
            for u in chunk {
                let u = NI::new(u);
                if dss.find(u) == skip_component {
                    continue;
                }

                if graph.out_degree(u).index() > config.neighbor_rounds {
                    for v in &graph.out_neighbors(u)[config.neighbor_rounds..] {
                        dss.union(u, *v);
                    }
                }

                for v in graph.in_neighbors(u) {
                    dss.union(u, *v);
                }
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_components() {
        let graph: DirectedCsrGraph<usize> =
            GraphBuilder::new().edges(vec![(0, 1), (2, 3)]).build();

        let dss = wcc_afforest_dss(&graph, WccConfig::default());

        assert_eq!(dss.find(0), dss.find(1));
        assert_eq!(dss.find(2), dss.find(3));
        assert_ne!(dss.find(1), dss.find(2));
    }
}

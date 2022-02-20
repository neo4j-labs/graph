use log::info;
use std::{collections::HashMap, hash::Hash, time::Instant};

use crate::prelude::*;
use rayon::prelude::*;

pub use crate::afforest::Afforest;
pub use crate::dss::DisjointSetStruct;

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

#[allow(clippy::len_without_is_empty)]
pub trait UnionFind<NI> {
    /// Joins the set of `id1` with the set of `id2`.
    fn union(&self, u: NI, v: NI);
    /// Find the set of `id`.
    fn find(&self, u: NI) -> NI;
    /// Returns the number of elements in the union find,
    /// also referred to as its 'length'.
    fn len(&self) -> usize;
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
    let comp = DisjointSetStruct::new(graph.node_count().index());
    info!("Components creation took {:?}", start.elapsed());

    let start = Instant::now();
    sample_subgraph(graph, &comp, config);
    info!("Link subgraph took {:?}", start.elapsed());

    let start = Instant::now();
    let largest_component = find_largest_component(&comp, config);
    info!("Get component took {:?}", start.elapsed());

    let start = Instant::now();
    link_remaining(graph, &comp, largest_component, config);
    info!("Link remaining took {:?}", start.elapsed());

    comp
}

pub fn wcc_afforest<NI: Idx + Hash>(
    graph: &DirectedCsrGraph<NI>,
    config: WccConfig,
) -> Afforest<NI> {
    let start = Instant::now();
    let comp = Afforest::new(graph.node_count().index());
    info!("Components creation took {:?}", start.elapsed());

    let start = Instant::now();
    sample_subgraph(graph, &comp, config);
    info!("Link subgraph took {:?}", start.elapsed());

    let start = Instant::now();
    comp.compress();
    info!("Sample compress took {:?}", start.elapsed());

    let start = Instant::now();
    let largest_component = find_largest_component(&comp, config);
    info!("Get component took {:?}", start.elapsed());

    let start = Instant::now();
    link_remaining(graph, &comp, largest_component, config);
    info!("Link remaining took {:?}", start.elapsed());

    let start = Instant::now();
    comp.compress();
    info!("Final compress took {:?}", start.elapsed());

    comp
}

// Sample a subgraph by looking at the first `NEIGHBOR_ROUNDS` many targets of each node.
fn sample_subgraph<NI, UF>(graph: &DirectedCsrGraph<NI>, uf: &UF, config: WccConfig)
where
    NI: Idx,
    UF: UnionFind<NI> + Send + Sync,
{
    (0..graph.node_count().index())
        .into_par_iter()
        .chunks(config.chunk_size)
        .for_each(|chunk| {
            for u in chunk {
                let u = NI::new(u);
                let limit = usize::min(graph.out_degree(u).index(), config.neighbor_rounds);

                for v in &graph.out_neighbors(u)[..limit] {
                    uf.union(u, *v);
                }
            }
        });
}

// Sample a subgraph by looking at the first `NEIGHBOR_ROUNDS` many targets of each node.
// In contrast to `sample_subgraph`, the method calls `compress` for each neighbor round.
#[allow(dead_code)]
fn sample_subgraph_afforest<NI>(graph: &DirectedCsrGraph<NI>, af: &Afforest<NI>, config: WccConfig)
where
    NI: Idx,
{
    let neighbor_rounds = config.neighbor_rounds;
    for r in 0..neighbor_rounds {
        info!("Neighbor round {} of {neighbor_rounds}", r + 1);

        let start = Instant::now();
        (0..graph.node_count().index())
            .into_par_iter()
            .chunks(config.chunk_size)
            .for_each(|chunk| {
                for u in chunk {
                    let u = NI::new(u);
                    if r < graph.out_degree(u).index() {
                        for v in &graph.out_neighbors(u)[r..r + 1] {
                            af.union(u, *v);
                        }
                    }
                }
            });

        info!(
            "Neighbor round {r} of {neighbor_rounds} took {:?}",
            start.elapsed()
        );

        let start = Instant::now();
        af.compress();
        info!("Compress took {:?}", start.elapsed());
    }
}

// Find the largest component after running wcc on the sampled graph.
fn find_largest_component<NI, UF>(uf: &UF, config: WccConfig) -> NI
where
    NI: Idx + Hash,
    UF: UnionFind<NI> + Send + Sync,
{
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut sample_counts = HashMap::<NI, usize>::new();

    for _ in 0..config.sampling_size {
        let component = uf.find(NI::new(rng.gen_range(0..uf.len())));
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
fn link_remaining<NI, UF>(
    graph: &DirectedCsrGraph<NI>,
    uf: &UF,
    skip_component: NI,
    config: WccConfig,
) where
    NI: Idx,
    UF: UnionFind<NI> + Send + Sync,
{
    (0..graph.node_count().index())
        .into_par_iter()
        .chunks(config.chunk_size)
        .for_each(|chunk| {
            for u in chunk {
                let u = NI::new(u);
                if uf.find(u) == skip_component {
                    continue;
                }

                if graph.out_degree(u).index() > config.neighbor_rounds {
                    for v in &graph.out_neighbors(u)[config.neighbor_rounds..] {
                        uf.union(u, *v);
                    }
                }

                for v in graph.in_neighbors(u) {
                    uf.union(u, *v);
                }
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_components_afforest_dss() {
        let graph: DirectedCsrGraph<usize> =
            GraphBuilder::new().edges(vec![(0, 1), (2, 3)]).build();

        let dss = wcc_afforest_dss(&graph, WccConfig::default());

        assert_eq!(dss.find(0), dss.find(1));
        assert_eq!(dss.find(2), dss.find(3));
        assert_ne!(dss.find(1), dss.find(2));
    }

    #[test]
    fn two_components_afforest() {
        let graph: DirectedCsrGraph<usize> =
            GraphBuilder::new().edges(vec![(0, 1), (2, 3)]).build();

        let dss = wcc_afforest(&graph, WccConfig::default());

        assert_eq!(dss.find(0), dss.find(1));
        assert_eq!(dss.find(2), dss.find(3));
        assert_ne!(dss.find(1), dss.find(2));
    }
}

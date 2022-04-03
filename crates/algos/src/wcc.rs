//! Weakly Connected Components (WCC) algorithm.
//!
//! The algorithm finds all weakly connected components of a graph
//! and assigns each node to its corresponding component. Weakly
//! connected means that all nodes that belong to the same component
//! are connected via an undirected path.
//!
//! The implementation is based on the Afforest paper [1] which
//! introduced the idea of using a sampled subgraph to identify
//! an intermediate largest community. Nodes within that community
//! do not need to be considered when linking the remaining edges.
//! The idea is motivated by power law graphs, which usually have
//! a very large component.
//!
//! The module contains three functions to compute wcc:
//!
//! - `wcc_baseline` computes components by linking all connected
//!   nodes using a disjoint set struct [2]
//! - `wcc_afforest` implements the algorithm presented in [1]
//! - `wcc_afforest_dss` implements the algorithm presented in [1]
//!   but uses a disjoint set struct [2] to represent components
//!
//! [1] Michael Sutton, Tal Ben-Nun, Amnon Barak:
//! "Optimizing Parallel Graph Connectivity Computation via Subgraph Sampling",
//! Symposium on Parallel and Distributed Processing, IPDPS 2018
//! [2] Richard J. Anderson , Heather Woll:
//! "Wait-free Parallel Algorithms for the Union-Find Problem",
//! In Proc. 23rd ACM Symposium on Theory of Computing, 1994

use ahash::AHashMap;
use log::info;
use std::{hash::Hash, time::Instant};

use crate::prelude::*;
use rayon::prelude::*;

pub use crate::afforest::Afforest;
pub use crate::dss::DisjointSetStruct;

#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "clap", derive(clap::Args))]
pub struct WccConfig {
    /// Number of nodes to be processed in batch by a single thread.
    #[cfg_attr(feature = "clap", clap(long, default_value_t = WccConfig::DEFAULT_CHUNK_SIZE))]
    pub chunk_size: usize,

    /// Number of relationships of each node to sample during subgraph linking.
    #[cfg_attr(feature = "clap", clap(long, default_value_t = WccConfig::DEFAULT_NEIGHBOR_ROUNDS))]
    pub neighbor_rounds: usize,

    /// Number of samples to draw from the DSS to find the largest component.
    #[cfg_attr(feature = "clap", clap(long, default_value_t = WccConfig::DEFAULT_SAMPLING_SIZE))]
    pub sampling_size: usize,
}

impl Default for WccConfig {
    fn default() -> Self {
        Self {
            chunk_size: WccConfig::DEFAULT_CHUNK_SIZE,
            neighbor_rounds: WccConfig::DEFAULT_NEIGHBOR_ROUNDS,
            sampling_size: WccConfig::DEFAULT_SAMPLING_SIZE,
        }
    }
}

impl WccConfig {
    pub const DEFAULT_CHUNK_SIZE: usize = 16384;
    pub const DEFAULT_NEIGHBOR_ROUNDS: usize = 2;
    pub const DEFAULT_SAMPLING_SIZE: usize = 1024;

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
    /// Compress the data if possible.
    /// After that operation each index stores the final set id.
    fn compress(&self);
}

pub trait Components<NI> {
    fn component(&self, node: NI) -> NI;

    fn to_vec(self) -> Vec<NI>;
}

/// Computes Wcc by iterating all relationships in parallel and
/// linking source and target nodes using a disjoint set struct.
pub fn wcc_baseline<NI, G>(graph: &G, config: WccConfig) -> impl Components<NI>
where
    NI: Idx,
    G: Graph<NI> + DirectedNeighbors<NI> + Sync,
{
    let node_count = graph.node_count().index();
    let dss = DisjointSetStruct::new(node_count);

    (0..node_count)
        .into_par_iter()
        .chunks(config.chunk_size)
        .for_each(|chunk| {
            for u in chunk {
                let u = NI::new(u);
                graph.out_neighbors(u).for_each(|v| dss.union(u, *v));
            }
        });

    dss
}

/// Computes Wcc using the Afforest algorithm backed by a disjoint
/// set struct. The disjoint set struct performans path compression
/// while searching the set id for a given node.
pub fn wcc_afforest<NI, G>(graph: &G, config: WccConfig) -> impl Components<NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    let start = Instant::now();
    let comp = Afforest::new(graph.node_count().index());
    info!("Afforest creation took {:?}", start.elapsed());

    wcc(graph, &comp, config);

    comp
}

/// Computes Wcc using the Afforest algorithm as described in the original
/// paper (see module description). The backing union find structure can
/// achieve better cache locality compared to the disjoint set struct variant.
pub fn wcc_afforest_dss<NI, G>(graph: &G, config: WccConfig) -> impl Components<NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    let start = Instant::now();
    let dss = DisjointSetStruct::new(graph.node_count().index());
    info!("DSS creation took {:?}", start.elapsed());

    wcc(graph, &dss, config);

    dss
}

fn wcc<NI, G, UF>(graph: &G, comp: &UF, config: WccConfig)
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
    UF: UnionFind<NI> + Send + Sync,
{
    let start = Instant::now();
    sample_subgraph(graph, comp, config);
    info!("Link subgraph took {:?}", start.elapsed());

    let start = Instant::now();
    comp.compress();
    info!("Sample compress took {:?}", start.elapsed());

    let start = Instant::now();
    let largest_component = find_largest_component(comp, config);
    info!("Get component took {:?}", start.elapsed());

    let start = Instant::now();
    link_remaining(graph, comp, largest_component, config);
    info!("Link remaining took {:?}", start.elapsed());

    let start = Instant::now();
    comp.compress();
    info!("Final compress took {:?}", start.elapsed());
}

// Sample a subgraph by looking at the first `NEIGHBOR_ROUNDS` many targets of each node.
fn sample_subgraph<NI, G, UF>(graph: &G, uf: &UF, config: WccConfig)
where
    NI: Idx,
    G: Graph<NI> + DirectedNeighbors<NI> + Sync,
    UF: UnionFind<NI> + Send + Sync,
{
    (0..graph.node_count().index())
        .into_par_iter()
        .chunks(config.chunk_size)
        .for_each(|chunk| {
            for u in chunk {
                let u = NI::new(u);

                for v in graph.out_neighbors(u).take(config.neighbor_rounds) {
                    uf.union(u, *v);
                }
            }
        });
}

// Sample a subgraph by looking at the first `NEIGHBOR_ROUNDS` many targets of each node.
// In contrast to `sample_subgraph`, the method calls `compress` for each neighbor round.
#[allow(dead_code)]
fn sample_subgraph_afforest<NI, G>(graph: &G, af: &Afforest<NI>, config: WccConfig)
where
    NI: Idx,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
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
                        for v in graph.out_neighbors(u).skip(r).take(1) {
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
    use nanorand::{Rng, WyRand};
    let mut rng = WyRand::new();
    let mut sample_counts = AHashMap::<NI, usize>::new();

    for _ in 0..config.sampling_size {
        let component = uf.find(NI::new(rng.generate_range(0..uf.len())));
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
fn link_remaining<NI, G, UF>(graph: &G, uf: &UF, skip_component: NI, config: WccConfig)
where
    NI: Idx,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
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
                    for v in graph.out_neighbors(u).skip(config.neighbor_rounds) {
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

        let res = wcc_afforest_dss(&graph, WccConfig::default());

        assert_eq!(res.component(0), res.component(1));
        assert_eq!(res.component(2), res.component(3));
        assert_ne!(res.component(1), res.component(2));
    }

    #[test]
    fn two_components_afforest() {
        let graph: DirectedCsrGraph<usize> =
            GraphBuilder::new().edges(vec![(0, 1), (2, 3)]).build();

        let res = wcc_afforest(&graph, WccConfig::default());

        assert_eq!(res.component(0), res.component(1));
        assert_eq!(res.component(2), res.component(3));
        assert_ne!(res.component(1), res.component(2));
    }
}

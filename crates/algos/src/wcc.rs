use std::sync::{atomic::Ordering, Arc};

use crate::{dss::DisjointSetStruct, prelude::*};
use rayon::prelude::*;

const CHUNK_SIZE: usize = 64;

pub fn wcc<NI: Idx>(graph: &DirectedCsrGraph<NI>) -> DisjointSetStruct<NI> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_components() {
        let graph: DirectedCsrGraph<usize> =
            GraphBuilder::new().edges(vec![(0, 1), (2, 3)]).build();

        let dss = wcc_rayon_chunks(&graph);

        assert_eq!(dss.find(0), dss.find(1));
        assert_eq!(dss.find(2), dss.find(3));
        assert_ne!(dss.find(1), dss.find(2));
    }
}

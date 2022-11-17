use crate::{prelude::*, DEFAULT_PARALLELISM};

use log::info;
use num_format::{Locale, ToFormattedString};

use std::sync::atomic::AtomicU64;
use std::thread::available_parallelism;
use std::{sync::atomic::Ordering, time::Instant};

const CHUNK_SIZE: usize = 64;

pub fn relabel_graph<NI, G, EV>(graph: &mut G)
where
    NI: Idx,
    G: RelabelByDegreeOp<NI, EV>,
{
    let start = Instant::now();
    graph.make_degree_ordered();
    info!("Relabeled graph in {:?}", start.elapsed());
}

pub fn global_triangle_count<NI, G>(graph: &G) -> u64
where
    NI: Idx,
    G: Graph<NI> + UndirectedNeighbors<NI> + Sync,
{
    let start = Instant::now();

    let next_chunk = Atomic::new(NI::zero());
    let total_triangles = AtomicU64::new(0);

    std::thread::scope(|s| {
        let num_threads = available_parallelism().map_or(DEFAULT_PARALLELISM, |p| p.get());

        for _ in 0..num_threads {
            s.spawn(|| {
                let mut triangles = 0;

                loop {
                    let start = NI::fetch_add(&next_chunk, NI::new(CHUNK_SIZE), Ordering::AcqRel);
                    if start >= graph.node_count() {
                        break;
                    }

                    let end = (start + NI::new(CHUNK_SIZE)).min(graph.node_count());

                    for u in start.range(end) {
                        for &v in graph.neighbors(u) {
                            if v > u {
                                break;
                            }

                            let mut it = put_back_iterator(graph.neighbors(u));

                            for &w in graph.neighbors(v) {
                                if w > v {
                                    break;
                                }
                                while let Some(x) = it.next() {
                                    if x >= &w {
                                        if x == &w {
                                            triangles += 1;
                                        }
                                        it.put_back(x);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                total_triangles.fetch_add(triangles, Ordering::AcqRel);
            });
        }
    });

    let tc = total_triangles.load(Ordering::SeqCst);

    info!(
        "Computed {} triangles in {:?}",
        tc.to_formatted_string(&Locale::en),
        start.elapsed()
    );

    tc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{CsrLayout, GraphBuilder, UndirectedCsrGraph};

    #[test]
    fn test_tc_two_components() {
        let gdl = "(a)-->()-->()<--(a),(b)-->()-->()<--(b)";

        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        assert_eq!(global_triangle_count(&graph), 2);
    }

    #[test]
    fn test_tc_connected_triangles() {
        let gdl = "(a)-->()-->()<--(a),(a)-->()-->()<--(a)";

        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        assert_eq!(global_triangle_count(&graph), 2);
    }

    #[test]
    fn test_tc_diamond() {
        let gdl = "(a)-->(b)-->(c)<--(a),(b)-->(d)<--(c)";

        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        assert_eq!(global_triangle_count(&graph), 2);
    }
}

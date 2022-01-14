use crate::prelude::*;

use log::info;
use num_format::{Locale, ToFormattedString};

use std::sync::atomic::AtomicU64;
use std::{sync::atomic::Ordering, time::Instant};

const CHUNK_SIZE: usize = 64;

pub fn relabel_graph<NI: Idx>(graph: &mut UndirectedCsrGraph<NI>) {
    let start = Instant::now();
    graph.to_degree_ordered();
    info!("Relabeled graph in {:?}", start.elapsed());
}

pub fn global_triangle_count<NI: Idx>(graph: &UndirectedCsrGraph<NI>) -> u64 {
    let start = Instant::now();

    let next_chunk = NI::zero().atomic();
    let total_triangles = AtomicU64::new(0);

    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let mut triangles = 0;

                loop {
                    let start = next_chunk.fetch_add(NI::new(CHUNK_SIZE), Ordering::AcqRel);
                    if start >= graph.node_count() {
                        break;
                    }

                    let end = (start + NI::new(CHUNK_SIZE)).min(graph.node_count());

                    for u in start..end {
                        for &v in graph.neighbors(u) {
                            if v > u {
                                break;
                            }

                            let mut it = graph.neighbors(u);

                            for &w in graph.neighbors(v) {
                                if w > v {
                                    break;
                                }
                                while let Some(&x) = it.first() {
                                    if x >= w {
                                        if x == w {
                                            triangles += 1;
                                        }
                                        break;
                                    }
                                    it = &it[1..];
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

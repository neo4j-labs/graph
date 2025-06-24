use crate::DEFAULT_PARALLELISM;
use graph_builder::prelude::*;
use graph_builder::SharedMut;
use rayon::prelude::*;
use std::sync::atomic::{
    AtomicBool,
    Ordering::{Acquire, Relaxed, Release},
};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::thread::available_parallelism;

/// Graph coloring assigns a color to every node so that
/// 1. No node has a neighbor with the same color
/// 2. The total number of colors is as low as possible.
///
/// Finding the lowest possible number of colors is an NP-complete problem. This implementation,
/// based on the speculation/correction paradigm of [1], instead uses a parallel greedy algorithm
/// to find a solution with few-enough colors. See [2] for a Java implementation.
///
/// [1]  Gebremedhin, A.H. and Manne, F. (2000), Scalable parallel graph coloring algorithms.
///      Concurrency: Pract. Exper., 12: 1131-1146.
///      https://doi.org/10.1002/1096-9128(200010)12:12<1131::AID-CPE528>3.0.CO;2-2
/// [2] [Java] (https://github.com/neo4j/graph-data-science/blob/2dd419ed5a55d43bbaf0575d4bc34e517768d69f/algo/src/main/java/org/neo4j/gds/k1coloring/K1Coloring.java)

const CHUNK_SIZE: usize = 16384;

pub struct GraphColoringConfig<NI> {
    pub(crate) max_colors: NI,
}
struct BitField {
    data: Vec<AtomicU64>, //(must be 64 bit!) but color as usize might be fine.
}

impl BitField {
    fn new(capacity: usize) -> Self {
        BitField {
            //capacity / 64 = capacity >> 6 (64->32->16->8->4->2->1)
            data: (0..(capacity >> 6)).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn update(&self, new_ids: &Vec<usize>) {
        let mut data = vec![0u64; self.data.len()];
        for &idx in new_ids {
            data[idx >> 6] |= 1 << (idx & 63); //iff 63 = 2**uint - 1
                                               // data[color / 64] |= 1 << (color % 64);
        }
        for (idx, &new_word) in data.iter().enumerate() {
            self.data[idx].store(new_word, Release);
        }
    }

    fn is_set(&self, idx: usize) -> bool {
        // self.data[idx / 64].load(Relaxed) >> (idx % 64) & 1 != 0
        self.data[idx >> 6].load(Relaxed) >> (idx & 63) & 1 != 0
    }

    fn set(&self, idx: usize) {
        self.data[idx >> 6].fetch_or(1 << (idx & 63), Relaxed);
    }

    fn trailing_zeros(&self) -> Option<usize> {
        for (big_idx, bit_field_ref) in self.data.iter().enumerate() {
            let bit_field = bit_field_ref.load(Acquire).clone();
            if bit_field != u64::MAX {
                //not all ones
                return Some((big_idx << 6) + ((!bit_field).trailing_zeros() as usize));
            }
        }
        None
    }
}

//Parallel speculation/correction-based greedy graph coloring
// todo: add better description
#[inline(never)]
pub fn coloring<NI, G>(graph: &G, config: GraphColoringConfig<NI>) -> Result<Vec<usize>, String>
where
    NI: Idx,
    G: Graph<NI> + UndirectedNeighbors<NI> + UndirectedDegrees<NI> + Sync,
{
    let max_colors = config.max_colors.index(); //todo: add option to automatically pick and rerun if it was too small?
    assert_eq!(max_colors % 64, 0);
    let node_count = graph.node_count().index();
    let mut colors: Vec<usize> = vec![0; node_count]; //mutated through 'unsafe' use of pointer //init values not used, malloc?
    let colors_ptr = SharedMut::new(colors.as_mut_ptr());
    let mut color_forbidden = (0..node_count)
        .into_par_iter() //todo: parallelization had non-measurable impact on performance. also unlimited threads
        .map(|_| BitField::new(max_colors))
        .collect();
    let mut nodes_to_color: Vec<NI> = (0..node_count).map(NI::new).collect();
    if let Err(err) =
        assign_colors_in_parallel(graph, &nodes_to_color, &colors_ptr, &mut color_forbidden)
    {
        return Err(err);
    }
    nodes_to_color = find_incorrect_nodes(nodes_to_color, &colors_ptr, &color_forbidden);
    while !nodes_to_color.is_empty() {
        if let Err(err) =
            assign_colors_in_parallel(graph, &nodes_to_color, &colors_ptr, &mut color_forbidden)
        {
            return Err(err);
        }
        update_forbidden_colors(graph, &nodes_to_color, &colors_ptr, &mut color_forbidden); //nodes to color, whose neighbors have been corrected, needs to get their forbidden_colors updated.
        nodes_to_color = find_incorrect_nodes(nodes_to_color, &colors_ptr, &color_forbidden);
    }
    colors = make_consecutive(colors);
    Ok(colors)
}

#[inline(never)]
fn assign_colors_in_parallel<NI, G>(
    graph: &G,
    nodes: &Vec<NI>,
    colors_ptr: &SharedMut<usize>,
    color_forbidden: &mut Vec<BitField>,
) -> Result<(), String>
where
    NI: Idx,
    G: Graph<NI> + UndirectedNeighbors<NI> + Sync,
{
    let success = AtomicBool::new(true);
    let next_chunk = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let num_threads = available_parallelism().map_or(DEFAULT_PARALLELISM, |p| p.get());
        for _ in 0..num_threads {
            s.spawn(|| loop {
                let start = AtomicUsize::fetch_add(&next_chunk, CHUNK_SIZE, Acquire);
                if start >= nodes.len() {
                    break;
                }
                let end = (start + CHUNK_SIZE).min(nodes.len());

                for i in start..end {
                    let u = nodes[i];
                    if let Some(color) = color_forbidden[u.index()].trailing_zeros() {
                        for v in graph.neighbors(u) {
                            color_forbidden[v.index()].set(color);
                        }
                        unsafe {
                            colors_ptr.add(u.index()).write(color);
                        }
                    } else {
                        success.store(false, Relaxed);
                    }
                }
            });
        }
    });
    if success.load(Relaxed) {
        Ok(())
    } else {
        Err("Not enough colors :/".to_string())
    }
}

fn find_incorrect_nodes<NI>(
    nodes: Vec<NI>,
    colors_ptr: &SharedMut<usize>,
    color_forbidden: &Vec<BitField>,
) -> Vec<NI>
where
    NI: Idx,
{
    nodes
        .into_par_iter() //todo: restrict #threads?
        .filter(|v| color_forbidden[v.index()].is_set(unsafe { colors_ptr.add(v.index()).read() }))
        .collect()
}

fn update_forbidden_colors<G, NI>(
    graph: &G,
    nodes_to_color: &Vec<NI>,
    colors_ptr: &SharedMut<usize>,
    color_forbidden: &mut Vec<BitField>,
) where
    G: Graph<NI> + UndirectedNeighbors<NI> + UndirectedDegrees<NI> + Sync,
    NI: Idx,
{
    {
        nodes_to_color.into_par_iter().for_each(|v| unsafe {
            //todo: restrict #threads?
            //we only care for nodes_just_colored, but filter on them is probably more expensive than using all
            let nearby_colors = graph
                .neighbors(*v)
                .map(|u| colors_ptr.add(u.index()).read())
                .collect();
            color_forbidden[v.index()].update(&nearby_colors); //each node writes to separate entry. Should be safe.
        });
    }
}

fn make_consecutive(mut colors: Vec<usize>) -> Vec<usize> {
    let mut top_color = *colors.iter().max().unwrap();
    let used_colors = BitField::new((top_color + 1) - (top_color + 1) % 64 + 64);
    used_colors.update(&colors);
    let free_colors = (0..=top_color).filter(|&color| !used_colors.is_set(color));
    let mut color_map: Vec<usize> = (0..=top_color).collect();
    for free_color in free_colors {
        color_map[top_color] = free_color;
        top_color -= 1;
    }
    colors
        .iter_mut()
        .for_each(|color| *color = color_map[*color]);
    colors
}

pub mod tests {
    use crate::coloring::{coloring, GraphColoringConfig};
    use graph_builder::prelude::*;

    pub fn check_correct<NI: Idx, G: Graph<NI> + UndirectedNeighbors<NI>>(
        graph: &G,
        color: &Vec<usize>,
    ) -> bool {
        for node in NI::zero().range(graph.node_count()) {
            for neighbor in graph.neighbors(node) {
                if color[node.index()] == color[neighbor.index()] {
                    return false;
                }
            }
        }
        true
    }
    #[test]
    fn test_coloring_two_components() {
        let gdl = "(a)-->()-->()<--(a),(b)-->()-->()<--(b)";

        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        let coloring = coloring(&graph, GraphColoringConfig { max_colors: 64 })
            .ok()
            .unwrap();
        assert!(check_correct(&graph, &coloring));
    }
}

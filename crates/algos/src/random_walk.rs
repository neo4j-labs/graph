use rand::prelude::*;
use rayon::prelude::*;

use crate::prelude::*;

#[derive(Debug)]
pub struct RandomWalkConfig {
    walks_per_node: u32,
    walk_length: u32,
    normalized_return_probability: f32,
    normalized_same_distance_probability: f32,
    normalized_in_out_probability: f32,
}

impl RandomWalkConfig {
    fn new(walks_per_node: u32, walk_length: u32, in_out_factor: f32, return_factor: f32) -> Self {
        let max_probability = f32::max(f32::max(1.0 / return_factor, 1.0), 1.0 / in_out_factor);
        let normalized_return_probability = (1.0 / return_factor) / max_probability;
        let normalized_same_distance_probability = 1.0 / max_probability;
        let normalized_in_out_probability = (1.0 / in_out_factor) / max_probability;

        Self {
            walks_per_node,
            walk_length,
            normalized_return_probability,
            normalized_same_distance_probability,
            normalized_in_out_probability,
        }
    }
}

impl Default for RandomWalkConfig {
    fn default() -> Self {
        Self::new(10, 80, 1.0, 1.0)
    }
}

pub fn second_order_random_walk<'graph, NI: Idx>(
    graph: &'graph DirectedCsrGraph<NI>,
    config: &'graph RandomWalkConfig,
) -> impl rayon::iter::ParallelIterator<Item = Vec<NI>> + 'graph {
    let node_count = graph.node_count().index();

    (0..node_count)
        .into_par_iter()
        .filter_map(|node| {
            let id = NI::new(node);
            if graph.out_degree(id).index() > 0 {
                Some(id)
            } else {
                None
            }
        })
        .flat_map_iter(|node| random_walks_for_node(&node, graph, config))
}

fn random_walks_for_node<NI: Idx>(
    node: &NI,
    graph: &DirectedCsrGraph<NI>,
    config: &RandomWalkConfig,
) -> Vec<Vec<NI>> {
    (0..config.walks_per_node)
        .into_iter()
        .map(|_| random_walk_for_node(node, graph, config))
        .collect()
}

fn random_walk_for_node<NI: Idx>(
    node: &NI,
    graph: &DirectedCsrGraph<NI>,
    config: &RandomWalkConfig,
) -> Vec<NI> {
    let mut walk = Vec::with_capacity(config.walk_length as usize);
    walk.push(*node);

    if graph.out_degree(*node).index() == 1 {
        return vec![*node];
    }

    walk.push(random_neighbour(node, graph));

    for i in 2_usize..config.walk_length as usize {
        let maybe_next_node = walk_one_step(&walk[i - 2], &walk[i - 1], graph, config);
        if maybe_next_node.is_none() {
            walk.resize(i, NI::new(0));
            return walk;
        }
        walk.push(maybe_next_node.unwrap());
    }

    walk
}

fn random_neighbour<NI: Idx>(node: &NI, graph: &DirectedCsrGraph<NI>) -> NI {
    let degree = graph.out_degree(*node);
    let mut rng = rand::thread_rng();

    let index = rng.gen_range(0..degree.index());

    graph.out_neighbors(*node)[index]
}

fn walk_one_step<NI: Idx>(
    prev_node: &NI,
    current_node: &NI,
    graph: &DirectedCsrGraph<NI>,
    config: &RandomWalkConfig,
) -> Option<NI> {
    let current_node_degree = graph.out_degree(*current_node);
    let mut rng = rand::thread_rng();

    match current_node_degree.index() {
        0 => None,
        1 => {
            let neighbour = graph.out_neighbors(*current_node)[0];
            Some(neighbour)
        }
        _ => {
            let mut tries = 0;

            while tries < 10 {
                let new_node = random_neighbour(current_node, graph);
                let r: f32 = rng.gen();

                if new_node == *prev_node {
                    if r < config.normalized_return_probability {
                        return Some(new_node);
                    }
                } else if is_neighbour(graph, prev_node, &new_node) {
                    if r < config.normalized_same_distance_probability {
                        return Some(new_node);
                    }
                } else if r < config.normalized_in_out_probability {
                    return Some(new_node);
                }

                tries += 1;
            }

            let neighbour = graph.out_neighbors(*current_node)[0];
            Some(neighbour)
        }
    }
}

fn is_neighbour<NI: Idx>(graph: &DirectedCsrGraph<NI>, node1: &NI, node2: &NI) -> bool {
    graph.out_neighbors(*node1).binary_search(node2).is_ok()
}

#[cfg(test)]
mod tests {
    use crate::prelude::{CsrLayout, DirectedCsrGraph, GraphBuilder};

    use super::*;

    #[test]
    fn test_tc_two_components() {
        let gdl = "(a)-->()-->()<--(a),(b)-->()-->()<--(b)";

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        second_order_random_walk(&graph, &RandomWalkConfig::default());
    }
    //
    // #[test]
    // fn test_tc_connected_triangles() {
    //     let gdl = "(a)-->()-->()<--(a),(a)-->()-->()<--(a)";
    //
    //     let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
    //         .csr_layout(CsrLayout::Deduplicated)
    //         .gdl_str::<usize, _>(gdl)
    //         .build()
    //         .unwrap();
    //
    //     assert_eq!(global_triangle_count(&graph), 2);
    // }
    //
    // #[test]
    // fn test_tc_diamond() {
    //     let gdl = "(a)-->(b)-->(c)<--(a),(b)-->(d)<--(c)";
    //
    //     let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
    //         .csr_layout(CsrLayout::Deduplicated)
    //         .gdl_str::<usize, _>(gdl)
    //         .build()
    //         .unwrap();
    //
    //     assert_eq!(global_triangle_count(&graph), 2);
    // }
}

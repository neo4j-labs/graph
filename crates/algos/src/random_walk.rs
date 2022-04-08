use rand::prelude::*;
use rayon::prelude::*;

use crate::prelude::*;

#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

pub fn random_walks<'graph, NI: Idx>(
    graph: &'graph DirectedCsrGraph<NI>,
    config: &'graph RandomWalkConfig,
) -> impl ParallelIterator<Item = Vec<NI>> + 'graph {
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

    if graph.out_degree(*node).index() == 0 {
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
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_random_walks_with_default_parameters() {
        let gdl = "(a)-->(b)-->(a),(a)-->(c)-->(a),(b)-->(c)-->(b),(d),(e)";

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_str::<usize, _>(gdl)
            .build()
            .unwrap();

        let config = RandomWalkConfig::default();

        let walks: Vec<Vec<usize>> = random_walks(&graph, &config).collect();

        let expected_walk_count = config.walks_per_node as usize * graph.node_count();

        assert_eq!(expected_walk_count, walks.len());

        walks
            .iter()
            .map(|walk| walk.len())
            .for_each(|walk_len| assert_eq!(walk_len, config.walk_length as usize));

        assert!(!walks.iter().any(|walk| walk[0] == 3 || walk[0] == 4));
    }

    #[test]
    fn test_return_factor_should_make_walks_include_start_node_more_often() {
        let gdl = r#"
              (a)-->(b)-->(a)
            , (b)-->(c)-->(a)
            , (c)-->(d)-->(a)
            , (d)-->(e)-->(a)
            , (e)-->(f)-->(a)
            , (f)-->(g)-->(a)
            , (g)-->(h)-->(a)"#;

        let mut gdl_graph = ::gdl::Graph::default();
        gdl_graph.append(gdl).unwrap();

        let a_id = gdl_graph.get_node("a").unwrap().id();
        let b_id = gdl_graph.get_node("b").unwrap().id();
        let c_id = gdl_graph.get_node("c").unwrap().id();

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_graph::<usize>(&gdl_graph)
            .build()
            .unwrap();

        let config = RandomWalkConfig::new(100, 10, 1.0, 0.01);

        let node_counter: HashMap<usize, usize> = count_node_occurrences(a_id, &graph, &config);

        let a_count = *node_counter.get(&a_id).unwrap();
        let b_count = *node_counter.get(&b_id).unwrap();
        let c_count = *node_counter.get(&c_id).unwrap();

        // (a) and (b) have similar occurrences, since from (a) the only reachable node is (b)
        assert!(a_count.abs_diff(b_count) <= 1000);

        // all other nodes should occur far less often because of the high return probability
        assert!(a_count > c_count * 40);
    }

    #[test]
    fn test_large_in_out_factor_should_make_the_walk_keep_the_same_distance() {
        let gdl = r#"
              (a)-->(b)
            , (a)-->(c)
            , (a)-->(d)
            , (b)-->(a)
            , (b)-->(e)
            , (c)-->(a)
            , (c)-->(d)
            , (c)-->(e)
            , (d)-->(a)
            , (d)-->(c)
            , (d)-->(e)
            , (e)-->(a)"#;

        let mut gdl_graph = ::gdl::Graph::default();
        gdl_graph.append(gdl).unwrap();

        let a_id = gdl_graph.get_node("a").unwrap().id();
        let b_id = gdl_graph.get_node("b").unwrap().id();
        let c_id = gdl_graph.get_node("c").unwrap().id();
        let d_id = gdl_graph.get_node("d").unwrap().id();
        let e_id = gdl_graph.get_node("e").unwrap().id();

        let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .gdl_graph::<usize>(&gdl_graph)
            .build()
            .unwrap();

        let config = RandomWalkConfig::new(1000, 10, 100000.0, 0.1);

        let node_counter: HashMap<usize, usize> = count_node_occurrences(a_id, &graph, &config);

        let a_count = *node_counter.get(&a_id).unwrap();
        let b_count = *node_counter.get(&b_id).unwrap();
        let c_count = *node_counter.get(&c_id).unwrap();
        let d_count = *node_counter.get(&d_id).unwrap();
        let e_count = *node_counter.get(&e_id).unwrap();

        // (a), (b), (c), (d) should be much more common than (e)
        assert!(a_count - e_count > 4000);
        assert!(b_count - e_count > 1200);
        assert!(c_count - e_count > 1200);
        assert!(d_count - e_count > 1200);
    }

    fn count_node_occurrences(
        a_id: usize,
        graph: &DirectedCsrGraph<usize>,
        config: &RandomWalkConfig,
    ) -> HashMap<usize, usize> {
        random_walks(graph, config)
            .collect::<Vec<Vec<usize>>>()
            .iter()
            .filter(|walk| walk[0] == a_id)
            .flatten()
            .fold(HashMap::new(), |mut acc, node| {
                *acc.entry(*node).or_default() += 1;
                acc
            })
    }
}

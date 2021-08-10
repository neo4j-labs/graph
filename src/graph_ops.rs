use log::info;
use rayon::prelude::*;

use crate::graph::csr::{prefix_sum_non_atomic, CSR};
use crate::index::Idx;
use crate::{DirectedGraph, Error, Graph, SharedMut, UndirectedGraph};

use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

pub trait DegreePartitionOp<Node: Idx> {
    fn degree_partition(&self, concurrency: usize) -> Vec<Range<Node>>;
}

pub trait OutDegreePartitionOp<Node: Idx> {
    fn out_degree_partition(&self, concurrency: usize) -> Vec<Range<Node>>;
}

pub trait InDegreePartitionOp<Node: Idx> {
    fn in_degree_partition(&self, concurrency: usize) -> Vec<Range<Node>>;
}

pub trait ForEachNodeOp<Node: Idx> {
    fn for_each_node<T, F>(
        &self,
        partition: &[Range<Node>],
        node_values: &mut [T],
        node_fn: F,
    ) -> Result<(), Error>
    where
        T: Send,
        F: Fn(&Self, Node, &mut T) + Send + Sync;
}

pub trait RelabelByDegreeOp<Node: Idx> {
    fn relabel_by_degree(self) -> Self;
}

impl<Node, G> RelabelByDegreeOp<Node> for G
where
    Node: Idx,
    G: From<CSR<Node>> + UndirectedGraph<Node> + Sync,
{
    fn relabel_by_degree(self) -> Self {
        relabel_by_degree(self)
    }
}

impl<Node, G> ForEachNodeOp<Node> for G
where
    Node: Idx,
    G: Graph<Node> + Sync,
{
    fn for_each_node<T, F>(
        &self,
        partition: &[Range<Node>],
        node_values: &mut [T],
        node_fn: F,
    ) -> Result<(), Error>
    where
        T: Send,
        F: Fn(&Self, Node, &mut T) + Send + Sync,
    {
        if partition.iter().map(|r| r.end - r.start).sum::<Node>() != self.node_count() {
            return Err(Error::InvalidPartitioning);
        }

        if node_values.len() != self.node_count().index() {
            return Err(Error::InvalidNodeValues);
        }

        let node_value_splits = split_by_partition(partition, node_values);

        let node_fn = Arc::new(node_fn);

        node_value_splits
            .into_par_iter()
            .zip(partition.into_par_iter())
            .for_each_with(node_fn, |node_fn, (mutable_chunk, range)| {
                for (node_state, node) in mutable_chunk.iter_mut().zip(range.start..range.end) {
                    node_fn(self, node, node_state);
                }
            });

        Ok(())
    }
}

impl<Node: Idx, U: UndirectedGraph<Node>> DegreePartitionOp<Node> for U {
    fn degree_partition(&self, concurrency: usize) -> Vec<Range<Node>> {
        let batch_size = (self.edge_count().index() * 2) / concurrency;
        node_map_partition(
            |node| self.degree(node).index(),
            self.node_count(),
            batch_size,
        )
    }
}

impl<Node: Idx, D: DirectedGraph<Node>> OutDegreePartitionOp<Node> for D {
    fn out_degree_partition(&self, concurrency: usize) -> Vec<Range<Node>> {
        let batch_size = self.edge_count().index() / concurrency;
        node_map_partition(
            |node| self.out_degree(node).index(),
            self.node_count(),
            batch_size,
        )
    }
}

impl<Node: Idx, D: DirectedGraph<Node>> InDegreePartitionOp<Node> for D {
    fn in_degree_partition(&self, concurrency: usize) -> Vec<Range<Node>> {
        let batch_size = self.edge_count().index() / concurrency;
        node_map_partition(
            |node| self.in_degree(node).index(),
            self.node_count(),
            batch_size,
        )
    }
}

fn split_by_partition<'a, Node: Idx, T>(
    partition: &[Range<Node>],
    slice: &'a mut [T],
) -> Vec<&'a mut [T]> {
    debug_assert_eq!(
        partition
            .iter()
            .map(|r| r.end - r.start)
            .sum::<Node>()
            .index(),
        slice.len()
    );

    let mut splits = Vec::with_capacity(partition.len());

    let mut remainder = slice;
    let mut current_start = Node::zero();
    for range in partition.iter() {
        let next_end = range.end - current_start;
        current_start += next_end;

        let (left, right) = remainder.split_at_mut(next_end.index());

        splits.push(left);
        remainder = right;
    }

    splits
}

fn node_map_partition<Node, F>(node_map: F, node_count: Node, batch_size: usize) -> Vec<Range<Node>>
where
    F: Fn(Node) -> usize,
    Node: Idx,
{
    let mut partitions = Vec::new();

    let mut partition_size = 0;
    let mut partition_start = Node::zero();
    let upper_bound = node_count - Node::new(1);

    for node in Node::zero()..node_count {
        partition_size += node_map(node);

        if partition_size >= batch_size || node == upper_bound {
            let partition_end = node + Node::new(1);
            partitions.push(partition_start..partition_end);
            partition_size = 0;
            partition_start = partition_end;
        }
    }

    partitions
}

fn relabel_by_degree<Node, G>(graph: G) -> G
where
    Node: Idx,
    G: From<CSR<Node>> + UndirectedGraph<Node> + Sync,
{
    let node_count = graph.node_count();

    let mut degree_node_pairs = Vec::with_capacity(node_count.index());

    let start = Instant::now();

    (0..node_count.index())
        .into_par_iter()
        .map(Node::new)
        .map(|node_id| (graph.degree(node_id), node_id))
        .collect_into_vec(&mut degree_node_pairs);

    // sort node-degree pairs descending by degree
    degree_node_pairs.par_sort_unstable_by(|left, right| left.cmp(right).reverse());
    info!("Relabel: sorted degree-node-pairs in {:?}", start.elapsed());

    let mut degrees = Vec::<Node>::with_capacity(node_count.index());
    let mut new_ids = Vec::<Node>::with_capacity(node_count.index());

    let new_ids_ptr = SharedMut(new_ids.as_mut_ptr());

    let start = Instant::now();
    (0..node_count.index())
        .into_par_iter()
        .map(|n| {
            let (degree, node) = degree_node_pairs[n.index()];

            // SAFETY:
            //   node is the node_id from degree_node_pairs which is created
            //   from 0..node_count -- the values are all distinct and we will
            //   not write into the same location in parallel
            unsafe {
                new_ids_ptr.add(node.index()).write(Node::new(n));
            }

            degree
        })
        .collect_into_vec(&mut degrees);

    unsafe {
        new_ids.set_len(node_count.index());
    }

    info!(
        "Relabel: built degrees and node mapping in {:?}",
        start.elapsed()
    );

    let offsets = prefix_sum_non_atomic(degrees);
    let edge_count = offsets[node_count.index()].index();

    let mut targets = Vec::<Node>::with_capacity(edge_count);
    let targets_ptr = SharedMut(targets.as_mut_ptr());

    let start = Instant::now();
    (0..node_count.index())
        .into_par_iter()
        .map(Node::new)
        .for_each(|u| {
            let new_u = new_ids[u.index()];
            let start_offset = offsets[new_u.index()].index();
            let mut end_offset = start_offset;

            for &v in graph.neighbors(u) {
                unsafe {
                    targets_ptr.add(end_offset).write(new_ids[v.index()]);
                }
                end_offset += 1;
            }

            unsafe {
                std::slice::from_raw_parts_mut(
                    targets_ptr.add(start_offset),
                    end_offset - start_offset,
                )
            }
            .sort_unstable();
        });

    unsafe {
        targets.set_len(edge_count);
    }

    info!("Relabel: built and sorted targets in {:?}", start.elapsed());

    G::from(CSR::new(
        offsets.into_boxed_slice(),
        targets.into_boxed_slice(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_by_partition_3_parts() {
        let partition = vec![0..2, 2..5, 5..10];
        let mut slice = (0..10).into_iter().collect::<Vec<_>>();
        let splits = split_by_partition(&partition, &mut slice);

        assert_eq!(splits.len(), partition.len());
        for (s, p) in splits.into_iter().zip(partition) {
            assert_eq!(s, p.into_iter().collect::<Vec<usize>>());
        }
    }

    #[test]
    fn split_by_partition_8_parts() {
        let partition = vec![0..1, 1..2, 2..3, 3..4, 4..6, 6..7, 7..8, 8..10];
        let mut slice = (0..10).into_iter().collect::<Vec<_>>();
        let splits = split_by_partition(&partition, &mut slice);

        assert_eq!(splits.len(), partition.len());
        for (s, p) in splits.into_iter().zip(partition) {
            assert_eq!(s, p.into_iter().collect::<Vec<usize>>());
        }
    }

    #[test]
    fn node_map_partition_1_part() {
        let partitions = node_map_partition::<usize, _>(|_| 1_usize, 10, 10);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], 0..10);
    }

    #[test]
    fn node_map_partition_2_parts() {
        let partitions = node_map_partition::<usize, _>(|x| x % 2_usize, 10, 4);
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0], 0..8);
        assert_eq!(partitions[1], 8..10);
    }

    #[test]
    fn node_map_partition_6_parts() {
        let partitions = node_map_partition::<usize, _>(|x| x as usize, 10, 6);
        assert_eq!(partitions.len(), 6);
        assert_eq!(partitions[0], 0..4);
        assert_eq!(partitions[1], 4..6);
        assert_eq!(partitions[2], 6..7);
        assert_eq!(partitions[3], 7..8);
        assert_eq!(partitions[4], 8..9);
        assert_eq!(partitions[5], 9..10);
    }
}

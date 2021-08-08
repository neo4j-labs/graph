use crate::index::Idx;
use crate::{
    DirectedGraph, DirectedGraphOps, Graph, GraphOps, UndirectedGraph, UndirectedGraphOps,
};
use std::{
    io::{Error, ErrorKind},
    ops::Range,
};

impl<Node: Idx, G: Graph<Node>> GraphOps<Node> for G {
    fn for_each_node<T, F>(
        &self,
        partition: &[Range<Node>],
        per_node_mutable_state: &mut [T],
        node_fn: F,
    ) -> Result<(), Error>
    where
        T: Send,
        F: Fn(&Self, Node, &mut T) + Send + Sync + Copy,
    {
        if partition.iter().map(|r| r.end - r.start).sum::<Node>() != self.node_count() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid partition of nodes",
            ));
        }

        if per_node_mutable_state.len() != self.node_count().index() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Length of mutable_state must be the same as graph's node count",
            ));
        }

        let mutable_split = split_by_partition(partition, per_node_mutable_state);

        rayon::scope(|s| {
            for (mutable_chunk, range) in mutable_split.into_iter().zip(partition) {
                s.spawn(move |_| {
                    for (node_state, node) in mutable_chunk.iter_mut().zip(range.start..range.end) {
                        node_fn(self, node, node_state);
                    }
                });
            }
        });

        Ok(())
    }
}

impl<Node: Idx, D: DirectedGraph<Node>> DirectedGraphOps<Node> for D {
    fn out_degree_partition(&self, concurrency: Node) -> Vec<Range<Node>> {
        let batch_size = self.edge_count() / concurrency;
        node_map_partition(|node| self.out_degree(node), self.node_count(), batch_size)
    }

    fn in_degree_partition(&self, concurrency: Node) -> Vec<Range<Node>> {
        let batch_size = self.edge_count() / concurrency;
        node_map_partition(|node| self.in_degree(node), self.node_count(), batch_size)
    }
}

impl<Node: Idx, U: UndirectedGraph<Node>> UndirectedGraphOps<Node> for U {
    fn degree_partition(&self, concurrency: Node) -> Vec<Range<Node>> {
        let batch_size = (self.edge_count() * Node::new(2)) / concurrency;
        node_map_partition(|node| self.degree(node), self.node_count(), batch_size)
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

    let mut split: Vec<&mut [T]> = Vec::with_capacity(partition.len());

    let mut remainder = slice;
    let mut current_start = Node::zero();
    for r in partition.iter() {
        let next_end = r.end - current_start;
        current_start += next_end;

        let t = remainder.split_at_mut(next_end.index());

        split.push(t.0);
        remainder = t.1;
    }

    split
}

fn node_map_partition<Node: Idx, F>(
    node_map: F,
    node_count: Node,
    batch_size: Node,
) -> Vec<Range<Node>>
where
    F: Fn(Node) -> Node,
{
    let mut partitions = Vec::new();

    let mut partition_size = Node::zero();
    let mut partition_start = Node::zero();
    for i in 0..node_count.index() {
        partition_size += node_map(Node::new(i));

        if partition_size >= batch_size || i == node_count.index() - 1 {
            partitions.push(partition_start..Node::new(i + 1));
            partition_size = Node::zero();
            partition_start = Node::new(i + 1);
        }
    }

    partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_by_3_partition() {
        let partition: Vec<Range<usize>> = vec![0..2, 2..5, 5..10];
        let mut slice: Vec<usize> = (0..10).into_iter().collect();
        let split = split_by_partition(&partition, &mut slice);
        assert_eq!(split.len(), partition.len());
        for (s, p) in split.into_iter().zip(partition) {
            assert_eq!(s, p.into_iter().collect::<Vec<usize>>());
        }
    }

    #[test]
    fn split_by_8_partition() {
        let partition: Vec<Range<usize>> = vec![0..1, 1..2, 2..3, 3..4, 4..6, 6..7, 7..8, 8..10];
        let mut slice: Vec<usize> = (0..10).into_iter().collect();
        let split = split_by_partition(&partition, &mut slice);
        assert_eq!(split.len(), partition.len());
        for (s, p) in split.into_iter().zip(partition) {
            assert_eq!(s, p.into_iter().collect::<Vec<usize>>());
        }
    }

    #[test]
    fn node_map_1_partition() {
        let partitions = node_map_partition(|_| 1_usize, 10, 10);
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0], 0..10);
    }

    #[test]
    fn node_map_2_partitions() {
        let partitions = node_map_partition(|x| x % 2_usize, 10, 4);
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0], 0..8);
        assert_eq!(partitions[1], 8..10);
    }

    #[test]
    fn node_map_6_partitions() {
        let partitions = node_map_partition(|x| x as usize, 10, 6);
        assert_eq!(partitions.len(), 6);
        assert_eq!(partitions[0], 0..4);
        assert_eq!(partitions[1], 4..6);
        assert_eq!(partitions[2], 6..7);
        assert_eq!(partitions[3], 7..8);
        assert_eq!(partitions[4], 8..9);
        assert_eq!(partitions[5], 9..10);
    }
}

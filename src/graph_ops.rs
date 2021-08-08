use crate::index::Idx;
use crate::{DirectedGraph, DirectedGraphOps, UndirectedGraph, UndirectedGraphOps};
use std::ops::Range;

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

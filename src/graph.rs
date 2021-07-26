use log::info;
use std::{
    collections::HashMap, mem::transmute, ops::Range, sync::atomic::Ordering::SeqCst, time::Instant,
};

use rayon::iter::IndexedParallelIterator;
use rayon::{
    iter::{
        IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator,
    },
    slice::ParallelSliceMut,
};

use crate::{
    index::{AtomicIdx, Idx},
    input::{Direction, DotGraph, EdgeList},
    DirectedGraph, Graph, UndirectedGraph,
};

#[derive(Clone, Copy)]
pub enum CSROption {
    Unsorted,
    Sorted,
    Deduplicated,
}

impl Default for CSROption {
    fn default() -> Self {
        CSROption::Sorted
    }
}

pub struct CSR<Node: Idx> {
    offsets: Box<[Node]>,
    targets: Box<[Node]>,
}

impl<Node: Idx> CSR<Node> {
    #[inline]
    fn node_count(&self) -> Node {
        Node::new(self.offsets.len() - 1)
    }

    #[inline]
    fn edge_count(&self) -> Node {
        Node::new(self.targets.len())
    }

    #[inline]
    fn degree(&self, node: Node) -> Node {
        let from = self.offsets[node.index()];
        let to = self.offsets[(node + Node::new(1)).index()];

        to - from
    }

    #[inline]
    fn neighbors(&self, node: Node) -> &[Node] {
        let from = self.offsets[node.index()];
        let to = self.offsets[(node + Node::new(1)).index()];

        &self.targets[from.index()..to.index()]
    }
}

type CSRConfiguration<'a, Node> = (&'a EdgeList<Node>, Node, Direction, CSROption);

impl<Node: Idx> From<CSRConfiguration<'_, Node>> for CSR<Node> {
    fn from((edge_list, node_count, direction, _csr_option): CSRConfiguration<'_, Node>) -> Self {
        let mut start = Instant::now();

        let degrees = edge_list.degrees(node_count, direction);
        info!("degrees took {} ms", start.elapsed().as_millis());
        start = Instant::now();

        let offsets = prefix_sum(degrees);
        info!("prefix_sum took {} ms", start.elapsed().as_millis());
        start = Instant::now();

        let targets_len = offsets[node_count.index()].load(SeqCst);
        let mut targets = Vec::with_capacity(targets_len.index());
        targets.resize_with(targets_len.index(), Node::Atomic::zero);

        match direction {
            Direction::Outgoing => edge_list.par_iter().for_each(|(s, t)| {
                targets[offsets[s.index()].fetch_add(1, SeqCst).index()].store(*t, SeqCst);
            }),
            Direction::Incoming => edge_list.par_iter().for_each(|(s, t)| {
                targets[offsets[t.index()].fetch_add(1, SeqCst).index()].store(*s, SeqCst);
            }),
            Direction::Undirected => edge_list.par_iter().for_each(|(s, t)| {
                targets[offsets[s.index()].fetch_add(1, SeqCst).index()].store(*t, SeqCst);
                targets[offsets[t.index()].fetch_add(1, SeqCst).index()].store(*s, SeqCst);
            }),
        }
        info!("targets took {} ms", start.elapsed().as_millis());
        start = Instant::now();

        let mut offsets = unsafe { transmute::<_, Vec<Node>>(offsets) };
        let mut targets = unsafe { transmute::<_, Vec<Node>>(targets) };

        // the previous loop moves all offsets one index to the right
        // we need to correct this to have proper offsets
        offsets.rotate_right(1);
        offsets[0] = Node::zero();

        sort_targets(&offsets, &mut targets);
        info!("sort_targets took {} ms", start.elapsed().as_millis());

        CSR {
            offsets: offsets.into_boxed_slice(),
            targets: targets.into_boxed_slice(),
        }
    }
}

pub struct DirectedCSRGraph<Node: Idx> {
    node_count: Node,
    edge_count: Node,
    out_edges: CSR<Node>,
    in_edges: CSR<Node>,
}

impl<Node: Idx> DirectedCSRGraph<Node> {
    pub fn new(out_edges: CSR<Node>, in_edges: CSR<Node>) -> Self {
        let node_count = out_edges.node_count();
        let edge_count = out_edges.edge_count();

        info!(
            "Created directed CSR graph (node_count = {:?}, edge_count = {:?})",
            node_count, edge_count
        );

        Self {
            node_count,
            edge_count,
            out_edges,
            in_edges,
        }
    }
}

impl<Node: Idx> Graph<Node> for DirectedCSRGraph<Node> {
    fn node_count(&self) -> Node {
        self.node_count
    }

    fn edge_count(&self) -> Node {
        self.edge_count
    }
}

impl<Node: Idx> DirectedGraph<Node> for DirectedCSRGraph<Node> {
    fn out_degree(&self, node: Node) -> Node {
        self.out_edges.degree(node)
    }

    fn out_neighbors(&self, node: Node) -> &[Node] {
        self.out_edges.neighbors(node)
    }

    fn in_degree(&self, node: Node) -> Node {
        self.in_edges.degree(node)
    }

    fn in_neighbors(&self, node: Node) -> &[Node] {
        self.in_edges.neighbors(node)
    }
}

impl<Node: Idx> From<(EdgeList<Node>, CSROption)> for DirectedCSRGraph<Node> {
    fn from((edge_list, csr_option): (EdgeList<Node>, CSROption)) -> Self {
        let node_count = edge_list.max_node_id() + Node::new(1);
        let out_edges = CSR::from((&edge_list, node_count, Direction::Outgoing, csr_option));
        let in_edges = CSR::from((&edge_list, node_count, Direction::Incoming, csr_option));

        DirectedCSRGraph::new(out_edges, in_edges)
    }
}

pub struct UndirectedCSRGraph<Node: Idx> {
    node_count: Node,
    edge_count: Node,
    edges: CSR<Node>,
}

impl<Node: Idx> UndirectedCSRGraph<Node> {
    pub fn new(edges: CSR<Node>) -> Self {
        let node_count = edges.node_count();
        let edge_count = edges.edge_count() / Node::new(2);

        info!(
            "Created undirected CSR graph (node_count = {:?}, edge_count = {:?})",
            node_count, edge_count
        );

        Self {
            node_count,
            edge_count,
            edges,
        }
    }

    pub fn relabel_by_degrees(self) -> Self {
        let node_count = self.node_count();

        let mut degree_node_pairs = Vec::with_capacity(node_count.index());

        (0..node_count.index())
            .into_par_iter()
            .map(Node::new)
            .map(|node_id| (self.degree(node_id), node_id))
            .collect_into_vec(&mut degree_node_pairs);

        // sort node-degree pairs descending by degree
        degree_node_pairs.par_sort_unstable_by(|left, right| left.cmp(right).reverse());

        let mut degrees = Vec::with_capacity(node_count.index());
        degrees.resize_with(node_count.index(), Node::Atomic::zero);

        let mut new_ids = Vec::with_capacity(node_count.index());
        new_ids.resize_with(node_count.index(), Node::Atomic::zero);

        (0..node_count.index())
            .into_par_iter()
            .map(Node::new)
            .for_each(|n| {
                let (degree, node) = degree_node_pairs[n.index()];
                degrees[n.index()].store(degree, SeqCst);
                new_ids[node.index()].store(n, SeqCst);
            });

        let offsets = prefix_sum(degrees);

        let edge_count = offsets[node_count.index()].load(SeqCst).index();
        let mut targets = Vec::with_capacity(edge_count);
        targets.resize_with(edge_count, Node::Atomic::zero);

        (0..node_count.index())
            .into_par_iter()
            .map(Node::new)
            .for_each(|u| {
                let new_u = new_ids[u.index()].load(SeqCst);

                for &v in self.neighbors(u) {
                    let new_v = new_ids[v.index()].load(SeqCst);
                    let offset = offsets[new_u.index()].fetch_add(1, SeqCst);
                    targets[offset.index()].store(new_v, SeqCst);
                }
            });

        let mut offsets = unsafe { transmute::<_, Vec<Node>>(offsets) };
        let mut targets = unsafe { transmute::<_, Vec<Node>>(targets) };

        // the previous loop moves all offsets one index to the right
        // we need to correct this to have proper offsets
        offsets.rotate_right(1);
        offsets[0] = Node::zero();

        sort_targets(&offsets, &mut targets);

        let csr = CSR {
            offsets: offsets.into_boxed_slice(),
            targets: targets.into_boxed_slice(),
        };

        UndirectedCSRGraph::new(csr)
    }
}

impl<Node: Idx> Graph<Node> for UndirectedCSRGraph<Node> {
    fn node_count(&self) -> Node {
        self.node_count
    }

    fn edge_count(&self) -> Node {
        self.edge_count
    }
}

impl<Node: Idx> UndirectedGraph<Node> for UndirectedCSRGraph<Node> {
    fn degree(&self, node: Node) -> Node {
        self.edges.degree(node)
    }

    fn neighbors(&self, node: Node) -> &[Node] {
        self.edges.neighbors(node)
    }
}

impl<Node: Idx> From<(EdgeList<Node>, CSROption)> for UndirectedCSRGraph<Node> {
    fn from((edge_list, csr_option): (EdgeList<Node>, CSROption)) -> Self {
        let node_count = edge_list.max_node_id() + Node::new(1);
        let edges = CSR::from((&edge_list, node_count, Direction::Undirected, csr_option));

        UndirectedCSRGraph::new(edges)
    }
}

pub struct NodeLabeledCSRGraph<G> {
    graph: G,
    label_index: Box<[usize]>,
    label_index_offsets: Box<[usize]>,
    max_degree: usize,
    max_label: usize,
    max_label_frequency: usize,
    label_frequency: HashMap<usize, usize>,
    neighbor_label_frequencies: Option<Box<[HashMap<usize, usize>]>>,
}

impl<Node: Idx, G: Graph<Node>> Graph<Node> for NodeLabeledCSRGraph<G> {
    #[inline]
    fn node_count(&self) -> Node {
        self.graph.node_count()
    }

    #[inline]
    fn edge_count(&self) -> Node {
        self.graph.edge_count()
    }
}

impl<Node: Idx, G: DirectedGraph<Node>> DirectedGraph<Node> for NodeLabeledCSRGraph<G> {
    fn out_degree(&self, node: Node) -> Node {
        self.graph.out_degree(node)
    }

    fn out_neighbors(&self, node: Node) -> &[Node] {
        self.graph.out_neighbors(node)
    }

    fn in_degree(&self, node: Node) -> Node {
        self.graph.in_degree(node)
    }

    fn in_neighbors(&self, node: Node) -> &[Node] {
        self.graph.in_neighbors(node)
    }
}

impl<Node: Idx, G: UndirectedGraph<Node>> UndirectedGraph<Node> for NodeLabeledCSRGraph<G> {
    fn degree(&self, node: Node) -> Node {
        self.graph.degree(node)
    }

    fn neighbors(&self, node: Node) -> &[Node] {
        self.graph.neighbors(node)
    }
}

impl<Node: Idx, G: From<(EdgeList<Node>, CSROption)>> From<(DotGraph<Node>, CSROption)>
    for NodeLabeledCSRGraph<G>
{
    fn from(_: (DotGraph<Node>, CSROption)) -> Self {
        todo!()
    }
}

fn prefix_sum<Node: AtomicIdx>(degrees: Vec<Node>) -> Vec<Node> {
    let mut last = degrees.last().unwrap().copied();
    let mut sums = degrees
        .into_iter()
        .scan(Node::zero(), |total, degree| {
            let value = total.copied();
            total.add(degree);
            Some(value)
        })
        .collect::<Vec<_>>();

    last.add_ref(sums.last().unwrap());
    sums.push(last);

    sums
}

fn sort_targets<Node: Idx>(offsets: &[Node], targets: &mut [Node]) {
    let node_count = offsets.len() - 1;
    let mut target_chunks = Vec::with_capacity(node_count);
    let mut tail = targets;
    let mut prev_offset = offsets[0];

    for &offset in &offsets[1..node_count] {
        let (list, remainder) = tail.split_at_mut((offset - prev_offset).index());
        target_chunks.push(list);
        tail = remainder;
        prev_offset = offset;
    }

    // do the actual sorting of individual target lists
    target_chunks
        .par_iter_mut()
        .for_each(|list| list.sort_unstable());
}

pub(crate) fn node_map_partition<Node: Idx, F>(
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
            partitions.push(partition_start..Node::new(i));
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
        assert_eq!(partitions[0], 0..9);
    }

    #[test]
    fn node_map_2_partitions() {
        let partitions = node_map_partition(|x| x % 2_usize, 10, 4);
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0], 0..7);
        assert_eq!(partitions[1], 8..9);
    }

    #[test]
    fn node_map_6_partitions() {
        let partitions = node_map_partition(|x| x as usize, 10, 6);
        assert_eq!(partitions.len(), 6);
        assert_eq!(partitions[0], 0..3);
        assert_eq!(partitions[1], 4..5);
        assert_eq!(partitions[2], 6..6);
        assert_eq!(partitions[3], 7..7);
        assert_eq!(partitions[4], 8..8);
        assert_eq!(partitions[5], 9..9);
    }
}

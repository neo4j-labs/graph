use log::info;
use std::{
    mem::{transmute, MaybeUninit},
    sync::atomic::Ordering::Acquire,
    time::Instant,
};

use rayon::prelude::*;

use crate::{
    index::{AtomicIdx, Idx},
    input::{Direction, EdgeList},
    DirectedGraph, Graph, SharedMut, UndirectedGraph,
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
    pub(crate) fn new(offsets: Box<[Node]>, targets: Box<[Node]>) -> Self {
        Self { offsets, targets }
    }

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
    fn from((edge_list, node_count, direction, csr_option): CSRConfiguration<'_, Node>) -> Self {
        let mut start = Instant::now();

        let degrees = edge_list.degrees(node_count, direction);
        info!("degrees took {:?}", start.elapsed());
        start = Instant::now();

        let offsets = prefix_sum(degrees);
        info!("prefix_sum took {:?}", start.elapsed());
        start = Instant::now();

        let targets_len = offsets[node_count.index()].load(Acquire);

        let mut targets = Vec::<Node>::with_capacity(targets_len.index());
        let targets_ptr = SharedMut(targets.as_mut_ptr());

        // The following loop writes all targets into their correct position.
        // The offsets are a prefix sum of all degrees, which will produce
        // non-overlapping positions for all node values
        //
        // SAFETY:
        //   for any (s, t) tuple from the same edge_list we use the prefix_sum to find
        //   a unique position for the target value, so that we only write once into each
        //   position and every thread that might run will write into different positions.
        if matches!(direction, Direction::Outgoing | Direction::Undirected) {
            edge_list.par_iter().for_each(|(s, t)| {
                let offset = offsets[s.index()].get_and_increment(Acquire);

                unsafe {
                    targets_ptr.0.add(offset.index()).write(*t);
                }
            })
        }

        if matches!(direction, Direction::Incoming | Direction::Undirected) {
            edge_list.par_iter().for_each(|(s, t)| {
                let offset = offsets[t.index()].get_and_increment(Acquire);
                unsafe {
                    targets_ptr.0.add(offset.index()).write(*s);
                }
            })
        }

        unsafe {
            targets.set_len(targets_len.index());
        }

        info!("targets took {:?}", start.elapsed());
        start = Instant::now();

        let mut offsets = unsafe { transmute::<_, Vec<Node>>(offsets) };

        // the previous loop moves all offsets one index to the right
        // we need to correct this to have proper offsets
        offsets.rotate_right(1);
        offsets[0] = Node::zero();

        let (offsets, targets) = match csr_option {
            CSROption::Unsorted => (offsets, targets),
            CSROption::Sorted => {
                sort_targets(&offsets, &mut targets);
                info!("sort_targets took {:?}", start.elapsed());
                (offsets, targets)
            }
            CSROption::Deduplicated => {
                let offsets_targets = sort_and_deduplicate_targets(&offsets, &mut targets);
                info!("sort_and_deduplicate_targets took {:?}", start.elapsed());
                offsets_targets
            }
        };

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

impl<Node: Idx> From<CSR<Node>> for UndirectedCSRGraph<Node> {
    fn from(csr: CSR<Node>) -> Self {
        UndirectedCSRGraph::new(csr)
    }
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

fn prefix_sum<Node: AtomicIdx>(degrees: Vec<Node>) -> Vec<Node> {
    let mut last = degrees.last().unwrap().load(Acquire);
    let mut sums = degrees
        .into_iter()
        .scan(Node::Inner::zero(), |total, degree| {
            let value = *total;
            *total += degree.into_inner();
            Some(value.atomic())
        })
        .collect::<Vec<_>>();

    last += sums.last().unwrap().load(Acquire);
    sums.push(last.atomic());

    sums
}

pub(crate) fn prefix_sum_non_atomic<Node: Idx>(degrees: Vec<Node>) -> Vec<Node> {
    let mut last = *degrees.last().unwrap();
    let mut sums = degrees
        .into_iter()
        .scan(Node::zero(), |total, degree| {
            let value = *total;
            *total += degree;
            Some(value)
        })
        .collect::<Vec<_>>();
    last += *sums.last().unwrap();
    sums.push(last);
    sums
}

fn sort_targets<Node: Idx>(offsets: &[Node], targets: &mut [Node]) {
    to_mut_slices(offsets, targets)
        .par_iter_mut()
        .for_each(|list| list.sort_unstable());
}

fn sort_and_deduplicate_targets<Node: Idx>(
    offsets: &[Node],
    targets: &mut [Node],
) -> (Vec<Node>, Vec<Node>) {
    let node_count = offsets.len() - 1;

    let mut new_degrees = Vec::with_capacity(node_count);
    let mut target_slices = to_mut_slices(offsets, targets);

    target_slices
        .par_iter_mut()
        .enumerate()
        .map(|(node, slice)| {
            slice.sort_unstable();
            // deduplicate
            let (dedup, _) = slice.partition_dedup();
            let mut new_degree = dedup.len();
            // remove self loops .. there is at most once occurence of node inside dedup
            if let Ok(idx) = dedup.binary_search(&Node::new(node)) {
                dedup[idx..].rotate_left(1);
                new_degree -= 1;
            }
            Node::new(new_degree).atomic()
        })
        .collect_into_vec(&mut new_degrees);

    // for node in 0..node_count {
    //     let mut slice: &mut [Node] = &mut [];
    //     std::mem::swap(&mut target_slices[node], &mut slice);
    //     slice.sort_unstable();
    //     // sort
    //     let mut new_degree = 0;
    //     // deduplicate
    //     let (mut dedup, _) = slice.partition_dedup();
    //     if let Ok(idx) = dedup.binary_search(&Node::new(node)) {
    //         dedup[idx..].rotate_left(1);
    //         if let Some((_, tail)) = dedup.split_last_mut() {
    //             dedup = tail;
    //             new_degree = dedup.len();
    //         }
    //     }
    //     // remove self loops
    //     new_degrees[node] = new_degree;
    //     std::mem::swap(&mut target_slices[node], &mut slice);
    // }

    let new_offsets = unsafe { transmute::<_, Vec<Node>>(prefix_sum(new_degrees)) };
    assert_eq!(new_offsets.len(), node_count + 1);

    let edge_count = new_offsets[node_count].index();

    let mut new_targets: Vec<Node> = Vec::with_capacity(edge_count);

    let new_target_slices = to_mut_slices(&new_offsets, new_targets.spare_capacity_mut());

    for (old_slice, new_slice) in target_slices.into_iter().zip(new_target_slices.into_iter()) {
        MaybeUninit::write_slice(new_slice, &old_slice[..new_slice.len()]);
    }

    // SAFETY: We copied all (potentially shortened) target ids from the old target list to the new one.
    unsafe {
        new_targets.set_len(edge_count);
    }

    (new_offsets, new_targets)
}

fn to_mut_slices<'targets, Node: Idx, T>(
    offsets: &[Node],
    targets: &'targets mut [T],
) -> Vec<&'targets mut [T]> {
    let node_count = offsets.len() - 1;
    let mut target_slices = Vec::with_capacity(node_count);
    let mut tail = targets;
    let mut prev_offset = offsets[0];

    for &offset in &offsets[1..] {
        let (list, remainder) = tail.split_at_mut((offset - prev_offset).index());
        target_slices.push(list);
        tail = remainder;
        prev_offset = offset;
    }

    target_slices
}

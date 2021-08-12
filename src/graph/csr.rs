use byte_slice_cast::{AsByteSlice, AsMutByteSlice, ToByteSlice, ToMutByteSlice};
use log::info;
use std::{
    io::{Read, Write},
    mem::{transmute, MaybeUninit},
    sync::atomic::Ordering::Acquire,
    time::Instant,
};

use rayon::prelude::*;

use crate::{
    graph_ops::{DeserializeGraphOp, SerializeGraphOp},
    index::{AtomicIdx, Idx},
    input::{Direction, EdgeList},
    DirectedGraph, Error, Graph, SharedMut, UndirectedGraph,
};

/// Defines how the neighbor list of individual nodes are organized within the
/// CSR target array.
#[derive(Clone, Copy)]
pub enum CsrLayout {
    /// Neighbor lists are sorted and may contain duplicate target ids. This is
    /// the default representation.
    Sorted,
    /// Neighbor lists are not in any particular order.
    Unsorted,
    /// Neighbor lists are sorted and do not contain duplicate target ids.
    /// Self-loops, i.e., edges in the form of `(u, u)` are removed.
    Deduplicated,
}

impl Default for CsrLayout {
    fn default() -> Self {
        CsrLayout::Sorted
    }
}

/// A Compressed-Sparse-Row data structure to represent sparse graphs.
///
/// The data structure is composed of two arrays: `offsets` and `targets`. For a
/// graph with node count `n` and edge count `m`, `offsets` has exactly `n + 1`
/// and `targets` exactly `m` entries.
///
/// For a given node `u`, `offsets[u]` stores the start index of the neighbor
/// list of `u` in `targets`. The degree of `u`, i.e., the length of the
/// neighbor list is defined by `offsets[u + 1] - offsets[u]`. The neighbor list
/// of `u` is defined by the slice `&targets[offsets[u]..offsets[u + 1]]`.
pub struct Csr<Node: Idx> {
    offsets: Box<[Node]>,
    targets: Box<[Node]>,
}

impl<Node: Idx> Csr<Node> {
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

type CsrInput<'a, Node> = (&'a mut EdgeList<Node>, Node, Direction, CsrLayout);

impl<Node: Idx> From<CsrInput<'_, Node>> for Csr<Node> {
    fn from((edge_list, node_count, direction, csr_layout): CsrInput<'_, Node>) -> Self {
        let start = Instant::now();
        let degrees = edge_list.degrees(node_count, direction);
        info!("Computed degrees in {:?}", start.elapsed());

        let start = Instant::now();
        let offsets = prefix_sum_atomic(degrees);
        info!("Computed prefix sum in {:?}", start.elapsed());

        let start = Instant::now();
        let edge_count = offsets[node_count.index()].load(Acquire).index();
        let mut targets = Vec::<Node>::with_capacity(edge_count);
        let targets_ptr = SharedMut::new(targets.as_mut_ptr());

        // The following loop writes all targets into their correct position.
        // The offsets are a prefix sum of all degrees, which will produce
        // non-overlapping positions for all node values.
        //
        // SAFETY: for any (s, t) tuple from the same edge_list we use the
        // prefix_sum to find a unique position for the target value, so that we
        // only write once into each position and every thread that might run
        // will write into different positions.
        if matches!(direction, Direction::Outgoing | Direction::Undirected) {
            edge_list.par_iter().for_each(|(s, t)| {
                let offset = offsets[s.index()].get_and_increment(Acquire);

                unsafe {
                    targets_ptr.add(offset.index()).write(*t);
                }
            })
        }

        if matches!(direction, Direction::Incoming | Direction::Undirected) {
            edge_list.par_iter().for_each(|(s, t)| {
                let offset = offsets[t.index()].get_and_increment(Acquire);
                unsafe {
                    targets_ptr.add(offset.index()).write(*s);
                }
            })
        }

        // SAFETY: The previous loops iterated the input edge list once (twice
        // for undirected) and inserted one node id for each edge. The
        // `edge_count` is defined by the highest offset value.
        unsafe {
            targets.set_len(edge_count);
        }
        info!("Computed target array in {:?}", start.elapsed());

        let start = Instant::now();
        // SAFETY: Node and Node::Atomic have the same memory layout
        let mut offsets = unsafe { transmute::<_, Vec<Node>>(offsets) };

        // Each insert into the target array in the previous loops incremented
        // the offset for the corresponding node by one. As a consequence the
        // offset values are shifted one index to the right. We need to correct
        // this in order to get correct offsets.
        offsets.rotate_right(1);
        offsets[0] = Node::zero();
        info!("Finalized offset array in {:?}", start.elapsed());

        let (offsets, targets) = match csr_layout {
            CsrLayout::Unsorted => (offsets, targets),
            CsrLayout::Sorted => {
                let start = Instant::now();
                sort_targets(&offsets, &mut targets);
                info!("Sorted targets in {:?}", start.elapsed());
                (offsets, targets)
            }
            CsrLayout::Deduplicated => {
                let start = Instant::now();
                let offsets_targets = sort_and_deduplicate_targets(&offsets, &mut targets);
                info!("Sorted and deduplicated targets in {:?}", start.elapsed());
                offsets_targets
            }
        };

        Csr {
            offsets: offsets.into_boxed_slice(),
            targets: targets.into_boxed_slice(),
        }
    }
}

impl<Node: Idx + ToByteSlice> Csr<Node> {
    fn serialize<W: Write>(&self, output: &mut W) -> Result<(), Error> {
        let node_count = self.node_count();
        let edge_count = self.edge_count();

        let meta = [node_count, edge_count];
        output.write_all(meta.as_byte_slice())?;
        output.write_all(self.offsets.as_byte_slice())?;
        output.write_all(self.targets.as_byte_slice())?;

        Ok(())
    }
}

impl<Node: Idx + ToMutByteSlice> Csr<Node> {
    fn deserialize<R: Read>(read: &mut R) -> Result<Csr<Node>, Error> {
        let mut meta = [Node::zero(); 2];
        read.read_exact(meta.as_mut_byte_slice())?;

        let [node_count, edge_count] = meta;

        let offsets = Box::<[Node]>::new_uninit_slice(node_count.index() + 1);
        let mut offsets = unsafe { offsets.assume_init() };
        read.read_exact(offsets.as_mut_byte_slice())?;

        let targets = Box::<[Node]>::new_uninit_slice(edge_count.index());
        let mut targets = unsafe { targets.assume_init() };
        read.read_exact(targets.as_mut_byte_slice())?;

        Ok(Csr::new(offsets, targets))
    }
}

pub struct DirectedCsrGraph<Node: Idx> {
    node_count: Node,
    edge_count: Node,
    out_edges: Csr<Node>,
    in_edges: Csr<Node>,
}

impl<Node: Idx> DirectedCsrGraph<Node> {
    pub fn new(out_edges: Csr<Node>, in_edges: Csr<Node>) -> Self {
        let node_count = out_edges.node_count();
        let edge_count = out_edges.edge_count();

        info!(
            "Created directed graph (node_count = {:?}, edge_count = {:?})",
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

impl<Node: Idx> Graph<Node> for DirectedCsrGraph<Node> {
    fn node_count(&self) -> Node {
        self.node_count
    }

    fn edge_count(&self) -> Node {
        self.edge_count
    }
}

impl<Node: Idx> DirectedGraph<Node> for DirectedCsrGraph<Node> {
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

impl<Node: Idx> From<(EdgeList<Node>, CsrLayout)> for DirectedCsrGraph<Node> {
    fn from((mut edge_list, csr_option): (EdgeList<Node>, CsrLayout)) -> Self {
        info!("Creating directed graph");
        let node_count = edge_list.max_node_id() + Node::new(1);

        let start = Instant::now();
        let out_edges = Csr::from((&mut edge_list, node_count, Direction::Outgoing, csr_option));
        info!("Created outgoing csr in {:?}.", start.elapsed());

        let start = Instant::now();
        let in_edges = Csr::from((&mut edge_list, node_count, Direction::Incoming, csr_option));
        info!("Created incoming csr in {:?}.", start.elapsed());

        DirectedCsrGraph::new(out_edges, in_edges)
    }
}

impl<W: Write, Node: Idx + ToByteSlice> SerializeGraphOp<W> for DirectedCsrGraph<Node> {
    fn serialize(&self, mut output: W) -> Result<(), Error> {
        let DirectedCsrGraph {
            node_count: _,
            edge_count: _,
            out_edges,
            in_edges,
        } = self;

        out_edges.serialize(&mut output)?;
        in_edges.serialize(&mut output)?;

        Ok(())
    }
}

impl<R: Read, Node: Idx + ToMutByteSlice> DeserializeGraphOp<R, Self> for DirectedCsrGraph<Node> {
    fn deserialize(mut read: R) -> Result<Self, Error> {
        let out_edges: Csr<Node> = Csr::deserialize(&mut read)?;
        let in_edges: Csr<Node> = Csr::deserialize(&mut read)?;
        Ok(DirectedCsrGraph::new(out_edges, in_edges))
    }
}

pub struct UndirectedCsrGraph<Node: Idx> {
    node_count: Node,
    edge_count: Node,
    edges: Csr<Node>,
}

impl<Node: Idx> From<Csr<Node>> for UndirectedCsrGraph<Node> {
    fn from(csr: Csr<Node>) -> Self {
        UndirectedCsrGraph::new(csr)
    }
}

impl<Node: Idx> UndirectedCsrGraph<Node> {
    pub fn new(edges: Csr<Node>) -> Self {
        let node_count = edges.node_count();
        let edge_count = edges.edge_count() / Node::new(2);

        info!(
            "Created undirected graph (node_count = {:?}, edge_count = {:?})",
            node_count, edge_count
        );

        Self {
            node_count,
            edge_count,
            edges,
        }
    }
}

impl<Node: Idx> Graph<Node> for UndirectedCsrGraph<Node> {
    fn node_count(&self) -> Node {
        self.node_count
    }

    fn edge_count(&self) -> Node {
        self.edge_count
    }
}

impl<Node: Idx> UndirectedGraph<Node> for UndirectedCsrGraph<Node> {
    fn degree(&self, node: Node) -> Node {
        self.edges.degree(node)
    }

    fn neighbors(&self, node: Node) -> &[Node] {
        self.edges.neighbors(node)
    }
}

impl<Node: Idx> From<(EdgeList<Node>, CsrLayout)> for UndirectedCsrGraph<Node> {
    fn from((mut edge_list, csr_option): (EdgeList<Node>, CsrLayout)) -> Self {
        info!("Creating undirected graph");
        let node_count = edge_list.max_node_id() + Node::new(1);

        let start = Instant::now();
        let edges = Csr::from((
            &mut edge_list,
            node_count,
            Direction::Undirected,
            csr_option,
        ));
        info!("Created csr in {:?}.", start.elapsed());

        UndirectedCsrGraph::new(edges)
    }
}

fn prefix_sum_atomic<Node: AtomicIdx>(degrees: Vec<Node>) -> Vec<Node> {
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

pub(crate) fn prefix_sum<Node: Idx>(degrees: Vec<Node>) -> Vec<Node> {
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
            Node::new(new_degree)
        })
        .collect_into_vec(&mut new_degrees);

    let new_offsets = prefix_sum(new_degrees);
    debug_assert_eq!(new_offsets.len(), node_count + 1);

    let edge_count = new_offsets[node_count].index();
    let mut new_targets: Vec<Node> = Vec::with_capacity(edge_count);
    let new_target_slices = to_mut_slices(&new_offsets, new_targets.spare_capacity_mut());

    target_slices
        .into_par_iter()
        .zip(new_target_slices.into_par_iter())
        .for_each(|(old_slice, new_slice)| {
            MaybeUninit::write_slice(new_slice, &old_slice[..new_slice.len()]);
        });

    // SAFETY: We copied all (potentially shortened) target ids from the old
    // target list to the new one.
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    use super::*;

    #[test]
    fn to_mut_slices_test() {
        let offsets = &[0, 2, 5, 5, 8];
        let targets = &mut [0, 1, 2, 3, 4, 5, 6, 7];
        let slices = to_mut_slices::<usize, usize>(offsets, targets);

        assert_eq!(
            slices,
            vec![vec![0, 1], vec![2, 3, 4], vec![], vec![5, 6, 7]]
        );
    }

    #[test]
    fn sort_targets_test() {
        let offsets = &[0, 2, 5, 5, 8];
        let mut targets = vec![1, 0, 4, 2, 3, 5, 6, 7];
        sort_targets::<usize>(offsets, &mut targets);

        assert_eq!(targets, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn sort_and_deduplicate_targets_test() {
        let offsets = &[0, 3, 7, 7, 10];
        // 0: [1, 1, 0]    => [1] (removed duplicate and self loop)
        // 1: [4, 2, 3, 2] => [2, 3, 4] (removed duplicate)
        let mut targets = vec![1, 1, 0, 4, 2, 3, 2, 5, 6, 7];
        let (offsets, targets) = sort_and_deduplicate_targets::<usize>(offsets, &mut targets);

        assert_eq!(offsets, vec![0, 1, 4, 4, 7]);
        assert_eq!(targets, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn prefix_sum_test() {
        let degrees = vec![42, 0, 1337, 4, 2, 0];
        let prefix_sum = prefix_sum::<usize>(degrees);

        assert_eq!(prefix_sum, vec![0, 42, 42, 1379, 1383, 1385, 1385]);
    }

    #[test]
    fn prefix_sum_atomic_test() {
        let degrees = vec![42, 0, 1337, 4, 2, 0]
            .into_iter()
            .map(AtomicUsize::new)
            .collect::<Vec<_>>();

        let prefix_sum = prefix_sum_atomic(degrees)
            .into_iter()
            .map(|n| n.load(SeqCst))
            .collect::<Vec<_>>();

        assert_eq!(prefix_sum, vec![0, 42, 42, 1379, 1383, 1385, 1385]);
    }
}

use std::{
    collections::HashMap,
    intrinsics::transmute,
    sync::atomic::{AtomicU32, AtomicUsize, Ordering},
    time::Instant,
};

use rayon::{
    iter::{
        IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator,
        IntoParallelRefMutIterator, ParallelIterator,
    },
    slice::ParallelSliceMut,
};

use crate::{
    input::{Direction, DotGraph, EdgeList},
    AtomicNode, DirectedGraph, Graph, Node, UndirectedGraph,
};

pub struct CSR {
    offsets: Box<[Node]>,
    targets: Box<[Node]>,
}

impl CSR {
    #[inline]
    fn node_count(&self) -> Node {
        (self.offsets.len() - 1) as Node
    }

    #[inline]
    fn edge_count(&self) -> Node {
        self.targets.len() as Node
    }

    #[inline]
    fn degree(&self, node: Node) -> Node {
        (self.offsets[node as usize + 1] - self.offsets[node as usize]) as Node
    }

    #[inline]
    fn neighbors(&self, node: Node) -> &[Node] {
        let from = self.offsets[node as usize] as usize;
        let to = self.offsets[node as usize + 1] as usize;
        &self.targets[from..to]
    }
}

impl From<(&EdgeList, Node, Direction)> for CSR {
    fn from((edge_list, node_count, direction): (&EdgeList, Node, Direction)) -> Self {
        let mut start = Instant::now();

        println!("Start: degrees()");
        let degrees = edge_list.degrees(node_count, direction);
        println!("Finish: degrees() took {} ms", start.elapsed().as_millis());
        start = Instant::now();

        println!("Start: prefix_sum()");
        let offsets = into_prefix_sum(degrees);
        println!(
            "Finish: prefix_sum() took {} ms",
            start.elapsed().as_millis()
        );
        start = Instant::now();

        let targets_len = offsets[node_count as usize].load(Ordering::SeqCst);
        let mut targets = Vec::with_capacity(targets_len as usize);
        targets.resize_with(targets_len as usize, || AtomicNode::new(0));

        // vec![0_usize; offsets[node_count].load(Ordering::SeqCst)];

        // let targets = unsafe { transmute::<_, Vec<AtomicUsize>>(targets) };
        // let offsets = unsafe { transmute::<_, Vec<AtomicUsize>>(offsets) };

        println!("Start: targets");
        match direction {
            Direction::Outgoing => edge_list.par_iter().for_each(|(s, t)| {
                targets[offsets[*s as usize].fetch_add(1, Ordering::SeqCst) as usize]
                    .store(*t, Ordering::SeqCst);
            }),
            Direction::Incoming => edge_list.par_iter().for_each(|(s, t)| {
                targets[offsets[*t as usize].fetch_add(1, Ordering::SeqCst) as usize]
                    .store(*s, Ordering::SeqCst);
            }),
            Direction::Undirected => edge_list.par_iter().for_each(|(s, t)| {
                targets[offsets[*s as usize].fetch_add(1, Ordering::SeqCst) as usize]
                    .store(*t, Ordering::SeqCst);
                targets[offsets[*t as usize].fetch_add(1, Ordering::SeqCst) as usize]
                    .store(*s, Ordering::SeqCst);
            }),
        }
        println!("Finish: targets took {} ms", start.elapsed().as_millis());
        start = Instant::now();

        let mut offsets = unsafe { transmute::<_, Vec<Node>>(offsets) };
        let mut targets = unsafe { transmute::<_, Vec<Node>>(targets) };

        // the previous loop moves all offsets one index to the right
        // we need to correct this to have proper offsets
        offsets.pop();
        offsets.insert(0, 0);

        println!("Start: sort_targets()");
        sort_targets(&offsets, &mut targets);
        println!(
            "Finish: sort_targets() took {} ms",
            start.elapsed().as_millis()
        );

        CSR {
            offsets: offsets.into_boxed_slice(),
            targets: targets.into_boxed_slice(),
        }
    }
}

trait NodeLike
where
    Self: Sized,
{
    fn zero() -> Self;

    fn copied(&self) -> Self;

    fn add(&mut self, other: Self);

    fn add_ref(&mut self, other: &Self);
}

impl NodeLike for usize {
    #[inline]
    fn zero() -> Self {
        0
    }

    #[inline]
    fn copied(&self) -> Self {
        *self
    }

    #[inline]
    fn add(&mut self, other: Self) {
        *self += other;
    }

    #[inline]
    fn add_ref(&mut self, other: &Self) {
        *self += *other;
    }
}

impl NodeLike for AtomicUsize {
    #[inline]
    fn zero() -> Self {
        AtomicUsize::new(0)
    }

    #[inline]
    fn copied(&self) -> Self {
        AtomicUsize::new(self.load(Ordering::SeqCst))
    }

    #[inline]
    fn add(&mut self, other: Self) {
        *self.get_mut() += other.into_inner();
    }

    #[inline]
    fn add_ref(&mut self, other: &Self) {
        *self.get_mut() += other.load(Ordering::SeqCst);
    }
}

impl NodeLike for u32 {
    #[inline]
    fn zero() -> Self {
        0
    }

    #[inline]
    fn copied(&self) -> Self {
        *self
    }

    #[inline]
    fn add(&mut self, other: Self) {
        *self += other;
    }

    #[inline]
    fn add_ref(&mut self, other: &Self) {
        *self += *other;
    }
}

impl NodeLike for AtomicU32 {
    #[inline]
    fn zero() -> Self {
        AtomicU32::new(0)
    }

    #[inline]
    fn copied(&self) -> Self {
        AtomicU32::new(self.load(Ordering::SeqCst))
    }

    #[inline]
    fn add(&mut self, other: Self) {
        *self.get_mut() += other.into_inner();
    }

    #[inline]
    fn add_ref(&mut self, other: &Self) {
        *self.get_mut() += other.load(Ordering::SeqCst);
    }
}

fn prefix_sum<T: NodeLike>(degrees: &[T]) -> Vec<T> {
    let mut sums = Vec::with_capacity(degrees.len() + 1);
    sums.resize_with(degrees.len() + 1, T::zero);
    let mut total = T::zero();

    for (i, degree) in degrees.iter().enumerate() {
        sums[i] = total.copied();
        total.add_ref(degree);
    }

    sums[degrees.len()] = total;

    sums
}

fn into_prefix_sum<T: NodeLike>(degrees: Vec<T>) -> Vec<T> {
    let mut last = degrees.last().unwrap().copied();
    let mut sums = degrees
        .into_iter()
        .scan(T::zero(), |total, degree| {
            let value = total.copied();
            total.add(degree);
            Some(value)
        })
        .collect::<Vec<_>>();

    last.add_ref(sums.last().unwrap());
    sums.push(last);

    sums
}

fn sort_targets(offsets: &[Node], targets: &mut [Node]) {
    let node_count = offsets.len() - 1;
    let mut target_chunks = Vec::with_capacity(node_count);
    let mut tail = targets;
    let mut prev_offset = offsets[0];

    for &offset in &offsets[1..node_count] {
        let (list, remainder) = tail.split_at_mut((offset - prev_offset) as usize);
        target_chunks.push(list);
        tail = remainder;
        prev_offset = offset;
    }

    // do the actual sorting of individual target lists
    target_chunks
        .par_iter_mut()
        .for_each(|list| list.sort_unstable());
}

pub struct DirectedCSRGraph {
    node_count: Node,
    edge_count: Node,
    out_edges: CSR,
    in_edges: CSR,
}

impl DirectedCSRGraph {
    pub fn new(out_edges: CSR, in_edges: CSR) -> Self {
        Self {
            node_count: out_edges.node_count(),
            edge_count: out_edges.edge_count(),
            out_edges,
            in_edges,
        }
    }
}

impl Graph for DirectedCSRGraph {
    fn node_count(&self) -> Node {
        self.node_count
    }

    fn edge_count(&self) -> Node {
        self.edge_count
    }
}

impl DirectedGraph for DirectedCSRGraph {
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

impl From<EdgeList> for DirectedCSRGraph {
    fn from(edge_list: EdgeList) -> Self {
        let node_count = edge_list.max_node_id() + 1;
        let out_edges = CSR::from((&edge_list, node_count, Direction::Outgoing));
        let in_edges = CSR::from((&edge_list, node_count, Direction::Incoming));

        DirectedCSRGraph::new(out_edges, in_edges)
    }
}

pub struct UndirectedCSRGraph {
    node_count: Node,
    edge_count: Node,
    edges: CSR,
}

impl UndirectedCSRGraph {
    pub fn new(edges: CSR) -> Self {
        Self {
            node_count: edges.node_count(),
            edge_count: edges.edge_count() / 2,
            edges,
        }
    }

    pub fn relabel_by_degrees(self) -> Self {
        let node_count = self.node_count();

        let mut degree_node_pairs = Vec::with_capacity(node_count);

        (0..node_count)
            .into_par_iter()
            .map(|node_id| (self.degree(node_id), node_id))
            .collect_into_vec(&mut degree_node_pairs);

        // sort node-degree pairs descending by degree
        degree_node_pairs.par_sort_unstable_by(|left, right| left.cmp(right).reverse());

        let mut degrees = Vec::with_capacity(node_count);
        degrees.resize_with(node_count, || AtomicUsize::new(0));

        let mut new_ids = Vec::with_capacity(node_count);
        new_ids.resize_with(node_count, || AtomicUsize::new(0));

        (0..node_count).into_par_iter().for_each(|n| {
            let (degree, node) = degree_node_pairs[n];
            degrees[n].store(degree, Ordering::SeqCst);
            new_ids[node].store(n, Ordering::SeqCst);
        });

        let degrees = unsafe { transmute::<_, Vec<usize>>(degrees) };
        let new_ids = unsafe { transmute::<_, Vec<usize>>(new_ids) };

        let offsets = prefix_sum(&degrees);
        let offsets = unsafe { transmute::<_, Vec<AtomicUsize>>(offsets) };

        let targets = vec![0_usize; offsets[node_count].load(Ordering::SeqCst)];
        let targets = unsafe { transmute::<_, Vec<AtomicUsize>>(targets) };

        (0..node_count).into_par_iter().for_each(|u| {
            let new_u = new_ids[u];

            for &v in self.neighbors(u) {
                let new_v = new_ids[v];
                let offset = offsets[new_u].fetch_add(1, Ordering::SeqCst);
                targets[offset].store(new_v, Ordering::SeqCst);
            }
        });

        let mut offsets = unsafe { transmute::<_, Vec<usize>>(offsets) };
        let mut targets = unsafe { transmute::<_, Vec<usize>>(targets) };

        // the previous loop moves all offsets one index to the right
        // we need to correct this to have proper offsets
        offsets.pop();
        offsets.insert(0, 0);

        sort_targets(&offsets, &mut targets);

        let csr = CSR {
            offsets: offsets.into_boxed_slice(),
            targets: targets.into_boxed_slice(),
        };

        UndirectedCSRGraph::new(csr)
    }
}

impl Graph for UndirectedCSRGraph {
    fn node_count(&self) -> Node {
        self.node_count
    }

    fn edge_count(&self) -> Node {
        self.edge_count
    }
}

impl UndirectedGraph for UndirectedCSRGraph {
    fn degree(&self, node: Node) -> Node {
        self.edges.degree(node)
    }

    fn neighbors(&self, node: Node) -> &[Node] {
        self.edges.neighbors(node)
    }
}

impl From<EdgeList> for UndirectedCSRGraph {
    fn from(edge_list: EdgeList) -> Self {
        let node_count = edge_list.max_node_id() + 1;
        let edges = CSR::from((&edge_list, node_count, Direction::Undirected));

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

impl<G: Graph> Graph for NodeLabeledCSRGraph<G> {
    #[inline]
    fn node_count(&self) -> Node {
        self.graph.node_count()
    }

    #[inline]
    fn edge_count(&self) -> Node {
        self.graph.edge_count()
    }
}

impl<G: DirectedGraph> DirectedGraph for NodeLabeledCSRGraph<G> {
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

impl<G: UndirectedGraph> UndirectedGraph for NodeLabeledCSRGraph<G> {
    fn degree(&self, node: Node) -> Node {
        self.graph.degree(node)
    }

    fn neighbors(&self, node: Node) -> &[Node] {
        self.graph.neighbors(node)
    }
}

impl<G: From<EdgeList>> From<DotGraph> for NodeLabeledCSRGraph<G> {
    fn from(_: DotGraph) -> Self {
        todo!()
    }
}

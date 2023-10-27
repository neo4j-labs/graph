use crate::EdgeMutation;
use crate::{
    index::Idx, prelude::Direction, prelude::Edges, prelude::NodeValues as NodeValuesTrait,
    CsrLayout, DirectedDegrees, DirectedNeighbors, DirectedNeighborsWithValues, Graph, Target,
    UndirectedDegrees, UndirectedNeighbors, UndirectedNeighborsWithValues,
};

use log::info;
use std::sync::Mutex;
use std::time::Instant;

use crate::graph::csr::NodeValues;
use rayon::prelude::*;

#[derive(Debug)]
pub struct AdjacencyList<NI, EV> {
    edges: Vec<Vec<Target<NI, EV>>>,
    layout: CsrLayout,
}

impl<NI: Idx, EV> AdjacencyList<NI, EV> {
    pub fn new(edges: Vec<Vec<Target<NI, EV>>>) -> Self {
        Self::with_layout(edges, CsrLayout::Unsorted)
    }

    pub fn with_layout(edges: Vec<Vec<Target<NI, EV>>>, layout: CsrLayout) -> Self {
        Self { edges, layout }
    }

    #[inline]
    pub(crate) fn node_count(&self) -> NI {
        NI::new(self.edges.len())
    }

    #[inline]
    pub(crate) fn edge_count(&self) -> NI
    where
        NI: Send + Sync,
        EV: Send + Sync,
    {
        NI::new(self.edges.par_iter().map(|v| v.len()).sum())
    }

    #[inline]
    pub(crate) fn degree(&self, node: NI) -> NI {
        NI::new(self.edges[node.index()].len())
    }
}

impl<NI: Idx, EV> AdjacencyList<NI, EV> {
    #[inline]
    pub(crate) fn targets_with_values(&self, node: NI) -> &[Target<NI, EV>] {
        self.edges[node.index()].as_slice()
    }
}

impl<NI: Idx> AdjacencyList<NI, ()> {
    #[inline]
    pub(crate) fn targets(&self, node: NI) -> &[NI] {
        assert_eq!(
            std::mem::size_of::<Target<NI, ()>>(),
            std::mem::size_of::<NI>()
        );
        assert_eq!(
            std::mem::align_of::<Target<NI, ()>>(),
            std::mem::align_of::<NI>()
        );

        let targets = self.edges[node.index()].as_slice();

        // SAFETY: The types Target<T, ()> and T are verified to have the same
        //         size and alignment.
        unsafe { std::slice::from_raw_parts(targets.as_ptr().cast(), targets.len()) }
    }

    #[inline]
    fn insert(&mut self, source: NI, target: Target<NI, ()>) {
        match self.layout {
            CsrLayout::Sorted => {
                let edges = &mut self.edges[source.index()];
                match edges.binary_search(&target) {
                    Ok(i) => edges.insert(i, target),
                    Err(i) => edges.insert(i, target),
                }
            }
            CsrLayout::Unsorted => self.edges[source.index()].push(target),
            CsrLayout::Deduplicated => {
                let edges = &mut self.edges[source.index()];
                match edges.binary_search(&target) {
                    Ok(_) => {}
                    Err(i) => edges.insert(i, target),
                }
            }
        };
    }
}

impl<NI, EV, E> From<(&'_ E, NI, Direction, CsrLayout)> for AdjacencyList<NI, EV>
where
    NI: Idx,
    EV: Copy + Send + Sync,
    E: Edges<NI = NI, EV = EV>,
{
    fn from(
        (edge_list, node_count, direction, csr_layout): (&'_ E, NI, Direction, CsrLayout),
    ) -> Self {
        let start = Instant::now();
        let mut thread_safe_vec = Vec::with_capacity(node_count.index());
        thread_safe_vec.resize_with(node_count.index(), || Mutex::new(Vec::new()));
        let thread_safe_vec = thread_safe_vec;

        edge_list.edges().for_each(|(s, t, v)| {
            if matches!(direction, Direction::Outgoing | Direction::Undirected) {
                thread_safe_vec[s.index()]
                    .lock()
                    .expect("Cannot lock node-local list")
                    .push(Target::new(t, v));
            }
            if matches!(direction, Direction::Incoming | Direction::Undirected) {
                thread_safe_vec[t.index()]
                    .lock()
                    .expect("Cannot lock node-local list")
                    .push(Target::new(s, v));
            }
        });
        info!("Grouped edge tuples in {:?}", start.elapsed());

        let start = Instant::now();
        let mut edges = Vec::with_capacity(node_count.index());
        thread_safe_vec
            .into_par_iter()
            .map(|list| {
                let mut list = list.into_inner().expect("Cannot move out of Mutex");

                match csr_layout {
                    CsrLayout::Sorted => list.sort_unstable_by_key(|t| t.target),
                    CsrLayout::Unsorted => {}
                    CsrLayout::Deduplicated => {
                        list.sort_unstable_by_key(|t| t.target);
                        list.dedup_by_key(|t| t.target)
                    }
                }

                list
            })
            .collect_into_vec(&mut edges);

        info!(
            "Applied list layout and finalized edge list in {:?}",
            start.elapsed()
        );

        AdjacencyList::with_layout(edges, csr_layout)
    }
}

pub struct DirectedALGraph<NI: Idx, NV = (), EV = ()> {
    node_values: NodeValues<NV>,
    al_out: AdjacencyList<NI, EV>,
    al_inc: AdjacencyList<NI, EV>,
}

impl<NI: Idx, NV, EV> DirectedALGraph<NI, NV, EV>
where
    NV: Send + Sync,
    EV: Send + Sync,
{
    pub fn new(
        node_values: NodeValues<NV>,
        al_out: AdjacencyList<NI, EV>,
        al_inc: AdjacencyList<NI, EV>,
    ) -> Self {
        let g = Self {
            node_values,
            al_out,
            al_inc,
        };

        info!(
            "Created directed graph (node_count = {:?}, edge_count = {:?})",
            g.node_count(),
            g.edge_count()
        );

        g
    }
}

impl<NI: Idx, NV, EV> Graph<NI> for DirectedALGraph<NI, NV, EV>
where
    NV: Send + Sync,
    EV: Send + Sync,
{
    delegate::delegate! {
        to self.al_out {
            fn node_count(&self) -> NI;
            fn edge_count(&self) -> NI;
        }
    }
}

impl<NI: Idx, NV, EV> NodeValuesTrait<NI, NV> for DirectedALGraph<NI, NV, EV> {
    fn node_value(&self, node: NI) -> &NV {
        &self.node_values.0[node.index()]
    }
}

impl<NI: Idx, NV, EV> DirectedDegrees<NI> for DirectedALGraph<NI, NV, EV> {
    fn out_degree(&self, node: NI) -> NI {
        self.al_out.degree(node)
    }

    fn in_degree(&self, node: NI) -> NI {
        self.al_inc.degree(node)
    }
}

impl<NI: Idx, NV> DirectedNeighbors<NI> for DirectedALGraph<NI, NV, ()> {
    type NeighborsIterator<'a> = std::slice::Iter<'a, NI> where NV: 'a;

    fn out_neighbors(&self, node: NI) -> Self::NeighborsIterator<'_> {
        self.al_out.targets(node).iter()
    }

    fn in_neighbors(&self, node: NI) -> Self::NeighborsIterator<'_> {
        self.al_inc.targets(node).iter()
    }
}

impl<NI: Idx, NV, EV> DirectedNeighborsWithValues<NI, EV> for DirectedALGraph<NI, NV, EV> {
    type NeighborsIterator<'a> = std::slice::Iter<'a, Target<NI, EV>> where NV: 'a, EV: 'a;

    fn out_neighbors_with_values(&self, node: NI) -> Self::NeighborsIterator<'_> {
        self.al_out.targets_with_values(node).iter()
    }

    fn in_neighbors_with_values(&self, node: NI) -> Self::NeighborsIterator<'_> {
        self.al_inc.targets_with_values(node).iter()
    }
}

impl<NI: Idx, NV> EdgeMutation<NI> for DirectedALGraph<NI, NV> {
    fn add_edge(&mut self, source: NI, target: NI) -> Result<(), crate::Error> {
        if source >= self.al_out.node_count() {
            return Err(crate::Error::MissingNode {
                node: format!("{}", source.index()),
            });
        }
        if target >= self.al_inc.node_count() {
            return Err(crate::Error::MissingNode {
                node: format!("{}", target.index()),
            });
        }

        self.al_out.insert(source, Target::new(target, ()));
        self.al_inc.insert(target, Target::new(source, ()));

        Ok(())
    }
}

impl<NI, EV, E> From<(E, CsrLayout)> for DirectedALGraph<NI, (), EV>
where
    NI: Idx,
    EV: Copy + Send + Sync,
    E: Edges<NI = NI, EV = EV>,
{
    fn from((edge_list, csr_layout): (E, CsrLayout)) -> Self {
        info!("Creating directed graph");
        let node_count = edge_list.max_node_id() + NI::new(1);
        let node_values = NodeValues::new(vec![(); node_count.index()]);

        let start = Instant::now();
        let al_out = AdjacencyList::from((&edge_list, node_count, Direction::Outgoing, csr_layout));
        info!("Created outgoing adjacency list in {:?}", start.elapsed());

        let start = Instant::now();
        let al_inc = AdjacencyList::from((&edge_list, node_count, Direction::Incoming, csr_layout));
        info!("Created incoming adjacency list in {:?}", start.elapsed());

        DirectedALGraph::new(node_values, al_out, al_inc)
    }
}

impl<NI, NV, EV, E> From<(NodeValues<NV>, E, CsrLayout)> for DirectedALGraph<NI, NV, EV>
where
    NI: Idx,
    NV: Send + Sync,
    EV: Copy + Send + Sync,
    E: Edges<NI = NI, EV = EV>,
{
    fn from((node_values, edge_list, csr_layout): (NodeValues<NV>, E, CsrLayout)) -> Self {
        info!("Creating directed graph");
        let node_count = edge_list.max_node_id() + NI::new(1);

        let start = Instant::now();
        let al_out = AdjacencyList::from((&edge_list, node_count, Direction::Outgoing, csr_layout));
        info!("Created outgoing adjacency list in {:?}", start.elapsed());

        let start = Instant::now();
        let al_inc = AdjacencyList::from((&edge_list, node_count, Direction::Incoming, csr_layout));
        info!("Created incoming adjacency list in {:?}", start.elapsed());

        DirectedALGraph::new(node_values, al_out, al_inc)
    }
}

pub struct UndirectedALGraph<NI: Idx, NV = (), EV = ()> {
    node_values: NodeValues<NV>,
    al: AdjacencyList<NI, EV>,
}

impl<NI: Idx, NV, EV> UndirectedALGraph<NI, NV, EV>
where
    NV: Send + Sync,
    EV: Send + Sync,
{
    pub fn new(node_values: NodeValues<NV>, al: AdjacencyList<NI, EV>) -> Self {
        let g = Self { node_values, al };

        info!(
            "Created undirected graph (node_count = {:?}, edge_count = {:?})",
            g.node_count(),
            g.edge_count()
        );

        g
    }
}

impl<NI: Idx, NV, EV> Graph<NI> for UndirectedALGraph<NI, NV, EV>
where
    NV: Send + Sync,
    EV: Send + Sync,
{
    fn node_count(&self) -> NI {
        self.al.node_count()
    }

    fn edge_count(&self) -> NI {
        self.al.edge_count() / NI::new(2)
    }
}

impl<NI: Idx, NV, EV> NodeValuesTrait<NI, NV> for UndirectedALGraph<NI, NV, EV> {
    fn node_value(&self, node: NI) -> &NV {
        &self.node_values.0[node.index()]
    }
}

impl<NI: Idx, NV, EV> UndirectedDegrees<NI> for UndirectedALGraph<NI, NV, EV> {
    fn degree(&self, node: NI) -> NI {
        self.al.degree(node)
    }
}

impl<NI: Idx, NV> UndirectedNeighbors<NI> for UndirectedALGraph<NI, NV, ()> {
    type NeighborsIterator<'a> = std::slice::Iter<'a, NI> where NV: 'a;

    fn neighbors(&self, node: NI) -> Self::NeighborsIterator<'_> {
        self.al.targets(node).iter()
    }
}

impl<NI: Idx, NV, EV> UndirectedNeighborsWithValues<NI, EV> for UndirectedALGraph<NI, NV, EV> {
    type NeighborsIterator<'a> = std::slice::Iter<'a, Target<NI, EV>> where NV: 'a, EV: 'a;

    fn neighbors_with_values(&self, node: NI) -> Self::NeighborsIterator<'_> {
        self.al.targets_with_values(node).iter()
    }
}

impl<NI, EV, E> From<(E, CsrLayout)> for UndirectedALGraph<NI, (), EV>
where
    NI: Idx,
    EV: Copy + Send + Sync,
    E: Edges<NI = NI, EV = EV>,
{
    fn from((edge_list, csr_layout): (E, CsrLayout)) -> Self {
        info!("Creating undirected graph");
        let node_count = edge_list.max_node_id() + NI::new(1);
        let node_values = NodeValues::new(vec![(); node_count.index()]);

        let start = Instant::now();
        let al = AdjacencyList::from((&edge_list, node_count, Direction::Undirected, csr_layout));
        info!("Created adjacency list in {:?}", start.elapsed());

        UndirectedALGraph::new(node_values, al)
    }
}

impl<NI, NV, EV, E> From<(NodeValues<NV>, E, CsrLayout)> for UndirectedALGraph<NI, NV, EV>
where
    NI: Idx,
    NV: Send + Sync,
    EV: Copy + Send + Sync,
    E: Edges<NI = NI, EV = EV>,
{
    fn from((node_values, edge_list, csr_layout): (NodeValues<NV>, E, CsrLayout)) -> Self {
        info!("Creating undirected graph");
        let node_count = edge_list.max_node_id() + NI::new(1);

        let start = Instant::now();
        let al = AdjacencyList::from((&edge_list, node_count, Direction::Undirected, csr_layout));
        info!("Created adjacency list in {:?}", start.elapsed());

        UndirectedALGraph::new(node_values, al)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::prelude::EdgeList;
    use crate::GraphBuilder;
    use tap::prelude::*;

    #[test]
    fn empty_list() {
        let list = AdjacencyList::<u32, u32>::new(vec![]);
        assert_eq!(list.node_count(), 0);
        assert_eq!(list.edge_count(), 0);
    }

    #[test]
    fn degree() {
        let list = AdjacencyList::<u32, u32>::new(vec![
            /* node 0 */ vec![Target::new(1, 42)],
            /* node 1 */ vec![Target::new(0, 1337)],
        ]);
        assert_eq!(list.node_count(), 2);
        assert_eq!(list.edge_count(), 2);
        assert_eq!(list.degree(0), 1);
        assert_eq!(list.degree(1), 1);
    }

    #[test]
    fn targets_with_values() {
        let list = AdjacencyList::<u32, u32>::new(vec![
            /* node 0 */ vec![Target::new(1, 42)],
            /* node 1 */ vec![Target::new(0, 1337)],
        ]);

        assert_eq!(list.targets_with_values(0), &[Target::new(1, 42)]);
        assert_eq!(list.targets_with_values(1), &[Target::new(0, 1337)]);
    }

    #[test]
    fn targets() {
        let list = AdjacencyList::<u32, ()>::new(vec![
            /* node 0 */ vec![Target::new(1, ())],
            /* node 1 */ vec![Target::new(0, ())],
        ]);

        assert_eq!(list.targets(0), &[1]);
        assert_eq!(list.targets(1), &[0]);
    }

    #[test]
    fn from_edges_outgoing() {
        let edges = vec![(0, 1, 42), (0, 2, 1337), (1, 0, 43), (2, 0, 1338)];
        let edges = EdgeList::new(edges);
        let list =
            AdjacencyList::<u32, u32>::from((&edges, 3, Direction::Outgoing, CsrLayout::Unsorted));

        assert_eq!(
            list.targets_with_values(0)
                .iter()
                .collect::<Vec<_>>()
                .tap_mut(|v| v.sort_by_key(|t| t.target)),
            &[&Target::new(1, 42), &Target::new(2, 1337)]
        );
        assert_eq!(list.targets_with_values(1), &[Target::new(0, 84)]);
        assert_eq!(list.targets_with_values(2), &[Target::new(0, 1337)]);
    }

    #[test]
    fn from_edges_incoming() {
        let edges = vec![(0, 1, 42), (0, 2, 1337), (1, 0, 43), (2, 0, 1338)];
        let edges = EdgeList::new(edges);
        let list =
            AdjacencyList::<u32, u32>::from((&edges, 3, Direction::Incoming, CsrLayout::Unsorted));

        assert_eq!(
            list.targets_with_values(0)
                .iter()
                .collect::<Vec<_>>()
                .tap_mut(|v| v.sort_by_key(|t| t.target)),
            &[&Target::new(1, 42), &Target::new(2, 1337)]
        );
        assert_eq!(list.targets_with_values(1), &[Target::new(0, 42)]);
        assert_eq!(list.targets_with_values(2), &[Target::new(0, 1337)]);
    }

    #[test]
    fn from_edges_undirected() {
        let edges = vec![(0, 1, 42), (0, 2, 1337), (1, 0, 43), (2, 0, 1338)];
        let edges = EdgeList::new(edges);
        let list = AdjacencyList::<u32, u32>::from((
            &edges,
            3,
            Direction::Undirected,
            CsrLayout::Unsorted,
        ));

        assert_eq!(
            list.targets_with_values(0)
                .iter()
                .collect::<Vec<_>>()
                .tap_mut(|v| v.sort_by_key(|t| t.target)),
            &[
                &Target::new(1, 42),
                &Target::new(1, 43),
                &Target::new(2, 1337),
                &Target::new(2, 1338)
            ]
        );
        assert_eq!(
            list.targets_with_values(1)
                .iter()
                .collect::<Vec<_>>()
                .tap_mut(|v| v.sort_by_key(|t| t.target)),
            &[&Target::new(0, 42), &Target::new(0, 43)]
        );
        assert_eq!(
            list.targets_with_values(2)
                .iter()
                .collect::<Vec<_>>()
                .tap_mut(|v| v.sort_by_key(|t| t.target)),
            &[&Target::new(0, 1337), &Target::new(0, 1338)]
        );
    }

    #[test]
    fn from_edges_sorted() {
        let edges = vec![
            (0, 1, ()),
            (0, 3, ()),
            (0, 2, ()),
            (1, 3, ()),
            (1, 2, ()),
            (1, 0, ()),
        ];
        let edges = EdgeList::new(edges);
        let list =
            AdjacencyList::<u32, ()>::from((&edges, 3, Direction::Outgoing, CsrLayout::Sorted));

        assert_eq!(list.targets(0), &[1, 2, 3]);
        assert_eq!(list.targets(1), &[0, 2, 3]);
    }

    #[test]
    fn from_edges_deduplicated() {
        let edges = vec![
            (0, 1, ()),
            (0, 3, ()),
            (0, 3, ()),
            (0, 2, ()),
            (0, 2, ()),
            (1, 3, ()),
            (1, 3, ()),
            (1, 2, ()),
            (1, 2, ()),
            (1, 0, ()),
            (1, 0, ()),
        ];
        let edges = EdgeList::new(edges);
        let list = AdjacencyList::<u32, ()>::from((
            &edges,
            3,
            Direction::Outgoing,
            CsrLayout::Deduplicated,
        ));

        assert_eq!(list.targets(0), &[1, 2, 3]);
        assert_eq!(list.targets(1), &[0, 2, 3]);
    }

    #[test]
    fn directed_al_graph() {
        let g = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges([(0, 1), (0, 2), (1, 2)])
            .build::<DirectedALGraph<u32, ()>>();

        assert_eq!(g.node_count(), 3);
        assert_eq!(g.edge_count(), 3);
        assert_eq!(g.out_degree(0), 2);
        assert_eq!(g.out_neighbors(0).as_slice(), &[1, 2]);
        assert_eq!(g.in_degree(2), 2);
        assert_eq!(g.in_neighbors(2).as_slice(), &[0, 1]);
    }

    #[test]
    fn directed_al_graph_with_node_values() {
        let g = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges([(0, 1), (0, 2), (1, 2)])
            .node_values(vec!["foo", "bar", "baz"])
            .build::<DirectedALGraph<u32, &str>>();

        assert_eq!(g.node_value(0), &"foo");
        assert_eq!(g.node_value(1), &"bar");
        assert_eq!(g.node_value(2), &"baz");
    }

    #[test]
    fn directed_al_graph_add_edge_unsorted() {
        let mut g = GraphBuilder::new()
            .csr_layout(CsrLayout::Unsorted)
            .edges([(0, 2), (1, 2)])
            .build::<DirectedALGraph<u32>>();

        assert_eq!(g.out_neighbors(0).as_slice(), &[2]);
        g.add_edge(0, 1).expect("add edge failed");
        assert_eq!(g.out_neighbors(0).as_slice(), &[2, 1]);
        g.add_edge(0, 2).expect("add edge failed");
        assert_eq!(g.out_neighbors(0).as_slice(), &[2, 1, 2]);
    }

    #[test]
    fn directed_al_graph_add_edge_sorted() {
        let mut g = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges([(0, 2), (1, 2)])
            .build::<DirectedALGraph<u32>>();

        assert_eq!(g.out_neighbors(0).as_slice(), &[2]);
        g.add_edge(0, 1).expect("add edge failed");
        assert_eq!(g.out_neighbors(0).as_slice(), &[1, 2]);
        g.add_edge(0, 1).expect("add edge failed");
        assert_eq!(g.out_neighbors(0).as_slice(), &[1, 1, 2]);
    }

    #[test]
    fn directed_al_graph_add_edge_deduplicated() {
        let mut g = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .edges([(0, 2), (1, 2), (1, 3)])
            .build::<DirectedALGraph<u32>>();

        assert_eq!(g.out_neighbors(0).as_slice(), &[2]);
        g.add_edge(0, 1).expect("add edge failed");
        assert_eq!(g.out_neighbors(0).as_slice(), &[1, 2]);
        g.add_edge(0, 1).expect("add edge failed");
        assert_eq!(g.out_neighbors(0).as_slice(), &[1, 2]);
        g.add_edge(0, 3).expect("add edge failed");
        assert_eq!(g.out_neighbors(0).as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn directed_al_graph_add_edge_missing_node() {
        let mut g = GraphBuilder::new()
            .csr_layout(CsrLayout::Unsorted)
            .edges([(0, 2), (1, 2)])
            .build::<DirectedALGraph<u32>>();

        let err = g.add_edge(0, 3).unwrap_err();

        assert!(matches!(err, crate::Error::MissingNode { node } if node == "3" ));
    }

    #[test]
    fn undirected_al_graph() {
        let g = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges([(0, 1), (0, 2), (1, 2)])
            .build::<UndirectedALGraph<u32, ()>>();

        assert_eq!(g.node_count(), 3);
        assert_eq!(g.edge_count(), 3);
        assert_eq!(g.degree(0), 2);
        assert_eq!(g.degree(2), 2);
        assert_eq!(g.neighbors(0).as_slice(), &[1, 2]);
        assert_eq!(g.neighbors(2).as_slice(), &[0, 1]);
    }

    #[test]
    fn undirected_al_graph_cycle() {
        let g = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges([(0, 1), (1, 0)])
            .build::<UndirectedALGraph<u32, ()>>();

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 2);
        assert_eq!(g.degree(0), 2);
        assert_eq!(g.degree(1), 2);
        assert_eq!(g.neighbors(0).as_slice(), &[1, 1]);
        assert_eq!(g.neighbors(1).as_slice(), &[0, 0]);
    }

    #[test]
    fn undirected_al_graph_with_node_values() {
        let g = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges([(0, 1), (0, 2), (1, 2)])
            .node_values(vec!["foo", "bar", "baz"])
            .build::<UndirectedALGraph<u32, &str>>();

        assert_eq!(g.node_value(0), &"foo");
        assert_eq!(g.node_value(1), &"bar");
        assert_eq!(g.node_value(2), &"baz");
    }
}

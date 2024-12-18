use std::{collections::VecDeque, hash::Hash};

use bitvec::prelude::*;

use crate::prelude::*;

pub fn bfs_directed<NI, G>(
    graph: &G,
    node_ids: impl IntoIterator<Item = NI>,
    direction: Direction,
) -> DirectedBreadthFirst<'_, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    DirectedBreadthFirst::new(graph, node_ids, direction)
}

pub struct DirectedBreadthFirst<'a, G, NI> {
    graph: &'a G,
    seen: BitVec<usize>,
    visited: BitVec<usize>,
    queue: VecDeque<NI>,
    direction: Direction,
}

impl<'a, G, NI> DirectedBreadthFirst<'a, G, NI>
where
    NI: Idx + Hash + std::fmt::Debug,
    G: Graph<NI> + DirectedNeighbors<NI> + Sync,
{
    pub fn new(graph: &'a G, node_ids: impl IntoIterator<Item = NI>, direction: Direction) -> Self {
        let bitvec = BitVec::repeat(false, graph.node_count().index());
        let visited = bitvec.clone();

        let mut seen = bitvec;
        let mut queue = VecDeque::new();
        Self::enqueue_into(&mut seen, &mut queue, node_ids);

        Self {
            graph,
            seen,
            visited,
            queue,
            direction,
        }
    }

    fn dequeue(&mut self) -> Option<NI> {
        loop {
            let node_id = self.queue.pop_front()?;
            if !self.visited.replace(node_id.index(), true) {
                return Some(node_id);
            }
        }
    }

    fn enqueue_into(
        seen: &mut BitVec<usize>,
        queue: &mut VecDeque<NI>,
        node_ids: impl IntoIterator<Item = NI>,
    ) {
        for node_id in node_ids {
            if !seen.replace(node_id.index(), true) {
                queue.push_back(node_id);
            }
        }
    }

    fn enqueue_out_neighbors_of(&mut self, node_id: NI) {
        let node_ids = self
            .graph
            .out_neighbors(node_id)
            .copied()
            .filter(|node_id| !self.visited[node_id.index()]);

        Self::enqueue_into(&mut self.seen, &mut self.queue, node_ids);
    }

    fn enqueue_in_neighbors_of(&mut self, node_id: NI) {
        let node_ids = self
            .graph
            .in_neighbors(node_id)
            .copied()
            .filter(|node_id| !self.visited[node_id.index()]);

        Self::enqueue_into(&mut self.seen, &mut self.queue, node_ids);
    }
}

impl<'a, G, NI> Iterator for DirectedBreadthFirst<'a, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedNeighbors<NI> + Sync,
{
    type Item = NI;

    fn next(&mut self) -> Option<Self::Item> {
        let node_id = self.dequeue()?;

        match self.direction {
            Direction::Outgoing => self.enqueue_out_neighbors_of(node_id),
            Direction::Incoming => self.enqueue_in_neighbors_of(node_id),
            Direction::Undirected => {
                self.enqueue_out_neighbors_of(node_id);
                self.enqueue_in_neighbors_of(node_id);
            }
        }

        Some(node_id)
    }
}

pub fn bfs_undirected<NI, G>(
    graph: &G,
    node_ids: impl IntoIterator<Item = NI>,
) -> UndirectedBreadthFirst<'_, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + UndirectedDegrees<NI> + UndirectedNeighbors<NI> + Sync,
{
    UndirectedBreadthFirst::new(graph, node_ids)
}

pub struct UndirectedBreadthFirst<'a, G, NI> {
    graph: &'a G,
    seen: BitVec<usize>,
    visited: BitVec<usize>,
    queue: VecDeque<NI>,
}

impl<'a, G, NI> UndirectedBreadthFirst<'a, G, NI>
where
    NI: Idx + Hash + std::fmt::Debug,
    G: Graph<NI> + UndirectedNeighbors<NI> + Sync,
{
    pub fn new(graph: &'a G, node_ids: impl IntoIterator<Item = NI>) -> Self {
        let bitvec = BitVec::repeat(false, graph.node_count().index());
        let visited = bitvec.clone();

        let mut seen = bitvec;
        let mut queue = VecDeque::new();
        Self::enqueue_into(&mut seen, &mut queue, node_ids);

        Self {
            graph,
            seen,
            visited,
            queue,
        }
    }

    fn dequeue(&mut self) -> Option<NI> {
        loop {
            let node_id = self.queue.pop_front()?;

            if !self.visited.replace(node_id.index(), true) {
                return Some(node_id);
            }
        }
    }

    fn enqueue_into(
        seen: &mut BitVec<usize>,
        queue: &mut VecDeque<NI>,
        node_ids: impl IntoIterator<Item = NI>,
    ) {
        for node_id in node_ids {
            if !seen.replace(node_id.index(), true) {
                queue.push_back(node_id);
            }
        }
    }

    fn enqueue_neighbors_of(&mut self, node_id: NI) {
        let node_ids = self
            .graph
            .neighbors(node_id)
            .copied()
            .filter(|&node_id| !self.visited[node_id.index()]);

        Self::enqueue_into(&mut self.seen, &mut self.queue, node_ids);
    }
}

impl<'a, G, NI> Iterator for UndirectedBreadthFirst<'a, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + UndirectedNeighbors<NI> + Sync,
{
    type Item = NI;

    fn next(&mut self) -> Option<Self::Item> {
        let node_id = self.dequeue()?;

        self.enqueue_neighbors_of(node_id);

        Some(node_id)
    }
}

#[cfg(test)]
mod tests {
    use graph::prelude::{CsrLayout, GraphBuilder};

    use super::*;

    mod directed {
        use super::*;

        #[test]
        fn acyclic() {
            let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
                .csr_layout(CsrLayout::Deduplicated)
                .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 3), (2, 1), (3, 1)])
                .build();

            let actual: Vec<usize> = bfs_directed(&graph, [0], Direction::Outgoing).collect();
            let expected: Vec<usize> = vec![0, 1, 2, 3];

            assert_eq!(actual, expected);
        }

        #[test]
        fn cyclic() {
            let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
                .csr_layout(CsrLayout::Deduplicated)
                .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 1), (2, 1), (3, 1)])
                .build();

            let actual: Vec<usize> = bfs_directed(&graph, [0], Direction::Outgoing).collect();
            let expected: Vec<usize> = vec![0, 1, 2, 3];

            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn undirected() {
        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .csr_layout(CsrLayout::Deduplicated)
            .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 3), (2, 1), (3, 1)])
            .build();

        let actual: Vec<usize> = bfs_undirected(&graph, [0]).collect();
        let expected: Vec<usize> = vec![0, 1, 2, 3];

        assert_eq!(actual, expected);
    }
}

use std::{collections::VecDeque, hash::Hash};

use bitvec::prelude::*;

use crate::prelude::*;

pub fn depth_first_directed<NI, G>(
    graph: &G,
    node_id: NI,
    direction: Direction,
) -> DirectedDepthFirst<'_, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    DirectedDepthFirst::new(graph, node_id, direction)
}

pub struct DirectedDepthFirst<'a, G, NI> {
    graph: &'a G,
    visited: BitVec<usize>,
    stack: Vec<NI>,
    direction: Direction,
}

impl<'a, G, NI> DirectedDepthFirst<'a, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedNeighbors<NI> + Sync,
{
    pub fn new(graph: &'a G, node_id: NI, direction: Direction) -> Self {
        Self {
            graph,
            visited: BitVec::repeat(false, graph.node_count().index()),
            stack: Vec::from_iter([node_id]),
            direction,
        }
    }

    fn dequeue(&mut self) -> Option<NI> {
        loop {
            let node_id = self.stack.pop()?;

            if !self.visited.replace(node_id.index(), true) {
                return Some(node_id);
            }
        }
    }

    fn enqueue_out_neighbors(&mut self, node_id: NI) {
        let neighbors = self
            .graph
            .out_neighbors(node_id)
            .filter(|&node_id| !self.visited[node_id.index()]);
        self.stack.extend(neighbors);
    }

    fn enqueue_in_neighbors(&mut self, node_id: NI) {
        let neighbors = self
            .graph
            .in_neighbors(node_id)
            .filter(|&node_id| !self.visited[node_id.index()]);
        self.stack.extend(neighbors);
    }
}

impl<'a, G, NI> Iterator for DirectedDepthFirst<'a, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedNeighbors<NI> + Sync,
{
    type Item = NI;

    fn next(&mut self) -> Option<Self::Item> {
        let node_id = self.dequeue()?;

        match self.direction {
            Direction::Outgoing => self.enqueue_out_neighbors(node_id),
            Direction::Incoming => self.enqueue_in_neighbors(node_id),
            Direction::Undirected => {
                self.enqueue_out_neighbors(node_id);
                self.enqueue_in_neighbors(node_id);
            }
        }

        Some(node_id)
    }
}

pub fn depth_first_undirected<NI, G>(graph: &G, node_id: NI) -> UndirectedDepthFirst<'_, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + UndirectedDegrees<NI> + UndirectedNeighbors<NI> + Sync,
{
    UndirectedDepthFirst::new(graph, node_id)
}

pub struct UndirectedDepthFirst<'a, G, NI> {
    graph: &'a G,
    visited: BitVec<usize>,
    stack: Vec<NI>,
}

impl<'a, G, NI> UndirectedDepthFirst<'a, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + UndirectedNeighbors<NI> + Sync,
{
    pub fn new(graph: &'a G, node_id: NI) -> Self {
        Self {
            graph,
            visited: BitVec::repeat(false, graph.node_count().index()),
            stack: Vec::from_iter([node_id]),
        }
    }

    fn dequeue(&mut self) -> Option<NI> {
        loop {
            let node_id = self.stack.pop()?;

            if !self.visited.replace(node_id.index(), true) {
                return Some(node_id);
            }
        }
    }

    fn enqueue_neighbors(&mut self, node_id: NI) {
        let neighbors = self
            .graph
            .neighbors(node_id)
            .filter(|&node_id| !self.visited[node_id.index()]);
        self.stack.extend(neighbors);
    }
}

impl<'a, G, NI> Iterator for UndirectedDepthFirst<'a, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + UndirectedNeighbors<NI> + Sync,
{
    type Item = NI;

    fn next(&mut self) -> Option<Self::Item> {
        let node_id = self.dequeue()?;

        self.enqueue_neighbors(node_id);

        Some(node_id)
    }
}

pub fn bfs_directed<NI, G>(
    graph: &G,
    node_id: NI,
    direction: Direction,
) -> DirectedBreadthFirst<'_, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    DirectedBreadthFirst::new(graph, node_id, direction)
}

pub struct DirectedBreadthFirst<'a, G, NI> {
    graph: &'a G,
    visited: BitVec<usize>,
    queue: VecDeque<NI>,
    direction: Direction,
}

impl<'a, G, NI> DirectedBreadthFirst<'a, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedNeighbors<NI> + Sync,
{
    pub fn new(graph: &'a G, node_id: NI, direction: Direction) -> Self {
        Self {
            graph,
            visited: BitVec::repeat(false, graph.node_count().index()),
            queue: VecDeque::from_iter([node_id]),
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

    fn enqueue_out_neighbors(&mut self, node_id: NI) {
        let neighbors = self
            .graph
            .out_neighbors(node_id)
            .filter(|&node_id| !self.visited[node_id.index()]);
        self.queue.extend(neighbors);
    }

    fn enqueue_in_neighbors(&mut self, node_id: NI) {
        let neighbors = self
            .graph
            .in_neighbors(node_id)
            .filter(|&node_id| !self.visited[node_id.index()]);
        self.queue.extend(neighbors);
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
            Direction::Outgoing => self.enqueue_out_neighbors(node_id),
            Direction::Incoming => self.enqueue_in_neighbors(node_id),
            Direction::Undirected => {
                self.enqueue_out_neighbors(node_id);
                self.enqueue_in_neighbors(node_id);
            }
        }

        Some(node_id)
    }
}

pub fn bfs_undirected<NI, G>(graph: &G, node_id: NI) -> UndirectedBreadthFirst<'_, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + UndirectedDegrees<NI> + UndirectedNeighbors<NI> + Sync,
{
    UndirectedBreadthFirst::new(graph, node_id)
}

pub struct UndirectedBreadthFirst<'a, G, NI> {
    graph: &'a G,
    visited: BitVec<usize>,
    queue: VecDeque<NI>,
}

impl<'a, G, NI> UndirectedBreadthFirst<'a, G, NI>
where
    NI: Idx + Hash + std::fmt::Debug,
    G: Graph<NI> + UndirectedNeighbors<NI> + Sync,
{
    pub fn new(graph: &'a G, node_id: NI) -> Self {
        Self {
            graph,
            visited: BitVec::repeat(false, graph.node_count().index()),
            queue: VecDeque::from_iter([node_id]),
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

    fn enqueue_neighbors(&mut self, node_id: NI) {
        let neighbors = self
            .graph
            .neighbors(node_id)
            .filter(|&node_id| !self.visited[node_id.index()]);

        let neighbors: Vec<_> = neighbors.collect();

        self.queue.extend(neighbors);
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

        self.enqueue_neighbors(node_id);

        Some(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{CsrLayout, GraphBuilder};

    mod dfs {
        use super::*;

        mod directed {
            use super::*;

            #[test]
            fn acyclic() {
                let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
                    .csr_layout(CsrLayout::Deduplicated)
                    .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 3), (2, 1), (3, 1)])
                    .build();

                let actual: Vec<usize> =
                    depth_first_directed(&graph, 0, Direction::Outgoing).collect();
                let expected: Vec<usize> = vec![0, 2, 3, 1];

                assert_eq!(actual, expected);
            }

            #[test]
            fn cyclic() {
                let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
                    .csr_layout(CsrLayout::Deduplicated)
                    .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 1), (2, 1), (3, 1)])
                    .build();

                let actual: Vec<usize> =
                    depth_first_directed(&graph, 0, Direction::Outgoing).collect();
                let expected: Vec<usize> = vec![0, 2, 1, 3];

                assert_eq!(actual, expected);
            }
        }

        #[test]
        fn undirected() {
            let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
                .csr_layout(CsrLayout::Deduplicated)
                .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 3), (2, 1), (3, 1)])
                .build();

            let actual: Vec<usize> = depth_first_undirected(&graph, 0).collect();
            let expected: Vec<usize> = vec![0, 2, 3, 1];

            assert_eq!(actual, expected);
        }
    }

    mod bfs {
        use super::*;
        mod directed {
            use super::*;

            #[test]
            fn acyclic() {
                let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
                    .csr_layout(CsrLayout::Deduplicated)
                    .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 3), (2, 1), (3, 1)])
                    .build();

                let actual: Vec<usize> = bfs_directed(&graph, 0, Direction::Outgoing).collect();
                let expected: Vec<usize> = vec![0, 1, 2, 3];

                assert_eq!(actual, expected);
            }

            #[test]
            fn cyclic() {
                let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
                    .csr_layout(CsrLayout::Deduplicated)
                    .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 1), (2, 1), (3, 1)])
                    .build();

                let actual: Vec<usize> = bfs_directed(&graph, 0, Direction::Outgoing).collect();
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

            let actual: Vec<usize> = bfs_undirected(&graph, 0).collect();
            let expected: Vec<usize> = vec![0, 1, 2, 3];

            assert_eq!(actual, expected);
        }
    }
}

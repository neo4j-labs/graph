use std::hash::Hash;

use bitvec::prelude::*;

use crate::prelude::*;

pub fn dfs_directed<NI, G>(
    graph: &G,
    node_ids: impl IntoIterator<Item = NI>,
    direction: Direction,
) -> DirectedDepthFirst<'_, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    DirectedDepthFirst::new(graph, node_ids, direction)
}

pub struct DirectedDepthFirst<'a, G, NI> {
    graph: &'a G,
    seen: BitVec<usize>,
    visited: BitVec<usize>,
    stack: Vec<NI>,
    direction: Direction,
}

impl<'a, G, NI> DirectedDepthFirst<'a, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + DirectedNeighbors<NI> + Sync,
{
    pub fn new(graph: &'a G, node_ids: impl IntoIterator<Item = NI>, direction: Direction) -> Self {
        let bitvec = BitVec::repeat(false, graph.node_count().index());
        let visited = bitvec.clone();

        let mut seen = bitvec;
        let mut stack = Vec::new();
        Self::enqueue_into(&mut seen, &mut stack, node_ids);

        Self {
            graph,
            seen,
            visited,
            stack,
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

    fn enqueue_into(
        seen: &mut BitVec<usize>,
        stack: &mut Vec<NI>,
        node_ids: impl IntoIterator<Item = NI>,
    ) {
        for node_id in node_ids {
            if !seen.replace(node_id.index(), true) {
                stack.push(node_id);
            }
        }
    }

    fn enqueue_out_neighbors_of(&mut self, node_id: NI) {
        let node_ids = self
            .graph
            .out_neighbors(node_id)
            .copied()
            .filter(|node_id| !self.visited[node_id.index()]);

        Self::enqueue_into(&mut self.seen, &mut self.stack, node_ids);
    }

    fn enqueue_in_neighbors_of(&mut self, node_id: NI) {
        let node_ids = self
            .graph
            .in_neighbors(node_id)
            .copied()
            .filter(|node_id| !self.visited[node_id.index()]);

        Self::enqueue_into(&mut self.seen, &mut self.stack, node_ids);
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

pub fn dfs_undirected<NI, G>(
    graph: &G,
    node_ids: impl IntoIterator<Item = NI>,
) -> UndirectedDepthFirst<'_, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + UndirectedDegrees<NI> + UndirectedNeighbors<NI> + Sync,
{
    UndirectedDepthFirst::new(graph, node_ids)
}

pub struct UndirectedDepthFirst<'a, G, NI> {
    graph: &'a G,
    seen: BitVec<usize>,
    visited: BitVec<usize>,
    stack: Vec<NI>,
}

impl<'a, G, NI> UndirectedDepthFirst<'a, G, NI>
where
    NI: Idx + Hash,
    G: Graph<NI> + UndirectedNeighbors<NI> + Sync,
{
    pub fn new(graph: &'a G, node_ids: impl IntoIterator<Item = NI>) -> Self {
        let bitvec = BitVec::repeat(false, graph.node_count().index());
        let visited = bitvec.clone();

        let mut seen = bitvec;
        let mut stack = Vec::new();
        Self::enqueue_into(&mut seen, &mut stack, node_ids);

        Self {
            graph,
            seen,
            visited,
            stack,
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

    fn enqueue_into(
        seen: &mut BitVec<usize>,
        stack: &mut Vec<NI>,
        node_ids: impl IntoIterator<Item = NI>,
    ) {
        for node_id in node_ids {
            if !seen.replace(node_id.index(), true) {
                stack.push(node_id);
            }
        }
    }

    fn enqueue_neighbors_of(&mut self, node_id: NI) {
        let node_ids = self
            .graph
            .neighbors(node_id)
            .copied()
            .filter(|&node_id| !self.visited[node_id.index()]);

        Self::enqueue_into(&mut self.seen, &mut self.stack, node_ids);
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

            let actual: Vec<usize> = dfs_directed(&graph, [0], Direction::Outgoing).collect();
            let expected: Vec<usize> = vec![0, 2, 3, 1];

            assert_eq!(actual, expected);
        }

        #[test]
        fn cyclic() {
            let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
                .csr_layout(CsrLayout::Deduplicated)
                .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 1), (2, 1), (3, 1)])
                .build();

            let actual: Vec<usize> = dfs_directed(&graph, [0], Direction::Outgoing).collect();
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

        let actual: Vec<usize> = dfs_undirected(&graph, [0]).collect();
        let expected: Vec<usize> = vec![0, 2, 3, 1];

        assert_eq!(actual, expected);
    }
}

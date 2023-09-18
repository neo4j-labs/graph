use crate::index::Idx;
use crate::Target;

use rayon::prelude::*;

#[derive(Debug)]
pub struct AdjacencyList<NI, EV> {
    edges: Vec<Vec<Target<NI, EV>>>,
}

pub struct List<NI, EV>(pub Vec<Target<NI, EV>>);

impl<NI: Idx, EV> AdjacencyList<NI, EV> {
    pub fn new(edges: Vec<Vec<Target<NI, EV>>>) -> Self {
        Self { edges }
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
        unsafe { std::slice::from_raw_parts(targets.as_ptr() as *const NI, targets.len()) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
}

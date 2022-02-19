use std::sync::Arc;

use crate::{dss::DisjointSetStruct, prelude::*};
use rayon::prelude::*;

pub fn wcc<NI: Idx>(graph: &DirectedCsrGraph<NI>) -> DisjointSetStruct<NI> {
    let node_count = graph.node_count().index();
    let dss = Arc::new(DisjointSetStruct::new(node_count));

    (0..node_count)
        .into_par_iter()
        .map(NI::new)
        .for_each(|node| {
            graph
                .out_neighbors(node)
                .iter()
                .for_each(|target| dss.union(node, *target));
        });

    Arc::try_unwrap(dss).ok().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_components() {
        let graph: DirectedCsrGraph<usize> =
            GraphBuilder::new().edges(vec![(0, 1), (2, 3)]).build();

        let dss = wcc(&graph);

        assert_eq!(dss.find(0), dss.find(1));
        assert_eq!(dss.find(2), dss.find(3));
        assert_ne!(dss.find(1), dss.find(2));
    }
}

use graph_builder::prelude::*;

pub fn msbfs<NI, G, F>(graph: &G, sources: &[NI], mut compute: F)
where
    NI: Idx,
    G: Graph<NI> + UndirectedDegrees<NI> + UndirectedNeighbors<NI> + Sync,
    F: FnMut(NI, NI, usize),
{
    // input:
    // w = total number of BFSs (fixed to machine-specific width)
    // set B = { b1, ..., b_w } where each b_i represents a BFS
    // set S = { s1, ... , sw } that contains the source vertex s_i for each bfs_i

    assert!(
        sources.len() < usize::BITS as usize,
        "number of source nodes exceeds bit field width"
    );

    // w = total number of BFSs (fixed to machine-specific width)
    let w = usize::min(sources.len(), usize::BITS as usize);

    let node_count = graph.node_count().index();
    // seen_v = f1..fw where fi=1 if b1 has discovered v
    let mut seen: Vec<usize> = Vec::with_capacity(node_count);
    // visit_v = g1..gw where gi=1 if v needs to be explored by bi
    let mut visit: Vec<usize> = Vec::with_capacity(node_count);
    // visit_v = g1..gw where gi=1 if v needs to be explored by bi
    let mut visit_next: Vec<usize> = Vec::with_capacity(node_count);

    let empty_set = 0_usize;

    // init
    seen.resize(node_count, 0);
    visit.resize(node_count, 0);
    visit_next.resize(node_count, 0);

    for bfs in 0..w {
        seen[bfs] = 1 << bfs;
        visit[bfs] = 1 << bfs;
    }

    let mut level = 1;

    loop {
        for v in 0..node_count {
            if visit[v] == empty_set {
                continue;
            }

            graph.neighbors(NI::new(v)).for_each(|n| {
                let n = n.index();
                // d is set of all BFSs that need to explore n in the next level
                // A \ B := A & ~B
                let mut d = visit[v] & !seen[n];

                if d != empty_set {
                    // A U B := A | B
                    visit_next[n] |= d;
                    seen[n] |= d;

                    // compute sth
                    while d != empty_set {
                        let bfs = d.trailing_zeros() as usize;
                        compute(sources[bfs], NI::new(n), level);
                        d = d ^ (1 << bfs);
                    }
                }
            })
        }

        std::mem::swap(&mut visit, &mut visit_next);
        // check if there are still nodes to visit
        if visit.iter().any(|f| *f != empty_set) {
            visit_next.fill(0);
            level += 1;
        } else {
            break;
        }
    }
}

pub fn msbfs_anp<NI, G, F>(graph: &G, sources: &[NI], mut compute: F)
where
    NI: Idx,
    G: Graph<NI> + UndirectedDegrees<NI> + UndirectedNeighbors<NI> + Sync,
    F: FnMut(NI, NI, usize),
{
    // input:
    // w = total number of BFSs (fixed to machine-specific width)
    // set B = { b1, ..., b_w } where each b_i represents a BFS
    // set S = { s1, ... , sw } that contains the source vertex s_i for each bfs_i

    assert!(
        sources.len() < usize::BITS as usize,
        "number of source nodes exceeds bit field width"
    );

    // w = total number of BFSs (fixed to machine-specific width)
    let w = usize::min(sources.len(), usize::BITS as usize);

    let node_count = graph.node_count().index();
    // seen_v = f1..fw where fi=1 if b1 has discovered v
    let mut seen: Vec<usize> = Vec::with_capacity(node_count);
    // visit_v = g1..gw where gi=1 if v needs to be explored by bi
    let mut visit: Vec<usize> = Vec::with_capacity(node_count);
    // visit_v = g1..gw where gi=1 if v needs to be explored by bi
    let mut visit_next: Vec<usize> = Vec::with_capacity(node_count);

    let empty_set = 0_usize;

    // init
    seen.resize(node_count, 0);
    visit.resize(node_count, 0);
    visit_next.resize(node_count, 0);

    for bfs in 0..w {
        seen[bfs] = 1 << bfs;
        visit[bfs] = 1 << bfs;
    }

    let mut level = 1;

    loop {
        // stage 1: Explore all vertices in visit to determine
        // in which BFSs their neighbors should be visited.
        for v in 0..node_count {
            if visit[v] == empty_set {
                continue;
            }
            graph.neighbors(NI::new(v)).for_each(|n| {
                visit_next[n.index()] |= visit[v];
            })
        }
        // stage 2: Iterate over visitNext, update its bit fields
        // based on seen, and execute the BFS computation.
        for v in 0..node_count {
            if visit_next[v] == empty_set {
                continue;
            }
            visit_next[v] &= !seen[v];
            seen[v] |= visit_next[v];
            if visit_next[v] != empty_set {
                // d is set of all BFSs that explore v at the same time
                let mut d = visit_next[v];
                while d != empty_set {
                    let bfs = d.trailing_zeros() as usize;
                    compute(sources[bfs], NI::new(v), level);
                    d = d ^ (1 << bfs);
                }
            }
        }

        std::mem::swap(&mut visit, &mut visit_next);
        // check if there are still nodes to visit
        if visit.iter().any(|f| *f != empty_set) {
            visit_next.fill(0);
            level += 1;
        } else {
            break;
        }
    }
}

#[allow(dead_code)]
fn dbg(field: &[usize]) {
    field
        .iter()
        .enumerate()
        .for_each(|(i, f)| println!("{}: {f:010b}", i + 1));
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    // Example graph of Figure 2
    const EXAMPLE: &str = "(n1),(n2),(n3),(n4),(n5),(n6),(n3)-->(n1)<--(n4),(n3)-->(n2)<--(n4),(n3)-->(n5),(n4)-->(n6)";

    #[test]
    fn test_base() {
        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .gdl_str::<usize, _>(EXAMPLE)
            .build()
            .unwrap();

        let mut actual = HashMap::<(usize, usize), usize>::default();

        msbfs(&graph, &[0, 1], |s, t, d| {
            actual.insert((s, t), d);
        });

        let mut expected = HashMap::default();

        expected.insert((0, 2), 1); // (n1)-->(n3)
        expected.insert((0, 3), 1); // (n1)-->(n4)
        expected.insert((1, 2), 1); // (n2)-->(n3)
        expected.insert((1, 3), 1); // (n2)-->(n4)
        expected.insert((1, 0), 2); // (n2)-->(n3|n4)-->(n1)
        expected.insert((0, 1), 2); // (n1)-->(n3|n4)-->(n2)
        expected.insert((0, 4), 2); // (n1)-->(n3)-->(n5)
        expected.insert((1, 4), 2); // (n2)-->(n3)-->(n5)
        expected.insert((0, 5), 2); // (n1)-->(n4)-->(n6)
        expected.insert((1, 5), 2); // (n2)-->(n4)-->(n6)

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_anp() {
        let graph: UndirectedCsrGraph<usize> = GraphBuilder::new()
            .gdl_str::<usize, _>(EXAMPLE)
            .build()
            .unwrap();

        let mut actual = HashMap::<(usize, usize), usize>::default();

        msbfs_anp(&graph, &[0, 1], |s, t, d| {
            actual.insert((s, t), d);
        });

        let mut expected = HashMap::default();

        expected.insert((0, 2), 1); // (n1)-->(n3)
        expected.insert((0, 3), 1); // (n1)-->(n4)
        expected.insert((1, 2), 1); // (n2)-->(n3)
        expected.insert((1, 3), 1); // (n2)-->(n4)
        expected.insert((1, 0), 2); // (n2)-->(n3|n4)-->(n1)
        expected.insert((0, 1), 2); // (n1)-->(n3|n4)-->(n2)
        expected.insert((0, 4), 2); // (n1)-->(n3)-->(n5)
        expected.insert((1, 4), 2); // (n2)-->(n3)-->(n5)
        expected.insert((0, 5), 2); // (n1)-->(n4)-->(n6)
        expected.insert((1, 5), 2); // (n2)-->(n4)-->(n6)

        assert_eq!(actual, expected);
    }
}

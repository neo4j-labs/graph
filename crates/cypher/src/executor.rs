use fxhash::FxHashMap;

use graph_builder::prelude::{DirectedNeighbors, Graph, Idx};

use crate::ast::{Direction, PathPattern, Pattern, Predicate, Query};
use crate::lexer::tokenize;
use crate::parser::parse;
use crate::Error;

/// A value in a query result row.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// A node represented by its numeric ID.
    Node(usize),
    /// A null / missing value (returned when a variable is not bound).
    Null,
}

/// A single result row produced by a query.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl Row {
    fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        Row { columns, values }
    }

    /// Returns the value for the given column name, or `None` if the column is absent.
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c == column)
            .map(|i| &self.values[i])
    }

    /// Returns the column names in order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the values in column order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }
}

/// Cypher query engine backed by a directed graph.
///
/// Accepts any graph that implements [`Graph`] + [`DirectedNeighbors`], including
/// [`DirectedCsrGraph`].
///
/// Only topology is considered — labels and properties are not supported.
///
/// # Supported syntax
///
/// ```text
/// MATCH (n) RETURN n
/// MATCH (a)-[r]->(b) RETURN a, b
/// MATCH (a)<-[r]-(b) RETURN a, b
/// MATCH (a)-[r]-(b)  RETURN a, b
/// MATCH (a)-->(b)    RETURN a, b
/// MATCH (a)<--(b)    RETURN a, b
/// MATCH (a)--(b)     RETURN a, b
/// MATCH (a)-[r]->(b) WHERE id(a) = 0 RETURN b
/// ```
pub struct CypherEngine<'g, G> {
    graph: &'g G,
}

impl<'g, G> CypherEngine<'g, G> {
    /// Creates a new engine backed by `graph`.
    pub fn new(graph: &'g G) -> Self {
        CypherEngine { graph }
    }
}

impl<'g, G> CypherEngine<'g, G> {
    /// Parses and executes `query`, returning an iterator over result rows.
    ///
    /// The node index type `NI` is inferred from the graph. Rows are collected
    /// eagerly; the iterator is over an owned `Vec<Row>`.
    pub fn execute<NI>(&self, query: &str) -> Result<impl Iterator<Item = Row>, Error>
    where
        NI: Idx,
        G: Graph<NI> + DirectedNeighbors<NI>,
    {
        let tokens = tokenize(query)?;
        let query = parse(tokens)?;
        Ok(self.run::<NI>(&query)?.into_iter())
    }

    fn run<NI>(&self, query: &Query) -> Result<Vec<Row>, Error>
    where
        NI: Idx,
        G: Graph<NI> + DirectedNeighbors<NI>,
    {
        let bindings = self.generate_bindings::<NI>(&query.match_clause.pattern)?;

        let bindings: Vec<_> = match &query.where_clause {
            Some(wc) => bindings
                .into_iter()
                .filter(|b| matches_predicate(b, &wc.predicate))
                .collect(),
            // None => bindings,
        };

        Ok(bindings
            .into_iter()
            .map(|b| project(&b, &query.return_clause.items))
            .collect())
    }

    fn generate_bindings<NI>(
        &self,
        pattern: &Pattern,
    ) -> Result<Vec<FxHashMap<String, Value>>, Error>
    where
        NI: Idx,
        G: Graph<NI> + DirectedNeighbors<NI>,
    {
        match pattern {
            Pattern::Node(node) => {
                let var = node
                    .variable
                    .clone()
                    .unwrap_or_else(|| "_".to_string());
                let node_count = self.graph.node_count().index();
                Ok((0..node_count)
                    .map(|n| {
                        let mut b = FxHashMap::default();
                        b.insert(var.clone(), Value::Node(n));
                        b
                    })
                    .collect())
            }
            Pattern::Path(path) => self.generate_path_bindings::<NI>(path),
        }
    }

    fn generate_path_bindings<NI>(
        &self,
        path: &PathPattern,
    ) -> Result<Vec<FxHashMap<String, Value>>, Error>
    where
        NI: Idx,
        G: Graph<NI> + DirectedNeighbors<NI>,
    {
        let start_var = path
            .start
            .variable
            .clone()
            .unwrap_or_else(|| "_0".to_string());
        let node_count = self.graph.node_count().index();

        // Seed: one binding per node, bound to the start variable.
        let mut bindings: Vec<FxHashMap<String, Value>> = (0..node_count)
            .map(|n| {
                let mut b = FxHashMap::default();
                b.insert(start_var.clone(), Value::Node(n));
                b
            })
            .collect();

        // For each hop, extend every current binding by following the edge.
        let mut current_var = start_var.clone();
        for (rel, next_node_pat) in &path.hops {
            let next_var = next_node_pat
                .variable
                .clone()
                .unwrap_or_else(|| "_next".to_string());

            let mut next_bindings = Vec::new();
            for binding in bindings {
                let src = match binding.get(&current_var) {
                    Some(Value::Node(n)) => *n,
                    _ => continue,
                };
                let src_ni = NI::new(src);

                let neighbors: Vec<usize> = match rel.direction {
                    Direction::Outgoing => self
                        .graph
                        .out_neighbors(src_ni)
                        .copied()
                        .map(|n| n.index())
                        .collect(),
                    Direction::Incoming => self
                        .graph
                        .in_neighbors(src_ni)
                        .copied()
                        .map(|n| n.index())
                        .collect(),
                    Direction::Both => {
                        let out: Vec<usize> = self
                            .graph
                            .out_neighbors(src_ni)
                            .copied()
                            .map(|n| n.index())
                            .collect();
                        let inc: Vec<usize> = self
                            .graph
                            .in_neighbors(src_ni)
                            .copied()
                            .map(|n| n.index())
                            .collect();
                        out.into_iter().chain(inc).collect()
                    }
                };

                for dst in neighbors {
                    let mut new_binding = binding.clone();
                    new_binding.insert(next_var.clone(), Value::Node(dst));
                    next_bindings.push(new_binding);
                }
            }

            bindings = next_bindings;
            current_var = next_var;
        }

        Ok(bindings)
    }
}

fn matches_predicate(bindings: &FxHashMap<String, Value>, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::IdEquals(var, expected) => matches!(
            bindings.get(var.as_str()),
            Some(Value::Node(n)) if *n == *expected as usize
        ),
    }
}

fn project(bindings: &FxHashMap<String, Value>, columns: &[String]) -> Row {
    let values = columns
        .iter()
        .map(|col| {
            bindings
                .get(col.as_str())
                .cloned()
                .unwrap_or(Value::Null)
        })
        .collect();
    Row::new(columns.to_vec(), values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_builder::prelude::*;

    fn small_graph() -> DirectedCsrGraph<usize> {
        // Edges: 0→1, 0→2, 1→2, 2→3
        GraphBuilder::new()
            .edges(vec![(0, 1), (0, 2), (1, 2), (2, 3)])
            .build()
    }

    #[test]
    fn match_all_nodes() {
        let g = small_graph();
        let engine = CypherEngine::new(&g);
        let rows: Vec<_> = engine.execute::<usize>("MATCH (n) RETURN n").unwrap().collect();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].get("n"), Some(&Value::Node(0)));
        assert_eq!(rows[3].get("n"), Some(&Value::Node(3)));
    }

    #[test]
    fn match_outgoing_edges() {
        let g = small_graph();
        let engine = CypherEngine::new(&g);
        let rows: Vec<_> = engine
            .execute::<usize>("MATCH (a)-[r]->(b) RETURN a, b")
            .unwrap()
            .collect();
        // 4 edges in the graph
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn match_outgoing_shorthand() {
        let g = small_graph();
        let rows: Vec<_> = CypherEngine::new(&g)
            .execute::<usize>("MATCH (a)-->(b) RETURN a, b")
            .unwrap()
            .collect();
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn match_outgoing_with_where_on_source() {
        let g = small_graph();
        let engine = CypherEngine::new(&g);
        // Node 0 has two outgoing edges: 0→1 and 0→2
        let rows: Vec<_> = engine
            .execute::<usize>("MATCH (a)-[r]->(b) WHERE id(a) = 0 RETURN b")
            .unwrap()
            .collect();
        assert_eq!(rows.len(), 2);
        let targets: Vec<usize> = rows
            .iter()
            .map(|r| match r.get("b") {
                Some(Value::Node(n)) => *n,
                v => panic!("expected Node, got {v:?}"),
            })
            .collect();
        assert!(targets.contains(&1));
        assert!(targets.contains(&2));
    }

    #[test]
    fn match_outgoing_with_where_on_target() {
        let g = small_graph();
        let engine = CypherEngine::new(&g);
        // Nodes 0 and 1 both point to node 2
        let rows: Vec<_> = engine
            .execute::<usize>("MATCH (a)-[r]->(b) WHERE id(b) = 2 RETURN a")
            .unwrap()
            .collect();
        assert_eq!(rows.len(), 2);
        let sources: Vec<usize> = rows
            .iter()
            .map(|r| match r.get("a") {
                Some(Value::Node(n)) => *n,
                v => panic!("expected Node, got {v:?}"),
            })
            .collect();
        assert!(sources.contains(&0));
        assert!(sources.contains(&1));
    }

    #[test]
    fn match_incoming_edges() {
        let g = small_graph();
        let engine = CypherEngine::new(&g);
        // (a)<-[r]-(b): a has an incoming edge from b
        // With WHERE id(a) = 2: what nodes b point to 2? → 0 and 1
        let rows: Vec<_> = engine
            .execute::<usize>("MATCH (a)<-[r]-(b) WHERE id(a) = 2 RETURN b")
            .unwrap()
            .collect();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn match_both_directions() {
        let g = small_graph();
        let engine = CypherEngine::new(&g);
        // (a)--(b): match any edge in either orientation
        let rows: Vec<_> = engine
            .execute::<usize>("MATCH (a)--(b) RETURN a, b")
            .unwrap()
            .collect();
        // Each directed edge appears twice (once per orientation)
        assert_eq!(rows.len(), 8);
    }

    #[test]
    fn row_get_missing_column_returns_null() {
        let g = small_graph();
        let rows: Vec<_> = CypherEngine::new(&g)
            .execute::<usize>("MATCH (n) RETURN n")
            .unwrap()
            .collect();
        assert_eq!(rows[0].get("missing"), None);
    }

    // ── multi-hop tests ──────────────────────────────────────────────────────
    //
    // Graph used in all tests below:
    //   0 → 1
    //   0 → 2
    //   1 → 2
    //   2 → 3
    //
    // Two-hop paths (a→b→c):
    //   0→1→2, 0→2→3, 1→2→3          (3 paths)
    //
    // Three-hop paths (a→b→c→d):
    //   0→1→2→3                        (1 path)

    #[test]
    fn two_hop_path_count() {
        let g = small_graph();
        let rows: Vec<_> = CypherEngine::new(&g)
            .execute::<usize>("MATCH (a)-->(b)-->(c) RETURN a, b, c")
            .unwrap()
            .collect();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn two_hop_path_values() {
        let g = small_graph();
        let rows: Vec<_> = CypherEngine::new(&g)
            .execute::<usize>("MATCH (a)-->(b)-->(c) RETURN a, b, c")
            .unwrap()
            .collect();

        let mut paths: Vec<(usize, usize, usize)> = rows
            .iter()
            .map(|r| {
                let node = |col| match r.get(col) {
                    Some(Value::Node(n)) => *n,
                    v => panic!("expected Node for {col}, got {v:?}"),
                };
                (node("a"), node("b"), node("c"))
            })
            .collect();
        paths.sort_unstable();

        assert_eq!(paths, vec![(0, 1, 2), (0, 2, 3), (1, 2, 3)]);
    }

    #[test]
    fn two_hop_path_with_where_on_start() {
        let g = small_graph();
        // Starting from node 0: 0→1→2 and 0→2→3
        let rows: Vec<_> = CypherEngine::new(&g)
            .execute::<usize>("MATCH (a)-->(b)-->(c) WHERE id(a) = 0 RETURN c")
            .unwrap()
            .collect();
        assert_eq!(rows.len(), 2);
        let mut ends: Vec<usize> = rows
            .iter()
            .map(|r| match r.get("c") {
                Some(Value::Node(n)) => *n,
                v => panic!("expected Node, got {v:?}"),
            })
            .collect();
        ends.sort_unstable();
        assert_eq!(ends, vec![2, 3]);
    }

    #[test]
    fn two_hop_path_with_where_on_middle() {
        let g = small_graph();
        // Paths through b=2: 0→2→3 and 1→2→3
        let rows: Vec<_> = CypherEngine::new(&g)
            .execute::<usize>("MATCH (a)-->(b)-->(c) WHERE id(b) = 2 RETURN a, c")
            .unwrap()
            .collect();
        assert_eq!(rows.len(), 2);
        let mut pairs: Vec<(usize, usize)> = rows
            .iter()
            .map(|r| {
                let node = |col| match r.get(col) {
                    Some(Value::Node(n)) => *n,
                    v => panic!("expected Node for {col}, got {v:?}"),
                };
                (node("a"), node("c"))
            })
            .collect();
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(0, 3), (1, 3)]);
    }

    #[test]
    fn three_hop_path() {
        let g = small_graph();
        // Only path of length 3: 0→1→2→3
        let rows: Vec<_> = CypherEngine::new(&g)
            .execute::<usize>("MATCH (a)-->(b)-->(c)-->(d) RETURN a, b, c, d")
            .unwrap()
            .collect();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.get("a"), Some(&Value::Node(0)));
        assert_eq!(r.get("b"), Some(&Value::Node(1)));
        assert_eq!(r.get("c"), Some(&Value::Node(2)));
        assert_eq!(r.get("d"), Some(&Value::Node(3)));
    }

    #[test]
    fn two_hop_mixed_directions() {
        let g = small_graph();
        // (a)-->(b)<--(c): find all (a,b,c) where a→b and c→b
        // For b=2: a ∈ {0,1}, c ∈ {0,1} → 4 combinations
        // For b=1: a ∈ {0},   c ∈ {0}   → 1 combination
        // For b=3: a ∈ {2},   c ∈ {2}   → 1 combination
        let rows: Vec<_> = CypherEngine::new(&g)
            .execute::<usize>("MATCH (a)-->(b)<--(c) RETURN a, b, c")
            .unwrap()
            .collect();
        // 4 + 1 + 1 = 6
        assert_eq!(rows.len(), 6);
    }
}

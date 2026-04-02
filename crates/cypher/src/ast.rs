#[derive(Debug, Clone)]
pub(crate) struct Query {
    pub match_clause: MatchClause,
    pub where_clause: Option<WhereClause>,
    pub return_clause: ReturnClause,
}

#[derive(Debug, Clone)]
pub(crate) struct MatchClause {
    pub pattern: Pattern,
}

#[derive(Debug, Clone)]
pub(crate) enum Pattern {
    Node(NodePattern),
    Path(PathPattern),
}

#[derive(Debug, Clone)]
pub(crate) struct NodePattern {
    pub variable: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct PathPattern {
    pub start: NodePattern,
    /// One or more `(rel, node)` hops. Guaranteed non-empty by the parser.
    pub hops: Vec<(RelPattern, NodePattern)>,
}

#[derive(Debug, Clone)]
pub(crate) struct RelPattern {
    #[allow(dead_code)]
    pub variable: Option<String>,
    pub direction: Direction,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Direction {
    /// `(a)-[r]->(b)` — follow outgoing edges, a is source, b is target
    Outgoing,
    /// `(a)<-[r]-(b)` — follow incoming edges, a is target, b is source
    Incoming,
    /// `(a)-[r]-(b)` — match any edge between a and b regardless of direction
    Both,
}

#[derive(Debug, Clone)]
pub(crate) struct WhereClause {
    pub predicate: Predicate,
}

#[derive(Debug, Clone)]
pub(crate) enum Predicate {
    /// `id(variable) = value`
    IdEquals(String, u64),
}

#[derive(Debug, Clone)]
pub(crate) struct ReturnClause {
    pub items: Vec<String>,
}

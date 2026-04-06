use crate::value::Value;

/// A top-level Cypher statement.
#[derive(Debug, Clone)]
pub struct Statement {
    pub body: Vec<Clause>,
    pub unions: Vec<UnionPart>,
}

#[derive(Debug, Clone)]
pub struct UnionPart {
    pub all: bool,
    pub body: Vec<Clause>,
}

#[derive(Debug, Clone)]
pub enum Clause {
    Match(MatchClause),
    OptionalMatch(MatchClause),
    With(WithClause),
    Unwind(UnwindClause),
    Return(ReturnClause),
    Create(Vec<PatternPath>),
}

// -- MATCH --

#[derive(Debug, Clone)]
pub struct MatchClause {
    pub patterns: Vec<PatternPath>,
    pub where_clause: Option<Box<Expr>>,
}

#[derive(Debug, Clone)]
pub struct PatternPath {
    pub variable: Option<String>,
    pub start: NodePattern,
    pub hops: Vec<(RelPattern, NodePattern)>,
}

#[derive(Debug, Clone)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Option<Vec<(String, Expr)>>,
}

#[derive(Debug, Clone)]
pub struct RelPattern {
    pub variable: Option<String>,
    pub rel_types: Vec<String>,
    pub direction: Direction,
    pub length: Option<PathLength>,
    pub properties: Option<Vec<(String, Expr)>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone)]
pub enum PathLength {
    Exact(usize),
    Range(Option<usize>, Option<usize>),
}

// -- RETURN / WITH --

#[derive(Debug, Clone)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: ReturnItems,
    pub order_by: Option<Vec<SortItem>>,
    pub skip: Option<Expr>,
    pub limit: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum ReturnItems {
    Star,
    Expressions(Vec<ReturnItem>),
}

#[derive(Debug, Clone)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WithClause {
    pub return_body: ReturnClause,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct UnwindClause {
    pub expr: Expr,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct SortItem {
    pub expr: Expr,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

// -- Expressions --

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Value),
    Variable(String),
    Parameter(String),
    Property(Box<Expr>, String),

    // Arithmetic
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Mod(Box<Expr>, Box<Expr>),
    Pow(Box<Expr>, Box<Expr>),
    UnaryMinus(Box<Expr>),
    UnaryPlus(Box<Expr>),

    // Comparison
    Eq(Box<Expr>, Box<Expr>),
    Neq(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Lte(Box<Expr>, Box<Expr>),
    Gte(Box<Expr>, Box<Expr>),

    // Boolean
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Xor(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),

    // String predicates
    StartsWith(Box<Expr>, Box<Expr>),
    EndsWith(Box<Expr>, Box<Expr>),
    Contains(Box<Expr>, Box<Expr>),
    RegexMatch(Box<Expr>, Box<Expr>),

    // Null checks
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),

    // Label predicate
    HasLabel(Box<Expr>, String),
    HasLabels(Box<Expr>, Vec<String>),

    // Collection
    In(Box<Expr>, Box<Expr>),
    ListLiteral(Vec<Expr>),
    MapLiteral(Vec<(String, Expr)>),
    Index(Box<Expr>, Box<Expr>),
    Slice(Box<Expr>, Option<Box<Expr>>, Option<Box<Expr>>),

    // Function call
    FunctionCall {
        name: String,
        distinct: bool,
        args: Vec<Expr>,
    },
    CountStar,

    // CASE
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },

    // Pattern expression (for boolean context in WHERE)
    PatternExpr(PatternPath),

    // List comprehension [x IN list WHERE pred | expr]
    ListComprehension {
        variable: String,
        source: Box<Expr>,
        filter: Option<Box<Expr>>,
        projection: Option<Box<Expr>>,
    },

    // Pattern comprehension [(a)-[:REL]->(b) WHERE pred | expr]
    PatternComprehension {
        pattern: PatternPath,
        filter: Option<Box<Expr>>,
        projection: Box<Expr>,
    },

    // Existential subquery
    ExistsSubquery(Box<MatchClause>),

    // String concatenation (resolved as Add at eval time)
}

impl ReturnItem {
    /// Get the effective column name for this return item.
    pub fn column_name(&self) -> String {
        if let Some(ref alias) = self.alias {
            alias.clone()
        } else {
            expr_to_string(&self.expr)
        }
    }
}

/// Convert an expression to a string representation for column naming.
pub fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Variable(name) => name.clone(),
        Expr::Property(base, prop) => format!("{}.{}", expr_to_string(base), prop),
        Expr::FunctionCall { name, distinct, args } => {
            let args_str: Vec<String> = args.iter().map(|a| expr_to_string(a)).collect();
            if *distinct {
                format!("{}(DISTINCT {})", name, args_str.join(", "))
            } else {
                format!("{}({})", name, args_str.join(", "))
            }
        }
        Expr::CountStar => "count(*)".to_string(),
        Expr::Literal(v) => format!("{v}"),
        Expr::Parameter(p) => format!("${p}"),
        Expr::Add(l, r) => format!("{} + {}", expr_to_string(l), expr_to_string(r)),
        Expr::Sub(l, r) => format!("{} - {}", expr_to_string(l), expr_to_string(r)),
        Expr::Mul(l, r) => format!("{} * {}", expr_to_string(l), expr_to_string(r)),
        Expr::Div(l, r) => format!("{} / {}", expr_to_string(l), expr_to_string(r)),
        Expr::Mod(l, r) => format!("{} % {}", expr_to_string(l), expr_to_string(r)),
        Expr::Pow(l, r) => format!("{} ^ {}", expr_to_string(l), expr_to_string(r)),
        Expr::UnaryMinus(e) => format!("-{}", expr_to_string(e)),
        Expr::UnaryPlus(e) => format!("+{}", expr_to_string(e)),
        Expr::Not(e) => format!("NOT {}", expr_to_string(e)),
        Expr::IsNull(e) => format!("{} IS NULL", expr_to_string(e)),
        Expr::IsNotNull(e) => format!("{} IS NOT NULL", expr_to_string(e)),
        Expr::Eq(l, r) => format!("{} = {}", expr_to_string(l), expr_to_string(r)),
        Expr::Neq(l, r) => format!("{} <> {}", expr_to_string(l), expr_to_string(r)),
        Expr::Lt(l, r) => format!("{} < {}", expr_to_string(l), expr_to_string(r)),
        Expr::Gt(l, r) => format!("{} > {}", expr_to_string(l), expr_to_string(r)),
        Expr::Lte(l, r) => format!("{} <= {}", expr_to_string(l), expr_to_string(r)),
        Expr::Gte(l, r) => format!("{} >= {}", expr_to_string(l), expr_to_string(r)),
        Expr::And(l, r) => format!("{} AND {}", expr_to_string(l), expr_to_string(r)),
        Expr::Or(l, r) => format!("{} OR {}", expr_to_string(l), expr_to_string(r)),
        Expr::Xor(l, r) => format!("{} XOR {}", expr_to_string(l), expr_to_string(r)),
        Expr::In(l, r) => format!("{} IN {}", expr_to_string(l), expr_to_string(r)),
        Expr::StartsWith(l, r) => format!("{} STARTS WITH {}", expr_to_string(l), expr_to_string(r)),
        Expr::EndsWith(l, r) => format!("{} ENDS WITH {}", expr_to_string(l), expr_to_string(r)),
        Expr::Contains(l, r) => format!("{} CONTAINS {}", expr_to_string(l), expr_to_string(r)),
        Expr::RegexMatch(l, r) => format!("{} =~ {}", expr_to_string(l), expr_to_string(r)),
        Expr::HasLabel(e, l) => format!("{}:{}", expr_to_string(e), l),
        Expr::HasLabels(e, ls) => {
            let labels = ls.iter().map(|l| format!(":{l}")).collect::<Vec<_>>().join("");
            format!("{}{}", expr_to_string(e), labels)
        }
        Expr::ListLiteral(items) => {
            let items_str: Vec<String> = items.iter().map(|i| expr_to_string(i)).collect();
            format!("[{}]", items_str.join(", "))
        }
        Expr::MapLiteral(pairs) => {
            let pairs_str: Vec<String> = pairs.iter().map(|(k, v)| format!("{}: {}", k, expr_to_string(v))).collect();
            format!("{{{}}}", pairs_str.join(", "))
        }
        Expr::Index(base, idx) => format!("{}[{}]", expr_to_string(base), expr_to_string(idx)),
        Expr::Case { .. } => "CASE".to_string(),
        _ => format!("{expr:?}"),
    }
}

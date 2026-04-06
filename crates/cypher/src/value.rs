use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};

/// A Cypher value following openCypher type system semantics.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
    Node(NodeValue),
    Relationship(RelValue),
    Path(PathValue),
}

#[derive(Debug, Clone)]
pub struct NodeValue {
    pub id: usize,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct RelValue {
    pub id: usize,
    pub start_node: usize,
    pub end_node: usize,
    pub rel_type: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct PathValue {
    pub nodes: Vec<NodeValue>,
    pub relationships: Vec<RelValue>,
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(n) => Some(*n as f64),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_node(&self) -> Option<&NodeValue> {
        match self {
            Value::Node(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_relationship(&self) -> Option<&RelValue> {
        match self {
            Value::Relationship(r) => Some(r),
            _ => None,
        }
    }

    /// Returns a type ordering rank for Cypher's type comparison rules.
    /// Cypher ordering: MAP < NODE < RELATIONSHIP < LIST < PATH < STRING < BOOLEAN < NUMBER < VOID(null)
    /// For values within the same type, we compare normally.
    fn type_rank(&self) -> u8 {
        match self {
            Value::Map(_) => 0,
            Value::Node(_) => 1,
            Value::Relationship(_) => 2,
            Value::List(_) => 3,
            Value::Path(_) => 4,
            Value::String(_) => 5,
            Value::Bool(_) => 6,
            Value::Integer(_) | Value::Float(_) => 7,
            Value::Null => 8,
        }
    }

    /// Cypher truth check: only `true` is truthy. `null` and `false` are falsy.
    pub fn is_truthy(&self) -> bool {
        matches!(self, Value::Bool(true))
    }

    /// Try numeric promotion to compare integer and float.
    pub fn to_numeric_float(&self) -> Option<f64> {
        match self {
            Value::Integer(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Structural equality for TCK result comparison.
    /// Unlike Cypher semantics, this treats null == null as true,
    /// and compares nodes/relationships by labels+properties (not by ID).
    pub fn structural_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => {
                if a.is_nan() && b.is_nan() {
                    true
                } else {
                    a == b
                }
            }
            (Value::Integer(a), Value::Float(b)) | (Value::Float(b), Value::Integer(a)) => {
                *b == *a as f64
            }
            (Value::String(a), Value::String(b)) => a == b,
            (Value::List(a), Value::List(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b.iter()).all(|(x, y)| x.structural_eq(y))
            }
            (Value::Map(a), Value::Map(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(k, v)| b.get(k).map_or(false, |bv| v.structural_eq(bv)))
            }
            (Value::Node(a), Value::Node(b)) => {
                let mut al = a.labels.clone();
                let mut bl = b.labels.clone();
                al.sort();
                bl.sort();
                al == bl && map_structural_eq(&a.properties, &b.properties)
            }
            (Value::Relationship(a), Value::Relationship(b)) => {
                a.rel_type == b.rel_type && map_structural_eq(&a.properties, &b.properties)
            }
            (Value::Path(a), Value::Path(b)) => {
                a.nodes.len() == b.nodes.len()
                    && a.relationships.len() == b.relationships.len()
                    && a.nodes.iter().zip(b.nodes.iter()).all(|(x, y)| {
                        Value::Node(x.clone()).structural_eq(&Value::Node(y.clone()))
                    })
                    && a.relationships
                        .iter()
                        .zip(b.relationships.iter())
                        .all(|(x, y)| {
                            Value::Relationship(x.clone())
                                .structural_eq(&Value::Relationship(y.clone()))
                        })
            }
            _ => false,
        }
    }
}

fn map_structural_eq(a: &BTreeMap<String, Value>, b: &BTreeMap<String, Value>) -> bool {
    a.len() == b.len()
        && a.iter()
            .all(|(k, v)| b.get(k).map_or(false, |bv| v.structural_eq(bv)))
}

/// Cypher equality: null == anything => null (ternary logic).
/// Returns None for incomparable types or null involvement.
impl Value {
    pub fn cypher_eq(&self, other: &Value) -> Value {
        if self.is_null() || other.is_null() {
            return Value::Null;
        }
        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a == b),
            (Value::Integer(a), Value::Integer(b)) => Value::Bool(a == b),
            (Value::Float(a), Value::Float(b)) => Value::Bool(a == b),
            (Value::Integer(a), Value::Float(b)) => Value::Bool((*a as f64) == *b),
            (Value::Float(a), Value::Integer(b)) => Value::Bool(*a == (*b as f64)),
            (Value::String(a), Value::String(b)) => Value::Bool(a == b),
            (Value::List(a), Value::List(b)) => {
                if a.len() != b.len() {
                    return Value::Bool(false);
                }
                let mut has_null = false;
                for (x, y) in a.iter().zip(b.iter()) {
                    match x.cypher_eq(y) {
                        Value::Null => has_null = true,
                        Value::Bool(false) => return Value::Bool(false),
                        _ => {}
                    }
                }
                if has_null {
                    Value::Null
                } else {
                    Value::Bool(true)
                }
            }
            (Value::Map(a), Value::Map(b)) => {
                if a.len() != b.len() {
                    return Value::Bool(false);
                }
                let mut has_null = false;
                for (k, v) in a.iter() {
                    match b.get(k) {
                        None => return Value::Bool(false),
                        Some(bv) => match v.cypher_eq(bv) {
                            Value::Null => has_null = true,
                            Value::Bool(false) => return Value::Bool(false),
                            _ => {}
                        },
                    }
                }
                if has_null {
                    Value::Null
                } else {
                    Value::Bool(true)
                }
            }
            (Value::Node(a), Value::Node(b)) => Value::Bool(a.id == b.id),
            (Value::Relationship(a), Value::Relationship(b)) => Value::Bool(a.id == b.id),
            // Different types => false (not null)
            _ => Value::Bool(false),
        }
    }

    pub fn cypher_neq(&self, other: &Value) -> Value {
        match self.cypher_eq(other) {
            Value::Bool(b) => Value::Bool(!b),
            other => other, // null propagation
        }
    }

    pub fn cypher_lt(&self, other: &Value) -> Value {
        if self.is_null() || other.is_null() {
            return Value::Null;
        }
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Value::Bool(a < b),
            (Value::Float(a), Value::Float(b)) => Value::Bool(a < b),
            (Value::Integer(a), Value::Float(b)) => Value::Bool((*a as f64) < *b),
            (Value::Float(a), Value::Integer(b)) => Value::Bool(*a < (*b as f64)),
            (Value::String(a), Value::String(b)) => Value::Bool(a < b),
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(!a & *b), // false < true
            _ => Value::Null,
        }
    }

    pub fn cypher_lte(&self, other: &Value) -> Value {
        if self.is_null() || other.is_null() {
            return Value::Null;
        }
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Value::Bool(a <= b),
            (Value::Float(a), Value::Float(b)) => Value::Bool(a <= b),
            (Value::Integer(a), Value::Float(b)) => Value::Bool((*a as f64) <= *b),
            (Value::Float(a), Value::Integer(b)) => Value::Bool(*a <= (*b as f64)),
            (Value::String(a), Value::String(b)) => Value::Bool(a <= b),
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(a <= b),
            _ => Value::Null,
        }
    }

    pub fn cypher_gt(&self, other: &Value) -> Value {
        other.cypher_lt(self)
    }

    pub fn cypher_gte(&self, other: &Value) -> Value {
        other.cypher_lte(self)
    }
}

/// Cypher ordering for ORDER BY: null sorts last, cross-type ordering by type rank.
impl Value {
    pub fn order_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater, // null sorts last
            (_, Value::Null) => Ordering::Less,
            _ => {
                let rank_a = self.type_rank();
                let rank_b = other.type_rank();
                if rank_a != rank_b {
                    return rank_a.cmp(&rank_b);
                }
                self.same_type_cmp(other)
            }
        }
    }

    fn same_type_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Integer(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Integer(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::List(a), Value::List(b)) => {
                for (x, y) in a.iter().zip(b.iter()) {
                    match x.order_cmp(y) {
                        Ordering::Equal => continue,
                        ord => return ord,
                    }
                }
                a.len().cmp(&b.len())
            }
            (Value::Node(a), Value::Node(b)) => a.id.cmp(&b.id),
            (Value::Relationship(a), Value::Relationship(b)) => a.id.cmp(&b.id),
            _ => Ordering::Equal,
        }
    }
}

/// PartialEq uses Cypher identity semantics: null != null.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self.cypher_eq(other) {
            Value::Bool(b) => b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Integer(n) => n.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::List(l) => {
                l.len().hash(state);
                for v in l {
                    v.hash(state);
                }
            }
            Value::Map(m) => {
                m.len().hash(state);
                for (k, v) in m {
                    k.hash(state);
                    v.hash(state);
                }
            }
            Value::Node(n) => n.id.hash(state),
            Value::Relationship(r) => r.id.hash(state),
            Value::Path(p) => {
                p.nodes.len().hash(state);
                for n in &p.nodes {
                    n.id.hash(state);
                }
            }
        }
    }
}

/// Display for TCK result output format.
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Integer(n) => write!(f, "{n}"),
            Value::Float(v) => {
                if v.is_nan() {
                    write!(f, "NaN")
                } else if v.is_infinite() {
                    if v.is_sign_positive() {
                        write!(f, "Inf")
                    } else {
                        write!(f, "-Inf")
                    }
                } else if *v == v.floor() && v.abs() < 1e15 {
                    write!(f, "{v:.1}")
                } else {
                    write!(f, "{v}")
                }
            }
            Value::String(s) => write!(f, "'{s}'"),
            Value::List(l) => {
                write!(f, "[")?;
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Value::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            Value::Node(n) => write!(f, "{n}"),
            Value::Relationship(r) => write!(f, "{r}"),
            Value::Path(p) => write!(f, "{p}"),
        }
    }
}

impl fmt::Display for NodeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for label in &self.labels {
            write!(f, ":{label}")?;
        }
        if !self.properties.is_empty() {
            if !self.labels.is_empty() {
                write!(f, " ")?;
            }
            write!(f, "{{")?;
            for (i, (k, v)) in self.properties.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{k}: {v}")?;
            }
            write!(f, "}}")?;
        }
        write!(f, ")")
    }
}

impl fmt::Display for RelValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[:{}", self.rel_type)?;
        if !self.properties.is_empty() {
            write!(f, " {{")?;
            for (i, (k, v)) in self.properties.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{k}: {v}")?;
            }
            write!(f, "}}")?;
        }
        write!(f, "]")
    }
}

impl fmt::Display for PathValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<")?;
        if let Some(first) = self.nodes.first() {
            write!(f, "{first}")?;
        }
        for (rel, node) in self.relationships.iter().zip(self.nodes.iter().skip(1)) {
            write!(f, "-{rel}->{node}")?;
        }
        write!(f, ">")
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Integer(n)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        Value::List(v.into_iter().map(Into::into).collect())
    }
}

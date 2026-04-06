use std::collections::{BTreeMap, HashMap};

use crate::ast::{Clause, Direction, Expr, PatternPath, Statement};
use crate::error::Error;
use crate::lexer::Lexer;
use crate::parser::Parser;
use crate::value::{NodeValue, RelValue, Value};

/// Node data stored in the property graph.
#[derive(Debug, Clone)]
pub struct NodeData {
    pub id: usize,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

/// Relationship data stored in the property graph.
#[derive(Debug, Clone)]
pub struct RelData {
    pub id: usize,
    pub source: usize,
    pub target: usize,
    pub rel_type: String,
    pub properties: BTreeMap<String, Value>,
}

/// A property graph supporting labels, relationship types, and property maps.
/// Used as the execution target for Cypher queries.
#[derive(Debug, Clone)]
pub struct PropertyGraph {
    pub nodes: Vec<NodeData>,
    pub relationships: Vec<RelData>,
    outgoing: Vec<Vec<usize>>,
    incoming: Vec<Vec<usize>>,
}

impl PropertyGraph {
    pub fn new() -> Self {
        PropertyGraph {
            nodes: Vec::new(),
            relationships: Vec::new(),
            outgoing: Vec::new(),
            incoming: Vec::new(),
        }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn relationship_count(&self) -> usize {
        self.relationships.len()
    }

    pub fn add_node(&mut self, labels: Vec<String>, properties: BTreeMap<String, Value>) -> usize {
        let id = self.nodes.len();
        self.nodes.push(NodeData {
            id,
            labels,
            properties,
        });
        self.outgoing.push(Vec::new());
        self.incoming.push(Vec::new());
        id
    }

    pub fn add_relationship(
        &mut self,
        source: usize,
        target: usize,
        rel_type: String,
        properties: BTreeMap<String, Value>,
    ) -> usize {
        let id = self.relationships.len();
        self.relationships.push(RelData {
            id,
            source,
            target,
            rel_type,
            properties,
        });
        self.outgoing[source].push(id);
        self.incoming[target].push(id);
        id
    }

    pub fn node(&self, id: usize) -> &NodeData {
        &self.nodes[id]
    }

    pub fn relationship(&self, id: usize) -> &RelData {
        &self.relationships[id]
    }

    pub fn out_relationships(&self, node: usize) -> &[usize] {
        &self.outgoing[node]
    }

    pub fn in_relationships(&self, node: usize) -> &[usize] {
        &self.incoming[node]
    }

    /// Convert a node to a CypherValue.
    pub fn node_to_value(&self, id: usize) -> Value {
        let node = &self.nodes[id];
        Value::Node(NodeValue {
            id: node.id,
            labels: node.labels.clone(),
            properties: node.properties.clone(),
        })
    }

    /// Convert a relationship to a CypherValue.
    pub fn rel_to_value(&self, id: usize) -> Value {
        let rel = &self.relationships[id];
        Value::Relationship(RelValue {
            id: rel.id,
            start_node: rel.source,
            end_node: rel.target,
            rel_type: rel.type_name().to_string(),
            properties: rel.properties.clone(),
        })
    }

    /// Check if a node has a specific label.
    pub fn node_has_label(&self, node_id: usize, label: &str) -> bool {
        self.nodes[node_id].labels.iter().any(|l| l == label)
    }

    /// Build a property graph from one or more Cypher statements (typically CREATE).
    /// Used by the TCK test harness to construct test graphs.
    pub fn from_cypher(cypher: &str) -> Result<Self, Error> {
        let mut graph = PropertyGraph::new();
        if cypher.trim().is_empty() {
            return Ok(graph);
        }

        // Parse the Cypher
        let mut lexer = Lexer::new(cypher);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let statement = parser.parse()?;

        graph.execute_creates(&statement)?;

        Ok(graph)
    }

    /// Execute multiple Cypher statements to build the graph.
    pub fn execute_cypher(&mut self, cypher: &str) -> Result<(), Error> {
        if cypher.trim().is_empty() {
            return Ok(());
        }
        let mut lexer = Lexer::new(cypher);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let statement = parser.parse()?;
        self.execute_creates(&statement)
    }

    fn execute_creates(&mut self, statement: &Statement) -> Result<(), Error> {
        // Variable -> node ID mapping for cross-referencing
        let mut var_map: HashMap<String, usize> = HashMap::new();

        for clause in &statement.body {
            if let Clause::Create(patterns) = clause {
                for pattern in patterns {
                    self.create_pattern(pattern, &mut var_map)?;
                }
            }
        }

        Ok(())
    }

    fn create_pattern(
        &mut self,
        pattern: &PatternPath,
        var_map: &mut HashMap<String, usize>,
    ) -> Result<(), Error> {
        // Create or resolve the start node
        let start_id = self.create_or_resolve_node(&pattern.start, var_map)?;

        let mut current_id = start_id;

        for (rel_pat, node_pat) in &pattern.hops {
            let other_id = self.create_or_resolve_node(node_pat, var_map)?;

            // Create relationship
            let props = eval_map_literal(&rel_pat.properties)?;
            let rel_type = rel_pat
                .rel_types
                .first()
                .cloned()
                .unwrap_or_default();

            let (source, target) = match rel_pat.direction {
                Direction::Outgoing | Direction::Both => (current_id, other_id),
                Direction::Incoming => (other_id, current_id),
            };

            let rel_id = self.add_relationship(source, target, rel_type, props);

            if let Some(ref var) = rel_pat.variable {
                var_map.insert(var.clone(), rel_id);
            }

            current_id = other_id;
        }

        Ok(())
    }

    fn create_or_resolve_node(
        &mut self,
        node_pat: &crate::ast::NodePattern,
        var_map: &mut HashMap<String, usize>,
    ) -> Result<usize, Error> {
        // If variable exists, use the existing node (and merge labels/properties)
        if let Some(ref var) = node_pat.variable {
            if let Some(&existing_id) = var_map.get(var) {
                // Merge labels
                let node = &mut self.nodes[existing_id];
                for label in &node_pat.labels {
                    if !node.labels.contains(label) {
                        node.labels.push(label.clone());
                    }
                }
                // Merge properties
                if let Some(ref props) = node_pat.properties {
                    let evaluated = eval_map_literal(&Some(props.clone()))?;
                    for (k, v) in evaluated {
                        node.properties.insert(k, v);
                    }
                }
                return Ok(existing_id);
            }
        }

        // Create new node
        let labels = node_pat.labels.clone();
        let props = eval_map_literal(&node_pat.properties)?;
        let node_id = self.add_node(labels, props);

        if let Some(ref var) = node_pat.variable {
            var_map.insert(var.clone(), node_id);
        }

        Ok(node_id)
    }
}

impl Default for PropertyGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl RelData {
    pub fn type_name(&self) -> &str {
        &self.rel_type
    }
}

impl NodeData {
    pub fn to_value(&self) -> Value {
        Value::Node(NodeValue {
            id: self.id,
            labels: self.labels.clone(),
            properties: self.properties.clone(),
        })
    }
}

impl RelData {
    pub fn to_value(&self) -> Value {
        Value::Relationship(RelValue {
            id: self.id,
            start_node: self.source,
            end_node: self.target,
            rel_type: self.rel_type.clone(),
            properties: self.properties.clone(),
        })
    }
}

/// Evaluate a map literal from parsed AST expressions to concrete values.
/// Only handles literal values (no variable references).
fn eval_map_literal(
    props: &Option<Vec<(String, Expr)>>,
) -> Result<BTreeMap<String, Value>, Error> {
    let mut map = BTreeMap::new();
    if let Some(pairs) = props {
        for (key, expr) in pairs {
            let val = eval_literal_expr(expr)?;
            map.insert(key.clone(), val);
        }
    }
    Ok(map)
}

/// Evaluate a literal expression (no variable bindings needed).
fn eval_literal_expr(expr: &Expr) -> Result<Value, Error> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::UnaryMinus(inner) => {
            let v = eval_literal_expr(inner)?;
            match v {
                Value::Integer(n) => Ok(Value::Integer(-n)),
                Value::Float(f) => Ok(Value::Float(-f)),
                _ => Err(Error::Runtime("cannot negate non-numeric value".to_string())),
            }
        }
        Expr::ListLiteral(items) => {
            let values: Result<Vec<Value>, Error> =
                items.iter().map(eval_literal_expr).collect();
            Ok(Value::List(values?))
        }
        Expr::MapLiteral(pairs) => {
            let mut map = BTreeMap::new();
            for (k, v) in pairs {
                map.insert(k.clone(), eval_literal_expr(v)?);
            }
            Ok(Value::Map(map))
        }
        Expr::Add(l, r) => {
            let lv = eval_literal_expr(l)?;
            let rv = eval_literal_expr(r)?;
            match (&lv, &rv) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{a}{b}"))),
                _ => Err(Error::Runtime("cannot add these types in literal context".to_string())),
            }
        }
        _ => Err(Error::Runtime(format!(
            "non-literal expression in CREATE property: {expr:?}"
        ))),
    }
}

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
        use crate::ast::UnwindClause;
        use crate::expr::eval_expr;
        use crate::pattern_match;

        // Execute clauses as a pipeline of records (for UNWIND/MATCH support)
        let mut records: Vec<BTreeMap<String, Value>> = vec![BTreeMap::new()];
        // Variable -> node ID mapping shared across all CREATE clauses in the statement
        let mut var_map: HashMap<String, usize> = HashMap::new();

        for clause in &statement.body {
            match clause {
                Clause::Create(patterns) => {
                    // For each record in the pipeline, create the patterns
                    for record in &records {
                        for pattern in patterns {
                            self.create_pattern_with_record(pattern, &mut var_map, record)?;
                        }
                    }
                }
                Clause::Unwind(UnwindClause { expr, alias }) => {
                    let params = BTreeMap::new();
                    let mut new_records = Vec::new();
                    for record in &records {
                        let val = eval_expr(expr, record, self, &params)?;
                        if let Value::List(items) = val {
                            for item in items {
                                let mut new_rec = record.clone();
                                new_rec.insert(alias.clone(), item);
                                new_records.push(new_rec);
                            }
                        }
                    }
                    records = new_records;
                }
                Clause::Match(match_clause) => {
                    let params = BTreeMap::new();
                    let mut new_records = Vec::new();
                    for record in &records {
                        let matches = pattern_match::match_patterns_with_params(
                            self,
                            &match_clause.patterns,
                            record,
                            &params,
                        );
                        for matched in matches {
                            if let Some(ref where_expr) = match_clause.where_clause {
                                let val = eval_expr(where_expr, &matched, self, &params)?;
                                if val.is_truthy() {
                                    new_records.push(matched);
                                }
                            } else {
                                new_records.push(matched);
                            }
                        }
                    }
                    records = new_records;
                }
                Clause::With(with_clause) => {
                    use crate::ast::ReturnItems;
                    let params = BTreeMap::new();
                    let mut new_records = Vec::new();
                    for record in &records {
                        let mut new_rec = BTreeMap::new();
                        if let ReturnItems::Expressions(items) = &with_clause.return_body.items {
                            for item in items {
                                let val = eval_expr(&item.expr, record, self, &params)?;
                                let name = item.column_name();
                                new_rec.insert(name, val);
                            }
                        }
                        if let Some(ref where_expr) = with_clause.where_clause {
                            let val = eval_expr(where_expr, &new_rec, self, &params)?;
                            if val.is_truthy() {
                                new_records.push(new_rec);
                            }
                        } else {
                            new_records.push(new_rec);
                        }
                    }
                    records = new_records;
                }
                _ => {} // Skip other clauses (RETURN, etc.)
            }
        }

        Ok(())
    }

    fn create_pattern_with_record(
        &mut self,
        pattern: &PatternPath,
        var_map: &mut HashMap<String, usize>,
        record: &BTreeMap<String, Value>,
    ) -> Result<(), Error> {
        let start_id = self.create_or_resolve_node_with_record(&pattern.start, var_map, record)?;
        let mut current_id = start_id;

        for (rel_pat, node_pat) in &pattern.hops {
            let other_id = self.create_or_resolve_node_with_record(node_pat, var_map, record)?;
            let props = eval_map_literal_with_record(&rel_pat.properties, record)?;
            let rel_type = rel_pat.rel_types.first().cloned().unwrap_or_default();

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

    fn create_or_resolve_node_with_record(
        &mut self,
        node_pat: &crate::ast::NodePattern,
        var_map: &mut HashMap<String, usize>,
        record: &BTreeMap<String, Value>,
    ) -> Result<usize, Error> {
        if let Some(ref var) = node_pat.variable {
            if let Some(&existing_id) = var_map.get(var) {
                let node = &mut self.nodes[existing_id];
                for label in &node_pat.labels {
                    if !node.labels.contains(label) {
                        node.labels.push(label.clone());
                    }
                }
                if let Some(ref props) = node_pat.properties {
                    let evaluated = eval_map_literal_with_record(&Some(props.clone()), record)?;
                    for (k, v) in evaluated {
                        node.properties.insert(k, v);
                    }
                }
                return Ok(existing_id);
            }
        }

        let labels = node_pat.labels.clone();
        let props = eval_map_literal_with_record(&node_pat.properties, record)?;
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

/// Evaluate a map literal with a record for variable resolution.
fn eval_map_literal_with_record(
    props: &Option<Vec<(String, Expr)>>,
    record: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>, Error> {
    let mut map = BTreeMap::new();
    if let Some(pairs) = props {
        for (key, expr) in pairs {
            let val = eval_expr_for_create(expr, record)?;
            map.insert(key.clone(), val);
        }
    }
    Ok(map)
}

/// Evaluate an expression in CREATE context - supports literals and variable references.
fn eval_expr_for_create(expr: &Expr, record: &BTreeMap<String, Value>) -> Result<Value, Error> {
    match expr {
        Expr::Variable(name) => Ok(record.get(name).cloned().unwrap_or(Value::Null)),
        Expr::Parameter(name) => Ok(record.get(name).cloned().unwrap_or(Value::Null)),
        Expr::Property(base, prop) => {
            let base_val = eval_expr_for_create(base, record)?;
            Ok(crate::expr::get_property(&base_val, prop))
        }
        Expr::Add(l, r) => {
            let lv = eval_expr_for_create(l, record)?;
            let rv = eval_expr_for_create(r, record)?;
            match (&lv, &rv) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a + b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{a}{b}"))),
                _ => Ok(Value::Null),
            }
        }
        Expr::Sub(l, r) => {
            let lv = eval_expr_for_create(l, record)?;
            let rv = eval_expr_for_create(r, record)?;
            match (&lv, &rv) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a - b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
                _ => Ok(Value::Null),
            }
        }
        Expr::Mul(l, r) => {
            let lv = eval_expr_for_create(l, record)?;
            let rv = eval_expr_for_create(r, record)?;
            match (&lv, &rv) {
                (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a * b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
                _ => Ok(Value::Null),
            }
        }
        Expr::ListLiteral(items) => {
            let values: Result<Vec<Value>, Error> =
                items.iter().map(|e| eval_expr_for_create(e, record)).collect();
            Ok(Value::List(values?))
        }
        Expr::MapLiteral(pairs) => {
            let mut map = BTreeMap::new();
            for (k, v) in pairs {
                map.insert(k.clone(), eval_expr_for_create(v, record)?);
            }
            Ok(Value::Map(map))
        }
        Expr::FunctionCall { name, args, .. } => {
            // Handle some common functions for CREATE context
            let evaluated: Result<Vec<Value>, Error> =
                args.iter().map(|a| eval_expr_for_create(a, record)).collect();
            let args_val = evaluated?;
            match name.to_ascii_lowercase().as_str() {
                "tostring" => match args_val.first() {
                    Some(Value::Integer(n)) => Ok(Value::String(n.to_string())),
                    Some(Value::Float(f)) => Ok(Value::String(f.to_string())),
                    Some(Value::String(s)) => Ok(Value::String(s.clone())),
                    Some(Value::Bool(b)) => Ok(Value::String(b.to_string())),
                    _ => Ok(Value::Null),
                },
                "tointeger" => match args_val.first() {
                    Some(Value::Integer(n)) => Ok(Value::Integer(*n)),
                    Some(Value::String(s)) => Ok(s.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null)),
                    _ => Ok(Value::Null),
                },
                _ => eval_literal_expr(expr),
            }
        }
        _ => eval_literal_expr(expr),
    }
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
        Expr::FunctionCall { name, args, .. } => {
            // Handle temporal functions in CREATE by storing as string representation
            match name.to_ascii_lowercase().as_str() {
                "date" | "datetime" | "localdatetime" | "time" | "localtime" | "duration" => {
                    // Evaluate args and build a string representation
                    let evaluated_args: Result<Vec<Value>, Error> =
                        args.iter().map(eval_literal_expr).collect();
                    let args_val = evaluated_args?;
                    Ok(Value::String(format!("{}({:?})", name, args_val)))
                }
                "point" => {
                    let evaluated_args: Result<Vec<Value>, Error> =
                        args.iter().map(eval_literal_expr).collect();
                    let args_val = evaluated_args?;
                    Ok(Value::String(format!("point({:?})", args_val)))
                }
                _ => Err(Error::Runtime(format!(
                    "non-literal expression in CREATE property: {expr:?}"
                ))),
            }
        }
        _ => Err(Error::Runtime(format!(
            "non-literal expression in CREATE property: {expr:?}"
        ))),
    }
}

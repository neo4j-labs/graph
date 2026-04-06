use std::collections::BTreeMap;

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

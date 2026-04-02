//! Topology-only Cypher query engine for [`graph_builder`] CSR graphs.
//!
//! Parses a subset of Cypher, executes it against a directed graph, and
//! returns results as an iterator of [`Row`]s.
//!
//! # Supported syntax
//!
//! | Pattern            | Matches                              |
//! |--------------------|--------------------------------------|
//! | `(n)`              | every node                           |
//! | `(a)-[r]->(b)`     | every directed edge a → b            |
//! | `(a)<-[r]-(b)`     | every directed edge b → a            |
//! | `(a)-[r]-(b)`      | every edge in either direction       |
//! | `(a)-->(b)`        | shorthand for `(a)-[r]->(b)`         |
//! | `(a)<--(b)`        | shorthand for `(a)<-[r]-(b)`         |
//! | `(a)--(b)`         | shorthand for `(a)-[r]-(b)`          |
//!
//! A `WHERE id(var) = N` clause may be appended to filter results by node ID.
//!
//! # Example
//!
//! ```
//! use graph_builder::prelude::*;
//! use graph_cypher::{CypherEngine, Value};
//!
//! let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
//!     .edges(vec![(0, 1), (0, 2), (1, 2)])
//!     .build();
//!
//! let engine = CypherEngine::new(&graph);
//!
//! let rows: Vec<_> = engine
//!     .execute("MATCH (a)-[r]->(b) WHERE id(a) = 0 RETURN b")
//!     .unwrap()
//!     .collect();
//!
//! assert_eq!(rows.len(), 2);
//! assert!(matches!(rows[0].get("b"), Some(Value::Node(_))));
//! ```

mod ast;
mod lexer;
mod parser;

pub mod executor;

pub use error::Error;
pub use executor::{CypherEngine, Row, Value};

pub mod error;

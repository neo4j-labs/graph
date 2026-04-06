//! openCypher query engine for property graphs.
//!
//! Parses openCypher queries and executes them against a [`PropertyGraph`].
//! Supports pattern matching, filtering, aggregation, ordering, and more.

pub mod aggregation;
pub mod ast;
pub mod error;
pub mod executor;
pub mod expr;
pub mod functions;
pub mod graph;
pub mod lexer;
pub mod parser;
pub mod pattern_match;
pub mod token;
pub mod value;

pub use error::Error;
pub use executor::{CypherEngine, Row};
pub use graph::PropertyGraph;
pub use value::Value;

//! openCypher query engine for property graphs.
//!
//! Parses openCypher queries and executes them against a [`PropertyGraph`].
//! Supports pattern matching, filtering, aggregation, ordering, and more.

pub mod ast;
pub mod error;
pub mod graph;
pub mod lexer;
pub mod parser;
pub mod token;
pub mod value;

pub use error::Error;
pub use graph::PropertyGraph;
pub use value::Value;

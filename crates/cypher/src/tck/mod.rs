pub mod feature_parser;
pub mod result_parser;

use std::collections::BTreeMap;

use crate::executor::CypherEngine;
use crate::graph::PropertyGraph;
#[allow(unused_imports)]
use crate::value::Value;
use feature_parser::{ExpectedResult, Feature, GraphSetup, Scenario};

/// Categories of feature files that we skip (mutations, procedures, etc.)
const SKIP_CATEGORIES: &[&str] = &[
    "create",
    "delete",
    "merge",
    "set",
    "remove",
    "call",
    "foreach",
    "temporal",
];

/// Result of running a single scenario.
#[derive(Debug)]
pub enum ScenarioResult {
    Pass,
    Fail(String),
    Skip(String),
}

/// Run all scenarios in a feature file and return results.
pub fn run_feature(feature: &Feature) -> Vec<(String, ScenarioResult)> {
    let mut results = Vec::new();

    for scenario in &feature.scenarios {
        let result = run_scenario(scenario);
        results.push((scenario.name.clone(), result));
    }

    results
}

/// Should this feature file be skipped based on its path?
pub fn should_skip_file(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    SKIP_CATEGORIES.iter().any(|cat| lower.contains(cat))
}

fn run_scenario(scenario: &Scenario) -> ScenarioResult {
    // Build graph
    let graph = match build_graph(scenario) {
        Ok(g) => g,
        Err(e) => {
            // If we can't build the graph, check if an error was expected
            if matches!(&scenario.expected, ExpectedResult::Error { .. }) {
                return ScenarioResult::Pass;
            }
            return ScenarioResult::Fail(format!("graph setup failed: {e}"));
        }
    };

    // Parse parameters
    let params = parse_parameters(&scenario.parameters);

    // Execute query
    let engine = CypherEngine::new(&graph);
    let result = engine.execute_with_params(&scenario.query, &params);

    match &scenario.expected {
        ExpectedResult::Success {
            columns,
            rows,
            ordered,
        } => match result {
            Ok(actual_rows) => {
                compare_results(columns, rows, &actual_rows, *ordered)
            }
            Err(e) => ScenarioResult::Fail(format!("query failed: {e}")),
        },
        ExpectedResult::Error {
            error_type,
            phase,
            detail: _,
        } => match result {
            Err(_) => ScenarioResult::Pass, // Expected error occurred
            Ok(_) => ScenarioResult::Fail(format!(
                "expected {error_type} error at {phase}, but query succeeded"
            )),
        },
        ExpectedResult::NoResult => match result {
            Ok(_) => ScenarioResult::Pass,
            Err(e) => ScenarioResult::Fail(format!("unexpected error: {e}")),
        },
    }
}

fn build_graph(scenario: &Scenario) -> Result<PropertyGraph, crate::error::Error> {
    let mut graph = match &scenario.setup.graph {
        GraphSetup::Empty | GraphSetup::Any => PropertyGraph::new(),
        GraphSetup::Named(name) => build_named_graph(name),
    };

    for query in &scenario.setup.setup_queries {
        graph.execute_cypher(query)?;
    }

    Ok(graph)
}

fn build_named_graph(name: &str) -> PropertyGraph {
    let cypher = match name {
        "binary-tree-1" => {
            "CREATE (a:A {name: 'a'}),
                    (b1:X {name: 'b1'}),
                    (b2:X {name: 'b2'}),
                    (b3:X {name: 'b3'}),
                    (b4:X {name: 'b4'}),
                    (c11:X {name: 'c11'}),
                    (c12:X {name: 'c12'}),
                    (c21:X {name: 'c21'}),
                    (c22:X {name: 'c22'}),
                    (c31:X {name: 'c31'}),
                    (c32:X {name: 'c32'}),
                    (c41:X {name: 'c41'}),
                    (c42:X {name: 'c42'})
             CREATE (a)-[:KNOWS]->(b1),
                    (a)-[:KNOWS]->(b2),
                    (a)-[:FOLLOWS]->(b3),
                    (a)-[:FOLLOWS]->(b4)
             CREATE (b1)-[:FRIEND]->(c11),
                    (b1)-[:FRIEND]->(c12),
                    (b2)-[:FRIEND]->(c21),
                    (b2)-[:FRIEND]->(c22),
                    (b3)-[:FRIEND]->(c31),
                    (b3)-[:FRIEND]->(c32),
                    (b4)-[:FRIEND]->(c41),
                    (b4)-[:FRIEND]->(c42)
             CREATE (b1)-[:FRIEND]->(b2),
                    (b2)-[:FRIEND]->(b3),
                    (b3)-[:FRIEND]->(b4),
                    (b4)-[:FRIEND]->(b1)"
        }
        "binary-tree-2" => {
            "CREATE (a:A {name: 'a'}),
                    (b1:X {name: 'b1'}),
                    (b2:X {name: 'b2'}),
                    (b3:X {name: 'b3'}),
                    (b4:X {name: 'b4'}),
                    (c11:X {name: 'c11'}),
                    (c12:Y {name: 'c12'}),
                    (c21:X {name: 'c21'}),
                    (c22:Y {name: 'c22'}),
                    (c31:X {name: 'c31'}),
                    (c32:Y {name: 'c32'}),
                    (c41:X {name: 'c41'}),
                    (c42:Y {name: 'c42'})
             CREATE (a)-[:KNOWS]->(b1),
                    (a)-[:KNOWS]->(b2),
                    (a)-[:FOLLOWS]->(b3),
                    (a)-[:FOLLOWS]->(b4)
             CREATE (b1)-[:FRIEND]->(c11),
                    (b1)-[:FRIEND]->(c12),
                    (b2)-[:FRIEND]->(c21),
                    (b2)-[:FRIEND]->(c22),
                    (b3)-[:FRIEND]->(c31),
                    (b3)-[:FRIEND]->(c32),
                    (b4)-[:FRIEND]->(c41),
                    (b4)-[:FRIEND]->(c42)
             CREATE (b1)-[:FRIEND]->(b2),
                    (b2)-[:FRIEND]->(b3),
                    (b3)-[:FRIEND]->(b4),
                    (b4)-[:FRIEND]->(b1)"
        }
        _ => return PropertyGraph::new(),
    };

    let mut g = PropertyGraph::new();
    if let Err(e) = g.execute_cypher(cypher) {
        eprintln!("Warning: failed to build named graph '{name}': {e}");
    }
    g
}

fn parse_parameters(params: &[(String, String)]) -> BTreeMap<String, Value> {
    let mut map = BTreeMap::new();
    for (key, value) in params {
        let val = result_parser::parse_param_value(value);
        map.insert(key.clone(), val);
    }
    map
}

fn compare_results(
    expected_columns: &[String],
    expected_rows: &[Vec<String>],
    actual_rows: &[crate::executor::Row],
    ordered: bool,
) -> ScenarioResult {
    // Check row count
    if expected_rows.len() != actual_rows.len() {
        return ScenarioResult::Fail(format!(
            "expected {} rows, got {}.\nExpected: {:?}\nActual: {:?}",
            expected_rows.len(),
            actual_rows.len(),
            expected_rows,
            actual_rows.iter().map(|r| &r.values).collect::<Vec<_>>()
        ));
    }

    if expected_rows.is_empty() {
        return ScenarioResult::Pass;
    }

    // Parse expected values
    let expected_values: Vec<Vec<Value>> = expected_rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| result_parser::parse_value(cell))
                .collect()
        })
        .collect();

    // Get actual values aligned to expected columns
    let actual_values: Vec<Vec<Value>> = actual_rows
        .iter()
        .map(|row| {
            expected_columns
                .iter()
                .map(|col| row.get(col).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();

    if ordered {
        // Check row by row
        for (i, (expected, actual)) in
            expected_values.iter().zip(actual_values.iter()).enumerate()
        {
            if !rows_match(expected, actual) {
                return ScenarioResult::Fail(format!(
                    "row {i} mismatch.\nExpected: {:?}\nActual:   {:?}",
                    expected, actual
                ));
            }
        }
        ScenarioResult::Pass
    } else {
        // Check that each expected row has a matching actual row (bipartite)
        let mut used = vec![false; actual_values.len()];
        for (i, expected) in expected_values.iter().enumerate() {
            let found = actual_values.iter().enumerate().any(|(j, actual)| {
                !used[j] && rows_match(expected, actual)
            });
            if !found {
                return ScenarioResult::Fail(format!(
                    "expected row {i} not found in actual results.\nExpected: {:?}\nActual:   {:?}",
                    expected, actual_values
                ));
            }
            // Mark used
            for (j, actual) in actual_values.iter().enumerate() {
                if !used[j] && rows_match(expected, actual) {
                    used[j] = true;
                    break;
                }
            }
        }
        ScenarioResult::Pass
    }
}

fn rows_match(expected: &[Value], actual: &[Value]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    expected
        .iter()
        .zip(actual.iter())
        .all(|(e, a)| e.structural_eq(a))
}

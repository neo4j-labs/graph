pub mod feature_parser;
pub mod result_parser;

use std::collections::BTreeMap;

use crate::executor::CypherEngine;
use crate::graph::PropertyGraph;
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
    // Pre-defined named graphs from the TCK
    match name {
        "binary-tree-1" => build_binary_tree_1(),
        "binary-tree-2" => build_binary_tree_2(),
        _ => PropertyGraph::new(),
    }
}

fn build_binary_tree_1() -> PropertyGraph {
    let mut g = PropertyGraph::new();
    // Root
    let root = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("a".into()))]));
    let b = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("b".into()))]));
    let c = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("c".into()))]));
    let d = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("d".into()))]));
    let e = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("e".into()))]));
    let f = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("f".into()))]));
    let gg = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("g".into()))]));

    g.add_relationship(root, b, "T".into(), BTreeMap::new());
    g.add_relationship(root, c, "T".into(), BTreeMap::new());
    g.add_relationship(b, d, "T".into(), BTreeMap::new());
    g.add_relationship(b, e, "T".into(), BTreeMap::new());
    g.add_relationship(c, f, "T".into(), BTreeMap::new());
    g.add_relationship(c, gg, "T".into(), BTreeMap::new());
    g
}

fn build_binary_tree_2() -> PropertyGraph {
    let mut g = build_binary_tree_1();
    // Add second level of children
    let h = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("h".into()))]));
    let i = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("i".into()))]));
    let j = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("j".into()))]));
    let k = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("k".into()))]));
    let l = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("l".into()))]));
    let m = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("m".into()))]));
    let n = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("n".into()))]));
    let o = g.add_node(vec![], BTreeMap::from([("name".into(), Value::String("o".into()))]));

    // d=3, e=4, f=5, g=6
    g.add_relationship(3, h, "T".into(), BTreeMap::new());
    g.add_relationship(3, i, "T".into(), BTreeMap::new());
    g.add_relationship(4, j, "T".into(), BTreeMap::new());
    g.add_relationship(4, k, "T".into(), BTreeMap::new());
    g.add_relationship(5, l, "T".into(), BTreeMap::new());
    g.add_relationship(5, m, "T".into(), BTreeMap::new());
    g.add_relationship(6, n, "T".into(), BTreeMap::new());
    g.add_relationship(6, o, "T".into(), BTreeMap::new());
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

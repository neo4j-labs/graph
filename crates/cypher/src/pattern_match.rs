use std::collections::{BTreeMap, HashSet};

use crate::ast::{Direction, NodePattern, PathLength, PatternPath, RelPattern};
use crate::expr::{eval_expr, Params, Record};
use crate::graph::PropertyGraph;
use crate::value::{PathValue, Value};

/// Match a single pattern path against the graph, producing records.
pub fn match_pattern(graph: &PropertyGraph, pattern: &PatternPath, input: &Record) -> Vec<Record> {
    match_pattern_with_params(graph, pattern, input, &BTreeMap::new())
}

pub fn match_pattern_with_params(
    graph: &PropertyGraph,
    pattern: &PatternPath,
    input: &Record,
    params: &Params,
) -> Vec<Record> {
    let mut results = Vec::new();

    // Start with all candidate nodes for the start pattern
    let start_candidates = candidate_nodes(graph, &pattern.start, input, params);

    // State: (record, current_node, used_rels, path_node_ids, path_rel_ids)
    type State = (Record, usize, HashSet<usize>, Vec<usize>, Vec<usize>);

    for node_id in start_candidates {
        let mut record = input.clone();
        if let Some(ref var) = pattern.start.variable {
            if let Some(existing) = record.get(var) {
                // Variable already bound - check it matches
                if let Value::Node(n) = existing {
                    if n.id != node_id {
                        continue;
                    }
                } else {
                    continue;
                }
            }
            record.insert(var.clone(), graph.node_to_value(node_id));
        }

        if pattern.hops.is_empty() {
            // Zero-length path
            if let Some(ref path_var) = pattern.variable {
                let path = PathValue {
                    nodes: vec![graph.node_value(node_id)],
                    relationships: vec![],
                };
                record.insert(path_var.clone(), Value::Path(path));
            }
            results.push(record);
        } else {
            // Extend through hops
            let mut current_records: Vec<State> =
                vec![(record, node_id, HashSet::new(), vec![node_id], Vec::new())];

            for (rel_pat, node_pat) in &pattern.hops {
                let mut next_records: Vec<State> = Vec::new();

                for (rec, current_node, used_rels, path_nodes, path_rels) in &current_records {
                    match &rel_pat.length {
                        None => {
                            // Single hop
                            let edges = get_edges(graph, *current_node, &rel_pat.direction);
                            for (rel_id, other_node) in edges {
                                if used_rels.contains(&rel_id) {
                                    continue; // Relationship uniqueness
                                }
                                if !rel_matches(graph, rel_id, rel_pat, rec, params) {
                                    continue;
                                }
                                if !node_matches(graph, other_node, node_pat, rec, params) {
                                    continue;
                                }
                                let mut new_rec = rec.clone();
                                let mut new_used = used_rels.clone();
                                new_used.insert(rel_id);

                                if let Some(ref var) = rel_pat.variable {
                                    new_rec.insert(var.clone(), graph.rel_to_value(rel_id));
                                }
                                if let Some(ref var) = node_pat.variable {
                                    if let Some(existing) = rec.get(var) {
                                        if let Value::Node(n) = existing {
                                            if n.id != other_node {
                                                continue;
                                            }
                                        }
                                    }
                                    new_rec
                                        .insert(var.clone(), graph.node_to_value(other_node));
                                }

                                let mut new_path_nodes = path_nodes.clone();
                                new_path_nodes.push(other_node);
                                let mut new_path_rels = path_rels.clone();
                                new_path_rels.push(rel_id);

                                next_records.push((
                                    new_rec,
                                    other_node,
                                    new_used,
                                    new_path_nodes,
                                    new_path_rels,
                                ));
                            }
                        }
                        Some(length) => {
                            // Variable-length path
                            let (min, max) = match length {
                                PathLength::Exact(n) => (*n, *n),
                                PathLength::Range(min, max) => {
                                    (min.unwrap_or(1), max.unwrap_or(15))
                                }
                            };
                            let vl_results = expand_variable_length(
                                graph,
                                *current_node,
                                rel_pat,
                                node_pat,
                                min,
                                max,
                                used_rels,
                                rec,
                                params,
                            );
                            for (vl_rec, end_node, vl_used, vl_path_nodes, vl_path_rels) in
                                vl_results
                            {
                                let mut new_path_nodes = path_nodes.clone();
                                new_path_nodes.extend_from_slice(&vl_path_nodes);
                                let mut new_path_rels = path_rels.clone();
                                new_path_rels.extend_from_slice(&vl_path_rels);
                                next_records.push((
                                    vl_rec,
                                    end_node,
                                    vl_used,
                                    new_path_nodes,
                                    new_path_rels,
                                ));
                            }
                        }
                    }
                }

                current_records = next_records;
            }

            // Build path value from tracked traversal
            for (mut rec, _, _, path_nodes, path_rels) in current_records {
                if let Some(ref path_var) = pattern.variable {
                    let path = PathValue {
                        nodes: path_nodes.iter().map(|&id| graph.node_value(id)).collect(),
                        relationships: path_rels
                            .iter()
                            .map(|&id| graph.rel_value(id))
                            .collect(),
                    };
                    rec.insert(path_var.clone(), Value::Path(path));
                }
                results.push(rec);
            }
        }
    }

    results
}

/// Match multiple comma-separated patterns (cross-join with shared variables).
pub fn match_patterns(
    graph: &PropertyGraph,
    patterns: &[PatternPath],
    input: &Record,
) -> Vec<Record> {
    match_patterns_with_params(graph, patterns, input, &BTreeMap::new())
}

pub fn match_patterns_with_params(
    graph: &PropertyGraph,
    patterns: &[PatternPath],
    input: &Record,
    params: &Params,
) -> Vec<Record> {
    let mut records = vec![input.clone()];

    for pattern in patterns {
        let mut next_records = Vec::new();
        for rec in &records {
            let matches = match_pattern_with_params(graph, pattern, rec, params);
            next_records.extend(matches);
        }
        records = next_records;
    }

    records
}

/// Get candidate nodes for a node pattern.
fn candidate_nodes(
    graph: &PropertyGraph,
    pattern: &NodePattern,
    record: &Record,
    params: &Params,
) -> Vec<usize> {
    // If variable is already bound, use that single node
    if let Some(ref var) = pattern.variable {
        if let Some(Value::Node(n)) = record.get(var) {
            let id = n.id;
            if node_matches(graph, id, pattern, record, params) {
                return vec![id];
            } else {
                return Vec::new();
            }
        }
    }

    let mut candidates: Vec<usize> = (0..graph.node_count()).collect();

    // Filter by labels
    for label in &pattern.labels {
        candidates.retain(|&id| graph.node_has_label(id, label));
    }

    // Filter by inline properties
    if let Some(ref props) = pattern.properties {
        candidates.retain(|&id| {
            let node = graph.node(id);
            props.iter().all(|(key, expr)| {
                let expected = eval_expr(expr, record, graph, params).unwrap_or(Value::Null);
                let actual = node.properties.get(key).cloned().unwrap_or(Value::Null);
                matches!(actual.cypher_eq(&expected), Value::Bool(true))
            })
        });
    }

    candidates
}

fn node_matches(
    graph: &PropertyGraph,
    node_id: usize,
    pattern: &NodePattern,
    record: &Record,
    params: &Params,
) -> bool {
    let node = graph.node(node_id);

    // Check labels
    for label in &pattern.labels {
        if !node.labels.contains(label) {
            return false;
        }
    }

    // Check inline properties
    if let Some(ref props) = pattern.properties {
        for (key, expr) in props {
            let expected = eval_expr(expr, record, graph, params).unwrap_or(Value::Null);
            let actual = node.properties.get(key).cloned().unwrap_or(Value::Null);
            if !matches!(actual.cypher_eq(&expected), Value::Bool(true)) {
                return false;
            }
        }
    }

    true
}

fn rel_matches(
    graph: &PropertyGraph,
    rel_id: usize,
    pattern: &RelPattern,
    record: &Record,
    params: &Params,
) -> bool {
    let rel = graph.relationship(rel_id);

    // Check relationship types
    if !pattern.rel_types.is_empty() && !pattern.rel_types.contains(&rel.rel_type) {
        return false;
    }

    // Check variable binding
    if let Some(ref var) = pattern.variable {
        if let Some(existing) = record.get(var) {
            if let Value::Relationship(r) = existing {
                if r.id != rel_id {
                    return false;
                }
            }
        }
    }

    // Check inline properties
    if let Some(ref props) = pattern.properties {
        for (key, expr) in props {
            let expected = eval_expr(expr, record, graph, params).unwrap_or(Value::Null);
            let actual = rel.properties.get(key).cloned().unwrap_or(Value::Null);
            if !matches!(actual.cypher_eq(&expected), Value::Bool(true)) {
                return false;
            }
        }
    }

    true
}

/// Get edges from a node based on direction.
/// Returns (relationship_id, other_node_id) pairs.
fn get_edges(graph: &PropertyGraph, node: usize, direction: &Direction) -> Vec<(usize, usize)> {
    let mut edges = Vec::new();

    match direction {
        Direction::Outgoing => {
            for &rel_id in graph.out_relationships(node) {
                let rel = graph.relationship(rel_id);
                edges.push((rel_id, rel.target));
            }
        }
        Direction::Incoming => {
            for &rel_id in graph.in_relationships(node) {
                let rel = graph.relationship(rel_id);
                edges.push((rel_id, rel.source));
            }
        }
        Direction::Both => {
            let mut seen = HashSet::new();
            for &rel_id in graph.out_relationships(node) {
                let rel = graph.relationship(rel_id);
                seen.insert(rel_id);
                edges.push((rel_id, rel.target));
            }
            for &rel_id in graph.in_relationships(node) {
                if seen.contains(&rel_id) {
                    // Self-loop already seen from outgoing side
                    continue;
                }
                let rel = graph.relationship(rel_id);
                edges.push((rel_id, rel.source));
            }
        }
    }

    edges
}

/// Expand variable-length paths using DFS.
#[allow(clippy::too_many_arguments)]
/// Returns (record, end_node, used_rels, path_node_ids, path_rel_ids)
fn expand_variable_length(
    graph: &PropertyGraph,
    start: usize,
    rel_pat: &RelPattern,
    end_pat: &NodePattern,
    min: usize,
    max: usize,
    used_rels: &HashSet<usize>,
    record: &Record,
    params: &Params,
) -> Vec<(Record, usize, HashSet<usize>, Vec<usize>, Vec<usize>)> {
    let mut results = Vec::new();

    // DFS state: (current_node, depth, used_rels, path_rels, path_nodes)
    let mut stack: Vec<(usize, usize, HashSet<usize>, Vec<usize>, Vec<usize>)> = Vec::new();
    stack.push((start, 0, used_rels.clone(), Vec::new(), vec![start]));

    while let Some((current, depth, used, path_rels, path_nodes)) = stack.pop() {
        // At valid depth and end node matches
        if depth >= min && node_matches(graph, current, end_pat, record, params) {
            let mut new_rec = record.clone();

            // Bind the relationship variable to a list of relationships
            if let Some(ref var) = rel_pat.variable {
                let rel_list: Vec<Value> = path_rels
                    .iter()
                    .map(|&rid| graph.rel_to_value(rid))
                    .collect();
                new_rec.insert(var.clone(), Value::List(rel_list));
            }
            if let Some(ref var) = end_pat.variable {
                if let Some(existing) = record.get(var) {
                    if let Value::Node(n) = existing {
                        if n.id != current {
                            continue;
                        }
                    }
                }
                new_rec.insert(var.clone(), graph.node_to_value(current));
            }
            // Return path nodes excluding start (caller prepends its own prefix)
            // and all path rels from this variable-length expansion
            results.push((
                new_rec,
                current,
                used.clone(),
                path_nodes[1..].to_vec(),
                path_rels.clone(),
            ));
        }

        if depth < max {
            let edges = get_edges(graph, current, &rel_pat.direction);
            for (rel_id, other) in edges {
                if used.contains(&rel_id) {
                    continue;
                }
                // Check rel type
                let rel = graph.relationship(rel_id);
                if !rel_pat.rel_types.is_empty() && !rel_pat.rel_types.contains(&rel.rel_type) {
                    continue;
                }
                let mut new_used = used.clone();
                new_used.insert(rel_id);
                let mut new_path_rels = path_rels.clone();
                new_path_rels.push(rel_id);
                let mut new_path_nodes = path_nodes.clone();
                new_path_nodes.push(other);
                stack.push((other, depth + 1, new_used, new_path_rels, new_path_nodes));
            }
        }
    }

    results
}


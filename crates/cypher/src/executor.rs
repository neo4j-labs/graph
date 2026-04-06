use std::collections::BTreeMap;

use crate::aggregation::aggregate;
use crate::ast::*;
use crate::error::Error;
use crate::expr::{eval_expr, Params, Record};
use crate::graph::PropertyGraph;
use crate::lexer::Lexer;
use crate::parser::Parser;
use crate::pattern_match;
use crate::value::Value;

/// Result row with named columns.
#[derive(Debug, Clone)]
pub struct Row {
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

impl Row {
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .position(|c| c == column)
            .map(|i| &self.values[i])
    }
}

/// The Cypher query engine.
pub struct CypherEngine<'g> {
    graph: &'g PropertyGraph,
}

impl<'g> CypherEngine<'g> {
    pub fn new(graph: &'g PropertyGraph) -> Self {
        CypherEngine { graph }
    }

    /// Execute a Cypher query and return result rows.
    pub fn execute(&self, query: &str) -> Result<Vec<Row>, Error> {
        self.execute_with_params(query, &BTreeMap::new())
    }

    /// Execute a Cypher query with parameters.
    pub fn execute_with_params(
        &self,
        query: &str,
        params: &Params,
    ) -> Result<Vec<Row>, Error> {
        let mut lexer = Lexer::new(query);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let statement = parser.parse()?;
        self.run_statement(&statement, params)
    }

    fn run_statement(
        &self,
        statement: &Statement,
        params: &Params,
    ) -> Result<Vec<Row>, Error> {
        let mut rows = self.run_clauses(&statement.body, params)?;

        for union in &statement.unions {
            let union_rows = self.run_clauses(&union.body, params)?;
            if union.all {
                rows.extend(union_rows);
            } else {
                // UNION: deduplicate
                for row in union_rows {
                    let exists = rows.iter().any(|r| {
                        r.columns == row.columns
                            && r.values
                                .iter()
                                .zip(row.values.iter())
                                .all(|(a, b)| a.structural_eq(b))
                    });
                    if !exists {
                        rows.push(row);
                    }
                }
            }
        }

        Ok(rows)
    }

    fn run_clauses(
        &self,
        clauses: &[Clause],
        params: &Params,
    ) -> Result<Vec<Row>, Error> {
        let mut records: Vec<Record> = vec![BTreeMap::new()];

        for clause in clauses {
            records = self.run_clause(clause, records, params)?;
        }

        // If the last clause was not a RETURN, create rows from records
        if let Some(last) = clauses.last() {
            if matches!(last, Clause::Return(_)) {
                // Already handled - records are the projected results
                // (but we returned rows from run_clause for RETURN)
                // Actually we need to handle this differently
            }
        }

        // Check if the final clause is a RETURN - if so, records are already rows
        // Otherwise convert records to rows
        Ok(records
            .into_iter()
            .map(|rec| {
                let columns: Vec<String> = rec.keys().cloned().collect();
                let values: Vec<Value> = rec.values().cloned().collect();
                Row { columns, values }
            })
            .collect())
    }

    fn run_clause(
        &self,
        clause: &Clause,
        records: Vec<Record>,
        params: &Params,
    ) -> Result<Vec<Record>, Error> {
        match clause {
            Clause::Match(match_clause) => {
                self.run_match(match_clause, records, false, params)
            }
            Clause::OptionalMatch(match_clause) => {
                self.run_match(match_clause, records, true, params)
            }
            Clause::With(with_clause) => self.run_with(with_clause, records, params),
            Clause::Unwind(unwind_clause) => {
                self.run_unwind(unwind_clause, records, params)
            }
            Clause::Return(return_clause) => {
                self.run_return(return_clause, records, params)
            }
            Clause::Create(_patterns) => {
                // CREATE is only used for test setup; should not appear in query execution
                Err(Error::Unsupported(
                    "CREATE is not supported in query execution".to_string(),
                ))
            }
        }
    }

    fn run_match(
        &self,
        match_clause: &MatchClause,
        records: Vec<Record>,
        optional: bool,
        params: &Params,
    ) -> Result<Vec<Record>, Error> {
        let mut results = Vec::new();

        for record in &records {
            let matches = pattern_match::match_patterns_with_params(
                self.graph,
                &match_clause.patterns,
                record,
                params,
            );

            if matches.is_empty() && optional {
                // OPTIONAL MATCH: keep input record with null bindings
                let mut null_rec = record.clone();
                for pattern in &match_clause.patterns {
                    add_null_bindings(&mut null_rec, pattern);
                }
                results.push(null_rec);
            } else {
                // Filter by WHERE clause
                let filtered: Vec<Record> = if let Some(ref where_expr) = match_clause.where_clause
                {
                    matches
                        .into_iter()
                        .filter(|r| {
                            eval_expr(where_expr, r, self.graph, params)
                                .map(|v| v.is_truthy())
                                .unwrap_or(false)
                        })
                        .collect()
                } else {
                    matches
                };

                if filtered.is_empty() && optional {
                    let mut null_rec = record.clone();
                    for pattern in &match_clause.patterns {
                        add_null_bindings(&mut null_rec, pattern);
                    }
                    results.push(null_rec);
                } else {
                    results.extend(filtered);
                }
            }
        }

        Ok(results)
    }

    fn run_return(
        &self,
        return_clause: &ReturnClause,
        records: Vec<Record>,
        params: &Params,
    ) -> Result<Vec<Record>, Error> {
        self.run_projection(return_clause, records, params)
    }

    fn run_with(
        &self,
        with_clause: &WithClause,
        records: Vec<Record>,
        params: &Params,
    ) -> Result<Vec<Record>, Error> {
        // Apply WHERE clause BEFORE projection (WHERE sees pre-projection variables)
        let filtered = if let Some(ref where_expr) = with_clause.where_clause {
            records
                .into_iter()
                .filter(|r| {
                    eval_expr(where_expr, r, self.graph, params)
                        .map(|v| v.is_truthy())
                        .unwrap_or(false)
                })
                .collect()
        } else {
            records
        };

        self.run_projection(&with_clause.return_body, filtered, params)
    }

    fn run_projection(
        &self,
        clause: &ReturnClause,
        records: Vec<Record>,
        params: &Params,
    ) -> Result<Vec<Record>, Error> {
        // Handle RETURN *
        let items = match &clause.items {
            ReturnItems::Star => {
                // Project all variables
                if records.is_empty() {
                    return Ok(Vec::new());
                }
                let columns: Vec<ReturnItem> = records[0]
                    .keys()
                    .map(|k| ReturnItem {
                        expr: Expr::Variable(k.clone()),
                        alias: None,
                    })
                    .collect();
                columns
            }
            ReturnItems::Expressions(items) => items.clone(),
        };

        // Check for aggregation
        let has_agg = items.iter().any(|item| crate::aggregation::is_aggregation(&item.expr));

        let mut result_records = if has_agg {
            aggregate(records, &items, self.graph, params)?
        } else {
            // Simple projection
            records
                .iter()
                .map(|rec| {
                    let mut new_rec = Record::new();
                    for item in &items {
                        let col = item.column_name();
                        let val = eval_expr(&item.expr, rec, self.graph, params)?;
                        new_rec.insert(col, val);
                    }
                    Ok(new_rec)
                })
                .collect::<Result<Vec<_>, Error>>()?
        };

        // DISTINCT
        if clause.distinct {
            let mut seen: Vec<Record> = Vec::new();
            result_records.retain(|rec| {
                let exists = seen.iter().any(|s| {
                    s.len() == rec.len()
                        && s.iter()
                            .all(|(k, v)| rec.get(k).map_or(false, |rv| v.structural_eq(rv)))
                });
                if exists {
                    false
                } else {
                    seen.push(rec.clone());
                    true
                }
            });
        }

        // ORDER BY
        if let Some(ref order_items) = clause.order_by {
            result_records.sort_by(|a, b| {
                for sort_item in order_items {
                    let va = eval_expr(&sort_item.expr, a, self.graph, params)
                        .unwrap_or(Value::Null);
                    let vb = eval_expr(&sort_item.expr, b, self.graph, params)
                        .unwrap_or(Value::Null);
                    let ord = va.order_cmp(&vb);
                    let ord = match sort_item.direction {
                        SortDirection::Asc => ord,
                        SortDirection::Desc => ord.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // SKIP
        if let Some(ref skip_expr) = clause.skip {
            let skip_val = eval_expr(skip_expr, &Record::new(), self.graph, params)?;
            if let Value::Integer(n) = skip_val {
                let n = n.max(0) as usize;
                if n >= result_records.len() {
                    result_records.clear();
                } else {
                    result_records = result_records[n..].to_vec();
                }
            }
        }

        // LIMIT
        if let Some(ref limit_expr) = clause.limit {
            let limit_val = eval_expr(limit_expr, &Record::new(), self.graph, params)?;
            if let Value::Integer(n) = limit_val {
                let n = n.max(0) as usize;
                result_records.truncate(n);
            }
        }

        Ok(result_records)
    }

    fn run_unwind(
        &self,
        unwind: &UnwindClause,
        records: Vec<Record>,
        params: &Params,
    ) -> Result<Vec<Record>, Error> {
        let mut results = Vec::new();

        for record in &records {
            let val = eval_expr(&unwind.expr, record, self.graph, params)?;
            match val {
                Value::List(items) => {
                    for item in items {
                        let mut new_rec = record.clone();
                        new_rec.insert(unwind.alias.clone(), item);
                        results.push(new_rec);
                    }
                }
                Value::Null => {
                    // UNWIND null produces no rows
                }
                _ => {
                    return Err(Error::Type(
                        "UNWIND requires a list expression".to_string(),
                    ));
                }
            }
        }

        Ok(results)
    }
}

/// Add null bindings for all variables in a pattern (for OPTIONAL MATCH).
fn add_null_bindings(record: &mut Record, pattern: &PatternPath) {
    if let Some(ref var) = pattern.variable {
        record.entry(var.clone()).or_insert(Value::Null);
    }
    if let Some(ref var) = pattern.start.variable {
        record.entry(var.clone()).or_insert(Value::Null);
    }
    for (rel_pat, node_pat) in &pattern.hops {
        if let Some(ref var) = rel_pat.variable {
            record.entry(var.clone()).or_insert(Value::Null);
        }
        if let Some(ref var) = node_pat.variable {
            record.entry(var.clone()).or_insert(Value::Null);
        }
    }
}

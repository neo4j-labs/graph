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
        validate_statement(&statement)?;
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
                // UNION DISTINCT: combine and deduplicate all rows
                rows.extend(union_rows);
                let mut deduped: Vec<Row> = Vec::new();
                for row in rows {
                    let exists = deduped.iter().any(|r| {
                        r.columns == row.columns
                            && r.values
                                .iter()
                                .zip(row.values.iter())
                                .all(|(a, b)| a.structural_eq(b))
                    });
                    if !exists {
                        deduped.push(row);
                    }
                }
                rows = deduped;
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
        // Project first
        let projected = self.run_projection(&with_clause.return_body, records.clone(), params)?;

        // Apply WHERE clause after projection, but with visibility into both
        // pre-projection and post-projection variables (projected takes precedence)
        if let Some(ref where_expr) = with_clause.where_clause {
            let has_agg = match &with_clause.return_body.items {
                ReturnItems::Star => false,
                ReturnItems::Expressions(items) => items.iter().any(|item| crate::aggregation::is_aggregation(&item.expr)),
            };
            if has_agg || projected.len() != records.len() {
                // After aggregation, only projected variables are visible
                Ok(projected
                    .into_iter()
                    .filter(|r| {
                        eval_expr(where_expr, r, self.graph, params)
                            .map(|v| v.is_truthy())
                            .unwrap_or(false)
                    })
                    .collect())
            } else {
                // Without aggregation, merge original + projected for WHERE evaluation
                Ok(records
                    .into_iter()
                    .zip(projected.into_iter())
                    .filter_map(|(orig, proj)| {
                        let mut merged = orig;
                        merged.extend(proj.iter().map(|(k, v)| (k.clone(), v.clone())));
                        if eval_expr(where_expr, &merged, self.graph, params)
                            .map(|v| v.is_truthy())
                            .unwrap_or(false)
                        {
                            Some(proj)
                        } else {
                            None
                        }
                    })
                    .collect())
            }
        } else {
            Ok(projected)
        }
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

        // Validate: ORDER BY must not contain aggregation unless the projection also aggregates
        if let Some(ref order_items) = clause.order_by {
            if !has_agg {
                for sort_item in order_items {
                    if crate::aggregation::is_aggregation(&sort_item.expr) {
                        return Err(Error::Parser(
                            "In a WITH/RETURN with no aggregation, \
                             it is not possible to use aggregation in ORDER BY"
                                .into(),
                        ));
                    }
                }
            }
        }

        // Keep original records for ORDER BY (which can reference pre-projection vars)
        let need_originals = clause.order_by.is_some();

        let mut result_records = if has_agg {
            aggregate(records.clone(), &items, self.graph, params)?
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

        // ORDER BY - merge projected record with original for expression evaluation
        if let Some(ref order_items) = clause.order_by {
            if need_originals && !has_agg && result_records.len() == records.len() {
                // Build merged records for sorting (original + projected)
                let mut pairs: Vec<(Record, Record)> = records
                    .into_iter()
                    .zip(result_records.into_iter())
                    .collect();
                pairs.sort_by(|(orig_a, proj_a), (orig_b, proj_b)| {
                    for sort_item in order_items {
                        // Try projected first, then original
                        let mut merged_a = orig_a.clone();
                        merged_a.extend(proj_a.iter().map(|(k, v)| (k.clone(), v.clone())));
                        let mut merged_b = orig_b.clone();
                        merged_b.extend(proj_b.iter().map(|(k, v)| (k.clone(), v.clone())));
                        let va = eval_expr(&sort_item.expr, &merged_a, self.graph, params)
                            .unwrap_or(Value::Null);
                        let vb = eval_expr(&sort_item.expr, &merged_b, self.graph, params)
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
                result_records = pairs.into_iter().map(|(_, proj)| proj).collect();
            } else {
                result_records.sort_by(|a, b| {
                    for sort_item in order_items {
                        let va = resolve_or_eval(&sort_item.expr, a, &items, self.graph, params);
                        let vb = resolve_or_eval(&sort_item.expr, b, &items, self.graph, params);
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
        }

        // SKIP
        if let Some(ref skip_expr) = clause.skip {
            let skip_val = eval_expr(skip_expr, &Record::new(), self.graph, params)?;
            match skip_val {
                Value::Integer(n) => {
                    if n < 0 {
                        return Err(Error::Runtime("SKIP: negative value".into()));
                    }
                    let n = n as usize;
                    if n >= result_records.len() {
                        result_records.clear();
                    } else {
                        result_records = result_records[n..].to_vec();
                    }
                }
                Value::Float(_) => {
                    return Err(Error::Runtime("SKIP: integer argument expected, got float".into()));
                }
                _ => {}
            }
        }

        // LIMIT
        if let Some(ref limit_expr) = clause.limit {
            let limit_val = eval_expr(limit_expr, &Record::new(), self.graph, params)?;
            match limit_val {
                Value::Integer(n) => {
                    if n < 0 {
                        return Err(Error::Runtime("LIMIT: negative value".into()));
                    }
                    result_records.truncate(n as usize);
                }
                Value::Float(_) => {
                    return Err(Error::Runtime("LIMIT: integer argument expected, got float".into()));
                }
                _ => {}
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

/// Try to resolve an expression by looking up its column name in the record,
/// falling back to eval_expr. This handles post-aggregation ORDER BY where
/// aggregate expressions (e.g. count(*)) are already computed as column values.
fn resolve_or_eval(
    expr: &Expr,
    record: &Record,
    items: &[ReturnItem],
    graph: &PropertyGraph,
    params: &Params,
) -> Value {
    // Direct column name match
    let col_name = expr_to_string(expr);
    if let Some(v) = record.get(&col_name) {
        return v.clone();
    }
    // Check if the sort expression matches a return item's expression
    // (e.g., ORDER BY a.name when RETURN a.name AS name)
    for item in items {
        if expr_to_string(&item.expr) == col_name {
            let item_col = item.column_name();
            if let Some(v) = record.get(&item_col) {
                return v.clone();
            }
        }
    }
    eval_expr(expr, record, graph, params).unwrap_or(Value::Null)
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

use std::collections::HashSet;

/// Variable type in a pattern context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VarType {
    Node,
    Relationship,
    Path,
}

/// Validate a statement for semantic errors before execution.
fn validate_statement(statement: &Statement) -> Result<(), Error> {
    validate_clauses(&statement.body)?;
    for union in &statement.unions {
        validate_clauses(&union.body)?;
    }

    // Validate UNION: all parts must have same columns, and cannot mix UNION/UNION ALL
    if !statement.unions.is_empty() {
        // Check for mixing UNION and UNION ALL
        let has_all = statement.unions.iter().any(|u| u.all);
        let has_distinct = statement.unions.iter().any(|u| !u.all);
        if has_all && has_distinct {
            return Err(Error::Parser(
                "Cannot mix UNION and UNION ALL in the same query".into(),
            ));
        }

        // Check column counts match (we can't easily check names before execution,
        // but we can validate RETURN clauses have matching column counts)
        let main_cols = get_return_column_count(&statement.body);
        for union in &statement.unions {
            let union_cols = get_return_column_count(&union.body);
            if let (Some(m), Some(u)) = (main_cols, union_cols) {
                if m != u {
                    return Err(Error::Parser(
                        "All sub queries in a UNION must have the same column names".into(),
                    ));
                }
            }
        }
    }

    Ok(())
}

fn check_duplicate_columns(items: &[ReturnItem]) -> Result<(), Error> {
    let mut seen = HashSet::new();
    for item in items {
        let col = item.column_name();
        if !seen.insert(col.clone()) {
            return Err(Error::Parser(format!(
                "Multiple result columns with the same name '{col}' are not supported"
            )));
        }
    }
    Ok(())
}

fn get_return_column_count(clauses: &[Clause]) -> Option<usize> {
    for clause in clauses.iter().rev() {
        if let Clause::Return(ret) = clause {
            return match &ret.items {
                ReturnItems::Star => None,
                ReturnItems::Expressions(items) => Some(items.len()),
            };
        }
    }
    None
}

fn validate_clauses(clauses: &[Clause]) -> Result<(), Error> {
    // Track variable types across all MATCH clauses in the statement
    let mut global_var_types: std::collections::HashMap<String, VarType> =
        std::collections::HashMap::new();

    for clause in clauses {
        match clause {
            Clause::Match(m) | Clause::OptionalMatch(m) => {
                validate_match_patterns(&m.patterns, &mut global_var_types)?;
                // WHERE clause must not contain aggregation
                if let Some(ref where_expr) = m.where_clause {
                    if crate::aggregation::is_aggregation(where_expr) {
                        return Err(Error::Parser(
                            "Cannot use aggregation in WHERE".into(),
                        ));
                    }
                }
            }
            Clause::With(w) => {
                // WITH requires aliases for non-variable expressions
                if let ReturnItems::Expressions(items) = &w.return_body.items {
                    for item in items {
                        if item.alias.is_none() && !matches!(&item.expr, Expr::Variable(_)) {
                            return Err(Error::Parser(format!(
                                "Expression in WITH must be aliased (use AS): {}",
                                expr_to_string(&item.expr)
                            )));
                        }
                    }
                    check_duplicate_columns(items)?;
                }
                // WHERE in WITH must not contain aggregation
                if let Some(ref where_expr) = w.where_clause {
                    if crate::aggregation::is_aggregation(where_expr) {
                        return Err(Error::Parser(
                            "Cannot use aggregation in WHERE".into(),
                        ));
                    }
                }
            }
            Clause::Return(r) => {
                if let ReturnItems::Expressions(items) = &r.items {
                    check_duplicate_columns(items)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Check for variable type conflicts in MATCH patterns.
/// A variable can only be used as one type (node, relationship, or path).
/// Relationship variables cannot be reused within the same MATCH.
fn validate_match_patterns(
    patterns: &[PatternPath],
    var_types: &mut std::collections::HashMap<String, VarType>,
) -> Result<(), Error> {
    let mut rel_vars_in_match: HashSet<String> = HashSet::new();

    for pattern in patterns {
        // Path variable
        if let Some(ref var) = pattern.variable {
            check_var_type(var_types, var, VarType::Path)?;
        }

        // Start node
        if let Some(ref var) = pattern.start.variable {
            check_var_type(var_types, var, VarType::Node)?;
        }

        for (rel_pat, node_pat) in &pattern.hops {
            // Relationship variable
            if let Some(ref var) = rel_pat.variable {
                check_var_type(var_types, var, VarType::Relationship)?;
                // Relationship variables must be unique within a MATCH
                if !rel_vars_in_match.insert(var.clone()) {
                    return Err(Error::Parser(format!(
                        "Cannot use the same relationship variable '{var}' for multiple patterns"
                    )));
                }
            }

            // Node variable
            if let Some(ref var) = node_pat.variable {
                check_var_type(var_types, var, VarType::Node)?;
            }
        }
    }

    Ok(())
}

fn check_var_type(
    var_types: &mut std::collections::HashMap<String, VarType>,
    var: &str,
    expected: VarType,
) -> Result<(), Error> {
    if let Some(&existing) = var_types.get(var) {
        if existing != expected {
            let existing_str = match existing {
                VarType::Node => "node",
                VarType::Relationship => "relationship",
                VarType::Path => "path",
            };
            let expected_str = match expected {
                VarType::Node => "node",
                VarType::Relationship => "relationship",
                VarType::Path => "path",
            };
            return Err(Error::Parser(format!(
                "Variable '{var}' already declared as {existing_str}, cannot be redeclared as {expected_str}"
            )));
        }
    } else {
        var_types.insert(var.to_string(), expected);
    }
    Ok(())
}


use crate::ast::{Expr, ReturnItem, ReturnItems};
use crate::error::Error;
use crate::expr::{eval_expr, Params, Record};
use crate::graph::PropertyGraph;
use crate::value::Value;

/// Check if an expression contains aggregation functions.
pub fn is_aggregation(expr: &Expr) -> bool {
    match expr {
        Expr::CountStar => true,
        Expr::FunctionCall { name, .. } => is_aggregate_function(name),
        Expr::Add(l, r)
        | Expr::Sub(l, r)
        | Expr::Mul(l, r)
        | Expr::Div(l, r)
        | Expr::Mod(l, r)
        | Expr::Pow(l, r) => is_aggregation(l) || is_aggregation(r),
        Expr::UnaryMinus(e) | Expr::UnaryPlus(e) | Expr::Not(e) => is_aggregation(e),
        Expr::Property(e, _) => is_aggregation(e),
        _ => false,
    }
}

fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "collect"
            | "stdev"
            | "stdevp"
            | "percentiledisc"
            | "percentilecont"
    )
}

/// Check if any return item uses aggregation.
pub fn has_aggregation(items: &ReturnItems) -> bool {
    match items {
        ReturnItems::Star => false,
        ReturnItems::Expressions(items) => items.iter().any(|item| is_aggregation(&item.expr)),
    }
}

/// Perform aggregation on a set of records.
/// Returns the aggregated records.
pub fn aggregate(
    records: Vec<Record>,
    items: &[ReturnItem],
    graph: &PropertyGraph,
    params: &Params,
) -> Result<Vec<Record>, Error> {
    // Separate grouping keys from aggregation expressions
    let mut group_indices = Vec::new();
    let mut agg_indices = Vec::new();

    for (i, item) in items.iter().enumerate() {
        if is_aggregation(&item.expr) {
            agg_indices.push(i);
        } else {
            group_indices.push(i);
        }
    }

    // If no grouping keys and there are aggregations, treat all records as one group
    if group_indices.is_empty() && !agg_indices.is_empty() {
        let mut result_record = Record::new();
        for &i in &agg_indices {
            let col = items[i].column_name();
            let val = compute_aggregate(&items[i].expr, &records, graph, params)?;
            result_record.insert(col, val);
        }
        return Ok(vec![result_record]);
    }

    // Group records by grouping key values
    let mut groups: Vec<(Vec<Value>, Vec<Record>)> = Vec::new();

    for record in &records {
        let key: Vec<Value> = group_indices
            .iter()
            .map(|&i| eval_expr(&items[i].expr, record, graph, params).unwrap_or(Value::Null))
            .collect();

        // Find existing group
        let found = groups.iter_mut().find(|(k, _)| {
            k.len() == key.len()
                && k.iter()
                    .zip(key.iter())
                    .all(|(a, b)| a.structural_eq(b))
        });

        if let Some((_, group_records)) = found {
            group_records.push(record.clone());
        } else {
            groups.push((key, vec![record.clone()]));
        }
    }

    // Compute results for each group
    let mut results = Vec::new();
    for (_, group_records) in &groups {
        let mut result_record = Record::new();

        // Add grouping key values
        let representative = &group_records[0];
        for &i in &group_indices {
            let col = items[i].column_name();
            let val = eval_expr(&items[i].expr, representative, graph, params)?;
            result_record.insert(col, val);
        }

        // Compute aggregates
        for &i in &agg_indices {
            let col = items[i].column_name();
            let val = compute_aggregate(&items[i].expr, group_records, graph, params)?;
            result_record.insert(col, val);
        }

        results.push(result_record);
    }

    Ok(results)
}

/// Compute an aggregate expression over a group of records.
fn compute_aggregate(
    expr: &Expr,
    records: &[Record],
    graph: &PropertyGraph,
    params: &Params,
) -> Result<Value, Error> {
    match expr {
        Expr::CountStar => Ok(Value::Integer(records.len() as i64)),

        Expr::FunctionCall {
            name,
            distinct,
            args,
        } => {
            let lower = name.to_ascii_lowercase();
            let mut values: Vec<Value> = records
                .iter()
                .map(|r| {
                    if args.is_empty() {
                        Ok(Value::Null)
                    } else {
                        eval_expr(&args[0], r, graph, params)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            if *distinct {
                let mut seen = Vec::new();
                values.retain(|v| {
                    if seen.iter().any(|s: &Value| v.structural_eq(s)) {
                        false
                    } else {
                        seen.push(v.clone());
                        true
                    }
                });
            }

            match lower.as_str() {
                "count" => {
                    let count = values.iter().filter(|v| !v.is_null()).count();
                    Ok(Value::Integer(count as i64))
                }
                "sum" => {
                    let non_null: Vec<&Value> =
                        values.iter().filter(|v| !v.is_null()).collect();
                    if non_null.is_empty() {
                        return Ok(Value::Integer(0));
                    }
                    let mut result = Value::Integer(0);
                    for v in non_null {
                        result = match (&result, v) {
                            (Value::Integer(a), Value::Integer(b)) => {
                                Value::Integer(a + b)
                            }
                            (Value::Integer(a), Value::Float(b)) => {
                                Value::Float(*a as f64 + b)
                            }
                            (Value::Float(a), Value::Integer(b)) => {
                                Value::Float(a + *b as f64)
                            }
                            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                            _ => result,
                        };
                    }
                    Ok(result)
                }
                "avg" => {
                    let non_null: Vec<&Value> =
                        values.iter().filter(|v| !v.is_null()).collect();
                    if non_null.is_empty() {
                        return Ok(Value::Null);
                    }
                    let count = non_null.len() as f64;
                    let mut sum = 0.0f64;
                    for v in non_null {
                        sum += match v {
                            Value::Integer(n) => *n as f64,
                            Value::Float(f) => *f,
                            _ => 0.0,
                        };
                    }
                    Ok(Value::Float(sum / count))
                }
                "min" => {
                    let non_null: Vec<&Value> =
                        values.iter().filter(|v| !v.is_null()).collect();
                    if non_null.is_empty() {
                        return Ok(Value::Null);
                    }
                    let mut min = non_null[0].clone();
                    for v in &non_null[1..] {
                        if v.order_cmp(&min) == std::cmp::Ordering::Less {
                            min = (*v).clone();
                        }
                    }
                    Ok(min)
                }
                "max" => {
                    let non_null: Vec<&Value> =
                        values.iter().filter(|v| !v.is_null()).collect();
                    if non_null.is_empty() {
                        return Ok(Value::Null);
                    }
                    let mut max = non_null[0].clone();
                    for v in &non_null[1..] {
                        if v.order_cmp(&max) == std::cmp::Ordering::Greater {
                            max = (*v).clone();
                        }
                    }
                    Ok(max)
                }
                "collect" => {
                    let collected: Vec<Value> =
                        values.into_iter().filter(|v| !v.is_null()).collect();
                    Ok(Value::List(collected))
                }
                "percentiledisc" | "percentilecont" => {
                    // Second arg is the percentile (constant)
                    let pct = if args.len() > 1 {
                        match eval_expr(&args[1], &records[0], graph, params)? {
                            Value::Float(f) => f,
                            Value::Integer(i) => i as f64,
                            _ => {
                                return Err(Error::Unsupported(
                                    "percentile requires a numeric argument".into(),
                                ))
                            }
                        }
                    } else {
                        return Err(Error::Unsupported(
                            "percentile requires two arguments".into(),
                        ));
                    };

                    let mut nums: Vec<f64> = values
                        .iter()
                        .filter_map(|v| match v {
                            Value::Integer(n) => Some(*n as f64),
                            Value::Float(f) => Some(*f),
                            _ => None,
                        })
                        .collect();

                    if nums.is_empty() {
                        return Ok(Value::Null);
                    }

                    nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

                    if lower == "percentiledisc" {
                        // Nearest rank method
                        let idx = (pct * (nums.len() - 1) as f64).ceil() as usize;
                        let idx = idx.min(nums.len() - 1);
                        // Return integer if all inputs were integers
                        let all_int = values.iter().all(|v| matches!(v, Value::Integer(_) | Value::Null));
                        if all_int {
                            Ok(Value::Integer(nums[idx] as i64))
                        } else {
                            Ok(Value::Float(nums[idx]))
                        }
                    } else {
                        // percentileCont - linear interpolation
                        let pos = pct * (nums.len() - 1) as f64;
                        let lower_idx = pos.floor() as usize;
                        let upper_idx = pos.ceil() as usize;
                        let frac = pos - lower_idx as f64;
                        let result = nums[lower_idx] * (1.0 - frac) + nums[upper_idx] * frac;
                        Ok(Value::Float(result))
                    }
                }
                _ => Err(Error::Unsupported(format!(
                    "unknown aggregate function: {name}"
                ))),
            }
        }

        // For non-aggregate expressions wrapping aggregates (e.g., count(x) + 1)
        // evaluate sub-aggregates first, then evaluate the outer expression
        Expr::Add(l, r) => {
            let lv = compute_or_eval(l, records, graph, params)?;
            let rv = compute_or_eval(r, records, graph, params)?;
            crate::expr::eval_expr(
                &Expr::Add(
                    Box::new(Expr::Literal(lv)),
                    Box::new(Expr::Literal(rv)),
                ),
                &Record::new(),
                graph,
                params,
            )
        }
        Expr::Sub(l, r) => {
            let lv = compute_or_eval(l, records, graph, params)?;
            let rv = compute_or_eval(r, records, graph, params)?;
            crate::expr::eval_expr(
                &Expr::Sub(
                    Box::new(Expr::Literal(lv)),
                    Box::new(Expr::Literal(rv)),
                ),
                &Record::new(),
                graph,
                params,
            )
        }
        Expr::Mul(l, r) => {
            let lv = compute_or_eval(l, records, graph, params)?;
            let rv = compute_or_eval(r, records, graph, params)?;
            crate::expr::eval_expr(
                &Expr::Mul(
                    Box::new(Expr::Literal(lv)),
                    Box::new(Expr::Literal(rv)),
                ),
                &Record::new(),
                graph,
                params,
            )
        }

        // Fallback: evaluate against first record
        other => {
            if records.is_empty() {
                Ok(Value::Null)
            } else {
                eval_expr(other, &records[0], graph, params)
            }
        }
    }
}

fn compute_or_eval(
    expr: &Expr,
    records: &[Record],
    graph: &PropertyGraph,
    params: &Params,
) -> Result<Value, Error> {
    if is_aggregation(expr) {
        compute_aggregate(expr, records, graph, params)
    } else if records.is_empty() {
        Ok(Value::Null)
    } else {
        eval_expr(expr, &records[0], graph, params)
    }
}

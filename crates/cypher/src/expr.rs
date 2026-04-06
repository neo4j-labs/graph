use std::collections::BTreeMap;

use crate::ast::Expr;
use crate::error::Error;
use crate::functions::call_function;
use crate::graph::PropertyGraph;
use crate::pattern_match;
use crate::value::Value;

/// A record is a single row of variable bindings.
pub type Record = BTreeMap<String, Value>;

/// Parameters passed to a query.
pub type Params = BTreeMap<String, Value>;

/// Evaluate an expression against a record and graph.
pub fn eval_expr(
    expr: &Expr,
    record: &Record,
    graph: &PropertyGraph,
    params: &Params,
) -> Result<Value, Error> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Variable(name) => Ok(record.get(name).cloned().unwrap_or(Value::Null)),
        Expr::Parameter(name) => Ok(params.get(name).cloned().unwrap_or(Value::Null)),

        Expr::Property(base, prop) => {
            let base_val = eval_expr(base, record, graph, params)?;
            Ok(get_property(&base_val, prop))
        }

        // Arithmetic
        Expr::Add(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            eval_add(&lv, &rv)
        }
        Expr::Sub(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            eval_sub(&lv, &rv)
        }
        Expr::Mul(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            eval_mul(&lv, &rv)
        }
        Expr::Div(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            eval_div(&lv, &rv)
        }
        Expr::Mod(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            eval_mod(&lv, &rv)
        }
        Expr::Pow(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            eval_pow(&lv, &rv)
        }
        Expr::UnaryMinus(e) => {
            let v = eval_expr(e, record, graph, params)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Integer(n) => Ok(Value::Integer(-n)),
                Value::Float(f) => Ok(Value::Float(-f)),
                _ => Err(Error::Type("unary minus requires a number".to_string())),
            }
        }
        Expr::UnaryPlus(e) => {
            let v = eval_expr(e, record, graph, params)?;
            match v {
                Value::Null => Ok(Value::Null),
                Value::Integer(_) | Value::Float(_) => Ok(v),
                _ => Err(Error::Type("unary plus requires a number".to_string())),
            }
        }

        // Comparison
        Expr::Eq(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            Ok(lv.cypher_eq(&rv))
        }
        Expr::Neq(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            Ok(lv.cypher_neq(&rv))
        }
        Expr::Lt(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            Ok(lv.cypher_lt(&rv))
        }
        Expr::Gt(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            Ok(lv.cypher_gt(&rv))
        }
        Expr::Lte(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            Ok(lv.cypher_lte(&rv))
        }
        Expr::Gte(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            Ok(lv.cypher_gte(&rv))
        }

        // Boolean
        Expr::And(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            Ok(eval_and(&lv, &rv))
        }
        Expr::Or(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            Ok(eval_or(&lv, &rv))
        }
        Expr::Xor(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            Ok(eval_xor(&lv, &rv))
        }
        Expr::Not(e) => {
            let v = eval_expr(e, record, graph, params)?;
            Ok(eval_not(&v))
        }

        // Null checks
        Expr::IsNull(e) => {
            let v = eval_expr(e, record, graph, params)?;
            Ok(Value::Bool(v.is_null()))
        }
        Expr::IsNotNull(e) => {
            let v = eval_expr(e, record, graph, params)?;
            Ok(Value::Bool(!v.is_null()))
        }

        // Label
        Expr::HasLabel(e, label) => {
            let v = eval_expr(e, record, graph, params)?;
            match &v {
                Value::Null => Ok(Value::Null),
                Value::Node(n) => Ok(Value::Bool(n.labels.contains(label))),
                _ => Err(Error::Type("label check requires a node".to_string())),
            }
        }
        Expr::HasLabels(e, labels) => {
            let v = eval_expr(e, record, graph, params)?;
            match &v {
                Value::Null => Ok(Value::Null),
                Value::Node(n) => Ok(Value::Bool(
                    labels.iter().all(|l| n.labels.contains(l)),
                )),
                _ => Err(Error::Type("label check requires a node".to_string())),
            }
        }

        // String predicates
        Expr::StartsWith(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            match (&lv, &rv) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::String(s), Value::String(prefix)) => {
                    Ok(Value::Bool(s.starts_with(prefix.as_str())))
                }
                _ => Err(Error::Type(
                    "STARTS WITH requires string arguments".to_string(),
                )),
            }
        }
        Expr::EndsWith(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            match (&lv, &rv) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::String(s), Value::String(suffix)) => {
                    Ok(Value::Bool(s.ends_with(suffix.as_str())))
                }
                _ => Err(Error::Type(
                    "ENDS WITH requires string arguments".to_string(),
                )),
            }
        }
        Expr::Contains(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            match (&lv, &rv) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::String(s), Value::String(sub)) => {
                    Ok(Value::Bool(s.contains(sub.as_str())))
                }
                _ => Err(Error::Type(
                    "CONTAINS requires string arguments".to_string(),
                )),
            }
        }
        Expr::RegexMatch(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            match (&lv, &rv) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::String(s), Value::String(pattern)) => {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| Error::Runtime(format!("invalid regex: {e}")))?;
                    Ok(Value::Bool(re.is_match(s)))
                }
                _ => Err(Error::Type("=~ requires string arguments".to_string())),
            }
        }

        // IN
        Expr::In(l, r) => {
            let lv = eval_expr(l, record, graph, params)?;
            let rv = eval_expr(r, record, graph, params)?;
            match &rv {
                Value::Null => Ok(Value::Null),
                Value::List(list) => {
                    if lv.is_null() {
                        // null IN [anything] => null if list is non-empty, else false
                        if list.is_empty() {
                            Ok(Value::Bool(false))
                        } else {
                            Ok(Value::Null)
                        }
                    } else {
                        let mut has_null = false;
                        for item in list {
                            match lv.cypher_eq(item) {
                                Value::Bool(true) => return Ok(Value::Bool(true)),
                                Value::Null => has_null = true,
                                _ => {}
                            }
                        }
                        if has_null {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Bool(false))
                        }
                    }
                }
                _ => Err(Error::Type("IN requires a list on the right".to_string())),
            }
        }

        // List/Map
        Expr::ListLiteral(items) => {
            let values: Result<Vec<Value>, Error> = items
                .iter()
                .map(|e| eval_expr(e, record, graph, params))
                .collect();
            Ok(Value::List(values?))
        }
        Expr::MapLiteral(pairs) => {
            let mut map = BTreeMap::new();
            for (key, expr) in pairs {
                let val = eval_expr(expr, record, graph, params)?;
                map.insert(key.clone(), val);
            }
            Ok(Value::Map(map))
        }
        Expr::Index(base, idx) => {
            let base_val = eval_expr(base, record, graph, params)?;
            let idx_val = eval_expr(idx, record, graph, params)?;
            eval_index(&base_val, &idx_val)
        }
        Expr::Slice(base, from, to) => {
            let base_val = eval_expr(base, record, graph, params)?;
            let from_val = from
                .as_ref()
                .map(|e| eval_expr(e, record, graph, params))
                .transpose()?;
            let to_val = to
                .as_ref()
                .map(|e| eval_expr(e, record, graph, params))
                .transpose()?;
            eval_slice(&base_val, from_val.as_ref(), to_val.as_ref())
        }

        // Function call
        Expr::FunctionCall {
            name,
            distinct: _,
            args,
        } => {
            let lower = name.to_ascii_lowercase();

            // Handle quantifier predicates: all/any/none/single(x IN list WHERE pred)
            // These have args: [Variable(var), list_expr, predicate_expr]
            if matches!(lower.as_str(), "all" | "any" | "none" | "single")
                && args.len() == 3
            {
                if let Expr::Variable(var_name) = &args[0] {
                    let list_val = eval_expr(&args[1], record, graph, params)?;
                    match list_val {
                        Value::List(items) => {
                            let mut true_count = 0;
                            let mut has_null = false;
                            for item in &items {
                                let mut inner = record.clone();
                                inner.insert(var_name.clone(), item.clone());
                                let pred_val = eval_expr(&args[2], &inner, graph, params)?;
                                match pred_val {
                                    Value::Bool(true) => true_count += 1,
                                    Value::Bool(false) => {}
                                    Value::Null => has_null = true,
                                    _ => {}
                                }
                            }
                            return Ok(match lower.as_str() {
                                "all" => {
                                    if true_count == items.len() && !has_null {
                                        Value::Bool(true)
                                    } else if items.len() - true_count > 0 && !has_null {
                                        Value::Bool(false)
                                    } else if has_null {
                                        Value::Null
                                    } else {
                                        Value::Bool(true)
                                    }
                                }
                                "any" => {
                                    if true_count > 0 {
                                        Value::Bool(true)
                                    } else if has_null {
                                        Value::Null
                                    } else {
                                        Value::Bool(false)
                                    }
                                }
                                "none" => {
                                    if true_count > 0 {
                                        Value::Bool(false)
                                    } else if has_null {
                                        Value::Null
                                    } else {
                                        Value::Bool(true)
                                    }
                                }
                                "single" => {
                                    if true_count == 1 && !has_null {
                                        Value::Bool(true)
                                    } else if true_count > 1 {
                                        Value::Bool(false)
                                    } else if has_null {
                                        Value::Null
                                    } else {
                                        Value::Bool(false)
                                    }
                                }
                                _ => Value::Null,
                            });
                        }
                        Value::Null => return Ok(Value::Null),
                        _ => {}
                    }
                }
            }

            // Note: aggregation functions (count, sum, avg, min, max, collect)
            // are handled specially by the executor, not here.
            let arg_values: Result<Vec<Value>, Error> = args
                .iter()
                .map(|a| eval_expr(a, record, graph, params))
                .collect();
            call_function(name, &arg_values?)
        }
        Expr::CountStar => {
            // Handled by aggregation in executor
            Ok(Value::Integer(1))
        }

        // CASE
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                // Simple CASE: CASE expr WHEN val THEN result ...
                let op_val = eval_expr(op, record, graph, params)?;
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr(when_expr, record, graph, params)?;
                    if matches!(op_val.cypher_eq(&when_val), Value::Bool(true)) {
                        return eval_expr(then_expr, record, graph, params);
                    }
                }
            } else {
                // Searched CASE: CASE WHEN pred THEN result ...
                for (when_expr, then_expr) in when_clauses {
                    let when_val = eval_expr(when_expr, record, graph, params)?;
                    if when_val.is_truthy() {
                        return eval_expr(then_expr, record, graph, params);
                    }
                }
            }
            if let Some(else_expr) = else_clause {
                eval_expr(else_expr, record, graph, params)
            } else {
                Ok(Value::Null)
            }
        }

        Expr::PatternExpr(pattern) => {
            // Boolean check: does this pattern exist?
            let matches = pattern_match::match_pattern(graph, pattern, record);
            Ok(Value::Bool(!matches.is_empty()))
        }

        Expr::ListComprehension {
            variable,
            source,
            filter,
            projection,
        } => {
            let source_val = eval_expr(source, record, graph, params)?;
            match source_val {
                Value::List(items) => {
                    let mut result = Vec::new();
                    for item in items {
                        let mut inner_record = record.clone();
                        inner_record.insert(variable.clone(), item);
                        if let Some(ref pred) = filter {
                            let pred_val = eval_expr(pred, &inner_record, graph, params)?;
                            if !pred_val.is_truthy() {
                                continue;
                            }
                        }
                        if let Some(ref proj) = projection {
                            result.push(eval_expr(proj, &inner_record, graph, params)?);
                        } else {
                            result.push(
                                inner_record.get(variable.as_str()).cloned().unwrap_or(Value::Null),
                            );
                        }
                    }
                    Ok(Value::List(result))
                }
                Value::Null => Ok(Value::Null),
                _ => Err(Error::Type(
                    "list comprehension requires a list source".to_string(),
                )),
            }
        }

        Expr::PatternComprehension {
            pattern,
            filter,
            projection,
        } => {
            let matches = pattern_match::match_pattern(graph, pattern, record);
            let mut result = Vec::new();
            for matched_record in matches {
                if let Some(ref pred) = filter {
                    let pred_val = eval_expr(pred, &matched_record, graph, params)?;
                    if !pred_val.is_truthy() {
                        continue;
                    }
                }
                result.push(eval_expr(projection, &matched_record, graph, params)?);
            }
            Ok(Value::List(result))
        }

        Expr::ExistsSubquery(match_clause) => {
            let matches = pattern_match::match_patterns(graph, &match_clause.patterns, record);
            let filtered = if let Some(ref where_expr) = match_clause.where_clause {
                matches
                    .into_iter()
                    .filter(|r| {
                        eval_expr(where_expr, r, graph, params)
                            .map(|v| v.is_truthy())
                            .unwrap_or(false)
                    })
                    .collect::<Vec<_>>()
            } else {
                matches
            };
            Ok(Value::Bool(!filtered.is_empty()))
        }
    }
}

pub fn get_property(value: &Value, prop: &str) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Node(n) => n.properties.get(prop).cloned().unwrap_or(Value::Null),
        Value::Relationship(r) => r.properties.get(prop).cloned().unwrap_or(Value::Null),
        Value::Map(m) => m.get(prop).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn eval_add(l: &Value, r: &Value) -> Result<Value, Error> {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.wrapping_add(*b))),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
        (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{a}{b}"))),
        (Value::String(a), other) => Ok(Value::String(format!("{a}{other}"))),
        (other, Value::String(b)) => Ok(Value::String(format!("{other}{b}"))),
        (Value::List(a), Value::List(b)) => {
            let mut result = a.clone();
            result.extend(b.iter().cloned());
            Ok(Value::List(result))
        }
        (Value::List(a), b) => {
            let mut result = a.clone();
            result.push(b.clone());
            Ok(Value::List(result))
        }
        _ => Err(Error::Type(format!(
            "cannot add {l} and {r}"
        ))),
    }
}

fn eval_sub(l: &Value, r: &Value) -> Result<Value, Error> {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.wrapping_sub(*b))),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a - *b as f64)),
        _ => Err(Error::Type(format!("cannot subtract {l} and {r}"))),
    }
}

fn eval_mul(l: &Value, r: &Value) -> Result<Value, Error> {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a.wrapping_mul(*b))),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a * *b as f64)),
        _ => Err(Error::Type(format!("cannot multiply {l} and {r}"))),
    }
}

fn eval_div(l: &Value, r: &Value) -> Result<Value, Error> {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(_), Value::Integer(0)) => Err(Error::Runtime("division by zero".to_string())),
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a / b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 / b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a / *b as f64)),
        _ => Err(Error::Type(format!("cannot divide {l} and {r}"))),
    }
}

fn eval_mod(l: &Value, r: &Value) -> Result<Value, Error> {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(_), Value::Integer(0)) => Err(Error::Runtime("modulo by zero".to_string())),
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a % b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a % b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 % b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a % *b as f64)),
        _ => Err(Error::Type(format!("cannot modulo {l} and {r}"))),
    }
}

fn eval_pow(l: &Value, r: &Value) -> Result<Value, Error> {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(a), Value::Integer(b)) => {
            if *b >= 0 {
                Ok(Value::Integer(a.wrapping_pow(*b as u32)))
            } else {
                Ok(Value::Float((*a as f64).powf(*b as f64)))
            }
        }
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.powf(*b))),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float((*a as f64).powf(*b))),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a.powf(*b as f64))),
        _ => Err(Error::Type(format!("cannot exponentiate {l} and {r}"))),
    }
}

/// Three-valued AND
fn eval_and(l: &Value, r: &Value) -> Value {
    match (l.as_bool(), r.as_bool()) {
        (Some(false), _) | (_, Some(false)) => Value::Bool(false),
        (Some(true), Some(true)) => Value::Bool(true),
        _ => {
            if l.is_null() || r.is_null() {
                Value::Null
            } else {
                Value::Bool(false)
            }
        }
    }
}

/// Three-valued OR
fn eval_or(l: &Value, r: &Value) -> Value {
    match (l.as_bool(), r.as_bool()) {
        (Some(true), _) | (_, Some(true)) => Value::Bool(true),
        (Some(false), Some(false)) => Value::Bool(false),
        _ => {
            if l.is_null() || r.is_null() {
                Value::Null
            } else {
                Value::Bool(false)
            }
        }
    }
}

/// Three-valued XOR
fn eval_xor(l: &Value, r: &Value) -> Value {
    match (l.as_bool(), r.as_bool()) {
        (Some(a), Some(b)) => Value::Bool(a ^ b),
        _ => Value::Null,
    }
}

/// Three-valued NOT
fn eval_not(v: &Value) -> Value {
    match v.as_bool() {
        Some(b) => Value::Bool(!b),
        None => {
            if v.is_null() {
                Value::Null
            } else {
                Value::Bool(false)
            }
        }
    }
}

fn eval_index(base: &Value, idx: &Value) -> Result<Value, Error> {
    match (base, idx) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::List(list), Value::Integer(i)) => {
            let idx = if *i < 0 {
                (list.len() as i64 + i) as usize
            } else {
                *i as usize
            };
            Ok(list.get(idx).cloned().unwrap_or(Value::Null))
        }
        (Value::Map(map), Value::String(key)) => {
            Ok(map.get(key).cloned().unwrap_or(Value::Null))
        }
        _ => Ok(Value::Null),
    }
}

fn eval_slice(base: &Value, from: Option<&Value>, to: Option<&Value>) -> Result<Value, Error> {
    match base {
        Value::Null => Ok(Value::Null),
        Value::List(list) => {
            let len = list.len() as i64;
            let start = match from {
                Some(Value::Integer(n)) => {
                    let n = *n;
                    if n < 0 {
                        (len + n).max(0) as usize
                    } else {
                        n.min(len) as usize
                    }
                }
                None => 0,
                _ => return Ok(Value::Null),
            };
            let end = match to {
                Some(Value::Integer(n)) => {
                    let n = *n;
                    if n < 0 {
                        (len + n).max(0) as usize
                    } else {
                        n.min(len) as usize
                    }
                }
                None => list.len(),
                _ => return Ok(Value::Null),
            };
            if start >= end {
                Ok(Value::List(Vec::new()))
            } else {
                Ok(Value::List(list[start..end].to_vec()))
            }
        }
        _ => Ok(Value::Null),
    }
}

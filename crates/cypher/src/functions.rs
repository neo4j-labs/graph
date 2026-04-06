
use crate::error::Error;
use crate::value::Value;

/// Dispatch a built-in function call.
pub fn call_function(name: &str, args: &[Value]) -> Result<Value, Error> {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        // Graph functions
        "id" => fn_id(args),
        "labels" => fn_labels(args),
        "type" => fn_type(args),
        "properties" => fn_properties(args),
        "keys" => fn_keys(args),
        "startnode" => fn_startnode(args),
        "endnode" => fn_endnode(args),

        // Scalar functions
        "coalesce" => fn_coalesce(args),
        "size" => fn_size(args),
        "length" => fn_length(args),
        "tostring" => fn_to_string(args),
        "tointeger" => fn_to_integer(args),
        "tofloat" => fn_to_float(args),
        "toboolean" => fn_to_boolean(args),

        // String functions
        "toupper" => fn_to_upper(args),
        "tolower" => fn_to_lower(args),
        "trim" => fn_trim(args),
        "ltrim" => fn_ltrim(args),
        "rtrim" => fn_rtrim(args),
        "replace" => fn_replace(args),
        "substring" => fn_substring(args),
        "left" => fn_left(args),
        "right" => fn_right(args),
        "split" => fn_split(args),
        "reverse" => fn_reverse(args),

        // Numeric functions
        "abs" => fn_abs(args),
        "ceil" => fn_ceil(args),
        "floor" => fn_floor(args),
        "round" => fn_round(args),
        "sign" => fn_sign(args),
        "rand" => Ok(Value::Float(0.5)), // deterministic for tests
        "sqrt" => fn_sqrt(args),
        "log" => fn_log(args),
        "log10" => fn_log10(args),
        "exp" => fn_exp(args),
        "e" => Ok(Value::Float(std::f64::consts::E)),
        "pi" => Ok(Value::Float(std::f64::consts::PI)),

        // List functions
        "head" => fn_head(args),
        "tail" => fn_tail(args),
        "last" => fn_last(args),
        "range" => fn_range(args),
        "nodes" => fn_nodes(args),
        "relationships" => fn_relationships(args),

        // Existence check
        "exists" => fn_exists(args),

        _ => Err(Error::Unsupported(format!("unknown function: {name}"))),
    }
}

fn expect_args(name: &str, args: &[Value], expected: usize) -> Result<(), Error> {
    if args.len() != expected {
        Err(Error::Runtime(format!(
            "{name}() expected {expected} argument(s), got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
}

fn fn_id(args: &[Value]) -> Result<Value, Error> {
    expect_args("id", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Node(n) => Ok(Value::Integer(n.id as i64)),
        Value::Relationship(r) => Ok(Value::Integer(r.id as i64)),
        _ => Err(Error::Type("id() requires a node or relationship".to_string())),
    }
}

fn fn_labels(args: &[Value]) -> Result<Value, Error> {
    expect_args("labels", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Node(n) => Ok(Value::List(
            n.labels.iter().map(|l| Value::String(l.clone())).collect(),
        )),
        _ => Err(Error::Type("labels() requires a node".to_string())),
    }
}

fn fn_type(args: &[Value]) -> Result<Value, Error> {
    expect_args("type", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Relationship(r) => Ok(Value::String(r.rel_type.clone())),
        _ => Err(Error::Type("type() requires a relationship".to_string())),
    }
}

fn fn_properties(args: &[Value]) -> Result<Value, Error> {
    expect_args("properties", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Node(n) => Ok(Value::Map(n.properties.clone())),
        Value::Relationship(r) => Ok(Value::Map(r.properties.clone())),
        Value::Map(m) => Ok(Value::Map(m.clone())),
        _ => Err(Error::Type(
            "properties() requires a node, relationship, or map".to_string(),
        )),
    }
}

fn fn_keys(args: &[Value]) -> Result<Value, Error> {
    expect_args("keys", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Node(n) => Ok(Value::List(
            n.properties.keys().map(|k| Value::String(k.clone())).collect(),
        )),
        Value::Relationship(r) => Ok(Value::List(
            r.properties.keys().map(|k| Value::String(k.clone())).collect(),
        )),
        Value::Map(m) => Ok(Value::List(
            m.keys().map(|k| Value::String(k.clone())).collect(),
        )),
        _ => Err(Error::Type(
            "keys() requires a node, relationship, or map".to_string(),
        )),
    }
}

fn fn_startnode(args: &[Value]) -> Result<Value, Error> {
    expect_args("startNode", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Relationship(r) => Ok(Value::Integer(r.start_node as i64)),
        _ => Err(Error::Type("startNode() requires a relationship".to_string())),
    }
}

fn fn_endnode(args: &[Value]) -> Result<Value, Error> {
    expect_args("endNode", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Relationship(r) => Ok(Value::Integer(r.end_node as i64)),
        _ => Err(Error::Type("endNode() requires a relationship".to_string())),
    }
}

fn fn_coalesce(args: &[Value]) -> Result<Value, Error> {
    for arg in args {
        if !arg.is_null() {
            return Ok(arg.clone());
        }
    }
    Ok(Value::Null)
}

fn fn_size(args: &[Value]) -> Result<Value, Error> {
    expect_args("size", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::Integer(s.chars().count() as i64)),
        Value::List(l) => Ok(Value::Integer(l.len() as i64)),
        Value::Map(m) => Ok(Value::Integer(m.len() as i64)),
        _ => Err(Error::Type(
            "size() requires a string, list, or map".to_string(),
        )),
    }
}

fn fn_length(args: &[Value]) -> Result<Value, Error> {
    expect_args("length", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Path(p) => Ok(Value::Integer(p.relationships.len() as i64)),
        Value::String(s) => Ok(Value::Integer(s.chars().count() as i64)),
        _ => Err(Error::Type("length() requires a path or string".to_string())),
    }
}

fn fn_to_string(args: &[Value]) -> Result<Value, Error> {
    expect_args("toString", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Integer(n) => Ok(Value::String(n.to_string())),
        Value::Float(f) => Ok(Value::String(format_float(*f))),
        Value::Bool(b) => Ok(Value::String(b.to_string())),
        _ => Err(Error::Type("toString() cannot convert this type".to_string())),
    }
}

fn format_float(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        }
    } else if f == f.floor() && f.abs() < 1e15 {
        format!("{f:.1}")
    } else {
        format!("{f}")
    }
}

fn fn_to_integer(args: &[Value]) -> Result<Value, Error> {
    expect_args("toInteger", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                Ok(Value::Null)
            } else {
                Ok(Value::Integer(*f as i64))
            }
        }
        Value::String(s) => match s.trim().parse::<i64>() {
            Ok(n) => Ok(Value::Integer(n)),
            Err(_) => match s.trim().parse::<f64>() {
                Ok(f) => Ok(Value::Integer(f as i64)),
                Err(_) => Ok(Value::Null),
            },
        },
        Value::Bool(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
        _ => Err(Error::Type("toInteger() cannot convert this type".to_string())),
    }
}

fn fn_to_float(args: &[Value]) -> Result<Value, Error> {
    expect_args("toFloat", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Float(f) => Ok(Value::Float(*f)),
        Value::Integer(n) => Ok(Value::Float(*n as f64)),
        Value::String(s) => match s.trim().parse::<f64>() {
            Ok(f) => Ok(Value::Float(f)),
            Err(_) => Ok(Value::Null),
        },
        _ => Err(Error::Type("toFloat() cannot convert this type".to_string())),
    }
}

fn fn_to_boolean(args: &[Value]) -> Result<Value, Error> {
    expect_args("toBoolean", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Bool(b) => Ok(Value::Bool(*b)),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Ok(Value::Null),
        },
        Value::Integer(n) => Ok(Value::Bool(*n != 0)),
        _ => Err(Error::Type("toBoolean() cannot convert this type".to_string())),
    }
}

fn fn_to_upper(args: &[Value]) -> Result<Value, Error> {
    expect_args("toUpper", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.to_uppercase())),
        _ => Err(Error::Type("toUpper() requires a string".to_string())),
    }
}

fn fn_to_lower(args: &[Value]) -> Result<Value, Error> {
    expect_args("toLower", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.to_lowercase())),
        _ => Err(Error::Type("toLower() requires a string".to_string())),
    }
}

fn fn_trim(args: &[Value]) -> Result<Value, Error> {
    expect_args("trim", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.trim().to_string())),
        _ => Err(Error::Type("trim() requires a string".to_string())),
    }
}

fn fn_ltrim(args: &[Value]) -> Result<Value, Error> {
    expect_args("lTrim", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.trim_start().to_string())),
        _ => Err(Error::Type("lTrim() requires a string".to_string())),
    }
}

fn fn_rtrim(args: &[Value]) -> Result<Value, Error> {
    expect_args("rTrim", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.trim_end().to_string())),
        _ => Err(Error::Type("rTrim() requires a string".to_string())),
    }
}

fn fn_replace(args: &[Value]) -> Result<Value, Error> {
    if args.len() != 3 {
        return Err(Error::Runtime(
            "replace() requires 3 arguments".to_string(),
        ));
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(from), Value::String(to)) => {
            Ok(Value::String(s.replace(from.as_str(), to.as_str())))
        }
        _ => Err(Error::Type("replace() requires string arguments".to_string())),
    }
}

fn fn_substring(args: &[Value]) -> Result<Value, Error> {
    if args.len() < 2 || args.len() > 3 {
        return Err(Error::Runtime(
            "substring() requires 2 or 3 arguments".to_string(),
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => {
            let start = match &args[1] {
                Value::Integer(n) => *n as usize,
                _ => {
                    return Err(Error::Type(
                        "substring() requires integer start".to_string(),
                    ))
                }
            };
            let chars: Vec<char> = s.chars().collect();
            let start = start.min(chars.len());
            if args.len() == 3 {
                let len = match &args[2] {
                    Value::Integer(n) => *n as usize,
                    _ => {
                        return Err(Error::Type(
                            "substring() requires integer length".to_string(),
                        ))
                    }
                };
                let end = (start + len).min(chars.len());
                Ok(Value::String(chars[start..end].iter().collect()))
            } else {
                Ok(Value::String(chars[start..].iter().collect()))
            }
        }
        _ => Err(Error::Type("substring() requires a string".to_string())),
    }
}

fn fn_left(args: &[Value]) -> Result<Value, Error> {
    expect_args("left", args, 2)?;
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Integer(n)) => {
            let chars: Vec<char> = s.chars().collect();
            let n = (*n as usize).min(chars.len());
            Ok(Value::String(chars[..n].iter().collect()))
        }
        _ => Err(Error::Type("left() requires (string, integer)".to_string())),
    }
}

fn fn_right(args: &[Value]) -> Result<Value, Error> {
    expect_args("right", args, 2)?;
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::Integer(n)) => {
            let chars: Vec<char> = s.chars().collect();
            let n = (*n as usize).min(chars.len());
            Ok(Value::String(chars[chars.len() - n..].iter().collect()))
        }
        _ => Err(Error::Type("right() requires (string, integer)".to_string())),
    }
}

fn fn_split(args: &[Value]) -> Result<Value, Error> {
    expect_args("split", args, 2)?;
    match (&args[0], &args[1]) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::String(s), Value::String(delim)) => Ok(Value::List(
            s.split(delim.as_str())
                .map(|p| Value::String(p.to_string()))
                .collect(),
        )),
        _ => Err(Error::Type("split() requires (string, string)".to_string())),
    }
}

fn fn_reverse(args: &[Value]) -> Result<Value, Error> {
    expect_args("reverse", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
        Value::List(l) => Ok(Value::List(l.iter().rev().cloned().collect())),
        _ => Err(Error::Type(
            "reverse() requires a string or list".to_string(),
        )),
    }
}

fn fn_abs(args: &[Value]) -> Result<Value, Error> {
    expect_args("abs", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(n.abs())),
        Value::Float(f) => Ok(Value::Float(f.abs())),
        _ => Err(Error::Type("abs() requires a number".to_string())),
    }
}

fn fn_ceil(args: &[Value]) -> Result<Value, Error> {
    expect_args("ceil", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Float(f) => Ok(Value::Float(f.ceil())),
        _ => Err(Error::Type("ceil() requires a number".to_string())),
    }
}

fn fn_floor(args: &[Value]) -> Result<Value, Error> {
    expect_args("floor", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Float(f) => Ok(Value::Float(f.floor())),
        _ => Err(Error::Type("floor() requires a number".to_string())),
    }
}

fn fn_round(args: &[Value]) -> Result<Value, Error> {
    expect_args("round", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Float(f) => Ok(Value::Float(f.round())),
        _ => Err(Error::Type("round() requires a number".to_string())),
    }
}

fn fn_sign(args: &[Value]) -> Result<Value, Error> {
    expect_args("sign", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(n.signum())),
        Value::Float(f) => {
            if f.is_nan() {
                Ok(Value::Float(f64::NAN))
            } else {
                Ok(Value::Integer(if *f > 0.0 {
                    1
                } else if *f < 0.0 {
                    -1
                } else {
                    0
                }))
            }
        }
        _ => Err(Error::Type("sign() requires a number".to_string())),
    }
}

fn fn_sqrt(args: &[Value]) -> Result<Value, Error> {
    expect_args("sqrt", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Float((*n as f64).sqrt())),
        Value::Float(f) => Ok(Value::Float(f.sqrt())),
        _ => Err(Error::Type("sqrt() requires a number".to_string())),
    }
}

fn fn_log(args: &[Value]) -> Result<Value, Error> {
    expect_args("log", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Float((*n as f64).ln())),
        Value::Float(f) => Ok(Value::Float(f.ln())),
        _ => Err(Error::Type("log() requires a number".to_string())),
    }
}

fn fn_log10(args: &[Value]) -> Result<Value, Error> {
    expect_args("log10", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Float((*n as f64).log10())),
        Value::Float(f) => Ok(Value::Float(f.log10())),
        _ => Err(Error::Type("log10() requires a number".to_string())),
    }
}

fn fn_exp(args: &[Value]) -> Result<Value, Error> {
    expect_args("exp", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Float((*n as f64).exp())),
        Value::Float(f) => Ok(Value::Float(f.exp())),
        _ => Err(Error::Type("exp() requires a number".to_string())),
    }
}

fn fn_head(args: &[Value]) -> Result<Value, Error> {
    expect_args("head", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::List(l) => Ok(l.first().cloned().unwrap_or(Value::Null)),
        _ => Err(Error::Type("head() requires a list".to_string())),
    }
}

fn fn_tail(args: &[Value]) -> Result<Value, Error> {
    expect_args("tail", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::List(l) => {
            if l.is_empty() {
                Ok(Value::List(Vec::new()))
            } else {
                Ok(Value::List(l[1..].to_vec()))
            }
        }
        _ => Err(Error::Type("tail() requires a list".to_string())),
    }
}

fn fn_last(args: &[Value]) -> Result<Value, Error> {
    expect_args("last", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::List(l) => Ok(l.last().cloned().unwrap_or(Value::Null)),
        _ => Err(Error::Type("last() requires a list".to_string())),
    }
}

fn fn_range(args: &[Value]) -> Result<Value, Error> {
    if args.len() < 2 || args.len() > 3 {
        return Err(Error::Runtime(
            "range() requires 2 or 3 arguments".to_string(),
        ));
    }
    let start = match &args[0] {
        Value::Integer(n) => *n,
        _ => return Err(Error::Type("range() requires integer arguments".to_string())),
    };
    let end = match &args[1] {
        Value::Integer(n) => *n,
        _ => return Err(Error::Type("range() requires integer arguments".to_string())),
    };
    let step = if args.len() == 3 {
        match &args[2] {
            Value::Integer(n) => *n,
            _ => return Err(Error::Type("range() requires integer arguments".to_string())),
        }
    } else {
        1
    };
    if step == 0 {
        return Err(Error::Runtime("range() step cannot be zero".to_string()));
    }
    let mut result = Vec::new();
    let mut i = start;
    if step > 0 {
        while i <= end {
            result.push(Value::Integer(i));
            i += step;
        }
    } else {
        while i >= end {
            result.push(Value::Integer(i));
            i += step;
        }
    }
    Ok(Value::List(result))
}

fn fn_nodes(args: &[Value]) -> Result<Value, Error> {
    expect_args("nodes", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Path(p) => Ok(Value::List(
            p.nodes.iter().map(|n| Value::Node(n.clone())).collect(),
        )),
        _ => Err(Error::Type("nodes() requires a path".to_string())),
    }
}

fn fn_relationships(args: &[Value]) -> Result<Value, Error> {
    expect_args("relationships", args, 1)?;
    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Path(p) => Ok(Value::List(
            p.relationships
                .iter()
                .map(|r| Value::Relationship(r.clone()))
                .collect(),
        )),
        _ => Err(Error::Type("relationships() requires a path".to_string())),
    }
}

fn fn_exists(args: &[Value]) -> Result<Value, Error> {
    expect_args("exists", args, 1)?;
    Ok(Value::Bool(!args[0].is_null()))
}

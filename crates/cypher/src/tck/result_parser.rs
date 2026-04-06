/// Parser for TCK expected result values.
/// Converts strings like "(:A {name: 'bar'})", "[1, 2, 3]", "'hello'", "42" into Values.
use std::collections::BTreeMap;

use crate::value::{NodeValue, PathValue, RelValue, Value};

pub fn parse_value(input: &str) -> Value {
    let input = input.trim();
    if input.is_empty() {
        return Value::Null;
    }

    let mut parser = ValueParser::new(input);
    parser.parse_value()
}

struct ValueParser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> ValueParser<'a> {
    fn new(input: &'a str) -> Self {
        ValueParser {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    fn parse_value(&mut self) -> Value {
        self.skip_whitespace();
        if self.pos >= self.input.len() {
            return Value::Null;
        }

        match self.input[self.pos] {
            b'\'' => self.parse_string(),
            b'"' => self.parse_string(),
            b'(' => self.parse_node_or_path(),
            b'[' => self.parse_list_or_rel(),
            b'{' => self.parse_map(),
            b'<' => self.parse_path(),
            b'-' => self.parse_number(),
            b'0'..=b'9' => self.parse_number(),
            b't' | b'T' => {
                if self.try_keyword("true") {
                    Value::Bool(true)
                } else {
                    self.parse_identifier()
                }
            }
            b'f' | b'F' => {
                if self.try_keyword("false") {
                    Value::Bool(false)
                } else {
                    self.parse_identifier()
                }
            }
            b'n' | b'N' => {
                if self.try_keyword("null") {
                    Value::Null
                } else if self.try_keyword("NaN") {
                    Value::Float(f64::NAN)
                } else {
                    self.parse_identifier()
                }
            }
            b'I' => {
                if self.try_keyword("Inf") {
                    Value::Float(f64::INFINITY)
                } else {
                    self.parse_identifier()
                }
            }
            _ => self.parse_identifier(),
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        if self.pos < self.input.len() {
            Some(self.input[self.pos])
        } else {
            None
        }
    }

    fn advance(&mut self) -> u8 {
        let ch = self.input[self.pos];
        self.pos += 1;
        ch
    }

    fn try_keyword(&mut self, kw: &str) -> bool {
        let kw_bytes = kw.as_bytes();
        if self.pos + kw_bytes.len() > self.input.len() {
            return false;
        }
        // Case insensitive for true/false/null
        let slice = &self.input[self.pos..self.pos + kw_bytes.len()];
        if slice.eq_ignore_ascii_case(kw_bytes) {
            // Make sure it's not followed by alphanumeric
            let end = self.pos + kw_bytes.len();
            if end < self.input.len() && (self.input[end].is_ascii_alphanumeric() || self.input[end] == b'_') {
                return false;
            }
            self.pos = end;
            true
        } else {
            false
        }
    }

    fn parse_string(&mut self) -> Value {
        let quote = self.advance();
        let mut s = String::new();
        while self.pos < self.input.len() {
            let ch = self.advance();
            if ch == quote {
                // Check for doubled quote
                if self.peek() == Some(quote) {
                    self.advance();
                    s.push(quote as char);
                } else {
                    break;
                }
            } else if ch == b'\\' && self.pos < self.input.len() {
                let esc = self.advance();
                match esc {
                    b'n' => s.push('\n'),
                    b'r' => s.push('\r'),
                    b't' => s.push('\t'),
                    b'\\' => s.push('\\'),
                    b'\'' => s.push('\''),
                    b'"' => s.push('"'),
                    _ => {
                        s.push('\\');
                        s.push(esc as char);
                    }
                }
            } else {
                s.push(ch as char);
            }
        }
        Value::String(s)
    }

    fn parse_number(&mut self) -> Value {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.advance();
            self.skip_whitespace();
            // Check for -Inf
            if self.try_keyword("Inf") {
                return Value::Float(f64::NEG_INFINITY);
            }
        }
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') && self.pos + 1 < self.input.len() && self.input[self.pos + 1].is_ascii_digit() {
            is_float = true;
            self.pos += 1;
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        if self.peek() == Some(b'e') || self.peek() == Some(b'E') {
            is_float = true;
            self.pos += 1;
            if self.peek() == Some(b'+') || self.peek() == Some(b'-') {
                self.pos += 1;
            }
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        let text = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
        if is_float {
            Value::Float(text.parse().unwrap_or(0.0))
        } else {
            Value::Integer(text.parse().unwrap_or(0))
        }
    }

    fn parse_node_or_path(&mut self) -> Value {
        // ( ...  ) - node
        self.advance(); // skip (
        self.skip_whitespace();

        let mut labels = Vec::new();
        let mut properties = BTreeMap::new();

        // Parse labels
        while self.peek() == Some(b':') {
            self.advance();
            let label = self.read_identifier();
            labels.push(label);
        }

        self.skip_whitespace();

        // Parse properties
        if self.peek() == Some(b'{') {
            properties = self.parse_map_inner();
        }

        self.skip_whitespace();
        if self.peek() == Some(b')') {
            self.advance();
        }

        Value::Node(NodeValue {
            id: 0, // ID doesn't matter for TCK comparison
            labels,
            properties,
        })
    }

    fn parse_list_or_rel(&mut self) -> Value {
        // Check if it's a relationship: [:TYPE ...]
        let saved = self.pos;
        self.advance(); // skip [
        self.skip_whitespace();

        if self.peek() == Some(b':') {
            // Relationship
            self.advance();
            let rel_type = self.read_identifier();
            self.skip_whitespace();

            let mut properties = BTreeMap::new();
            if self.peek() == Some(b'{') {
                properties = self.parse_map_inner();
            }

            self.skip_whitespace();
            if self.peek() == Some(b']') {
                self.advance();
            }

            return Value::Relationship(RelValue {
                id: 0,
                start_node: 0,
                end_node: 0,
                rel_type,
                properties,
            });
        }

        // It's a list
        self.pos = saved;
        self.advance(); // skip [
        self.skip_whitespace();

        if self.peek() == Some(b']') {
            self.advance();
            return Value::List(Vec::new());
        }

        let mut items = Vec::new();
        loop {
            self.skip_whitespace();
            let val = self.parse_value();
            items.push(val);
            self.skip_whitespace();
            if self.peek() == Some(b',') {
                self.advance();
            } else {
                break;
            }
        }
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.advance();
        }
        Value::List(items)
    }

    fn parse_map(&mut self) -> Value {
        Value::Map(self.parse_map_inner())
    }

    fn parse_map_inner(&mut self) -> BTreeMap<String, Value> {
        let mut map = BTreeMap::new();
        self.advance(); // skip {
        self.skip_whitespace();

        if self.peek() == Some(b'}') {
            self.advance();
            return map;
        }

        loop {
            self.skip_whitespace();
            let key = self.read_identifier();
            self.skip_whitespace();
            if self.peek() == Some(b':') {
                self.advance();
            }
            self.skip_whitespace();
            let val = self.parse_value();
            map.insert(key, val);
            self.skip_whitespace();
            if self.peek() == Some(b',') {
                self.advance();
            } else {
                break;
            }
        }
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.advance();
        }
        map
    }

    fn parse_path(&mut self) -> Value {
        // <(n1)-[:REL]->(n2)>
        self.advance(); // skip <
        self.skip_whitespace();

        let mut nodes = Vec::new();
        let mut relationships = Vec::new();

        // First node
        if self.peek() == Some(b'(') {
            if let Value::Node(n) = self.parse_node_or_path() {
                nodes.push(n);
            }
        }

        // Subsequent relationship->(node) or <-relationship-(node) segments
        loop {
            // Check for outgoing: -[:REL]-> or incoming: <-[:REL]-
            let is_incoming = self.peek() == Some(b'<');
            let is_outgoing_start = self.peek() == Some(b'-');

            if !is_incoming && !is_outgoing_start {
                break;
            }

            if is_incoming {
                self.advance(); // <
            }

            if self.peek() != Some(b'-') {
                break;
            }
            self.advance(); // -

            let mut rel_type = String::new();
            let mut rel_props = BTreeMap::new();

            if self.peek() == Some(b'[') {
                self.advance(); // [
                self.skip_whitespace();
                if self.peek() == Some(b':') {
                    self.advance();
                    rel_type = self.read_identifier();
                }
                self.skip_whitespace();
                if self.peek() == Some(b'{') {
                    rel_props = self.parse_map_inner();
                }
                self.skip_whitespace();
                if self.peek() == Some(b']') {
                    self.advance();
                }
            }

            if self.peek() == Some(b'-') {
                self.advance(); // -
            }
            if !is_incoming && self.peek() == Some(b'>') {
                self.advance(); // >
            }

            relationships.push(RelValue {
                id: 0,
                start_node: 0,
                end_node: 0,
                rel_type,
                properties: rel_props,
            });

            if self.peek() == Some(b'(') {
                if let Value::Node(n) = self.parse_node_or_path() {
                    nodes.push(n);
                }
            }
        }

        self.skip_whitespace();
        if self.peek() == Some(b'>') {
            self.advance();
        }

        Value::Path(PathValue {
            nodes,
            relationships,
        })
    }

    fn read_identifier(&mut self) -> String {
        let start = self.pos;
        while self.pos < self.input.len()
            && (self.input[self.pos].is_ascii_alphanumeric() || self.input[self.pos] == b'_')
        {
            self.pos += 1;
        }
        std::str::from_utf8(&self.input[start..self.pos])
            .unwrap()
            .to_string()
    }

    fn parse_identifier(&mut self) -> Value {
        let ident = self.read_identifier();
        if ident.is_empty() {
            // Skip unknown character and return null
            if self.pos < self.input.len() {
                self.pos += 1;
            }
            Value::Null
        } else {
            // Could be a literal like NaN, Inf, etc.
            match ident.as_str() {
                "NaN" => Value::Float(f64::NAN),
                "Inf" | "Infinity" => Value::Float(f64::INFINITY),
                _ => Value::String(ident),
            }
        }
    }
}

/// Parse TCK parameter values.
pub fn parse_param_value(input: &str) -> Value {
    parse_value(input)
}

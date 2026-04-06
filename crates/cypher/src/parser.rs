use crate::ast::*;
use crate::error::Error;
use crate::token::Token;
use crate::value::Value;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    pub fn parse(&mut self) -> Result<Statement, Error> {
        let body = self.parse_clause_sequence()?;
        let mut unions = Vec::new();
        while self.check(&Token::Union) {
            self.advance();
            let all = self.eat(&Token::All);
            let union_body = self.parse_clause_sequence()?;
            unions.push(UnionPart {
                all,
                body: union_body,
            });
        }
        if !self.check(&Token::Eof) {
            return Err(Error::Parser(format!(
                "expected end of input, found {}",
                self.peek()
            )));
        }
        Ok(Statement { body, unions })
    }

    fn parse_clause_sequence(&mut self) -> Result<Vec<Clause>, Error> {
        let mut clauses = Vec::new();
        loop {
            match self.peek() {
                Token::Match => {
                    self.advance();
                    let clause = self.parse_match()?;
                    clauses.push(Clause::Match(clause));
                }
                Token::Optional => {
                    self.advance();
                    self.expect(&Token::Match)?;
                    let clause = self.parse_match()?;
                    clauses.push(Clause::OptionalMatch(clause));
                }
                Token::With => {
                    self.advance();
                    let clause = self.parse_with()?;
                    clauses.push(Clause::With(clause));
                }
                Token::Unwind => {
                    self.advance();
                    let clause = self.parse_unwind()?;
                    clauses.push(Clause::Unwind(clause));
                }
                Token::Return => {
                    self.advance();
                    let clause = self.parse_return()?;
                    clauses.push(Clause::Return(clause));
                    break;
                }
                Token::Create => {
                    self.advance();
                    let patterns = self.parse_pattern_list()?;
                    clauses.push(Clause::Create(patterns));
                }
                _ => break,
            }
        }
        Ok(clauses)
    }

    // -- MATCH --

    fn parse_match(&mut self) -> Result<MatchClause, Error> {
        let patterns = self.parse_pattern_list()?;
        let where_clause = if self.eat(&Token::Where) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        Ok(MatchClause {
            patterns,
            where_clause,
        })
    }

    fn parse_pattern_list(&mut self) -> Result<Vec<PatternPath>, Error> {
        let mut patterns = vec![self.parse_pattern_path()?];
        while self.eat(&Token::Comma) {
            patterns.push(self.parse_pattern_path()?);
        }
        Ok(patterns)
    }

    fn parse_pattern_path(&mut self) -> Result<PatternPath, Error> {
        // Check for path variable: p = (...)
        let variable = if self.is_ident() && self.peek_at(1) == Some(&Token::Eq) {
            let name = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            Some(name)
        } else {
            None
        };

        let start = self.parse_node_pattern()?;
        let mut hops = Vec::new();

        while self.is_rel_start() {
            let rel = self.parse_rel_pattern()?;
            let node = self.parse_node_pattern()?;
            hops.push((rel, node));
        }

        Ok(PatternPath {
            variable,
            start,
            hops,
        })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, Error> {
        self.expect(&Token::LParen)?;
        let variable = if self.is_ident() {
            Some(self.expect_ident()?)
        } else {
            None
        };
        let labels = self.parse_labels()?;
        let properties = if self.check(&Token::LBrace) {
            Some(self.parse_map_literal_pairs()?)
        } else {
            None
        };
        self.expect(&Token::RParen)?;
        Ok(NodePattern {
            variable,
            labels,
            properties,
        })
    }

    fn parse_labels(&mut self) -> Result<Vec<String>, Error> {
        let mut labels = Vec::new();
        while self.eat(&Token::Colon) {
            labels.push(self.expect_ident()?);
        }
        Ok(labels)
    }

    fn is_rel_start(&self) -> bool {
        matches!(self.peek(), Token::Minus | Token::Lt)
    }

    fn parse_rel_pattern(&mut self) -> Result<RelPattern, Error> {
        // Determine direction prefix
        let left_arrow = self.eat(&Token::Lt);

        self.expect(&Token::Minus)?;

        // Check for relationship detail in brackets
        let (variable, rel_types, length, properties) = if self.eat(&Token::LBracket) {
            let var = if self.is_ident() {
                Some(self.expect_ident()?)
            } else {
                None
            };
            let types = self.parse_rel_types()?;
            let len = self.parse_path_length()?;
            let props = if self.check(&Token::LBrace) {
                Some(self.parse_map_literal_pairs()?)
            } else {
                None
            };
            self.expect(&Token::RBracket)?;
            (var, types, len, props)
        } else {
            (None, Vec::new(), None, None)
        };

        self.expect(&Token::Minus)?;

        let right_arrow = self.eat(&Token::Gt);

        let direction = match (left_arrow, right_arrow) {
            (true, false) => Direction::Incoming,
            (false, true) => Direction::Outgoing,
            (false, false) => Direction::Both,
            (true, true) => {
                return Err(Error::Parser(
                    "invalid relationship direction: both < and >".to_string(),
                ))
            }
        };

        Ok(RelPattern {
            variable,
            rel_types,
            direction,
            length,
            properties,
        })
    }

    fn parse_rel_types(&mut self) -> Result<Vec<String>, Error> {
        let mut types = Vec::new();
        if self.eat(&Token::Colon) {
            types.push(self.expect_ident()?);
            while self.eat(&Token::Pipe) {
                // Handle :TYPE1|TYPE2 and :TYPE1|:TYPE2
                self.eat(&Token::Colon);
                types.push(self.expect_ident()?);
            }
        }
        Ok(types)
    }

    fn parse_path_length(&mut self) -> Result<Option<PathLength>, Error> {
        if !self.eat(&Token::Star) {
            return Ok(None);
        }
        // *
        if !matches!(self.peek(), Token::Integer(_) | Token::DotDot) {
            // Unbounded: *
            return Ok(Some(PathLength::Range(None, None)));
        }
        // Check for exact or range
        let min = if let Token::Integer(n) = self.peek() {
            let n = *n as usize;
            self.advance();
            Some(n)
        } else {
            None
        };
        if self.eat(&Token::DotDot) {
            let max = if let Token::Integer(n) = self.peek() {
                let n = *n as usize;
                self.advance();
                Some(n)
            } else {
                None
            };
            Ok(Some(PathLength::Range(min, max)))
        } else if let Some(n) = min {
            Ok(Some(PathLength::Exact(n)))
        } else {
            Ok(Some(PathLength::Range(None, None)))
        }
    }

    // -- RETURN --

    fn parse_return(&mut self) -> Result<ReturnClause, Error> {
        self.parse_return_body()
    }

    fn parse_return_body(&mut self) -> Result<ReturnClause, Error> {
        let distinct = self.eat(&Token::Distinct);
        let items = if self.eat(&Token::Star) {
            ReturnItems::Star
        } else {
            let mut items = vec![self.parse_return_item()?];
            while self.eat(&Token::Comma) {
                items.push(self.parse_return_item()?);
            }
            ReturnItems::Expressions(items)
        };
        let order_by = self.parse_order_by()?;
        let skip = if self.eat(&Token::Skip) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let limit = if self.eat(&Token::Limit) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(ReturnClause {
            distinct,
            items,
            order_by,
            skip,
            limit,
        })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, Error> {
        let expr = self.parse_expr()?;
        let alias = if self.eat(&Token::As) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(ReturnItem { expr, alias })
    }

    fn parse_order_by(&mut self) -> Result<Option<Vec<SortItem>>, Error> {
        if !self.check(&Token::Order) {
            return Ok(None);
        }
        self.advance(); // ORDER
        self.expect(&Token::By)?;
        let mut items = vec![self.parse_sort_item()?];
        while self.eat(&Token::Comma) {
            items.push(self.parse_sort_item()?);
        }
        Ok(Some(items))
    }

    fn parse_sort_item(&mut self) -> Result<SortItem, Error> {
        let expr = self.parse_expr()?;
        let direction = if self.eat(&Token::Asc) || self.eat(&Token::Ascending) {
            SortDirection::Asc
        } else if self.eat(&Token::Desc) || self.eat(&Token::Descending) {
            SortDirection::Desc
        } else {
            SortDirection::Asc
        };
        Ok(SortItem { expr, direction })
    }

    // -- WITH --

    fn parse_with(&mut self) -> Result<WithClause, Error> {
        let return_body = self.parse_return_body()?;
        let where_clause = if self.eat(&Token::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(WithClause {
            return_body,
            where_clause,
        })
    }

    // -- UNWIND --

    fn parse_unwind(&mut self) -> Result<UnwindClause, Error> {
        let expr = self.parse_expr()?;
        self.expect(&Token::As)?;
        let alias = self.expect_ident()?;
        Ok(UnwindClause { expr, alias })
    }

    // -- Expressions (precedence climbing) --

    pub fn parse_expr(&mut self) -> Result<Expr, Error> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, Error> {
        let mut left = self.parse_xor()?;
        while self.eat(&Token::Or) {
            let right = self.parse_xor()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_xor(&mut self) -> Result<Expr, Error> {
        let mut left = self.parse_and()?;
        while self.eat(&Token::Xor) {
            let right = self.parse_and()?;
            left = Expr::Xor(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, Error> {
        let mut left = self.parse_not()?;
        while self.eat(&Token::And) {
            let right = self.parse_not()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, Error> {
        if self.eat(&Token::Not) {
            let expr = self.parse_not()?;
            Ok(Expr::Not(Box::new(expr)))
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expr, Error> {
        let mut left = self.parse_add()?;

        loop {
            if self.eat(&Token::Eq) {
                let right = self.parse_add()?;
                left = Expr::Eq(Box::new(left), Box::new(right));
            } else if self.eat(&Token::Neq) {
                let right = self.parse_add()?;
                left = Expr::Neq(Box::new(left), Box::new(right));
            } else if self.eat(&Token::Lt) {
                let right = self.parse_add()?;
                left = Expr::Lt(Box::new(left), Box::new(right));
            } else if self.eat(&Token::Gt) {
                let right = self.parse_add()?;
                left = Expr::Gt(Box::new(left), Box::new(right));
            } else if self.eat(&Token::Lte) {
                let right = self.parse_add()?;
                left = Expr::Lte(Box::new(left), Box::new(right));
            } else if self.eat(&Token::Gte) {
                let right = self.parse_add()?;
                left = Expr::Gte(Box::new(left), Box::new(right));
            } else if self.check(&Token::Is) {
                self.advance();
                if self.eat(&Token::Not) {
                    self.expect(&Token::Null)?;
                    left = Expr::IsNotNull(Box::new(left));
                } else {
                    self.expect(&Token::Null)?;
                    left = Expr::IsNull(Box::new(left));
                }
            } else if self.eat(&Token::In) {
                let right = self.parse_add()?;
                left = Expr::In(Box::new(left), Box::new(right));
            } else if self.check(&Token::Starts) {
                self.advance();
                self.expect_ident_matching("WITH")?;
                let right = self.parse_add()?;
                left = Expr::StartsWith(Box::new(left), Box::new(right));
            } else if self.check(&Token::Ends) {
                self.advance();
                self.expect_ident_matching("WITH")?;
                let right = self.parse_add()?;
                left = Expr::EndsWith(Box::new(left), Box::new(right));
            } else if self.eat(&Token::Contains) {
                let right = self.parse_add()?;
                left = Expr::Contains(Box::new(left), Box::new(right));
            } else if self.eat(&Token::RegexEq) {
                let right = self.parse_add()?;
                left = Expr::RegexMatch(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_add(&mut self) -> Result<Expr, Error> {
        let mut left = self.parse_mul()?;
        loop {
            if self.eat(&Token::Plus) {
                let right = self.parse_mul()?;
                left = Expr::Add(Box::new(left), Box::new(right));
            } else if self.eat(&Token::Minus) {
                let right = self.parse_mul()?;
                left = Expr::Sub(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> Result<Expr, Error> {
        let mut left = self.parse_pow()?;
        loop {
            if self.eat(&Token::Star) {
                let right = self.parse_pow()?;
                left = Expr::Mul(Box::new(left), Box::new(right));
            } else if self.eat(&Token::Slash) {
                let right = self.parse_pow()?;
                left = Expr::Div(Box::new(left), Box::new(right));
            } else if self.eat(&Token::Percent) {
                let right = self.parse_pow()?;
                left = Expr::Mod(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_pow(&mut self) -> Result<Expr, Error> {
        let base = self.parse_unary()?;
        if self.eat(&Token::Caret) {
            let exp = self.parse_pow()?; // right-associative
            Ok(Expr::Pow(Box::new(base), Box::new(exp)))
        } else {
            Ok(base)
        }
    }

    fn parse_unary(&mut self) -> Result<Expr, Error> {
        if self.eat(&Token::Minus) {
            let expr = self.parse_unary()?;
            Ok(Expr::UnaryMinus(Box::new(expr)))
        } else if self.eat(&Token::Plus) {
            let expr = self.parse_unary()?;
            Ok(Expr::UnaryPlus(Box::new(expr)))
        } else {
            self.parse_postfix()
        }
    }

    fn parse_postfix(&mut self) -> Result<Expr, Error> {
        let mut expr = self.parse_primary()?;

        loop {
            if self.eat(&Token::Dot) {
                let prop = self.expect_ident()?;
                expr = Expr::Property(Box::new(expr), prop);
            } else if self.check(&Token::LBracket) {
                self.advance();
                // Index or slice
                if self.check(&Token::DotDot) {
                    // [..to]
                    self.advance();
                    let to = self.parse_expr()?;
                    self.expect(&Token::RBracket)?;
                    expr = Expr::Slice(Box::new(expr), None, Some(Box::new(to)));
                } else {
                    let idx = self.parse_expr()?;
                    if self.eat(&Token::DotDot) {
                        // [from..to] or [from..]
                        let to = if self.check(&Token::RBracket) {
                            None
                        } else {
                            Some(Box::new(self.parse_expr()?))
                        };
                        self.expect(&Token::RBracket)?;
                        expr = Expr::Slice(Box::new(expr), Some(Box::new(idx)), to);
                    } else {
                        self.expect(&Token::RBracket)?;
                        expr = Expr::Index(Box::new(expr), Box::new(idx));
                    }
                }
            } else if self.check(&Token::Colon) && !self.is_in_node_context() {
                // Label check: expr:Label (only in WHERE context, handled by caller)
                // We handle n:Label in WHERE as postfix
                let mut labels = Vec::new();
                while self.eat(&Token::Colon) {
                    labels.push(self.expect_ident()?);
                }
                if labels.len() == 1 {
                    expr = Expr::HasLabel(Box::new(expr), labels.into_iter().next().unwrap());
                } else {
                    expr = Expr::HasLabels(Box::new(expr), labels);
                }
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn is_in_node_context(&self) -> bool {
        // Heuristic: don't parse colon as label when we're inside brackets or parens
        // that are part of a pattern. This is imperfect but handles common cases.
        false
    }

    fn parse_primary(&mut self) -> Result<Expr, Error> {
        match self.peek().clone() {
            Token::Integer(n) => {
                self.advance();
                Ok(Expr::Literal(Value::Integer(n)))
            }
            Token::Float(f) => {
                self.advance();
                Ok(Expr::Literal(Value::Float(f)))
            }
            Token::StringLit(s) => {
                self.advance();
                Ok(Expr::Literal(Value::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Value::Bool(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Value::Bool(false)))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Value::Null))
            }
            Token::Parameter(name) => {
                self.advance();
                Ok(Expr::Parameter(name))
            }
            Token::Count => {
                self.advance();
                self.expect(&Token::LParen)?;
                if self.eat(&Token::Star) {
                    self.expect(&Token::RParen)?;
                    Ok(Expr::CountStar)
                } else {
                    let distinct = self.eat(&Token::Distinct);
                    let arg = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expr::FunctionCall {
                        name: "count".to_string(),
                        distinct,
                        args: vec![arg],
                    })
                }
            }
            Token::Exists => {
                self.advance();
                self.expect(&Token::LParen)?;
                // Could be EXISTS((pattern)) for subquery or exists(expr) for property existence
                if self.check(&Token::LParen) || self.check(&Token::Match) {
                    // Subquery-like: EXISTS { MATCH ... }
                    // For now treat EXISTS((a)-[:REL]->(b)) as pattern
                    let patterns = self.parse_pattern_list()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expr::ExistsSubquery(Box::new(MatchClause {
                        patterns,
                        where_clause: None,
                    })))
                } else {
                    let arg = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expr::FunctionCall {
                        name: "exists".to_string(),
                        distinct: false,
                        args: vec![arg],
                    })
                }
            }
            Token::Case => {
                self.advance();
                self.parse_case()
            }
            Token::LBracket => {
                self.advance();
                // List literal or list comprehension
                if self.check(&Token::RBracket) {
                    self.advance();
                    Ok(Expr::ListLiteral(Vec::new()))
                } else {
                    // Check if this is a list comprehension: [x IN list ...]
                    let maybe_comp = self.try_list_comprehension()?;
                    if let Some(comp) = maybe_comp {
                        Ok(comp)
                    } else {
                        // Regular list literal
                        let first = self.parse_expr()?;
                        let mut items = vec![first];
                        while self.eat(&Token::Comma) {
                            items.push(self.parse_expr()?);
                        }
                        self.expect(&Token::RBracket)?;
                        Ok(Expr::ListLiteral(items))
                    }
                }
            }
            Token::LBrace => {
                // Map literal
                let pairs = self.parse_map_literal_pairs()?;
                Ok(Expr::MapLiteral(pairs))
            }
            Token::LParen => {
                self.advance();
                // Check if this is a pattern expression or a parenthesized expression
                if self.is_pattern_start() {
                    // Pattern expression - rewind the paren
                    self.pos -= 1;
                    let path = self.parse_pattern_path()?;
                    Ok(Expr::PatternExpr(path))
                } else {
                    let expr = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    Ok(expr)
                }
            }
            Token::Ident(name) => {
                self.advance();
                // Function call?
                if self.check(&Token::LParen) {
                    self.advance();
                    let distinct = self.eat(&Token::Distinct);
                    if self.check(&Token::RParen) {
                        self.advance();
                        Ok(Expr::FunctionCall {
                            name,
                            distinct,
                            args: Vec::new(),
                        })
                    } else {
                        let mut args = vec![self.parse_expr()?];
                        while self.eat(&Token::Comma) {
                            args.push(self.parse_expr()?);
                        }
                        self.expect(&Token::RParen)?;
                        Ok(Expr::FunctionCall {
                            name,
                            distinct,
                            args,
                        })
                    }
                } else {
                    Ok(Expr::Variable(name))
                }
            }
            // Some keywords can be used as identifiers in certain contexts
            Token::All => {
                self.advance();
                if self.check(&Token::LParen) {
                    self.advance();
                    let var = self.expect_ident()?;
                    self.expect(&Token::In)?;
                    let source = self.parse_expr()?;
                    self.expect(&Token::Where)?;
                    let pred = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expr::FunctionCall {
                        name: "all".to_string(),
                        distinct: false,
                        args: vec![
                            Expr::Variable(var),
                            source,
                            pred,
                        ],
                    })
                } else {
                    Ok(Expr::Variable("all".to_string()))
                }
            }
            tok => Err(Error::Parser(format!("unexpected token in expression: {tok}"))),
        }
    }

    fn parse_case(&mut self) -> Result<Expr, Error> {
        // CASE [expr] WHEN ... THEN ... [ELSE ...] END
        let operand = if !self.check(&Token::When) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        let mut when_clauses = Vec::new();
        while self.eat(&Token::When) {
            let condition = self.parse_expr()?;
            self.expect(&Token::Then)?;
            let result = self.parse_expr()?;
            when_clauses.push((condition, result));
        }
        let else_clause = if self.eat(&Token::Else) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect(&Token::End)?;
        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    fn try_list_comprehension(&mut self) -> Result<Option<Expr>, Error> {
        // Save position for backtracking
        let saved_pos = self.pos;

        // Try: ident IN expr [WHERE expr] [| expr]
        if let Token::Ident(ref name) = self.peek().clone() {
            let name = name.clone();
            self.advance();
            if self.eat(&Token::In) {
                let source = self.parse_expr()?;
                let filter = if self.eat(&Token::Where) {
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                let projection = if self.eat(&Token::Pipe) {
                    Some(Box::new(self.parse_expr()?))
                } else {
                    None
                };
                self.expect(&Token::RBracket)?;
                return Ok(Some(Expr::ListComprehension {
                    variable: name,
                    source: Box::new(source),
                    filter,
                    projection,
                }));
            }
            // Not a comprehension, backtrack
            self.pos = saved_pos;
        }
        Ok(None)
    }

    fn is_pattern_start(&self) -> bool {
        // After a '(', check if this looks like a node pattern
        // A node pattern starts with: identifier followed by : or ), or : directly, or )
        match self.peek() {
            Token::Ident(_) => {
                // Could be (n:Label... or (n)- or (n {
                matches!(
                    self.peek_at(1),
                    Some(Token::Colon) | Some(Token::RParen) | Some(Token::LBrace)
                ) && self.looks_like_pattern()
            }
            Token::Colon => true, // (:Label ...
            Token::RParen => {
                // () followed by - or < means pattern
                matches!(self.peek_at(1), Some(Token::Minus) | Some(Token::Lt))
            }
            _ => false,
        }
    }

    fn looks_like_pattern(&self) -> bool {
        // Scan ahead to see if after the closing ) there's a relationship indicator
        let mut depth = 1;
        let mut i = self.pos;
        while i < self.tokens.len() && depth > 0 {
            match &self.tokens[i] {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        // After closing paren, check for relationship start
        if i < self.tokens.len() {
            matches!(self.tokens[i], Token::Minus | Token::Lt)
        } else {
            false
        }
    }

    fn parse_map_literal_pairs(&mut self) -> Result<Vec<(String, Expr)>, Error> {
        self.expect(&Token::LBrace)?;
        if self.eat(&Token::RBrace) {
            return Ok(Vec::new());
        }
        let mut pairs = Vec::new();
        loop {
            let key = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let value = self.parse_expr()?;
            pairs.push((key, value));
            if !self.eat(&Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(pairs)
    }

    // -- Helpers --

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
    }

    fn advance(&mut self) -> &Token {
        let tok = self.tokens.get(self.pos).unwrap_or(&Token::Eof);
        self.pos += 1;
        tok
    }

    fn check(&self, expected: &Token) -> bool {
        std::mem::discriminant(self.peek()) == std::mem::discriminant(expected)
    }

    fn eat(&mut self, expected: &Token) -> bool {
        if self.check(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: &Token) -> Result<(), Error> {
        if self.check(expected) {
            self.advance();
            Ok(())
        } else {
            Err(Error::Parser(format!(
                "expected {expected}, found {}",
                self.peek()
            )))
        }
    }

    fn is_ident(&self) -> bool {
        matches!(self.peek(), Token::Ident(_))
    }

    fn expect_ident(&mut self) -> Result<String, Error> {
        match self.peek().clone() {
            Token::Ident(name) => {
                self.advance();
                Ok(name)
            }
            // Some keywords can be used as identifiers
            Token::Count => {
                self.advance();
                Ok("count".to_string())
            }
            Token::Exists => {
                self.advance();
                Ok("exists".to_string())
            }
            Token::All => {
                self.advance();
                Ok("all".to_string())
            }
            Token::Asc => {
                self.advance();
                Ok("asc".to_string())
            }
            Token::Desc => {
                self.advance();
                Ok("desc".to_string())
            }
            tok => Err(Error::Parser(format!("expected identifier, found {tok}"))),
        }
    }

    fn expect_ident_matching(&mut self, expected: &str) -> Result<(), Error> {
        // For "STARTS WITH" - the WITH after STARTS is the keyword Token::With
        if expected.eq_ignore_ascii_case("WITH") && self.check(&Token::With) {
            self.advance();
            return Ok(());
        }
        let name = self.expect_ident()?;
        if name.eq_ignore_ascii_case(expected) {
            Ok(())
        } else {
            Err(Error::Parser(format!(
                "expected '{expected}', found '{name}'"
            )))
        }
    }
}

use crate::ast::*;
use crate::lexer::Token;
use crate::Error;

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: Token) -> Result<(), Error> {
        match self.advance() {
            Some(tok) if tok == expected => Ok(()),
            Some(tok) => Err(Error::Parse(format!("expected {expected:?}, got {tok:?}"))),
            None => Err(Error::Parse(format!(
                "expected {expected:?}, got end of input"
            ))),
        }
    }

    fn expect_ident(&mut self) -> Result<String, Error> {
        match self.advance() {
            Some(Token::Ident(s)) => Ok(s),
            Some(tok) => Err(Error::Parse(format!("expected identifier, got {tok:?}"))),
            None => Err(Error::Parse(
                "expected identifier, got end of input".to_string(),
            )),
        }
    }

    fn parse_query(&mut self) -> Result<Query, Error> {
        self.expect(Token::Match)?;
        let match_clause = MatchClause {
            pattern: self.parse_pattern()?,
        };

        let where_clause = if matches!(self.peek(), Some(Token::Where)) {
            self.advance();
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        self.expect(Token::Return)?;
        let return_clause = self.parse_return_clause()?;

        Ok(Query {
            match_clause,
            where_clause,
            return_clause,
        })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, Error> {
        self.expect(Token::LParen)?;
        let variable = if matches!(self.peek(), Some(Token::Ident(_))) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        self.expect(Token::RParen)?;
        Ok(NodePattern { variable })
    }

    fn parse_pattern(&mut self) -> Result<Pattern, Error> {
        let start = self.parse_node_pattern()?;

        if !matches!(self.peek(), Some(Token::Dash) | Some(Token::Lt)) {
            return Ok(Pattern::Node(start));
        }

        let mut hops = Vec::new();
        while matches!(self.peek(), Some(Token::Dash) | Some(Token::Lt)) {
            hops.push(self.parse_rel_and_end()?);
        }
        Ok(Pattern::Path(PathPattern { start, hops }))
    }

    /// Parses the relationship portion and trailing node of a path pattern.
    ///
    /// Supported forms:
    ///
    /// | Syntax        | Direction  |
    /// |---------------|------------|
    /// | `-[r]->`      | Outgoing   |
    /// | `-->`         | Outgoing   |
    /// | `<-[r]-`      | Incoming   |
    /// | `<--`         | Incoming   |
    /// | `-[r]-`       | Both       |
    /// | `--`          | Both       |
    fn parse_rel_and_end(&mut self) -> Result<(RelPattern, NodePattern), Error> {
        let direction;
        let variable;

        match self.advance() {
            Some(Token::Dash) => {
                match self.peek() {
                    Some(Token::Gt) => {
                        // --> (outgoing shorthand)
                        self.advance();
                        direction = Direction::Outgoing;
                        variable = None;
                    }
                    Some(Token::Dash) => {
                        self.advance(); // consume second -
                        if matches!(self.peek(), Some(Token::Gt)) {
                            // --> (outgoing shorthand)
                            self.advance();
                            direction = Direction::Outgoing;
                        } else {
                            // -- (both shorthand)
                            direction = Direction::Both;
                        }
                        variable = None;
                    }
                    Some(Token::LBracket) => {
                        // -[var?]-> or -[var?]-
                        self.advance(); // consume [
                        variable = if matches!(self.peek(), Some(Token::Ident(_))) {
                            Some(self.expect_ident()?)
                        } else {
                            None
                        };
                        self.expect(Token::RBracket)?;
                        self.expect(Token::Dash)?;
                        if matches!(self.peek(), Some(Token::Gt)) {
                            self.advance();
                            direction = Direction::Outgoing;
                        } else {
                            direction = Direction::Both;
                        }
                    }
                    Some(tok) => {
                        return Err(Error::Parse(format!(
                            "unexpected token after '-': {tok:?}"
                        )))
                    }
                    None => {
                        return Err(Error::Parse(
                            "unexpected end of input after '-'".to_string(),
                        ))
                    }
                }
            }
            Some(Token::Lt) => {
                // <-- or <-[var?]-
                self.expect(Token::Dash)?; // consume -
                match self.peek() {
                    Some(Token::Dash) => {
                        // <-- (incoming shorthand)
                        self.advance();
                        direction = Direction::Incoming;
                        variable = None;
                    }
                    Some(Token::LBracket) => {
                        // <-[var?]-
                        self.advance(); // consume [
                        variable = if matches!(self.peek(), Some(Token::Ident(_))) {
                            Some(self.expect_ident()?)
                        } else {
                            None
                        };
                        self.expect(Token::RBracket)?;
                        self.expect(Token::Dash)?;
                        direction = Direction::Incoming;
                    }
                    Some(tok) => {
                        return Err(Error::Parse(format!(
                            "unexpected token after '<-': {tok:?}"
                        )))
                    }
                    None => {
                        return Err(Error::Parse(
                            "unexpected end of input after '<-'".to_string(),
                        ))
                    }
                }
            }
            Some(tok) => {
                return Err(Error::Parse(format!(
                    "expected relationship pattern, got {tok:?}"
                )))
            }
            None => {
                return Err(Error::Parse(
                    "unexpected end of input in pattern".to_string(),
                ))
            }
        }

        let end = self.parse_node_pattern()?;
        Ok((RelPattern { variable, direction }, end))
    }

    /// Parses `WHERE id(variable) = N`.
    fn parse_where_clause(&mut self) -> Result<WhereClause, Error> {
        let func = self.expect_ident()?;
        if func != "id" {
            return Err(Error::Parse(format!(
                "unsupported WHERE expression: expected 'id(var) = N', found '{func}(...)'"
            )));
        }
        self.expect(Token::LParen)?;
        let var = self.expect_ident()?;
        self.expect(Token::RParen)?;
        self.expect(Token::Eq)?;
        match self.advance() {
            Some(Token::Integer(n)) => Ok(WhereClause {
                predicate: Predicate::IdEquals(var, n),
            }),
            Some(tok) => Err(Error::Parse(format!("expected integer literal, got {tok:?}"))),
            None => Err(Error::Parse(
                "expected integer literal, got end of input".to_string(),
            )),
        }
    }

    fn parse_return_clause(&mut self) -> Result<ReturnClause, Error> {
        let mut items = vec![self.expect_ident()?];
        while matches!(self.peek(), Some(Token::Comma)) {
            self.advance();
            items.push(self.expect_ident()?);
        }
        Ok(ReturnClause { items })
    }
}

pub(crate) fn parse(tokens: Vec<Token>) -> Result<Query, Error> {
    let mut parser = Parser::new(tokens);
    let query = parser.parse_query()?;
    if parser.pos < parser.tokens.len() {
        return Err(Error::Parse(format!(
            "unexpected tokens after query end: {:?}",
            &parser.tokens[parser.pos..]
        )));
    }
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::tokenize;

    fn parse_query(input: &str) -> Query {
        let tokens = tokenize(input).unwrap();
        parse(tokens).unwrap()
    }

    #[test]
    fn parse_node_only() {
        let q = parse_query("MATCH (n) RETURN n");
        assert!(matches!(q.match_clause.pattern, Pattern::Node(_)));
        assert!(q.where_clause.is_none());
        assert_eq!(q.return_clause.items, vec!["n"]);
    }

    #[test]
    fn parse_outgoing_path() {
        let q = parse_query("MATCH (a)-[r]->(b) RETURN a, b");
        let Pattern::Path(ref path) = q.match_clause.pattern else {
            panic!("expected path pattern");
        };
        assert_eq!(path.hops.len(), 1);
        assert_eq!(path.hops[0].0.direction, Direction::Outgoing);
        assert_eq!(path.start.variable, Some("a".to_string()));
        assert_eq!(path.hops[0].1.variable, Some("b".to_string()));
    }

    #[test]
    fn parse_incoming_path() {
        let q = parse_query("MATCH (a)<-[r]-(b) RETURN a, b");
        let Pattern::Path(ref path) = q.match_clause.pattern else {
            panic!("expected path pattern");
        };
        assert_eq!(path.hops[0].0.direction, Direction::Incoming);
    }

    #[test]
    fn parse_undirected_path() {
        let q = parse_query("MATCH (a)-[r]-(b) RETURN a, b");
        let Pattern::Path(ref path) = q.match_clause.pattern else {
            panic!("expected path pattern");
        };
        assert_eq!(path.hops[0].0.direction, Direction::Both);
    }

    #[test]
    fn parse_shorthand_arrows() {
        let q = parse_query("MATCH (a)-->(b) RETURN a, b");
        let Pattern::Path(ref path) = q.match_clause.pattern else {
            panic!("expected path pattern");
        };
        assert_eq!(path.hops[0].0.direction, Direction::Outgoing);
        assert!(path.hops[0].0.variable.is_none());
    }

    #[test]
    fn parse_two_hop_path() {
        let q = parse_query("MATCH (a)-->(b)-->(c) RETURN a, b, c");
        let Pattern::Path(ref path) = q.match_clause.pattern else {
            panic!("expected path pattern");
        };
        assert_eq!(path.hops.len(), 2);
        assert_eq!(path.start.variable, Some("a".to_string()));
        assert_eq!(path.hops[0].1.variable, Some("b".to_string()));
        assert_eq!(path.hops[1].1.variable, Some("c".to_string()));
    }

    #[test]
    fn parse_where_clause() {
        let q = parse_query("MATCH (a)-[r]->(b) WHERE id(a) = 42 RETURN b");
        let Some(ref wc) = q.where_clause else {
            panic!("expected where clause");
        };
        assert!(matches!(wc.predicate, Predicate::IdEquals(ref v, 42) if v == "a"));
        assert_eq!(q.return_clause.items, vec!["b"]);
    }
}

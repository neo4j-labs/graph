use crate::error::Error;
use crate::token::Token;

pub struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Lexer {
            input: input.as_bytes(),
            pos: 0,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, Error> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                tokens.push(Token::Eof);
                break;
            }
            let tok = self.next_token()?;
            tokens.push(tok);
        }
        Ok(tokens)
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() {
            let ch = self.input[self.pos];
            if ch == b'/' && self.pos + 1 < self.input.len() && self.input[self.pos + 1] == b'/' {
                // Line comment
                self.pos += 2;
                while self.pos < self.input.len() && self.input[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else if ch == b'/'
                && self.pos + 1 < self.input.len()
                && self.input[self.pos + 1] == b'*'
            {
                // Block comment
                self.pos += 2;
                while self.pos + 1 < self.input.len()
                    && !(self.input[self.pos] == b'*' && self.input[self.pos + 1] == b'/')
                {
                    self.pos += 1;
                }
                if self.pos + 1 < self.input.len() {
                    self.pos += 2;
                }
            } else if ch.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        if self.pos < self.input.len() {
            Some(self.input[self.pos])
        } else {
            None
        }
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        let idx = self.pos + offset;
        if idx < self.input.len() {
            Some(self.input[idx])
        } else {
            None
        }
    }

    fn advance(&mut self) -> u8 {
        let ch = self.input[self.pos];
        self.pos += 1;
        ch
    }

    fn next_token(&mut self) -> Result<Token, Error> {
        let ch = self.input[self.pos];

        match ch {
            b'(' => {
                self.advance();
                Ok(Token::LParen)
            }
            b')' => {
                self.advance();
                Ok(Token::RParen)
            }
            b'[' => {
                self.advance();
                Ok(Token::LBracket)
            }
            b']' => {
                self.advance();
                Ok(Token::RBracket)
            }
            b'{' => {
                self.advance();
                Ok(Token::LBrace)
            }
            b'}' => {
                self.advance();
                Ok(Token::RBrace)
            }
            b',' => {
                self.advance();
                Ok(Token::Comma)
            }
            b':' => {
                self.advance();
                Ok(Token::Colon)
            }
            b';' => {
                self.advance();
                Ok(Token::Semicolon)
            }
            b'|' => {
                self.advance();
                Ok(Token::Pipe)
            }
            b'^' => {
                self.advance();
                Ok(Token::Caret)
            }
            b'%' => {
                self.advance();
                Ok(Token::Percent)
            }
            b'/' => {
                self.advance();
                Ok(Token::Slash)
            }
            b'*' => {
                self.advance();
                Ok(Token::Star)
            }
            b'.' => {
                if self.peek_at(1).is_some_and(|b| b.is_ascii_digit()) {
                    // Float without integer part: .5, .123e4
                    self.scan_float_from_dot()
                } else {
                    self.advance();
                    if self.peek() == Some(b'.') {
                        self.advance();
                        Ok(Token::DotDot)
                    } else {
                        Ok(Token::Dot)
                    }
                }
            }
            b'+' => {
                self.advance();
                if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(Token::PlusEq)
                } else {
                    Ok(Token::Plus)
                }
            }
            b'-' => {
                self.advance();
                Ok(Token::Minus)
            }
            b'=' => {
                self.advance();
                if self.peek() == Some(b'~') {
                    self.advance();
                    Ok(Token::RegexEq)
                } else {
                    Ok(Token::Eq)
                }
            }
            b'<' => {
                self.advance();
                if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(Token::Lte)
                } else if self.peek() == Some(b'>') {
                    self.advance();
                    Ok(Token::Neq)
                } else {
                    Ok(Token::Lt)
                }
            }
            b'>' => {
                self.advance();
                if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(Token::Gte)
                } else {
                    Ok(Token::Gt)
                }
            }
            b'!' => {
                self.advance();
                if self.peek() == Some(b'=') {
                    self.advance();
                    Ok(Token::Neq)
                } else {
                    Err(Error::Lexer(format!(
                        "unexpected character '!' at position {}",
                        self.pos - 1
                    )))
                }
            }
            b'$' => {
                self.advance();
                let name = self.read_identifier()?;
                Ok(Token::Parameter(name))
            }
            b'\'' | b'"' => self.read_string(),
            b'`' => self.read_backtick_ident(),
            b'0'..=b'9' => self.read_number(),
            b'_' | b'a'..=b'z' | b'A'..=b'Z' => self.read_ident_or_keyword(),
            _ => Err(Error::Lexer(format!(
                "unexpected character '{}' at position {}",
                ch as char, self.pos
            ))),
        }
    }

    fn read_string(&mut self) -> Result<Token, Error> {
        let quote = self.advance();
        let mut s = String::new();
        loop {
            if self.pos >= self.input.len() {
                return Err(Error::Lexer("unterminated string literal".to_string()));
            }
            let ch = self.advance();
            if ch == quote {
                // Check for escaped quote (doubled)
                if self.peek() == Some(quote) {
                    self.advance();
                    s.push(quote as char);
                } else {
                    break;
                }
            } else if ch == b'\\' {
                if self.pos >= self.input.len() {
                    return Err(Error::Lexer("unterminated escape sequence".to_string()));
                }
                let esc = self.advance();
                match esc {
                    b'n' => s.push('\n'),
                    b'r' => s.push('\r'),
                    b't' => s.push('\t'),
                    b'\\' => s.push('\\'),
                    b'\'' => s.push('\''),
                    b'"' => s.push('"'),
                    b'u' => {
                        // Unicode escape \uXXXX
                        let mut hex = String::new();
                        for _ in 0..4 {
                            if self.pos >= self.input.len() {
                                return Err(Error::Lexer(
                                    "unterminated unicode escape".to_string(),
                                ));
                            }
                            hex.push(self.advance() as char);
                        }
                        let cp = u32::from_str_radix(&hex, 16).map_err(|_| {
                            Error::Lexer(format!("invalid unicode escape: \\u{hex}"))
                        })?;
                        let ch = char::from_u32(cp).ok_or_else(|| {
                            Error::Lexer(format!("invalid unicode codepoint: {cp}"))
                        })?;
                        s.push(ch);
                    }
                    _ => {
                        s.push('\\');
                        s.push(esc as char);
                    }
                }
            } else {
                s.push(ch as char);
            }
        }
        Ok(Token::StringLit(s))
    }

    fn read_backtick_ident(&mut self) -> Result<Token, Error> {
        self.advance(); // skip `
        let mut s = String::new();
        loop {
            if self.pos >= self.input.len() {
                return Err(Error::Lexer(
                    "unterminated backtick identifier".to_string(),
                ));
            }
            let ch = self.advance();
            if ch == b'`' {
                if self.peek() == Some(b'`') {
                    self.advance();
                    s.push('`');
                } else {
                    break;
                }
            } else {
                s.push(ch as char);
            }
        }
        Ok(Token::Ident(s))
    }

    fn read_number(&mut self) -> Result<Token, Error> {
        let start = self.pos;

        // Check for hex (0x/0X) or octal (0o/0O) prefix
        if self.input[self.pos] == b'0' {
            if let Some(next) = self.peek_at(1) {
                if next == b'x' || next == b'X' {
                    self.pos += 2; // skip '0x'
                    let hex_start = self.pos;
                    while self.pos < self.input.len()
                        && self.input[self.pos].is_ascii_hexdigit()
                    {
                        self.pos += 1;
                    }
                    if self.pos == hex_start {
                        return Err(Error::Lexer("invalid hex integer: no digits".to_string()));
                    }
                    let hex_text =
                        std::str::from_utf8(&self.input[hex_start..self.pos]).unwrap();
                    let n = i64::from_str_radix(hex_text, 16).map_err(|_| {
                        Error::Lexer(format!("invalid hex integer: 0x{hex_text}"))
                    })?;
                    return Ok(Token::Integer(n));
                } else if next == b'o' || next == b'O' {
                    self.pos += 2; // skip '0o'
                    let oct_start = self.pos;
                    while self.pos < self.input.len()
                        && self.input[self.pos] >= b'0'
                        && self.input[self.pos] <= b'7'
                    {
                        self.pos += 1;
                    }
                    if self.pos == oct_start {
                        return Err(Error::Lexer(
                            "invalid octal integer: no digits".to_string(),
                        ));
                    }
                    let oct_text =
                        std::str::from_utf8(&self.input[oct_start..self.pos]).unwrap();
                    let n = i64::from_str_radix(oct_text, 8).map_err(|_| {
                        Error::Lexer(format!("invalid octal integer: 0o{oct_text}"))
                    })?;
                    return Ok(Token::Integer(n));
                }
            }
        }

        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
            self.pos += 1;
        }

        // Check for float
        let is_float = self.peek() == Some(b'.')
            && self.peek_at(1).map_or(false, |c| c.is_ascii_digit());

        if is_float {
            self.pos += 1; // skip '.'
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            // Scientific notation
            if self.peek() == Some(b'e') || self.peek() == Some(b'E') {
                self.pos += 1;
                if self.peek() == Some(b'+') || self.peek() == Some(b'-') {
                    self.pos += 1;
                }
                while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                    self.pos += 1;
                }
            }
            let text = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
            let f: f64 = text
                .parse()
                .map_err(|_| Error::Lexer(format!("invalid float: {text}")))?;
            Ok(Token::Float(f))
        } else if self.peek() == Some(b'e') || self.peek() == Some(b'E') {
            // Scientific notation without decimal point (still float)
            self.pos += 1;
            if self.peek() == Some(b'+') || self.peek() == Some(b'-') {
                self.pos += 1;
            }
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            let text = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
            let f: f64 = text
                .parse()
                .map_err(|_| Error::Lexer(format!("invalid float: {text}")))?;
            Ok(Token::Float(f))
        } else {
            let text = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
            let n: i64 = text
                .parse()
                .map_err(|_| Error::Lexer(format!("invalid integer: {text}")))?;
            Ok(Token::Integer(n))
        }
    }

    fn scan_float_from_dot(&mut self) -> Result<Token, Error> {
        let start = self.pos;
        self.pos += 1; // skip '.'
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        // Scientific notation
        if self.peek() == Some(b'e') || self.peek() == Some(b'E') {
            self.pos += 1;
            if self.peek() == Some(b'+') || self.peek() == Some(b'-') {
                self.pos += 1;
            }
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
        }
        let text = std::str::from_utf8(&self.input[start..self.pos]).unwrap();
        let f: f64 = text
            .parse()
            .map_err(|_| Error::Lexer(format!("invalid float: {text}")))?;
        Ok(Token::Float(f))
    }

    fn read_identifier(&mut self) -> Result<String, Error> {
        let start = self.pos;
        while self.pos < self.input.len()
            && (self.input[self.pos].is_ascii_alphanumeric() || self.input[self.pos] == b'_')
        {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(Error::Lexer(format!(
                "expected identifier at position {}",
                self.pos
            )));
        }
        Ok(std::str::from_utf8(&self.input[start..self.pos])
            .unwrap()
            .to_string())
    }

    fn read_ident_or_keyword(&mut self) -> Result<Token, Error> {
        let name = self.read_identifier()?;
        let upper = name.to_ascii_uppercase();
        let tok = match upper.as_str() {
            "MATCH" => Token::Match,
            "OPTIONAL" => Token::Optional,
            "WHERE" => Token::Where,
            "RETURN" => Token::Return,
            "WITH" => Token::With,
            "UNWIND" => Token::Unwind,
            "UNION" => Token::Union,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" => Token::Asc,
            "ASCENDING" => Token::Ascending,
            "DESC" => Token::Desc,
            "DESCENDING" => Token::Descending,
            "SKIP" => Token::Skip,
            "LIMIT" => Token::Limit,
            "CREATE" => Token::Create,
            "DELETE" => Token::Delete,
            "DETACH" => Token::Detach,
            "SET" => Token::Set,
            "REMOVE" => Token::Remove,
            "MERGE" => Token::Merge,
            "ON" => Token::On,
            "AND" => Token::And,
            "OR" => Token::Or,
            "XOR" => Token::Xor,
            "NOT" => Token::Not,
            "IN" => Token::In,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "AS" => Token::As,
            "DISTINCT" => Token::Distinct,
            "ALL" => Token::All,
            "CASE" => Token::Case,
            "WHEN" => Token::When,
            "THEN" => Token::Then,
            "ELSE" => Token::Else,
            "END" => Token::End,
            "STARTS" => Token::Starts,
            "ENDS" => Token::Ends,
            "CONTAINS" => Token::Contains,
            "EXISTS" => Token::Exists,
            "COUNT" => Token::Count,
            _ => Token::Ident(name),
        };
        Ok(tok)
    }
}

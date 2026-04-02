use crate::Error;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token {
    // Keywords
    Match,
    Where,
    Return,
    // Identifier (variable names, function names)
    Ident(String),
    // Literals
    Integer(u64),
    // Punctuation
    LParen,
    RParen,
    LBracket,
    RBracket,
    Dash,
    Lt,
    Gt,
    Eq,
    Comma,
}

pub(crate) fn tokenize(input: &str) -> Result<Vec<Token>, Error> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            c if c.is_ascii_whitespace() => {
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Dash);
                i += 1;
            }
            '<' => {
                tokens.push(Token::Lt);
                i += 1;
            }
            '>' => {
                tokens.push(Token::Gt);
                i += 1;
            }
            '=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                let token = match word.to_ascii_uppercase().as_str() {
                    "MATCH" => Token::Match,
                    "WHERE" => Token::Where,
                    "RETURN" => Token::Return,
                    _ => Token::Ident(word),
                };
                tokens.push(token);
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                let n = s
                    .parse::<u64>()
                    .map_err(|_| Error::Lex(start, format!("invalid integer: {s}")))?;
                tokens.push(Token::Integer(n));
            }
            c => {
                return Err(Error::Lex(i, format!("unexpected character: {c:?}")));
            }
        }
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple_match() {
        let tokens = tokenize("MATCH (n) RETURN n").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Match,
                Token::LParen,
                Token::Ident("n".to_string()),
                Token::RParen,
                Token::Return,
                Token::Ident("n".to_string()),
            ]
        );
    }

    #[test]
    fn tokenize_path_with_where() {
        let tokens = tokenize("MATCH (a)-[r]->(b) WHERE id(a) = 0 RETURN b").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Match,
                Token::LParen,
                Token::Ident("a".to_string()),
                Token::RParen,
                Token::Dash,
                Token::LBracket,
                Token::Ident("r".to_string()),
                Token::RBracket,
                Token::Dash,
                Token::Gt,
                Token::LParen,
                Token::Ident("b".to_string()),
                Token::RParen,
                Token::Where,
                Token::Ident("id".to_string()),
                Token::LParen,
                Token::Ident("a".to_string()),
                Token::RParen,
                Token::Eq,
                Token::Integer(0),
                Token::Return,
                Token::Ident("b".to_string()),
            ]
        );
    }
}

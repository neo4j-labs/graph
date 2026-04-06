use std::fmt;

#[derive(Debug)]
pub enum Error {
    Lexer(String),
    Parser(String),
    Runtime(String),
    Type(String),
    Unsupported(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Lexer(msg) => write!(f, "Lexer error: {msg}"),
            Error::Parser(msg) => write!(f, "Parser error: {msg}"),
            Error::Runtime(msg) => write!(f, "Runtime error: {msg}"),
            Error::Type(msg) => write!(f, "Type error: {msg}"),
            Error::Unsupported(msg) => write!(f, "Unsupported: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

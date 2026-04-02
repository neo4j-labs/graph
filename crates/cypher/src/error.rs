use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("lex error at position {0}: {1}")]
    Lex(usize, String),

    #[error("parse error: {0}")]
    Parse(String),
}

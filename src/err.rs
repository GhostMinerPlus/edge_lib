use std::fmt::Debug;

#[derive(Debug, Clone)]
pub enum ErrorKind {
    Other(String),
    /// Something not found, maybe a function.
    NotFound,
    /// Permission denied for some space.
    PermissionDenied,
    /// Syntax error
    SyntaxError,
    /// RuntimeError
    RuntimeError
}

pub type Result<T> = std::result::Result<T, moon_err::Error<ErrorKind>>;

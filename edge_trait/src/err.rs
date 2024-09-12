use std::fmt::Display;

#[derive(Debug)]
pub enum Error {
    Other(String),
    Question(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Other(msg) => write!(f, "{msg}"),
            Error::Question(msg) => write!(f, "{msg}"),
        }
    }
}

pub fn map_io_err(err: std::io::Error) -> Error {
    Error::Other(err.to_string())
}

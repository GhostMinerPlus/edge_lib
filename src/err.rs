#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    stack_v: Vec<String>,
}

#[derive(Debug)]
pub enum ErrorKind {
    Other,
    NotFound,
    PermissionDenied,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn new(kind: ErrorKind, message: String) -> Self {
        Self {
            kind,
            message,
            stack_v: vec![],
        }
    }

    pub fn append_stack(mut self, stack: String) -> Self {
        self.stack_v.push(stack);
        self
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

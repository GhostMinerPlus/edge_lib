mod mem;
mod cache;
mod temp;

use std::{io, pin::Pin, sync::Arc};

use crate::util::Path;

pub use mem::*;
pub use cache::*;
pub use temp::*;

#[derive(Clone)]
pub enum Auth {
    Writer(String, String),
    Printer(String),
}

impl Auth {
    pub fn is_root(&self) -> bool {
        match self {
            Self::Writer(paper, _) => paper == "root",
            Self::Printer(pen) => pen == "root",
        }
    }

    pub fn printer(pen: &str) -> Self {
        Self::Printer(pen.to_string())
    }

    pub fn writer(paper: &str, pen: &str) -> Self {
        Self::Writer(paper.to_string(), pen.to_string())
    }
}

pub trait AsDataManager: Send + Sync {
    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager>;

    fn get_auth(&self) -> Auth;

    /// Get all targets from `source->code`
    fn append(
        &self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

    /// Get all targets from `source->code`
    fn set(
        &self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

    /// Get all targets from `source->code`
    fn get(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>>;

    fn clear(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

    fn commit(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;
}

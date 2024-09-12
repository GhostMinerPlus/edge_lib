use std::{collections::HashSet, io, pin::Pin, sync::Arc};

use crate::util::Path;

mod mem;

pub use mem::*;

pub type Auth = Option<PermissionPair>;

#[derive(Clone)]
pub struct PermissionPair {
    pub writer: HashSet<String>,
    pub reader: HashSet<String>,
}

pub trait AsDataManager: Send + Sync {
    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager>;

    fn get_auth(&self) -> &Auth;

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

    #[allow(unused)]
    fn call(
        &self,
        output: Path,
        func: &str,
        input: Path,
        input1: Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        Box::pin(std::future::ready(Err(io::Error::other("Not found!"))))
    }
}

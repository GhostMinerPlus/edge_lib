mod mem;
mod rec;
mod united;

use std::{io, pin::Pin, sync::Arc};

use crate::util::Path;

pub use mem::*;
pub use rec::*;

#[derive(Clone)]
pub struct Auth {
    pub uid: String,
    pub gid: String,
    pub gid_v: Vec<String>,
}

pub trait AsDataManager: Send + Sync {
    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager>;

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

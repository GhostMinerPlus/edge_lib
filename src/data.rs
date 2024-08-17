use std::{collections::HashSet, io, pin::Pin, sync::Arc};

use crate::util::Path;

mod cache;
mod mem;
mod temp;

pub use cache::*;
pub use mem::*;
pub use temp::*;

pub type Auth = Option<HashSet<String>>;

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

    fn commit(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;
}

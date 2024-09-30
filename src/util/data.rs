use std::{collections::HashSet, future, io, pin::Pin, sync::Arc};

use crate::util::Path;

mod mem;
mod temp;

pub use mem::*;
pub use temp::*;

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
    fn append<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f;

    /// Get all targets from `source->code`
    fn set<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f;

    /// Get all targets from `source->code`
    fn get<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f;

    fn clear<'a, 'f>(
        &'a self,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f;

    fn get_code_v<'a, 'a1, 'a2, 'f>(
        &'a self,
        root: &'a1 str,
        space: &'a2 str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f;

    #[allow(unused)]
    fn call<'a, 'a1, 'a2, 'a3, 'a4, 'f>(
        &'a self,
        output: &'a1 Path,
        func: &'a2 str,
        input: &'a3 Path,
        input1: &'a4 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f,
        'a3: 'f,
        'a4: 'f,
    {
        Box::pin(future::ready(Err(io::Error::other("Not found!"))))
    }
}

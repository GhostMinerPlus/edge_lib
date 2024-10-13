use std::{
    collections::HashSet,
    future::{self, Future},
    io,
    pin::Pin,
};

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
    fn get_auth(&self) -> &Auth;

    /// Get all targets from `source->code`
    fn append<'a, 'a1, 'f>(
        &'a mut self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f;

    /// Get all targets from `source->code`
    fn set<'a, 'a1, 'f>(
        &'a mut self,
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

    fn get_code_v<'a, 'a1, 'a2, 'f>(
        &'a self,
        root: &'a1 str,
        space: &'a2 str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f;

    fn call<'a, 'a1, 'a2, 'a3, 'a4, 'f>(
        &'a mut self,
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
        Box::pin(async move {
            let rs = self.call_and_return(func, input, input1).await?;
            self.set(output, rs).await
        })
    }

    #[allow(unused)]
    fn call_and_return<'a, 'a1, 'a2, 'a3, 'f>(
        &'a mut self,
        func: &'a1 str,
        input: &'a2 Path,
        input1: &'a3 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f,
        'a3: 'f,
    {
        Box::pin(future::ready(Err(io::Error::other("error"))))
    }

    fn dump<'a, 'b, 'c, 'f>(
        &'a mut self,
        addr: &'b Path,
        paper: &'c str,
    ) -> Pin<Box<dyn Future<Output = io::Result<json::JsonValue>> + Send + 'f>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
    {
        Box::pin(async move {
            // root
            let root_v = self.get(addr).await?;
            let mut rj = json::array![];
            for root in &root_v {
                rj.push(crate::util::dump(self, root, paper).await?)
                    .unwrap();
            }
            Ok(rj)
        })
    }

    fn load<'a, 'a1, 'a2, 'f>(
        &'a mut self,
        data: &'a1 json::JsonValue,
        addr: &'a2 Path,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f,
    {
        Box::pin(async move {
            if data.is_null() {
                return Ok(());
            }

            if data.is_array() {
                for item in data.members() {
                    self.load(item, addr).await?;
                }
                return Ok(());
            }

            if !data.is_object() {
                self.append(addr, vec![data.as_str().unwrap().to_string()])
                    .await?;
                return Ok(());
            }

            self.append(addr, vec![super::gen_value()]).await?;

            for (k, v) in data.entries() {
                let sub_path = Path::from_str(&format!("{}->{k}", addr.to_string()));
                if v.is_array() {
                    for item in v.members() {
                        self.load(item, &sub_path).await?;
                    }
                } else {
                    self.load(v, &sub_path).await?;
                }
            }
            Ok(())
        })
    }
}

pub trait AsStack {
    fn push<'a, 'f>(
        &'a mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>;

    fn pop<'a, 'f>(
        &'a mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>;
}

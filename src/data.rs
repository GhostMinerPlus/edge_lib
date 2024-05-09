use std::{
    collections::HashSet,
    future, io,
    pin::Pin,
    sync::{Arc, Mutex},
};

use crate::mem_table::MemTable;

fn is_temp(code: &str) -> bool {
    code.starts_with('$')
}

// Public
pub trait AsDataManager: Send + Sync {
    fn divide(&self) -> Box<dyn AsDataManager>;

    fn append_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

    fn append_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

    fn set_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

    fn set_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

    /// Get a target from `source->code`
    fn get_target(
        &mut self,
        source: &str,
        code: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<String>> + Send>>;

    /// Get a source from `target<-code`
    fn get_source(
        &mut self,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<String>> + Send>>;

    /// Get all targets from `source->code`
    fn get_target_v(
        &mut self,
        source: &str,
        code: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>>;

    /// Get all targets from `source->code`
    fn get_source_v(
        &mut self,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>>;

    fn commit(&mut self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;
}

pub struct DataManager {
    global: Arc<Mutex<MemTable>>,
    cache: MemTable,
    cache_set: HashSet<String>,
}

impl DataManager {
    pub fn new() -> Self {
        Self {
            global: Arc::new(Mutex::new(MemTable::new())),
            cache: MemTable::new(),
            cache_set: HashSet::new(),
        }
    }
}

impl DataManager {
    fn is_cached(&self, path: &str) -> bool {
        self.cache_set.contains(path)
    }

    fn cache(&mut self, path: String) {
        self.cache_set.insert(path);
    }

    fn clear_cache(&mut self) {
        self.cache_set.clear();
    }
}

impl AsDataManager for DataManager {
    fn divide(&self) -> Box<dyn AsDataManager> {
        Box::new(Self {
            global: self.global.clone(),
            cache: MemTable::new(),
            cache_set: HashSet::new(),
        })
    }

    fn get_target(
        &mut self,
        source: &str,
        code: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = std::io::Result<String>> + Send>> {
        let rs = if self.is_cached(&format!("{source}->{code}")) {
            Ok(self.cache.get_target(source, code).unwrap_or_default())
        } else {
            let global = self.global.lock().unwrap();
            match global.get_target(source, code) {
                Some(target) => Ok(target),
                None => Ok(String::new()),
            }
        };
        Box::pin(future::ready(rs))
    }

    fn get_source(
        &mut self,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = std::io::Result<String>> + Send>> {
        let rs = if self.is_cached(&format!("{target}<-{code}")) {
            Ok(self.cache.get_source(code, target).unwrap_or_default())
        } else {
            let global = self.global.lock().unwrap();
            match global.get_source(code, target) {
                Some(source) => Ok(source),
                None => Ok(String::new()),
            }
        };
        Box::pin(future::ready(rs))
    }

    fn get_target_v(
        &mut self,
        source: &str,
        code: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = std::io::Result<Vec<String>>> + Send>> {
        let rs = {
            if self.is_cached(&format!("{source}->{code}")) {
                Ok(self.cache.get_target_v_unchecked(source, code))
            } else {
                let mut global = self.global.lock().unwrap();
                let rs = global.get_target_v_unchecked(source, code);
                drop(global);
                self.cache.delete_saved_edge_with_source_code(source, code);
                for target in &rs {
                    self.cache.insert_temp_edge(source, code, target);
                }
                self.cache(format!("{source}->{code}"));
                Ok(rs)
            }
        };
        Box::pin(future::ready(rs))
    }

    fn get_source_v(
        &mut self,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = std::io::Result<Vec<String>>> + Send>> {
        let rs = {
            if self.is_cached(&format!("{target}<-{code}")) {
                Ok(self.cache.get_source_v_unchecked(code, target))
            } else {
                let mut global = self.global.lock().unwrap();
                let rs = global.get_source_v_unchecked(code, target);
                drop(global);
                self.cache.delete_saved_edge_with_code_target(code, target);
                for source in &rs {
                    self.cache.insert_temp_edge(source, code, target);
                }
                self.cache(format!("{target}<-{code}"));
                Ok(rs)
            }
        };
        Box::pin(future::ready(rs))
    }

    fn append_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let rs = {
            if is_temp(code) {
                for target in target_v {
                    self.cache.insert_temp_edge(source, code, target);
                }
            } else {
                if !self.is_cached(&format!("{source}->{code}")) {
                    let mut global = self.global.lock().unwrap();
                    let rs = global.get_target_v_unchecked(source, code);
                    drop(global);
                    self.cache.delete_saved_edge_with_source_code(source, code);
                    for target in &rs {
                        self.cache.insert_temp_edge(source, code, target);
                    }
                }
                for target in target_v {
                    self.cache.insert_edge(source, code, target);
                }
            }
            self.cache(format!("{source}->{code}"));
            Ok(())
        };
        Box::pin(future::ready(rs))
    }

    fn append_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let rs = {
            if is_temp(code) {
                for source in source_v {
                    self.cache.insert_temp_edge(source, code, target);
                }
            } else {
                if !self.is_cached(&format!("{target}<-{code}")) {
                    let mut global = self.global.lock().unwrap();
                    let rs = global.get_source_v_unchecked(code, target);
                    drop(global);
                    self.cache.delete_saved_edge_with_code_target(code, target);
                    for source in &rs {
                        self.cache.insert_temp_edge(source, code, target);
                    }
                }
                for source in source_v {
                    self.cache.insert_edge(source, code, target);
                }
            }
            self.cache(format!("{target}<-{code}"));
            Ok(())
        };
        Box::pin(future::ready(rs))
    }

    fn set_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let rs = {
            self.cache.delete_edge_with_source_code(source, code);
            if is_temp(code) {
                for target in target_v {
                    self.cache.insert_temp_edge(source, code, target);
                }
            } else {
                let mut global = self.global.lock().unwrap();
                global.delete_edge_with_source_code(source, code);
                drop(global);
                for target in target_v {
                    self.cache.insert_edge(source, code, target);
                }
            }
            self.cache(format!("{source}->{code}"));
            Ok(())
        };
        Box::pin(future::ready(rs))
    }

    fn set_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let rs = {
            self.cache.delete_edge_with_code_target(code, target);
            if is_temp(code) {
                for source in source_v {
                    self.cache.insert_temp_edge(source, code, target);
                }
            } else {
                let mut global = self.global.lock().unwrap();
                global.delete_edge_with_code_target(code, target);
                drop(global);
                for source in source_v {
                    self.cache.insert_edge(source, code, target);
                }
            }
            self.cache(format!("{target}<-{code}"));
            Ok(())
        };
        Box::pin(future::ready(rs))
    }

    fn commit(&mut self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        self.clear_cache();
        let rs = {
            let mut global = self.global.lock().unwrap();
            for (_, edge) in self.cache.take() {
                global.insert_edge(&edge.source, &edge.code, &edge.target);
            }
            Ok(())
        };
        Box::pin(future::ready(rs))
    }
}

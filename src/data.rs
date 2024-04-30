use std::{
    collections::HashSet,
    io, mem,
    sync::{Arc, Mutex},
};

use crate::mem_table::MemTable;

fn is_temp(code: &str) -> bool {
    code.starts_with('$')
}

// Public
pub trait AsDataManager: Send {
    fn append_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;

    fn append_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;

    fn set_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;

    fn set_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send;

    /// Get a target from `source->code`
    fn get_target(
        &mut self,
        source: &str,
        code: &str,
    ) -> impl std::future::Future<Output = io::Result<String>> + Send;

    /// Get a source from `target<-code`
    fn get_source(
        &mut self,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = io::Result<String>> + Send;

    /// Get all targets from `source->code`
    fn get_target_v(
        &mut self,
        source: &str,
        code: &str,
    ) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send;

    /// Get all targets from `source->code`
    fn get_source_v(
        &mut self,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send;

    fn commit(&mut self) -> impl std::future::Future<Output = io::Result<()>> + Send;
}

pub struct DataManager {
    global: Arc<Mutex<MemTable>>,
    cache: MemTable,
    delete_list_by_source: HashSet<(String, String)>,
    delete_list_by_target: HashSet<(String, String)>,
}

impl DataManager {
    pub fn with_global(global: Arc<Mutex<MemTable>>) -> Self {
        Self {
            global,
            cache: MemTable::new(),
            delete_list_by_source: HashSet::new(),
            delete_list_by_target: HashSet::new(),
        }
    }
}

impl AsDataManager for DataManager {
    fn get_target(
        &mut self,
        source: &str,
        code: &str,
    ) -> impl std::future::Future<Output = std::io::Result<String>> + Send {
        async {
            if let Some(target) = self.cache.get_target(source, code) {
                Ok(target)
            } else {
                let global = self.global.lock().unwrap();
                match global.get_target(source, code) {
                    Some(target) => Ok(target),
                    None => Ok(String::new()),
                }
            }
        }
    }

    fn get_source(
        &mut self,
        code: &str,
        target: &str,
    ) -> impl std::future::Future<Output = std::io::Result<String>> + Send {
        async {
            if let Some(source) = self.cache.get_source(code, target) {
                Ok(source)
            } else {
                let global = self.global.lock().unwrap();
                match global.get_source(code, target) {
                    Some(source) => Ok(source),
                    None => Ok(String::new()),
                }
            }
        }
    }

    async fn get_target_v(&mut self, source: &str, code: &str) -> std::io::Result<Vec<String>> {
        let rs = self.cache.get_target_v_unchecked(source, code);
        if rs.is_empty() {
            let mut global = self.global.lock().unwrap();
            let rs = global.get_target_v_unchecked(source, code);
            for target in &rs {
                self.cache.insert_temp_edge(source, code, target);
            }
            Ok(rs)
        } else {
            Ok(rs)
        }
    }

    async fn commit(&mut self) -> std::io::Result<()> {
        let mut global = self.global.lock().unwrap();
        for (source, code) in mem::take(&mut self.delete_list_by_source) {
            global.delete_edge_with_source_code(&source, &code);
        }
        for (code, target) in mem::take(&mut self.delete_list_by_target) {
            global.delete_edge_with_code_target(&code, &target);
        }
        for (_, edge) in self.cache.take() {
            global.insert_edge(&edge.source, &edge.code, &edge.target);
        }
        Ok(())
    }

    async fn get_source_v(&mut self, code: &str, target: &str) -> std::io::Result<Vec<String>> {
        let rs = self.cache.get_source_v_unchecked(code, target);
        if rs.is_empty() {
            let mut global = self.global.lock().unwrap();
            let rs = global.get_source_v_unchecked(code, target);
            for source in &rs {
                self.cache.insert_temp_edge(source, code, target);
            }
            Ok(rs)
        } else {
            Ok(rs)
        }
    }

    async fn append_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> io::Result<()> {
        if is_temp(code) {
            for target in target_v {
                self.cache.insert_temp_edge(source, code, target);
            }
        } else {
            if let None = self.cache.get_target(source, code) {
                let mut global = self.global.lock().unwrap();
                let rs = global.get_target_v_unchecked(source, code);
                for target in &rs {
                    self.cache.insert_temp_edge(source, code, target);
                }
            }
            for target in target_v {
                self.cache.insert_edge(source, code, target);
            }
        }
        Ok(())
    }

    async fn append_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> io::Result<()> {
        if is_temp(code) {
            for source in source_v {
                self.cache.insert_temp_edge(source, code, target);
            }
        } else {
            if let None = self.cache.get_source(code, target) {
                let mut global = self.global.lock().unwrap();
                let rs = global.get_source_v_unchecked(code, target);
                for source in &rs {
                    self.cache.insert_temp_edge(source, code, target);
                }
            }
            for source in source_v {
                self.cache.insert_edge(source, code, target);
            }
        }
        Ok(())
    }

    async fn set_target_v(
        &mut self,
        source: &str,
        code: &str,
        target_v: &Vec<String>,
    ) -> io::Result<()> {
        self.cache.delete_edge_with_source_code(source, code);
        if is_temp(code) {
            for target in target_v {
                self.cache.insert_temp_edge(source, code, target);
            }
        } else {
            self.delete_list_by_source
                .insert((source.to_string(), code.to_string()));
            for target in target_v {
                self.cache.insert_edge(source, code, target);
            }
        }
        Ok(())
    }

    async fn set_source_v(
        &mut self,
        source_v: &Vec<String>,
        code: &str,
        target: &str,
    ) -> io::Result<()> {
        self.cache.delete_edge_with_code_target(code, target);
        if is_temp(code) {
            for source in source_v {
                self.cache.insert_temp_edge(source, code, target);
            }
        } else {
            self.delete_list_by_target
                .insert((code.to_string(), target.to_string()));
            for source in source_v {
                self.cache.insert_edge(source, code, target);
            }
        }
        Ok(())
    }
}

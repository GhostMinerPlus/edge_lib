use std::sync::{Arc, Mutex};

use crate::engine::EdgeEngine;

pub struct Computed<T> {
    pub is_latest: Arc<Mutex<bool>>,
    pub script: Vec<String>,
    pub cache: T,
    pub engine: EdgeEngine,
}

impl<T> Computed<T> {
    pub async fn fetch(&mut self) -> &T {
        let mut is_latest = self.is_latest.lock().unwrap();
        if !*is_latest {
            let rs = self.engine.execute_script(&self.script).await.unwrap();
            // TODO:
            *is_latest = true;
        }
        &self.cache
    }

    pub fn get_cache(&self) -> &T {
        &self.cache
    }

    pub fn get_latest_cache(&self) -> Option<&T> {
        let is_latest = self.is_latest.lock().unwrap();
        if !*is_latest {
            None
        } else {
            Some(&self.cache)
        }
    }
}

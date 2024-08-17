use std::{collections::HashMap, future, io, pin::Pin, sync::Arc};

use tokio::sync::Mutex;

use crate::util::Path;

use super::{AsDataManager, Auth};

mod main {
    use std::{collections::HashMap, io};

    use crate::util::Path;

    pub fn prune_cache_before_write(
        cache: &mut HashMap<Path, Vec<String>>,
        w_path: &Path,
    ) -> io::Result<()> {
        if w_path.step_v.is_empty() {
            return Ok(());
        }
        let path_v: Vec<Path> = cache.keys().cloned().collect();
        let code = &w_path.step_v.last().unwrap().code;
        for path in &path_v {
            if path == w_path || !path.contains(code) {
                // 同路径和与 code 无关的路径缓存仍有效
                continue;
            }
            cache.remove(path).unwrap();
        }
        Ok(())
    }
}

/// DataManager with cache and temp
#[derive(Clone)]
pub struct CacheDataManager {
    global: Arc<dyn AsDataManager>,
    cache: Arc<Mutex<HashMap<Path, Vec<String>>>>,
}

impl CacheDataManager {
    pub fn new(global: Arc<dyn AsDataManager>) -> Self {
        Self {
            global,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl AsDataManager for CacheDataManager {
    fn get_auth(&self) -> &Auth {
        self.global.get_auth()
    }

    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager> {
        Arc::new(Self {
            global: self.global.divide(auth),
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn clear(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        Box::pin(async move {
            let mut cache = this.cache.lock().await;
            cache.clear();
            drop(cache);
            this.global.clear().await
        })
    }

    fn append(
        &self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        if path.step_v.last().unwrap().arrow != "->" {
            return Box::pin(future::ready(Err(io::Error::other("can not set parents"))));
        }
        let this = self.clone();
        let path = path.clone();
        Box::pin(async move {
            let mut cache = this.cache.lock().await;
            main::prune_cache_before_write(&mut *cache, &path)?;
            if let Some(rs) = cache.get_mut(&path) {
                rs.extend(item_v.clone());
            }
            drop(cache);
            this.global.append(&path, item_v).await?;
            Ok(())
        })
    }

    fn set(
        &self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        if path.step_v.last().unwrap().arrow != "->" {
            return Box::pin(future::ready(Err(io::Error::other("can not set parents"))));
        }
        let this = self.clone();
        let path = path.clone();
        Box::pin(async move {
            let mut cache = this.cache.lock().await;
            main::prune_cache_before_write(&mut *cache, &path)?;
            cache.insert(path.clone(), item_v.clone());
            drop(cache);
            this.global.set(&path, item_v).await?;
            Ok(())
        })
    }

    fn get(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>> {
        if path.step_v.is_empty() {
            if path.root.is_empty() {
                return Box::pin(future::ready(Ok(vec![])));
            }
            return Box::pin(future::ready(Ok(vec![path.root.clone()])));
        }
        let this = self.clone();
        let path = path.clone();
        Box::pin(async move {
            let mut cache = this.cache.lock().await;
            if let Some(item_v) = cache.get(&path) {
                return Ok(item_v.clone());
            }
            let item_v = this.global.get(&path).await?;
            cache.insert(path, item_v.clone());
            Ok(item_v)
        })
    }

    fn commit(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        Box::pin(async move {
            this.global.commit().await?;
            let mut cache = this.cache.lock().await;
            cache.clear();
            Ok(())
        })
    }
}

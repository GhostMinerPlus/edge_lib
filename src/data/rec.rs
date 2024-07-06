use std::{collections::HashMap, future, io, pin::Pin, sync::Arc};

use tokio::sync::Mutex;

use crate::util::Path;

use super::{united, AsDataManager, Auth};

#[derive(Clone)]
struct CachePair {
    item_v: Vec<String>,
    offset: usize,
}

#[derive(Clone)]
pub struct RecDataManager {
    global: Arc<dyn AsDataManager>,
    cache: Arc<Mutex<HashMap<Path, CachePair>>>,
}

impl RecDataManager {
    pub fn new(global: Arc<dyn AsDataManager>) -> Self {
        Self {
            global: Arc::new(united::UnitedDataManager::new(global)),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn prune_cache_on_write(
        cache: &mut HashMap<Path, CachePair>,
        n_path: &Path,
        global: &dyn AsDataManager,
    ) -> io::Result<()> {
        if n_path.step_v.is_empty() {
            return Ok(());
        }
        let mut temp_cache = cache.clone();
        let path_v: Vec<Path> = cache.keys().cloned().collect();
        let code = &n_path.step_v.last().unwrap().code;
        for path in &path_v {
            if path == n_path || !path.contains(code) {
                continue;
            }
            let pair = cache.remove(path).unwrap();
            if let Some(l_step) = path.step_v.last() {
                if l_step.arrow != "->" {
                    continue;
                }
            }
            let item_v = &pair.item_v[pair.offset..];
            let mut path = path.clone();
            let step = path.step_v.pop().unwrap();
            let root_v = Self::get_from_other(&mut temp_cache, global, &path).await?;
            for source in &root_v {
                if pair.offset > 0 {
                    global
                        .append(
                            &Path::from_str(&format!("{source}->{}", step.code)),
                            item_v.to_vec(),
                        )
                        .await?;
                } else {
                    global
                        .set(
                            &Path::from_str(&format!("{source}->{}", step.code)),
                            item_v.to_vec(),
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn prune_cache_on_read(
        global: &dyn AsDataManager,
        cache: &mut HashMap<Path, CachePair>,
        n_path: &Path,
    ) -> io::Result<()> {
        if n_path.step_v.is_empty() {
            return Ok(());
        }
        let mut temp_cache = cache.clone();
        let path_v: Vec<Path> = cache.keys().cloned().collect();
        let n_step = &n_path.step_v.last().unwrap();
        for path in &path_v {
            if path == n_path {
                continue;
            }
            if path
                .step_v
                .iter()
                .filter(|step| step.code == n_step.code && step.arrow != n_step.arrow)
                .count()
                > 0
            {
                let mut root_path = path.clone();
                let step = root_path.step_v.pop().unwrap();
                let root_v = Self::get_from_other(&mut temp_cache, global, &root_path).await?;

                let pair = cache.get_mut(&path).unwrap();
                let item_v = &pair.item_v[pair.offset..];
                for source in &root_v {
                    global
                        .append(
                            &Path::from_str(&format!("{source}->{}", step.code)),
                            item_v.to_vec(),
                        )
                        .await?;
                }
                pair.offset = pair.item_v.len();
            }
        }
        Ok(())
    }

    #[async_recursion::async_recursion]
    async fn get_from_other(
        cache: &mut HashMap<Path, CachePair>,
        global: &dyn AsDataManager,
        path: &Path,
    ) -> io::Result<Vec<String>> {
        if path.step_v.is_empty() {
            if path.root.is_empty() {
                return Ok(vec![]);
            }
            return Ok(vec![path.root.clone()]);
        }

        if let Some(rs) = cache.get(&path) {
            return Ok(rs.item_v.clone());
        }
        let mut path_in_part = path.clone();
        let mut rest_apth = Path::from_str("root");
        let mut temp_v = None;
        while !path_in_part.step_v.is_empty() {
            rest_apth
                .step_v
                .insert(0, path_in_part.step_v.pop().unwrap());
            if let Some(rs) = cache.get(&path_in_part) {
                temp_v = Some(rs.item_v.clone());
                break;
            }
        }

        if let Some(temp_v) = temp_v {
            let mut rs = Vec::new();
            for root in temp_v {
                let mut sub_path = rest_apth.clone();
                sub_path.root = root;
                rs.extend(Self::get_from_other(cache, global, &sub_path).await?);
            }
            return Ok(rs);
        }

        Self::prune_cache_on_read(global, &mut *cache, &path).await?;
        let item_v = global.get(&path).await?;
        cache.insert(
            path.clone(),
            CachePair {
                item_v: item_v.clone(),
                offset: item_v.len(),
            },
        );
        Ok(item_v)
    }

    async fn get_in_cache(&mut self, path: &Path) -> io::Result<Option<Vec<String>>> {
        let cache = self.cache.lock().await;
        if let Some(rs) = cache.get(&path) {
            return Ok(Some(rs.item_v.clone()));
        }
        let mut path_in_part = path.clone();
        let mut rest_apth = Path::from_str("root");
        let mut temp_v = None;
        while !path_in_part.step_v.is_empty() {
            rest_apth
                .step_v
                .insert(0, path_in_part.step_v.pop().unwrap());
            if let Some(rs) = cache.get(&path_in_part) {
                temp_v = Some(rs.item_v.clone());
                break;
            }
        }
        drop(cache);
        if let Some(temp_v) = temp_v {
            let mut rs = Vec::new();
            for root in temp_v {
                let mut sub_path = rest_apth.clone();
                sub_path.root = root;
                rs.extend(self.get(&sub_path).await?);
            }
            return Ok(Some(rs));
        }
        Ok(None)
    }
}

impl AsDataManager for RecDataManager {
    fn get_auth(&self) -> Auth {
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

    fn commit(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        Box::pin(async move {
            let mut cache = this.cache.lock().await;
            let mut temp_cache = cache.clone();
            let mut arr: Vec<Path> = cache.keys().map(|k| k.clone()).collect();
            arr.sort_by(|p, q| p.step_v.len().cmp(&q.step_v.len()));
            for mut path in arr {
                let pair = &cache[&path];
                let item_v = &pair.item_v[pair.offset..];

                let step = path.step_v.pop().unwrap();
                let root_v = Self::get_from_other(&mut temp_cache, &*this.global, &path).await?;
                for source in &root_v {
                    this.global
                        .append(
                            &Path::from_str(&format!("{source}->{}", step.code)),
                            item_v.to_vec(),
                        )
                        .await?;
                }
            }

            cache.clear();
            Ok(())
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
            Self::prune_cache_on_write(&mut *cache, &path, &*this.global).await?;

            if let Some(rs) = cache.get_mut(&path) {
                rs.item_v.extend(item_v);
                return Ok(());
            }

            let mut rs0 = this.global.get(&path).await?;
            let offset = rs0.len();
            rs0.extend(item_v);
            cache.insert(
                path.clone(),
                CachePair {
                    item_v: rs0,
                    offset,
                },
            );
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

            Self::prune_cache_on_write(&mut *cache, &path, &*this.global).await?;

            log::debug!("delete {path}\nwhen RecDataManager::set");
            this.global.set(&path, vec![]).await?;
            cache.insert(path.clone(), CachePair { item_v, offset: 0 });
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
        let mut this = self.clone();
        let path = path.clone();
        Box::pin(async move {
            if let Some(rs) = this.get_in_cache(&path).await? {
                return Ok(rs);
            }

            let mut cache = this.cache.lock().await;
            Self::prune_cache_on_read(&*this.global, &mut *cache, &path).await?;
            let item_v = this.global.get(&path).await?;
            cache.insert(
                path,
                CachePair {
                    item_v: item_v.clone(),
                    offset: item_v.len(),
                },
            );
            Ok(item_v)
        })
    }
}

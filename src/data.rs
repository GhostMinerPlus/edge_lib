use std::{collections::HashMap, future, io, pin::Pin, sync::Arc};

use tokio::sync::Mutex;

use crate::{mem_table, Path};

pub fn is_temp(code: &str) -> bool {
    code.starts_with('$')
}

// Public
pub trait AsDataManager: Send + Sync {
    fn divide(&self) -> Box<dyn AsDataManager>;

    /// Get all targets from `source->code`
    fn append(
        &mut self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

    /// Get all targets from `source->code`
    fn set(
        &mut self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

    /// Get all targets from `source->code`
    fn get(
        &mut self,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>>;

    fn commit(&mut self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;
}

#[derive(Clone)]
pub struct MemDataManager {
    mem_table: Arc<Mutex<mem_table::MemTable>>,
}

impl MemDataManager {
    pub fn new() -> Self {
        Self {
            mem_table: Arc::new(Mutex::new(mem_table::MemTable::new())),
        }
    }
}

impl AsDataManager for MemDataManager {
    fn divide(&self) -> Box<dyn AsDataManager> {
        Box::new(self.clone())
    }

    fn commit(&mut self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        Box::pin(future::ready(Ok(())))
    }

    fn append(
        &mut self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        let mut mdm = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            let root_v = mdm.get(&path).await?;
            let mut mem_table = mdm.mem_table.lock().await;
            for source in &root_v {
                for target in &item_v {
                    mem_table.insert_edge(source, &step.code, target);
                }
            }
            Ok(())
        })
    }

    fn set(
        &mut self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        let mut mdm = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            let root_v = mdm.get(&path).await?;
            let mut mem_table = mdm.mem_table.lock().await;
            for source in &root_v {
                mem_table.delete_edge_with_source_code(source, &step.code);
            }
            for source in &root_v {
                for target in &item_v {
                    mem_table.insert_edge(source, &step.code, target);
                }
            }
            Ok(())
        })
    }

    fn get(
        &mut self,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>> {
        if path.step_v.is_empty() {
            if path.root.is_empty() {
                return Box::pin(future::ready(Ok(vec![])));
            }
            return Box::pin(future::ready(Ok(vec![path.root.clone()])));
        }
        let mdm = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            let mut mem_table = mdm.mem_table.lock().await;
            let mut rs = vec![path.root.clone()];
            while !path.step_v.is_empty() {
                let step = path.step_v.remove(0);
                if step.arrow == "->" {
                    let mut n_rs = Vec::new();
                    for source in &rs {
                        n_rs.extend(mem_table.get_target_v(source, &step.code));
                    }
                    rs = n_rs;
                } else {
                    let mut n_rs = Vec::new();
                    for target in &rs {
                        n_rs.extend(mem_table.get_source_v(&step.code, target));
                    }
                    rs = n_rs;
                }
            }
            Ok(rs)
        })
    }
}

struct CachePair {
    item_v: Vec<String>,
    offset: usize,
}

#[derive(Clone)]
pub struct RecDataManager {
    global: Arc<Mutex<Box<dyn AsDataManager>>>,
    cache: Arc<Mutex<HashMap<Path, CachePair>>>,
}

impl RecDataManager {
    pub fn new(global: Box<dyn AsDataManager>) -> Self {
        Self {
            global: Arc::new(Mutex::new(global)),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn prune_cache_on_write(
        cache: &mut HashMap<Path, CachePair>,
        n_path: &Path,
        global: &mut Box<dyn AsDataManager>,
    ) -> io::Result<()> {
        if n_path.step_v.is_empty() {
            return Ok(());
        }
        let path_v: Vec<Path> = cache.keys().cloned().collect();
        let code = &n_path.step_v.last().unwrap().code;
        for path in &path_v {
            if path == n_path {
                continue;
            }
            if path.step_v.iter().filter(|step| step.code == *code).count() > 0 {
                let pair = cache.remove(path).unwrap();
                let item_v = &pair.item_v[pair.offset..];

                let mut path = path.clone();
                let step = path.step_v.pop().unwrap();
                let root_v = Self::get_from_cache(&*cache, &path).await?;
                for source in &root_v {
                    global
                        .append(
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
        cache: &mut HashMap<Path, CachePair>,
        n_path: &Path,
        global: &mut Box<dyn AsDataManager>,
    ) -> io::Result<()> {
        if n_path.step_v.is_empty() {
            return Ok(());
        }
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
                let root_v = Self::get_from_cache(cache, &root_path).await?;

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
    async fn get_from_cache(
        cache: &HashMap<Path, CachePair>,
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
                rs.extend(Self::get_from_cache(cache, &sub_path).await?);
            }
            return Ok(rs);
        } else {
            return Ok(vec![]);
        }
    }
}

impl AsDataManager for RecDataManager {
    fn divide(&self) -> Box<dyn AsDataManager> {
        Box::new(Self {
            global: self.global.clone(),
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn commit(&mut self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        Box::pin(async move {
            let mut cache = this.cache.lock().await;
            let mut global = this.global.lock().await;
            let mut arr: Vec<Path> = cache.keys().map(|k| k.clone()).collect();
            arr.sort_by(|p, q| p.step_v.len().cmp(&q.step_v.len()));
            for mut path in arr {
                let pair = &cache[&path];
                let item_v = &pair.item_v[pair.offset..];

                let step = path.step_v.pop().unwrap();
                let root_v = Self::get_from_cache(&*cache, &path).await?;
                for source in &root_v {
                    global
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
        &mut self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        if path.step_v.last().unwrap().arrow != "->" {
            return Box::pin(future::ready(Err(io::Error::other("can not set parents"))));
        }
        let global = self.global.clone();
        let cache = self.cache.clone();
        let path = path.clone();
        Box::pin(async move {
            let mut cache = cache.lock().await;

            if is_temp(&path.step_v.last().unwrap().code) {
                match cache.get_mut(&path) {
                    Some(rs) => {
                        rs.item_v.extend(item_v);
                        rs.offset = rs.item_v.len();
                    }
                    None => {
                        let offset = item_v.len();
                        cache.insert(path.clone(), CachePair { item_v, offset });
                    }
                }
                return Ok(());
            }

            let mut global = global.lock().await;
            Self::prune_cache_on_write(&mut *cache, &path, &mut *global).await?;

            if let Some(rs) = cache.get_mut(&path) {
                rs.item_v.extend(item_v);
                return Ok(());
            }

            let mut rs0 = global.get(&path).await?;
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
        &mut self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        if path.step_v.last().unwrap().arrow != "->" {
            return Box::pin(future::ready(Err(io::Error::other("can not set parents"))));
        }
        let global = self.global.clone();
        let cache = self.cache.clone();
        let path = path.clone();
        Box::pin(async move {
            let mut cache = cache.lock().await;

            if is_temp(&path.step_v.last().unwrap().code) {
                let offset = item_v.len();
                cache.insert(path.clone(), CachePair { item_v, offset });
                return Ok(());
            }

            let mut global = global.lock().await;
            Self::prune_cache_on_write(&mut *cache, &path, &mut *global).await?;

            global.set(&path, vec![]).await?;
            cache.insert(path.clone(), CachePair { item_v, offset: 0 });
            Ok(())
        })
    }

    fn get(
        &mut self,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>> {
        if path.step_v.is_empty() {
            if path.root.is_empty() {
                return Box::pin(future::ready(Ok(vec![])));
            }
            return Box::pin(future::ready(Ok(vec![path.root.clone()])));
        }
        let global_mut = self.global.clone();
        let cache_mut = self.cache.clone();
        let path = path.clone();
        Box::pin(async move {
            let cache = cache_mut.lock().await;
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
            drop(cache);

            if let Some(temp_v) = temp_v {
                let mut rs = Vec::new();
                for root in temp_v {
                    let mut sub_path = rest_apth.clone();
                    sub_path.root = root;
                    let mut this = Self {
                        global: global_mut.clone(),
                        cache: cache_mut.clone(),
                    };
                    rs.extend(this.get(&sub_path).await?);
                }
                return Ok(rs);
            }

            let mut cache = cache_mut.lock().await;
            let mut global = global_mut.lock().await;
            Self::prune_cache_on_read(&mut *cache, &path, &mut *global).await?;
            let item_v = global.get(&path).await?;
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

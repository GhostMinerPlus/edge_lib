use std::{future, io, pin::Pin, sync::Arc};

use tokio::sync::Mutex;

use crate::{mem_table, util::Path};

use super::{AsDataManager, Auth};

mod main {
    #[cfg(test)]
    mod test_get_source_v {
        use crate::{
            data::{AsDataManager, MemDataManager},
            util::Path,
        };

        #[test]
        fn should_get_source_v() {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let dm = MemDataManager::new(None);
                    dm.set(&Path::from_str("root->web_server"), vec!["id".to_string()])
                        .await
                        .unwrap();
                    dm.set(&Path::from_str("id->name"), vec!["test".to_string()])
                        .await
                        .unwrap();
                    dm.commit().await.unwrap();
                    let test = dm.get(&Path::from_str("test<-name")).await.unwrap();
                    let test1 = dm.get(&Path::from_str("root->web_server")).await.unwrap();
                    assert_eq!(test, test1);
                })
        }

        #[test]
        fn should_get_source() {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let dm = MemDataManager::new(None);
                    dm.set(&Path::from_str("root->web_server"), vec!["id".to_string()])
                        .await
                        .unwrap();
                    dm.set(&Path::from_str("id->name"), vec!["test".to_string()])
                        .await
                        .unwrap();
                    dm.set(
                        &Path::from_str("root->web_server->name"),
                        vec!["test".to_string()],
                    )
                    .await
                    .unwrap();
                    dm.commit().await.unwrap();
                    let web_server = dm
                        .get(&Path::from_str("root->web_server->name"))
                        .await
                        .unwrap();
                    assert_eq!(web_server.len(), 1);
                })
        }
    }
}

#[derive(Clone)]
pub struct MemDataManager {
    auth: Auth,
    mem_table: Arc<Mutex<mem_table::MemTable>>,
}

impl MemDataManager {
    pub fn new(auth: Auth) -> Self {
        Self {
            auth,
            mem_table: Arc::new(Mutex::new(mem_table::MemTable::new())),
        }
    }
}

impl AsDataManager for MemDataManager {
    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager> {
        Arc::new(Self {
            auth,
            mem_table: self.mem_table.clone(),
        })
    }

    fn get_auth(&self) -> &Auth {
        &self.auth
    }

    fn clear(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        Box::pin(async move {
            let mut mem_table = this.mem_table.lock().await;
            match &this.auth {
                Some(auth) => {
                    for paper in auth {
                        mem_table.clear_paper(paper);
                    }
                }
                None => mem_table.clear(),
            }
            Ok(())
        })
    }

    fn commit(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        Box::pin(future::ready(Ok(())))
    }

    fn append(
        &self,
        path: &Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        let this = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            if let Some(auth) = &this.auth {
                if !auth.contains(&step.paper) {
                    return Err(io::Error::other("permision denied"));
                }
            }
            let root_v = this.get(&path).await?;
            let mut mem_table = this.mem_table.lock().await;
            for source in &root_v {
                for target in &item_v {
                    mem_table.insert_edge(source, &step.paper, &step.code, target);
                }
            }
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
        let this = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            if let Some(auth) = &this.auth {
                if !auth.contains(&step.paper) {
                    return Err(io::Error::other("permision denied"));
                }
            }
            let root_v = this.get(&path).await?;
            let mut mem_table = this.mem_table.lock().await;
            for source in &root_v {
                mem_table.delete_edge_with_source_code(source, &step.paper, &step.code);
            }
            for source in &root_v {
                for target in &item_v {
                    mem_table.insert_edge(source, &step.paper, &step.code, target);
                }
            }
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
        let mut path = path.clone();
        Box::pin(async move {
            let mut mem_table = this.mem_table.lock().await;
            let mut rs = vec![path.root.clone()];
            while !path.step_v.is_empty() {
                let step = path.step_v.remove(0);
                if let Some(auth) = &this.auth {
                    if !auth.contains(&step.paper) {
                        return Err(io::Error::other("permision denied"));
                    }
                }
                if step.arrow == "->" {
                    let mut n_rs = Vec::new();
                    for source in &rs {
                        n_rs.extend(mem_table.get_target_v(source, &step.paper, &step.code));
                    }
                    rs = n_rs;
                } else {
                    let mut n_rs = Vec::new();
                    for target in &rs {
                        n_rs.extend(mem_table.get_source_v(&step.paper, &step.code, target));
                    }
                    rs = n_rs;
                }
            }
            Ok(rs)
        })
    }
}

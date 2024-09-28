use std::{future, io, pin::Pin, sync::Arc};

use tokio::sync::Mutex;

use crate::util::{mem_table, Path};

use super::{AsDataManager, Auth};

mod main {
    #[cfg(test)]
    mod test_get_source_v {
        use crate::util::{
            data::{AsDataManager, MemDataManager},
            Path,
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

    fn clear<'a, 'f>(
        &'a self,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
    {
        Box::pin(async move {
            let mut mem_table = self.mem_table.lock().await;
            match &self.auth {
                Some(auth) => {
                    for paper in &auth.writer {
                        mem_table.clear_paper(paper);
                    }
                }
                None => mem_table.clear(),
            }
            Ok(())
        })
    }

    fn append<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            if let Some(auth) = &self.auth {
                if !auth.writer.contains(&step.paper) {
                    return Err(io::Error::other("permision denied"));
                }
            }
            let root_v = self.get(&path).await?;
            let mut mem_table = self.mem_table.lock().await;
            for source in &root_v {
                for target in &item_v {
                    mem_table.insert_edge(source, &step.paper, &step.code, target);
                }
            }
            Ok(())
        })
    }

    fn set<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            if let Some(auth) = &self.auth {
                if !auth.writer.contains(&step.paper) {
                    return Err(io::Error::other("permision denied"));
                }
            }
            let root_v = self.get(&path).await?;
            let mut mem_table = self.mem_table.lock().await;
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

    fn get<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            if let Some(root) = &path.root_op {
                return Box::pin(future::ready(Ok(vec![root.clone()])));
            } else {
                return Box::pin(future::ready(Ok(vec![])));
            }
        }
        let mut path = path.clone();
        Box::pin(async move {
            let mut mem_table = self.mem_table.lock().await;
            let mut rs = vec![path.root_op.clone().unwrap()];
            while !path.step_v.is_empty() {
                let step = path.step_v.remove(0);
                if let Some(auth) = &self.auth {
                    if !auth.writer.contains(&step.paper) && !auth.reader.contains(&step.paper) {
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

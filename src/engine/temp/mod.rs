use std::{future, io, pin::Pin, sync::Arc};

use crate::{
    data::{Auth, MemDataManager},
    util::{Path, PathPart},
};

use super::AsDataManager;

#[derive(Clone)]
pub struct TempDataManager {
    global: Arc<dyn AsDataManager>,
    temp: Arc<MemDataManager>,
}

impl TempDataManager {
    pub fn new(global: Arc<dyn AsDataManager>) -> Self {
        let auth = global.get_auth().clone();
        Self {
            global,
            temp: Arc::new(MemDataManager::new(auth)),
        }
    }

    pub fn get_temp(&self) -> Arc<dyn AsDataManager> {
        self.temp.clone()
    }

    pub fn get_global(&self) -> Arc<dyn AsDataManager> {
        self.global.clone()
    }

    pub async fn reset(&self) -> io::Result<()> {
        self.temp.clear().await
    }

    pub fn while1(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            let root_v = this.get(&path).await?;
            loop {
                for root in &root_v {
                    let path = Path {
                        root_op: Some(root.clone()),
                        step_v: vec![step.clone()],
                    };
                    if path.is_temp() {
                        if !this.temp.get(&path).await?.is_empty() {
                            return Ok(());
                        }
                    } else {
                        if !this.global.get(&path).await?.is_empty() {
                            return Ok(());
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
    }

    pub fn while0(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            let root_v = this.get(&path).await?;
            loop {
                for root in &root_v {
                    let path = Path {
                        root_op: Some(root.clone()),
                        step_v: vec![step.clone()],
                    };
                    if path.is_temp() {
                        if this.temp.get(&path).await?.is_empty() {
                            return Ok(());
                        }
                    } else {
                        if this.global.get(&path).await?.is_empty() {
                            return Ok(());
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
    }
}

impl AsDataManager for TempDataManager {
    fn get_auth(&self) -> &Auth {
        self.global.get_auth()
    }

    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager> {
        Arc::new(Self {
            global: self.global.divide(auth.clone()),
            temp: Arc::new(MemDataManager::new(auth)),
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
        let mut path = path.clone();
        Box::pin(async move {
            if path.is_temp() {
                let step = path.step_v.pop().unwrap();
                let root_v = this.get(&path).await?;
                for root in &root_v {
                    this.temp
                        .append(
                            &Path {
                                root_op: Some(root.clone()),
                                step_v: vec![step.clone()],
                            },
                            item_v.clone(),
                        )
                        .await?;
                }
            } else {
                let step = path.step_v.pop().unwrap();
                let root_v = this.get(&path).await?;
                for root in &root_v {
                    this.global
                        .append(
                            &Path {
                                root_op: Some(root.clone()),
                                step_v: vec![step.clone()],
                            },
                            item_v.clone(),
                        )
                        .await?;
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
        if path.step_v.last().unwrap().arrow != "->" {
            return Box::pin(future::ready(Err(io::Error::other("can not set parents"))));
        }
        let this = self.clone();
        let mut path = path.clone();
        Box::pin(async move {
            if path.is_temp() {
                let step = path.step_v.pop().unwrap();
                let root_v = this.get(&path).await?;
                log::debug!(
                    "set {}->{}: {}\nwhen UnitedDataManager::set",
                    path.to_string(),
                    step.code,
                    root_v.len()
                );
                for root in &root_v {
                    this.temp
                        .set(
                            &Path {
                                root_op: Some(root.clone()),
                                step_v: vec![step.clone()],
                            },
                            item_v.clone(),
                        )
                        .await?;
                }
            } else {
                let step = path.step_v.pop().unwrap();
                let root_v = this.get(&path).await?;
                log::debug!(
                    "set {}->{}: {}\nwhen UnitedDataManager::set",
                    path.to_string(),
                    step.code,
                    root_v.len()
                );
                for root in &root_v {
                    this.global
                        .set(
                            &Path {
                                root_op: Some(root.clone()),
                                step_v: vec![step.clone()],
                            },
                            item_v.clone(),
                        )
                        .await?;
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
            if let Some(root) = &path.root_op {
                return Box::pin(future::ready(Ok(vec![root.clone()])));
            } else {
                return Box::pin(future::ready(Ok(vec![])));
            }
        }
        let this = self.clone();
        let path = path.clone();
        Box::pin(async move {
            match path.first_part() {
                PathPart::Pure(part_path) => {
                    let item_v = this.global.get(&part_path).await?;

                    let mut rs = Vec::new();
                    for root in item_v {
                        rs.extend(
                            this.get(&Path {
                                root_op: Some(root),
                                step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                            })
                            .await?,
                        );
                    }

                    Ok(rs)
                }
                PathPart::Temp(part_path) => {
                    let item_v = this.temp.get(&part_path).await?;

                    let mut rs = Vec::new();
                    for root in item_v {
                        rs.extend(
                            this.get(&Path {
                                root_op: Some(root),
                                step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                            })
                            .await?,
                        );
                    }

                    Ok(rs)
                }
                PathPart::EntirePure => {
                    let item_v = this.global.get(&path).await?;
                    Ok(item_v)
                }
                PathPart::EntireTemp => {
                    let item_v = this.temp.get(&path).await?;
                    Ok(item_v)
                }
            }
        })
    }

    fn clear(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        Box::pin(async move {
            this.temp.clear().await?;
            this.global.clear().await
        })
    }

    fn call(
        &self,
        output: Path,
        func: &str,
        input: Path,
        input1: Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        self.global.call(output, func, input, input1)
    }
}

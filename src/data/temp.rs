use std::{future, io, pin::Pin, sync::Arc};

use crate::util::{Path, PathPart};

use super::{AsDataManager, Auth, MemDataManager};

#[derive(Clone)]
pub struct TempDataManager {
    global: Arc<dyn AsDataManager>,
    temp: Arc<dyn AsDataManager>,
}

impl TempDataManager {
    pub fn new(global: Arc<dyn AsDataManager>) -> Self {
        let auth = global.get_auth().clone();
        Self {
            global,
            temp: Arc::new(MemDataManager::new(auth)),
        }
    }
}

impl AsDataManager for TempDataManager {
    fn get_auth(&self) -> &Auth {
        self.global.get_auth()
    }

    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager> {
        Arc::new(Self {
            global: self.global.divide(auth.clone()),
            temp: self.temp.divide(auth),
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
                                root: root.clone(),
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
                                root: root.clone(),
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
                                root: root.clone(),
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
                                root: root.clone(),
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
            if path.root.is_empty() {
                return Box::pin(future::ready(Ok(vec![])));
            }
            return Box::pin(future::ready(Ok(vec![path.root.clone()])));
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
                                root,
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
                                root,
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

    fn commit(&self) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        let this = self.clone();
        Box::pin(async move {
            this.temp.clear().await?;
            this.global.commit().await
        })
    }
}

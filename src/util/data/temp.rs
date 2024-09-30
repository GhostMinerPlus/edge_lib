use std::{future, io, pin::Pin, sync::Arc};

use crate::util::{
    data::{Auth, MemDataManager},
    Path, PathPart,
};

use super::AsDataManager;

#[derive(Clone)]
pub struct TempDataManager {
    pub global: Arc<dyn AsDataManager>,
    pub temp: Arc<MemDataManager>,
}

impl TempDataManager {
    pub fn new(global: Arc<dyn AsDataManager>) -> Self {
        let auth = global.get_auth().clone();
        Self {
            global,
            temp: Arc::new(MemDataManager::new(auth)),
        }
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
            let path = Path {
                root_v,
                step_v: vec![step],
            };
            loop {
                if path.is_temp() {
                    if !this.temp.get(&path).await?.is_empty() {
                        return Ok(());
                    }
                } else {
                    if !this.global.get(&path).await?.is_empty() {
                        return Ok(());
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
            let path = Path {
                root_v,
                step_v: vec![step],
            };
            loop {
                if path.is_temp() {
                    if this.temp.get(&path).await?.is_empty() {
                        return Ok(());
                    }
                } else {
                    if this.global.get(&path).await?.is_empty() {
                        return Ok(());
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
        if path.step_v.last().unwrap().arrow != "->" {
            return Box::pin(future::ready(Err(io::Error::other("can not set parents"))));
        }
        let mut path = path.clone();
        Box::pin(async move {
            if path.is_temp() {
                let step = path.step_v.pop().unwrap();
                let root_v = self.get(&path).await?;
                self.temp
                    .append(
                        &Path {
                            root_v,
                            step_v: vec![step],
                        },
                        item_v,
                    )
                    .await?;
            } else {
                let step = path.step_v.pop().unwrap();
                let root_v = self.get(&path).await?;
                self.global
                    .append(
                        &Path {
                            root_v,
                            step_v: vec![step],
                        },
                        item_v,
                    )
                    .await?;
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
        if path.step_v.last().unwrap().arrow != "->" {
            return Box::pin(future::ready(Err(io::Error::other("can not set parents"))));
        }
        let mut path = path.clone();
        Box::pin(async move {
            if path.is_temp() {
                let step = path.step_v.pop().unwrap();
                let root_v = self.get(&path).await?;
                log::debug!(
                    "set {}->{}: {}\nwhen UnitedDataManager::set",
                    path.to_string(),
                    step.code,
                    root_v.len()
                );
                self.temp
                    .set(
                        &Path {
                            root_v,
                            step_v: vec![step],
                        },
                        item_v,
                    )
                    .await?;
            } else {
                let step = path.step_v.pop().unwrap();
                let root_v = self.get(&path).await?;
                log::debug!(
                    "set {}->{}: {}\nwhen UnitedDataManager::set",
                    path.to_string(),
                    step.code,
                    root_v.len()
                );
                self.global
                    .set(
                        &Path {
                            root_v,
                            step_v: vec![step],
                        },
                        item_v,
                    )
                    .await?;
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
            return Box::pin(future::ready(Ok(path.root_v.clone())));
        }
        let path = path.clone();
        Box::pin(async move {
            match path.first_part() {
                PathPart::Pure(part_path) => {
                    let item_v = self.global.get(&part_path).await?;

                    self.get(&Path {
                        root_v: item_v,
                        step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                    })
                    .await
                }
                PathPart::Temp(part_path) => {
                    let item_v = self.temp.get(&part_path).await?;

                    self.get(&Path {
                        root_v: item_v,
                        step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                    })
                    .await
                }
                PathPart::EntirePure => {
                    let item_v = self.global.get(&path).await?;
                    Ok(item_v)
                }
                PathPart::EntireTemp => {
                    let item_v = self.temp.get(&path).await?;
                    Ok(item_v)
                }
            }
        })
    }

    fn clear<'a, 'f>(
        &'a self,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
    {
        Box::pin(async move {
            self.temp.clear().await?;
            self.global.clear().await
        })
    }

    fn get_code_v<'a, 'a1, 'a2, 'f>(
        &'a self,
        root: &'a1 str,
        space: &'a2 str,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f,
    {
        Box::pin(async move {
            if space == "$" {
                self.temp.get_code_v(root, space).await
            } else {
                self.global.get_code_v(root, space).await
            }
        })
    }

    #[allow(unused)]
    fn call<'a, 'a1, 'a2, 'a3, 'a4, 'f>(
        &'a self,
        output: &'a1 Path,
        func: &'a2 str,
        input: &'a3 Path,
        input1: &'a4 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f,
        'a3: 'f,
        'a4: 'f,
    {
        Box::pin(async move {
            match func {
                // while
                "while0" => self.while0(input).await,
                "while1" => self.while1(input).await,
                _ => Err(io::Error::other("Not found!")),
            }
        })
    }
}

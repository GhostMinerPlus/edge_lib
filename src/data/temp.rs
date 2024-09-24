use std::{future, io, pin::Pin, sync::Arc};

use crate::{
    data::{Auth, MemDataManager},
    util::{Path, PathPart},
};

use super::AsDataManager;

mod func;

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

impl AsTempDataManager for TempDataManager {
    fn divide_dm(&self, auth: Auth) -> Arc<dyn AsDataManager> {
        Arc::new(Self {
            global: self.global.divide(auth.clone()),
            temp: Arc::new(MemDataManager::new(auth)),
        })
    }

    fn get_temp(&self) -> Arc<dyn AsDataManager> {
        self.temp.clone()
    }

    fn get_global(&self) -> Arc<dyn AsDataManager> {
        self.global.clone()
    }

    fn get_auth(&self) -> &Auth {
        self.global.get_auth()
    }

    fn divide(&self, auth: Auth) -> Arc<dyn AsTempDataManager> {
        Arc::new(Self {
            temp: Arc::new(MemDataManager::new(auth.clone())),
            global: self.global.divide(auth),
        })
    }

    fn call<'a, 'a1, 'f>(
        &'a self,
        output: Path,
        func: &'a1 str,
        input: Path,
        input1: Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        Box::pin(async move {
            match func {
                "new" => func::new(self, output, input, input1).await,
                "line" => func::line(self, output, input, input1).await,
                "rand" => func::rand(self, output, input, input1).await,
                //
                "append" => func::append(self, output, input, input1).await,
                "distinct" => func::distinct(self, output, input, input1).await,
                "left" => func::left(self, output, input, input1).await,
                "inner" => func::inner(self, output, input, input1).await,
                "if" => func::if_(self, output, input, input1).await,
                "if0" => func::if_0(self, output, input, input1).await,
                "if1" => func::if_1(self, output, input, input1).await,
                //
                "+" => func::add(self, output, input, input1).await,
                "-" => func::minus(self, output, input, input1).await,
                "*" => func::mul(self, output, input, input1).await,
                "/" => func::div(self, output, input, input1).await,
                "%" => func::rest(self, output, input, input1).await,
                //
                "==" => func::equal(self, output, input, input1).await,
                "!=" => func::not_equal(self, output, input, input1).await,
                ">" => func::greater(self, output, input, input1).await,
                "<" => func::smaller(self, output, input, input1).await,
                //
                "count" => func::count(self, output, input, input1).await,
                "sum" => func::sum(self, output, input, input1).await,
                //
                "=" => func::set(self, output, input, input1).await,
                //
                "slice" => func::slice(self, output, input, input1).await,
                "sort" => func::sort(self, output, input, input1).await,
                "sort_s" => func::sort_s(self, output, input, input1).await,
                // while
                "while0" => self.while0(&input).await,
                "while1" => self.while1(&input).await,
                _ => Err(io::Error::other("Not found!")),
            }
        })
    }
}

pub trait AsTempDataManager: Sync + Send + 'static {
    fn get_auth(&self) -> &Auth;

    fn get_temp(&self) -> Arc<dyn AsDataManager>;

    fn get_global(&self) -> Arc<dyn AsDataManager>;

    fn call<'a, 'a1, 'f>(
        &'a self,
        output: Path,
        func: &'a1 str,
        input: Path,
        input1: Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f;

    fn divide_dm(&self, auth: Auth) -> Arc<dyn AsDataManager>;

    fn divide(&self, auth: Auth) -> Arc<dyn AsTempDataManager>;
}

impl<T> AsDataManager for T
where
    T: AsTempDataManager + Sync + Send + ?Sized + 'static,
{
    fn get_auth(&self) -> &Auth {
        AsTempDataManager::get_auth(self)
    }

    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager> {
        AsTempDataManager::divide_dm(self, auth)
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
                for root in &root_v {
                    self.get_temp()
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
                let root_v = self.get(&path).await?;
                for root in &root_v {
                    self.get_global()
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
                for root in &root_v {
                    self.get_temp()
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
                let root_v = self.get(&path).await?;
                log::debug!(
                    "set {}->{}: {}\nwhen UnitedDataManager::set",
                    path.to_string(),
                    step.code,
                    root_v.len()
                );
                for root in &root_v {
                    self.get_global()
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
        let path = path.clone();
        Box::pin(async move {
            match path.first_part() {
                PathPart::Pure(part_path) => {
                    let item_v = self.get_global().get(&part_path).await?;

                    let mut rs = Vec::new();
                    for root in item_v {
                        rs.extend(
                            self.get(&Path {
                                root_op: Some(root),
                                step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                            })
                            .await?,
                        );
                    }

                    Ok(rs)
                }
                PathPart::Temp(part_path) => {
                    let item_v = self.get_temp().get(&part_path).await?;

                    let mut rs = Vec::new();
                    for root in item_v {
                        rs.extend(
                            self.get(&Path {
                                root_op: Some(root),
                                step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                            })
                            .await?,
                        );
                    }

                    Ok(rs)
                }
                PathPart::EntirePure => {
                    let item_v = self.get_global().get(&path).await?;
                    Ok(item_v)
                }
                PathPart::EntireTemp => {
                    let item_v = self.get_temp().get(&path).await?;
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
            self.get_temp().clear().await?;
            self.get_global().clear().await
        })
    }
}

use std::{future, io, pin::Pin};

use crate::util::{
    data::{Auth, MemDataManager},
    func, Path, PathPart,
};

use super::{AsDataManager, AsStack};

pub struct TempDataManager {
    pub global: Box<dyn AsDataManager>,
    pub temp: Vec<MemDataManager>,
}

impl TempDataManager {
    pub fn new(global: Box<dyn AsDataManager>) -> Self {
        Self {
            global,
            temp: vec![MemDataManager::new(None)],
        }
    }

    pub fn while1<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        Box::pin(async move {
            let mut path = path.clone();
            let step = path.step_v.pop().unwrap();
            let root_v = self.get(&path).await?;
            let path = Path {
                root_v,
                step_v: vec![step],
            };
            loop {
                if path.is_temp() {
                    if !self.temp.last().unwrap().get(&path).await?.is_empty() {
                        return Ok(());
                    }
                } else {
                    if !self.global.get(&path).await?.is_empty() {
                        return Ok(());
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
    }

    pub fn while0<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        Box::pin(async move {
            let mut path = path.clone();
            let step = path.step_v.pop().unwrap();
            let root_v = self.get(&path).await?;
            let path = Path {
                root_v,
                step_v: vec![step],
            };
            loop {
                if path.is_temp() {
                    if self.temp.last().unwrap().get(&path).await?.is_empty() {
                        return Ok(());
                    }
                } else {
                    if self.global.get(&path).await?.is_empty() {
                        return Ok(());
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
    }

    /// # Convert temp path to gloabl path.
    #[async_recursion::async_recursion]
    pub async fn temp_2_gloabl(&self, path: &Path) -> io::Result<Path> {
        match path.first_part() {
            PathPart::Pure(part_path) => {
                let item_v = self.global.get(&part_path).await?;

                self.temp_2_gloabl(&Path {
                    root_v: item_v,
                    step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                })
                .await
            }
            PathPart::Temp(part_path) => {
                let item_v = self.temp.last().unwrap().get(&part_path).await?;

                self.temp_2_gloabl(&Path {
                    root_v: item_v,
                    step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                })
                .await
            }
            PathPart::EntirePure => Ok(path.clone()),
            PathPart::EntireTemp => {
                let item_v = self.temp.last().unwrap().get(&path).await?;
                Ok(Path {
                    root_v: item_v,
                    step_v: vec![],
                })
            }
        }
    }
}

impl AsStack for TempDataManager {
    fn push<'a, 'f>(
        &'a mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>> {
        self.temp.push(MemDataManager::new(None));
        Box::pin(future::ready(Ok(())))
    }

    fn pop<'a, 'f>(
        &'a mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>> {
        if self.temp.len() > 1 {
            self.temp.pop();
        } else {
            self.temp = vec![MemDataManager::new(None)];
        }
        Box::pin(future::ready(Ok(())))
    }
}

impl AsDataManager for TempDataManager {
    fn get_auth(&self) -> &Auth {
        self.global.get_auth()
    }

    fn append<'a, 'a1, 'f>(
        &'a mut self,
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
                    .last_mut()
                    .unwrap()
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
        &'a mut self,
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
                    .last_mut()
                    .unwrap()
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
            let gloabl_path = self.temp_2_gloabl(&path).await?;
            self.global.get(&gloabl_path).await
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
                self.temp.last().unwrap().get_code_v(root, space).await
            } else {
                self.global.get_code_v(root, space).await
            }
        })
    }

    fn call_and_return<'a, 'a1, 'a2, 'a3, 'f>(
        &'a mut self,
        func: &'a1 str,
        input: &'a2 Path,
        input1: &'a3 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f,
        'a3: 'f,
    {
        Box::pin(async move {
            match func {
                // while
                "while0" => {
                    self.while0(input).await?;
                    Ok(vec![])
                }
                "while1" => {
                    self.while1(input).await?;
                    Ok(vec![])
                }
                _ => {
                    self.global
                        .call_and_return(
                            func,
                            &self.temp_2_gloabl(input).await?,
                            &self.temp_2_gloabl(input1).await?,
                        )
                        .await
                }
            }
        })
    }

    fn call<'a, 'a1, 'a2, 'a3, 'a4, 'f>(
        &'a mut self,
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
        Self: Sized + 'static,
    {
        Box::pin(async move {
            match func {
                "new" => func::new(self, output, input, input1).await,
                "line" => func::line(self, output, input, input1).await,
                "rand" => func::rand(self, output, input, input1).await,
                //
                "+=" => func::append(self, output, input, input1).await,
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
                "dump" => func::dump(self, output, input, input1).await,
                _ => {
                    if !output.is_temp() {
                        if let Ok(()) = self
                            .global
                            .call(
                                &self.temp_2_gloabl(output).await?,
                                func,
                                &self.temp_2_gloabl(input).await?,
                                &self.temp_2_gloabl(input1).await?,
                            )
                            .await
                        {
                            return Ok(());
                        }
                    }
                    let rs = self.call_and_return(func, input, input1).await?;
                    self.set(output, rs).await
                }
            }
        })
    }
}

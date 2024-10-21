use std::{future, pin::Pin};

use crate::{err, util::Path};

use super::{
    data::{AsDataManager, MemDataManager},
    func, PathPart,
};

mod dep {
    use crate::{
        err,
        util::{self, engine::Inc, Path},
    };

    pub fn parse_script1(script: &[String]) -> err::Result<Vec<Inc>> {
        let mut inc_v = Vec::new();
        for line in script {
            if line.is_empty() {
                continue;
            }

            let word_v: Vec<&str> = line.split(' ').collect();
            if word_v.len() < 4 {
                return Err(err::Error::new(
                    err::ErrorKind::Other,
                    format!("{line}: less than 4 words in a line"),
                ));
            }
            if word_v.len() == 5 {
                if word_v[1] == "=" {
                    inc_v.push(Inc {
                        output: Path::from_str(word_v[0].trim()),
                        function: Path::from_str(word_v[2].trim()),
                        input: Path::from_str(word_v[3].trim()),
                        input1: Path::from_str(word_v[4].trim()),
                    });
                } else if word_v[1] == "+=" {
                    inc_v.push(Inc {
                        output: Path::from_str("$->$:temp"),
                        function: Path::from_str(word_v[2].trim()),
                        input: Path::from_str(word_v[3].trim()),
                        input1: Path::from_str(word_v[4].trim()),
                    });
                    inc_v.push(Inc {
                        output: Path::from_str(word_v[0].trim()),
                        function: Path::from_str("+="),
                        input: Path::from_str(word_v[0].trim()),
                        input1: Path::from_str("$->$:temp"),
                    });
                } else {
                    return Err(err::Error::new(
                        err::ErrorKind::Other,
                        format!("when parse_script:\n\tunknown operator"),
                    ));
                }
                continue;
            }
            inc_v.push(Inc {
                output: Path::from_str(word_v[0].trim()),
                function: Path::from_str(word_v[1].trim()),
                input: Path::from_str(word_v[2].trim()),
                input1: Path::from_str(word_v[3].trim()),
            });
        }
        Ok(inc_v)
    }

    #[inline]
    pub fn unwrap_value(path: &mut Path) {
        if path.root_v.len() == 1 {
            if path.root_v[0] == "?" && path.step_v.is_empty() {
                path.root_v[0] = util::gen_value();
            }
        }
    }

    #[inline]
    pub fn unwrap_inc(inc: &mut Inc) {
        unwrap_value(&mut inc.output);
        unwrap_value(&mut inc.function);
        unwrap_value(&mut inc.input);
        unwrap_value(&mut inc.input1);
    }
}

pub trait AsEdgeEngine {
    fn execute_script<'a, 'a1, 'f>(
        &'a mut self,
        script: &'a1 [String],
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f;
}

/// Let impl [AsDataManager] be using for [AsEdgeEngine].
pub struct EdgeEngine<'g, DM>
where
    DM: AsDataManager,
{
    global: &'g mut DM,
    temp: MemDataManager,
}

impl<'g, DM> AsEdgeEngine for EdgeEngine<'g, DM>
where
    DM: AsDataManager,
{
    fn execute_script<'a, 'a1, 'f>(
        &'a mut self,
        script: &'a1 [String],
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        Box::pin(async move {
            let mut inc_v = dep::parse_script1(&script)?;
            if inc_v.is_empty() {
                return Ok(vec![]);
            }

            for inc in &mut inc_v {
                dep::unwrap_inc(inc);
                let func_name_v = self.get(&inc.function).await?;
                if func_name_v.is_empty() {
                    return Err(err::Error::new(
                        err::ErrorKind::Other,
                        format!("no funtion: {}\nat invoke_inc", inc.function.to_string()),
                    ));
                }

                if let Err(e) = self
                    .call(&inc.output, &func_name_v[0], &inc.input, &inc.input1)
                    .await
                {
                    if let err::ErrorKind::NotFound = e.kind() {
                        let input_item_v = self.get(&inc.input).await?;
                        let input1_item_v = self.get(&inc.input1).await?;

                        let rs = {
                            let mut sub_engine = EdgeEngine::new(self.get_global_mut());

                            let _ = sub_engine
                                .set(&Path::from_str("$->$:input"), input_item_v)
                                .await;
                            let _ = sub_engine
                                .set(&Path::from_str("$->$:input1"), input1_item_v)
                                .await;
                            sub_engine.execute_script(&func_name_v).await?
                        };

                        self.set(&inc.output, rs).await?;
                    } else {
                        return Err(e);
                    }
                }
            }

            self.get(&Path::from_str("$->$:output")).await
        })
    }
}

impl<'g, DM> EdgeEngine<'g, DM>
where
    DM: AsDataManager,
{
    pub fn new(global: &'g mut DM) -> Self {
        Self {
            global,
            temp: MemDataManager::new(None),
        }
    }

    pub fn reset_temp(&mut self) {
        self.temp = MemDataManager::new(None);
    }

    #[inline]
    pub fn get_temp_mut(&mut self) -> &mut MemDataManager {
        &mut self.temp
    }

    #[inline]
    pub fn get_temp(&self) -> &MemDataManager {
        &self.temp
    }

    #[inline]
    pub fn get_global(&self) -> &DM {
        &self.global
    }

    #[inline]
    pub fn get_global_mut(&mut self) -> &mut DM {
        self.global
    }

    pub fn new_with_temp(global: &'g mut DM, temp: MemDataManager) -> Self {
        Self { global, temp }
    }

    pub fn while1<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<()>> + Send + 'f>>
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
                    if !self.temp.get(&path).await?.is_empty() {
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
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<()>> + Send + 'f>>
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
                    if self.temp.get(&path).await?.is_empty() {
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
    pub async fn temp_2_global(&self, path: &Path) -> err::Result<Path> {
        match path.first_part() {
            PathPart::Pure(part_path) => {
                let item_v = self.global.get(&part_path).await?;

                self.temp_2_global(&Path {
                    root_v: item_v,
                    step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                })
                .await
            }
            PathPart::Temp(part_path) => {
                let item_v = self.temp.get(&part_path).await?;

                self.temp_2_global(&Path {
                    root_v: item_v,
                    step_v: path.step_v[part_path.step_v.len()..].to_vec(),
                })
                .await
            }
            PathPart::EntirePure => Ok(path.clone()),
            PathPart::EntireTemp => {
                let item_v = self.temp.get(&path).await?;
                Ok(Path {
                    root_v: item_v,
                    step_v: vec![],
                })
            }
        }
    }
}

impl<'g, DM> AsDataManager for EdgeEngine<'g, DM>
where
    DM: AsDataManager,
{
    fn get_auth(&self) -> &super::data::Auth {
        self.global.get_auth()
    }

    fn append<'a, 'a1, 'f>(
        &'a mut self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        if path.step_v.last().unwrap().arrow != "->" {
            return Box::pin(future::ready(Err(err::Error::new(
                err::ErrorKind::Other,
                format!("can not set parents"),
            ))));
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
        &'a mut self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }

        if path.step_v.last().unwrap().arrow != "->" {
            return Box::pin(future::ready(Err(err::Error::new(
                err::ErrorKind::Other,
                format!("can not set parents"),
            ))));
        }

        Box::pin(async move {
            let mut path = path.clone();

            if path.is_temp() {
                let step = path.step_v.pop().unwrap();
                let root_v = self.get(&path).await?;

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
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(path.root_v.clone())));
        }
        let path = path.clone();
        Box::pin(async move {
            let gloabl_path = self.temp_2_global(&path).await?;
            self.global.get(&gloabl_path).await
        })
    }

    fn get_code_v<'a, 'a1, 'a2, 'f>(
        &'a self,
        root: &'a1 str,
        space: &'a2 str,
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<Vec<String>>> + Send + 'f>>
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

    fn call_and_return<'a, 'a1, 'a2, 'a3, 'f>(
        &'a mut self,
        func: &'a1 str,
        input: &'a2 Path,
        input1: &'a3 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<Vec<String>>> + Send + 'f>>
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
                "get_code_v" => {
                    let root_v = self.get(input).await?;
                    let space_v = self.get(input1).await?;

                    self.get_code_v(&root_v[0], &space_v[0]).await
                }
                _ => {
                    self.global
                        .call_and_return(
                            func,
                            &self.temp_2_global(input).await?,
                            &self.temp_2_global(input1).await?,
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
    ) -> Pin<Box<dyn std::future::Future<Output = err::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f,
        'a3: 'f,
        'a4: 'f,
        Self: Sized,
    {
        Box::pin(async move {
            let input = self.temp_2_global(input).await?;
            let input1 = self.temp_2_global(input1).await?;

            if !output.is_temp() {
                let g_output = self.temp_2_global(output).await?;

                if let Ok(()) = self.global.call(&g_output, func, &input, &input1).await {
                    return Ok(());
                }
            } else {
                if let Ok(rs) = self.global.call_and_return(func, &input, &input1).await {
                    return self.set(output, rs).await;
                }
            }

            match func {
                "new" => func::new(self, output, &input, &input1).await,
                "line" => func::line(self, output, &input, &input1).await,
                "rand" => func::rand(self, output, &input, &input1).await,
                //
                "+=" => func::append(self, output, &input, &input1).await,
                "append" => func::append(self, output, &input, &input1).await,
                "distinct" => func::distinct(self, output, &input, &input1).await,
                "left" => func::left(self, output, &input, &input1).await,
                "inner" => func::inner(self, output, &input, &input1).await,
                "if" => func::if_(self, output, &input, &input1).await,
                "if0" => func::if_0(self, output, &input, &input1).await,
                "if1" => func::if_1(self, output, &input, &input1).await,
                //
                "+" => func::add(self, output, &input, &input1).await,
                "-" => func::minus(self, output, &input, &input1).await,
                "*" => func::mul(self, output, &input, &input1).await,
                "/" => func::div(self, output, &input, &input1).await,
                "%" => func::rest(self, output, &input, &input1).await,
                //
                "==" => func::equal(self, output, &input, &input1).await,
                "!=" => func::not_equal(self, output, &input, &input1).await,
                ">" => func::greater(self, output, &input, &input1).await,
                "<" => func::smaller(self, output, &input, &input1).await,
                //
                "count" => func::count(self, output, &input, &input1).await,
                "sum" => func::sum(self, output, &input, &input1).await,
                //
                "=" => func::set(self, output, &input, &input1).await,
                //
                "slice" => func::slice(self, output, &input, &input1).await,
                "sort" => func::sort(self, output, &input, &input1).await,
                "sort_s" => func::sort_s(self, output, &input, &input1).await,
                "dump" => func::dump(self, output, &input, &input1).await,
                _ => {
                    let rs = self.call_and_return(func, &input, &input1).await?;
                    self.set(output, rs).await
                }
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct Inc {
    pub output: Path,
    pub function: Path,
    pub input: Path,
    pub input1: Path,
}

#[cfg(test)]
mod tests {
    use crate::util::{
        data::{AsDataManager, MemDataManager},
        engine::{AsEdgeEngine, EdgeEngine},
        Path,
    };

    #[test]
    fn test() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut dm = MemDataManager::new(None);

            let mut engine = EdgeEngine::new(&mut dm);

            engine
                .execute_script(&vec![
                    "$->$:temp append $->$:temp '$->$:output\\s+\\s1\\s1'".to_string(),
                    "test->test:test = $->$:temp _".to_string(),
                ])
                .await
                .unwrap();

            let rs = engine
                .get(&Path::from_str("test->test:test"))
                .await
                .unwrap();

            assert_eq!(rs.len(), 1);
        });
    }

    #[test]
    fn test_string() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut dm = MemDataManager::new(None);

            let mut engine = EdgeEngine::new(&mut dm);

            let rs = engine
                .execute_script(&vec![
                    "$->$:output append $->$:temp 'running\\s=>\\s智\\s明'".to_string(),
                ])
                .await
                .unwrap();

            assert_eq!(rs.len(), 1);
            assert_eq!(rs[0], "running => 智 明");
        });
    }

    #[test]
    fn test_dump() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            // dm
            let mut dm = MemDataManager::new(None);

            // engine
            let mut engine = EdgeEngine::new(&mut dm);

            // data
            engine
                .execute_script(&vec![
                    //
                    format!("test->test:step1 = ? _"),
                    //
                    format!("test->test:step1->test:step2 = test1 _"),
                ])
                .await
                .unwrap();

            // rs
            let rs = engine
                .execute_script(&vec![format!("$->$:output dump test test")])
                .await
                .unwrap();

            // rj
            let rj = json::parse(&crate::util::rs_2_str(&rs)).unwrap();

            // assert
            assert_eq!(rj[0]["test:step1"][0]["test:step2"][0], "test1");
        });
    }

    #[test]
    fn test_load() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            // dm
            let mut dm = MemDataManager::new(None);

            // engine
            let mut engine = EdgeEngine::new(&mut dm);

            engine
                .load(
                    &json::object! {
                        "$:test": "test"
                    },
                    &Path::from_str("$->$:test"),
                )
                .await
                .unwrap();

            // rs
            let rs = engine
                .execute_script(&vec![format!("$->$:output dump $->$:test $")])
                .await
                .unwrap();

            // rj
            let rj = json::parse(&crate::util::rs_2_str(&rs)).unwrap();

            // assert
            assert_eq!(rj[0]["$:test"][0], "test");
        })
    }
}

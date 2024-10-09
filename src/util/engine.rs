use std::{io, pin::Pin};

use serde::{Deserialize, Serialize};

use crate::util::Path;

use super::data::{AsDataManager, AsStack};

mod dep {
    use std::io;

    use crate::util::{self, engine::Inc, Path};

    pub fn parse_script1(script: &[String]) -> io::Result<Vec<Inc>> {
        let mut inc_v = Vec::new();
        for line in script {
            if line.is_empty() {
                continue;
            }

            let word_v: Vec<&str> = line.split(' ').collect();
            if word_v.len() < 4 {
                return Err(io::Error::other(format!(
                    "{line}: less than 4 words in a line"
                )));
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
                    return Err(io::Error::other("when parse_script:\n\tunknown operator"));
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
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f;
}

impl<DM> AsEdgeEngine for DM
where
    DM: AsDataManager + AsStack,
{
    fn execute_script<'a, 'a1, 'f>(
        &'a mut self,
        script: &'a1 [String],
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        Box::pin(async move {
            let mut inc_v = dep::parse_script1(&script)?;
            if inc_v.is_empty() {
                return Ok(vec![]);
            }
            log::debug!("inc_v.len(): {}", inc_v.len());
            for inc in &mut inc_v {
                dep::unwrap_inc(inc);
                log::debug!("invoke_inc: {:?}", inc);
                let func_name_v = self.get(&inc.function).await?;
                if func_name_v.is_empty() {
                    return Err(io::Error::other(format!(
                        "no funtion: {}\nat invoke_inc",
                        inc.function.to_string()
                    )));
                }
                if let Err(_) = self
                    .call(&inc.output, &func_name_v[0], &inc.input, &inc.input1)
                    .await
                {
                    let input_item_v = self.get(&inc.input).await?;
                    let input1_item_v = self.get(&inc.input1).await?;

                    self.push().await?;

                    let _ = self.set(&Path::from_str("$->$:input"), input_item_v).await;
                    let _ = self
                        .set(&Path::from_str("$->$:input1"), input1_item_v)
                        .await;

                    let rs = self.execute_script(&func_name_v).await;

                    self.pop().await?;

                    self.set(&inc.output, rs?).await?;
                }
            }
            self.get(&Path::from_str(&format!("$->$:output"))).await
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ScriptTree {
    pub script: String,
    pub name: String,
    pub next_v: Vec<ScriptTree>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScriptTree1 {
    pub script: Vec<String>,
    pub name: String,
    pub next_v: Vec<ScriptTree1>,
}

#[cfg(test)]
mod tests {
    use crate::util::{
        data::{AsDataManager, AsStack, MemDataManager, TempDataManager},
        engine::AsEdgeEngine,
        Path,
    };

    #[test]
    fn test() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dm = Box::new(MemDataManager::new(None));
            let mut engine = TempDataManager::new(dm);
            engine
                .execute_script(&vec![
                    "$->$:temp append $->$:temp '$->$:output\\s+\\s1\\s1'".to_string(),
                    "test->test:test = $->$:temp _".to_string(),
                ])
                .await
                .unwrap();
            engine.pop().await.unwrap();
            let rs = engine
                .get(&Path::from_str("test->test:test"))
                .await
                .unwrap();
            assert_eq!(rs.len(), 1);
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
            let dm = Box::new(MemDataManager::new(None));

            // engine
            let mut engine = TempDataManager::new(dm);

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
            let dm = Box::new(MemDataManager::new(None));

            // engine
            let mut engine = TempDataManager::new(dm);

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

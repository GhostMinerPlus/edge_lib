use serde::{Deserialize, Serialize};
use std::{collections::HashSet, future::Future, io, pin::Pin, sync::Arc};

use crate::util::{
    data::{AsDataManager, PermissionPair},
    Path,
};

use super::data::{MemDataManager, TempDataManager};

mod dep {
    use std::io;

    use super::{AsEdgeEngine, Inc, ScriptTree, ScriptTree1};
    use crate::util::{data::AsDataManager, Path};

    pub fn gen_value() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    #[async_recursion::async_recursion]
    pub async fn inner_execute(
        engine: &mut impl AsEdgeEngine,
        input: &str,
        script_tree: &ScriptTree,
        out_tree: &mut json::JsonValue,
    ) -> io::Result<()> {
        engine
            .get_dm()
            .set(&Path::from_str("$->$:input"), vec![input.to_string()])
            .await?;
        let rs = engine
            .execute_script(
                &script_tree
                    .script
                    .split('\n')
                    .map(|line| line.to_string())
                    .collect::<Vec<String>>(),
            )
            .await?;
        if script_tree.next_v.is_empty() {
            let _ = out_tree.insert(&script_tree.name, rs);
            return Ok(());
        }
        let mut cur = json::object! {};
        for next_tree in &script_tree.next_v {
            // fork
            for input in &rs {
                let mut sub_out_tree = json::object! {};
                inner_execute(engine, input, next_tree, &mut sub_out_tree).await?;
                merge(&mut cur, &mut sub_out_tree);
            }
        }
        let _ = out_tree.insert(&script_tree.name, cur);
        Ok(())
    }

    #[async_recursion::async_recursion]
    pub async fn inner_execute1(
        engine: &mut impl AsEdgeEngine,
        input: &str,
        script_tree: &ScriptTree1,
        out_tree: &mut json::JsonValue,
    ) -> io::Result<()> {
        engine
            .get_dm()
            .set(&Path::from_str("$->input"), vec![input.to_string()])
            .await?;
        let rs = engine.execute_script(&script_tree.script).await?;
        if script_tree.next_v.is_empty() {
            let _ = out_tree.insert(&script_tree.name, rs);
            return Ok(());
        }
        let mut cur = json::object! {};
        for next_tree in &script_tree.next_v {
            // fork
            for input in &rs {
                let mut sub_out_tree = json::object! {};
                inner_execute1(engine, input, next_tree, &mut sub_out_tree).await?;
                merge(&mut cur, &mut sub_out_tree);
            }
        }
        let _ = out_tree.insert(&script_tree.name, cur);
        Ok(())
    }

    pub fn merge(p_tree: &mut json::JsonValue, s_tree: &mut json::JsonValue) {
        for (k, v) in s_tree.entries_mut() {
            if v.is_array() {
                if !p_tree.has_key(k) {
                    let _ = p_tree.insert(k, json::array![]);
                }
                let _ = p_tree[k].push(v.clone());
            } else {
                if !p_tree.has_key(k) {
                    let _ = p_tree.insert(k, json::object! {});
                }
                merge(&mut p_tree[k], v);
            }
        }
    }

    pub fn parse_script1(script: &[String]) -> io::Result<Vec<Inc>> {
        let mut inc_v = Vec::new();
        for line in script {
            if line.is_empty() {
                continue;
            }

            let word_v: Vec<&str> = line.split(' ').collect();
            if word_v.len() < 4 {
                return Err(io::Error::other(
                    "when parse_script:\n\tless than 4 words in a line",
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
                path.root_v[0] = gen_value();
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

mod main {
    use std::io;

    use super::{EdgeEngine, ScriptTree, ScriptTree1};

    /// 执行脚本树
    pub async fn execute1(
        this: &mut EdgeEngine,
        script_tree: &ScriptTree,
    ) -> io::Result<json::JsonValue> {
        let mut out_tree = json::object! {};
        super::dep::inner_execute(this, "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
    }

    #[cfg(test)]
    mod test_execute1 {
        use std::sync::Arc;

        use crate::util::{
            data::MemDataManager,
            engine::{main, AsEdgeEngine, EdgeEngine, ScriptTree},
        };

        #[test]
        fn test() {
            let task = async {
                let dm = Arc::new(MemDataManager::new(None));
                let mut engine = EdgeEngine::new(dm, "root").await;
                let rs = main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: [
                            "$->$:left = new 100 100",
                            "$->$:right = new 100 100",
                            "$->$:output = + $->$:left $->$:right",
                        ]
                        .join("\n"),
                        name: "root".to_string(),
                        next_v: vec![ScriptTree {
                            script: format!("$->$:output = rand $->$:input _"),
                            name: "then".to_string(),
                            next_v: vec![],
                        }],
                    },
                )
                .await
                .unwrap();
                engine.reset();
                let rs = &rs["root"]["then"];
                assert_eq!(rs.len(), 100);
                assert_eq!(rs[0].len(), 200);
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(task);
        }

        #[test]
        fn test_dc() {
            // env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("DEBUG"))
            //     .init();
            let task = async {
                let dm = Arc::new(MemDataManager::new(None));
                let mut engine = EdgeEngine::new(dm, "root").await;
                let rs = main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: [
                            "$->edge = ? _",
                            "$->point = ? _",
                            "$->point->width = 1 _",
                            "$->point->width append $->point->width 1",
                            "$->edge->point = $->point _",
                            "$->$:output = $->edge->point->width _",
                        ]
                        .join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                log::debug!("{rs}");
                assert_eq!(rs["result"].len(), 2);
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(task);
        }

        #[test]
        fn test_if() {
            let task = async {
                let dm = Arc::new(MemDataManager::new(None));
                let mut engine = EdgeEngine::new(dm, "root").await;
                let rs = main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: [
                            "$->$:server_exists = inner root->web_server huiwen<-name",
                            "$->$:web_server = if $->$:server_exists ?",
                            "$->$:output = = $->$:web_server _",
                        ]
                        .join("\n"),
                        name: "info".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                assert!(!rs["info"].is_empty());
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(task);
        }

        #[test]
        fn test_space() {
            let task = async {
                let dm = Arc::new(MemDataManager::new(None));
                let mut engine = EdgeEngine::new(dm, "root").await;
                let rs = main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: ["$->$:output = '1\\s' _"].join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                assert!(rs["result"][0].as_str() == Some("1 "));
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(task);
        }

        #[test]
        fn test_cache() {
            let task = async {
                let dm = Arc::new(MemDataManager::new(None));

                let mut engine = EdgeEngine::new(dm, "root").await;
                main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: ["root->name = edge _"].join("\n"),
                        name: "".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                engine.reset();

                let rs = main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: ["test->name = edge _", "$->$:output = edge<-name _"].join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                engine.reset();

                assert_eq!(rs["result"].len(), 2);
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(task);
        }

        #[test]
        fn test_set() {
            let task = async {
                let dm = Arc::new(MemDataManager::new(None));
                let mut engine = EdgeEngine::new(dm, "root").await;
                main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: [
                            "$->$:server_exists inner root->web_server {name}<-name",
                            "$->$:web_server if $->$:server_exists ?",
                            "$->$:web_server->name = {name} _",
                            "$->$:web_server->ip = {ip} _",
                            "$->$:web_server->port = {port} _",
                            "$->$:web_server->path = {path} _",
                            "$->$:web_server left $->$:web_server $->$:server_exists",
                            "root->web_server append root->web_server $->$:web_server",
                        ]
                        .join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                engine.reset();
                main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: [
                            "$->$:server_exists inner root->web_server {name}<-name",
                            "$->$:web_server if $->$:server_exists ?",
                            "$->$:web_server->name = {name} _",
                            "$->$:web_server->ip = {ip} _",
                            "$->$:web_server->port = {port} _",
                            "$->$:web_server->path = {path} _",
                            "$->$:web_server left $->$:web_server $->$:server_exists",
                            "root->web_server append root->web_server $->$:web_server",
                        ]
                        .join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                engine.reset();
                let rs = main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: ["$->$:output = root->web_server->ip _"].join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                assert_eq!(rs["result"].len(), 1);
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(task);
        }

        #[test]
        fn test_set_proxy() {
            let task = async {
                let dm = Arc::new(MemDataManager::new(None));
                let mut engine = EdgeEngine::new(dm, "root").await;
                main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: [
                            "$->$:proxy = ? _",
                            "$->$:proxy->name = editor _",
                            "$->$:proxy->path = /editor _",
                            "root->proxy append root->proxy $->$:proxy",
                        ]
                        .join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                engine.reset();
                let rs = main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: [
                            "$->$:proxy inner root->proxy editor<-name",
                            "$->$:proxy->path = /editor _",
                            "$->$:output = root->proxy->path _",
                        ]
                        .join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                assert_eq!(rs["result"].len(), 1);
                engine.reset();
                let rs = main::execute1(
                    &mut engine,
                    &ScriptTree {
                        script: ["$->$:output = root->proxy->path _"].join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                assert_eq!(rs["result"].len(), 1);
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(task);
        }
    }

    /// 执行脚本树
    pub async fn execute2(
        this: &mut EdgeEngine,
        script_tree: &ScriptTree1,
    ) -> io::Result<json::JsonValue> {
        let mut out_tree = json::object! {};
        super::dep::inner_execute1(this, "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
    }
}

pub trait AsEdgeEngine: Sync + Send {
    fn get_dm(&self) -> &TempDataManager;

    fn get_dm_mut(&mut self) -> &mut TempDataManager;

    fn reset(&mut self);

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
        self.get_dm().call(output, func, input, input1)
    }

    fn execute_script<'a, 'a1, 'f>(
        &'a mut self,
        script: &'a1 [String],
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        Self: Sized,
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
                let func_name_v = self.get_dm().get(&inc.function).await?;
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
                    let input_item_v = self.get_dm().get(&inc.input).await?;
                    let input1_item_v = self.get_dm().get(&inc.input1).await?;
                    let mem = Arc::new(MemDataManager::new(None));
                    mem.set(&Path::from_str("$->$:input"), input_item_v).await?;
                    mem.set(&Path::from_str("$->$:input1"), input1_item_v)
                        .await?;

                    let o_dm = self.get_dm_mut().temp.clone();
                    let rs = self.execute_script(&func_name_v).await;
                    self.get_dm_mut().temp = o_dm;
                    let rs = rs?;
                    self.get_dm().set(&inc.output, rs).await?;
                }
            }
            self.get_dm()
                .get(&Path::from_str(&format!("$->$:output")))
                .await
        })
    }

    fn dump<'a, 'b, 'c, 'f>(
        &'a self,
        addr: &'b Path,
        paper: &'c str,
    ) -> Pin<Box<dyn Future<Output = io::Result<json::JsonValue>> + 'f>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
    {
        Box::pin(async move {
            // root
            let root_v = self.get_dm().get(addr).await?;
            let mut rj = json::array![];
            for root in &root_v {
                rj.push(crate::util::dump(self.get_dm(), root, paper).await?)
                    .unwrap();
            }
            Ok(rj)
        })
    }

    fn load<'a, 'a1, 'a2, 'f>(
        &'a mut self,
        data: &'a1 json::JsonValue,
        addr: &'a2 Path,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f,
    {
        Box::pin(async move {
            if data.is_null() {
                return Ok(());
            }

            if data.is_array() {
                for item in data.members() {
                    self.load(item, addr).await?;
                }
                return Ok(());
            }

            if !data.is_object() {
                self.get_dm()
                    .append(addr, vec![data.as_str().unwrap().to_string()])
                    .await?;
                return Ok(());
            }

            self.get_dm().append(addr, vec![dep::gen_value()]).await?;

            for (k, v) in data.entries() {
                let sub_path = Path::from_str(&format!("{}->{k}", addr.to_string()));
                if v.is_array() {
                    for item in v.members() {
                        self.load(item, &sub_path).await?;
                    }
                } else {
                    self.load(v, &sub_path).await?;
                }
            }
            Ok(())
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

#[derive(Clone)]
pub struct EdgeEngine {
    dm: TempDataManager,
}

impl EdgeEngine {
    /// New edge engine
    /// # Parameters
    /// - dm: data manager in root
    /// - writer: writer
    pub async fn new(dm: Arc<dyn AsDataManager>, user: &str) -> Self {
        let temp_dm = if user == "root" {
            TempDataManager::new(dm)
        } else {
            let mut engine = EdgeEngine {
                dm: TempDataManager::new(dm.clone()),
            };
            // TODO: Maybe execute3(script: &str) -> JsonValue
            let rs = engine
                .execute2(&ScriptTree1 {
                    script: vec![
                        format!("$->$:output = ? _"),
                        format!("$->$:output->$:writer inner paper<-type {user}<-writer"),
                        format!("$->$:owner inner paper<-type {user}<-owner"),
                        format!("$->$:manager inner paper<-type {user}<-manager"),
                        format!("$->$:output->$:writer append $->$:output->$:writer $->$:owner"),
                        format!("$->$:output->$:writer append $->$:output->$:writer $->$:manager"),
                        format!("$->$:output->$:reader inner paper<-type {user}<-reader"),
                    ],
                    name: "rs".to_string(),
                    next_v: vec![
                        ScriptTree1 {
                            script: vec![format!("$->$:output = $->$:input->$:writer->name _")],
                            name: "writer".to_string(),
                            next_v: vec![],
                        },
                        ScriptTree1 {
                            script: vec![format!("$->$:output = $->$:input->$:reader->name _")],
                            name: "reader".to_string(),
                            next_v: vec![],
                        },
                    ],
                })
                .await
                .unwrap();

            let mut writer_set = rs["rs"]["writer"][0]
                .members()
                .into_iter()
                .map(|item| item.as_str().unwrap().to_string())
                .collect::<HashSet<String>>();
            writer_set.insert("$".to_string());

            let reader_set = rs["rs"]["reader"][0]
                .members()
                .into_iter()
                .map(|item| item.as_str().unwrap().to_string())
                .collect::<HashSet<String>>();
            TempDataManager::new(dm.divide(Some(PermissionPair {
                writer: writer_set,
                reader: reader_set,
            })))
        };
        EdgeEngine {
            dm: temp_dm.clone(),
        }
    }

    pub async fn execute1(&mut self, script_tree: &ScriptTree) -> io::Result<json::JsonValue> {
        main::execute1(self, script_tree).await
    }

    pub async fn execute2(&mut self, script_tree: &ScriptTree1) -> io::Result<json::JsonValue> {
        main::execute2(self, script_tree).await
    }
}

impl AsEdgeEngine for EdgeEngine {
    fn reset(&mut self) {
        self.dm.temp = Arc::new(MemDataManager::new(None));
    }

    fn get_dm(&self) -> &TempDataManager {
        &self.dm
    }

    fn get_dm_mut(&mut self) -> &mut TempDataManager {
        &mut self.dm
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::util::{
        data::{AsDataManager, MemDataManager},
        engine::AsEdgeEngine,
        Path,
    };

    use super::{EdgeEngine, ScriptTree1};

    #[test]
    fn test() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dm = Arc::new(MemDataManager::new(None));
            let mut engine = EdgeEngine::new(dm, "root").await;
            engine
                .execute2(&ScriptTree1 {
                    script: vec![
                        "$->$:temp append $->$:temp '$->$:output\\s+\\s1\\s1'".to_string(),
                        "test->test:test = $->$:temp _".to_string(),
                    ],
                    name: "rs".to_string(),
                    next_v: vec![],
                })
                .await
                .unwrap();
            engine.reset();
            let rs = engine
                .get_dm()
                .get(&Path::from_str("test->test:test"))
                .await
                .unwrap();
            assert_eq!(rs.len(), 1);
        });
    }

    #[test]
    fn test_rec() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dm = Arc::new(MemDataManager::new(None));
            let mut engine = EdgeEngine::new(dm, "root").await;

            let mut engine1 = engine.clone();
            rt.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                engine1
                    .execute2(&ScriptTree1 {
                        script: vec!["test->flag = 1 _".to_string()],
                        name: "rs".to_string(),
                        next_v: vec![],
                    })
                    .await
                    .unwrap();
                engine1.reset();
            });

            let handle = rt.spawn(async move {
                engine
                    .execute2(&ScriptTree1 {
                        script: vec!["_ while1 test->flag _".to_string()],
                        name: "rs".to_string(),
                        next_v: vec![],
                    })
                    .await
                    .unwrap();
            });
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            assert!(handle.is_finished());
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
            let dm = Arc::new(MemDataManager::new(None));

            // engine
            let mut engine = EdgeEngine::new(dm, "root").await;

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
            let dm = Arc::new(MemDataManager::new(None));

            // engine
            let mut engine = EdgeEngine::new(dm, "root").await;

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

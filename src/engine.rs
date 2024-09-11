use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::{io, sync::Arc};

use crate::data::PermissionPair;
use crate::util::Path;
use crate::{data::AsDataManager, func};

mod dep {
    use std::{collections::HashMap, io, sync::Mutex};

    use tokio::sync::RwLock;

    use super::{EdgeEngine, Inc, ScriptTree, ScriptTree1};
    use crate::{data::AsDataManager, func, util::Path};

    static mut ENGINE_FUNC_MAP_OP: Option<RwLock<HashMap<String, Box<dyn func::AsFunc>>>> = None;
    static mut ENGINE_FUNC_MAP_OP_LOCK: Mutex<()> = Mutex::new(());

    pub fn gen_value() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    #[async_recursion::async_recursion]
    pub async fn invoke_inc(engine: EdgeEngine, inc: &Inc) -> io::Result<()> {
        log::debug!("invoke_inc: {:?}", inc);
        let func_name_v = engine.dm.get(&inc.function).await?;
        if func_name_v.is_empty() {
            return Err(io::Error::other(format!(
                "no funtion: {}\nat invoke_inc",
                inc.function.to_string()
            )));
        }
        let func_mp = unsafe { ENGINE_FUNC_MAP_OP.as_ref().unwrap().read().await };
        match func_mp.get(&func_name_v[0]) {
            Some(func) => {
                func.invoke(
                    engine.dm.clone(),
                    inc.output.clone(),
                    inc.input.clone(),
                    inc.input1.clone(),
                )
                .await?;
            }
            None => {
                if func_name_v[0] == "func" {
                    // Return the names of all funtions.
                    let mut rs = Vec::with_capacity(func_mp.len());
                    for (name, _) in &*func_mp {
                        rs.push(name.clone());
                    }
                    engine.dm.set(&inc.output, rs).await?;
                } else if func_name_v[0] == "while1" {
                    engine.dm.while1(&inc.input).await?;
                } else if func_name_v[0] == "while0" {
                    engine.dm.while0(&inc.input).await?;
                } else {
                    let input_item_v = engine.dm.get(&inc.input).await?;
                    let input1_item_v = engine.dm.get(&inc.input1).await?;
                    let inc_v = parse_script1(&func_name_v)?;
                    let rs =
                        invoke_inc_v(engine.clone(), input_item_v, input1_item_v, inc_v).await?;
                    engine.dm.set(&inc.output, rs).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn set_func(name: &str, func_op: Option<Box<dyn func::AsFunc>>) {
        lazy_mp();
        let mut w_mp = unsafe { ENGINE_FUNC_MAP_OP.as_ref().unwrap().write() }.await;
        match func_op {
            Some(func) => w_mp.insert(name.to_string(), func),
            None => w_mp.remove(name),
        };
    }

    #[async_recursion::async_recursion]
    pub async fn inner_execute(
        engine: EdgeEngine,
        input: &str,
        script_tree: &ScriptTree,
        out_tree: &mut json::JsonValue,
    ) -> io::Result<()> {
        let inc_v = parse_script(&script_tree.script)?;
        let rs = invoke_inc_v(engine.clone(), vec![input.to_string()], vec![], inc_v).await?;
        if script_tree.next_v.is_empty() {
            let _ = out_tree.insert(&script_tree.name, rs);
            return Ok(());
        }
        let mut cur = json::object! {};
        for next_tree in &script_tree.next_v {
            // fork
            for input in &rs {
                let mut sub_out_tree = json::object! {};
                inner_execute(engine.clone(), input, next_tree, &mut sub_out_tree).await?;
                merge(&mut cur, &mut sub_out_tree);
            }
        }
        let _ = out_tree.insert(&script_tree.name, cur);
        Ok(())
    }

    #[async_recursion::async_recursion]
    pub async fn inner_execute1(
        engine: EdgeEngine,
        input: &str,
        script_tree: &ScriptTree1,
        out_tree: &mut json::JsonValue,
    ) -> io::Result<()> {
        let inc_v = parse_script1(&script_tree.script)?;
        let rs = invoke_inc_v(engine.clone(), vec![input.to_string()], vec![], inc_v).await?;
        if script_tree.next_v.is_empty() {
            let _ = out_tree.insert(&script_tree.name, rs);
            return Ok(());
        }
        let mut cur = json::object! {};
        for next_tree in &script_tree.next_v {
            // fork
            for input in &rs {
                let mut sub_out_tree = json::object! {};
                inner_execute1(engine.clone(), input, next_tree, &mut sub_out_tree).await?;
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

    pub fn parse_script(script: &str) -> io::Result<Vec<Inc>> {
        let mut inc_v = Vec::new();
        for line in script.lines() {
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
                        function: Path::from_str("append"),
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
                        function: Path::from_str("append"),
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

    pub fn invoke_inc_v(
        engine: EdgeEngine,
        input_item_v: Vec<String>,
        input1_item_v: Vec<String>,
        inc_v: Vec<Inc>,
    ) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
        async move {
            if inc_v.is_empty() {
                return Ok(vec![]);
            }
            let root = gen_value();
            let (engine, inc) = {
                let mut inc_v = inc_v;
                let mut last_inc = inc_v.pop().unwrap();
                let engine = engine.divide();
                engine
                    .dm
                    .append(&Path::from_str(&format!("{root}->$:input")), input_item_v)
                    .await?;
                engine
                    .dm
                    .append(&Path::from_str(&format!("{root}->$:input1")), input1_item_v)
                    .await?;
                log::debug!("inc_v.len(): {}", inc_v.len());
                for mut inc in inc_v {
                    unwrap_value(&root, &mut inc.output);
                    unwrap_value(&root, &mut inc.function);
                    unwrap_value(&root, &mut inc.input);
                    unwrap_value(&root, &mut inc.input1);
                    invoke_inc(engine.clone(), &inc).await?;
                }
                let engine1 = engine.divide();
                unwrap_value(&root, &mut last_inc.output);
                unwrap_value(&root, &mut last_inc.function);
                unwrap_value(&root, &mut last_inc.input);
                unwrap_value(&root, &mut last_inc.input1);
                if engine1.dm.get(&last_inc.output).await?.is_empty() {
                    engine1
                        .dm
                        .set(&last_inc.output, engine.dm.get(&last_inc.output).await?)
                        .await?;
                }
                if engine1.dm.get(&last_inc.function).await?.is_empty() {
                    engine1
                        .dm
                        .set(&last_inc.function, engine.dm.get(&last_inc.function).await?)
                        .await?;
                }
                if engine1.dm.get(&last_inc.input).await?.is_empty() {
                    engine1
                        .dm
                        .set(&last_inc.input, engine.dm.get(&last_inc.input).await?)
                        .await?;
                }
                if engine1.dm.get(&last_inc.input1).await?.is_empty() {
                    engine1
                        .dm
                        .set(&last_inc.input1, engine.dm.get(&last_inc.input1).await?)
                        .await?;
                }
                (engine1, last_inc)
            };
            invoke_inc(engine.clone(), &inc).await?;
            engine
                .dm
                .get(&Path::from_str(&format!("{root}->$:output")))
                .await
        }
    }

    pub fn lazy_mp() {
        let lk = unsafe { ENGINE_FUNC_MAP_OP_LOCK.lock().unwrap() };
        if unsafe { ENGINE_FUNC_MAP_OP.is_none() } {
            let mut func_mp: HashMap<String, Box<dyn func::AsFunc>> = HashMap::new();
            func_mp.insert("new".to_string(), Box::new(func::new));
            func_mp.insert("line".to_string(), Box::new(func::line));
            func_mp.insert("rand".to_string(), Box::new(func::rand));
            //
            func_mp.insert("append".to_string(), Box::new(func::append));
            func_mp.insert("distinct".to_string(), Box::new(func::distinct));
            func_mp.insert("left".to_string(), Box::new(func::left));
            func_mp.insert("inner".to_string(), Box::new(func::inner));
            func_mp.insert("if".to_string(), Box::new(func::if_));
            func_mp.insert("if0".to_string(), Box::new(func::if_0));
            func_mp.insert("if1".to_string(), Box::new(func::if_1));
            //
            func_mp.insert("+".to_string(), Box::new(func::add));
            func_mp.insert("-".to_string(), Box::new(func::minus));
            func_mp.insert("*".to_string(), Box::new(func::mul));
            func_mp.insert("/".to_string(), Box::new(func::div));
            func_mp.insert("%".to_string(), Box::new(func::rest));
            //
            func_mp.insert("==".to_string(), Box::new(func::equal));
            func_mp.insert("!=".to_string(), Box::new(func::not_equal));
            func_mp.insert(">".to_string(), Box::new(func::greater));
            func_mp.insert("<".to_string(), Box::new(func::smaller));
            //
            func_mp.insert("count".to_string(), Box::new(func::count));
            func_mp.insert("sum".to_string(), Box::new(func::sum));
            //
            func_mp.insert("=".to_string(), Box::new(func::set));
            //
            func_mp.insert("slice".to_string(), Box::new(func::slice));
            func_mp.insert("sort".to_string(), Box::new(func::sort));
            func_mp.insert("sort_s".to_string(), Box::new(func::sort_s));
            unsafe { ENGINE_FUNC_MAP_OP = Some(RwLock::new(func_mp)) };
        }
        drop(lk);
    }

    pub fn unwrap_value(root: &str, path: &mut Path) {
        if let Some(path_root) = &mut path.root_op {
            if path_root == "$" {
                *path_root = root.to_string();
            } else if path_root == "?" && path.step_v.is_empty() {
                *path_root = gen_value();
            }
        }
    }

    pub fn tree_2_entry(script_tree: &ScriptTree) -> json::JsonValue {
        let mut value = json::object! {};
        for next_item in &script_tree.next_v {
            let sub_script = format!("{}\n{}", next_item.script, next_item.name);
            let _ = value.insert(&sub_script, tree_2_entry(next_item));
        }
        value
    }
}

mod main {
    use std::{io, sync::Arc};

    use crate::{data::AsDataManager, func};

    use super::{temp, EdgeEngine, ScriptTree, ScriptTree1};

    pub fn new_engine(dm: Arc<dyn AsDataManager>) -> EdgeEngine {
        super::dep::lazy_mp();
        EdgeEngine {
            dm: Arc::new(temp::TempDataManager::new(dm)),
        }
    }

    /// 执行脚本树
    pub async fn execute1(
        this: &mut EdgeEngine,
        script_tree: &ScriptTree,
    ) -> io::Result<json::JsonValue> {
        let mut out_tree = json::object! {};
        super::dep::inner_execute(this.clone(), "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
    }

    #[cfg(test)]
    mod test_execute1 {
        use std::sync::Arc;

        use crate::{
            data::MemDataManager,
            engine::{main, EdgeEngine, ScriptTree},
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
                engine.reset().await.unwrap();
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
                let mut engine = EdgeEngine::new(dm.clone(), "root").await;
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
                        script: ["$->$:output = = '1\\s' _"].join("\n"),
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
                engine.reset().await.unwrap();

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
                engine.reset().await.unwrap();

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
                let mut engine = EdgeEngine::new(dm.clone(), "root").await;
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
                engine.reset().await.unwrap();
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
                engine.reset().await.unwrap();
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
                let mut engine = EdgeEngine::new(Arc::new(MemDataManager::new(None)), "root").await;
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
                engine.reset().await.unwrap();
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
                engine.reset().await.unwrap();
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
        super::dep::inner_execute1(this.clone(), "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
    }

    /// 配置函数
    pub async fn set_func(name: &str, func_op: Option<Box<dyn func::AsFunc>>) {
        super::dep::set_func(name, func_op).await
    }

    /// 参数转换
    pub fn tree_2_entry(script_tree: &ScriptTree) -> json::JsonValue {
        let script = format!("{}\n{}", script_tree.script, script_tree.name);
        let value = super::dep::tree_2_entry(script_tree);
        let mut json = json::object! {};
        let _ = json.insert(&script, value);
        json
    }
}

mod temp;

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
    dm: Arc<temp::TempDataManager>,
}

impl EdgeEngine {
    /// New edge engine
    /// # Parameters
    /// - dm: data manager in root
    /// - writer: writer
    pub async fn new(dm: Arc<dyn AsDataManager>, user: &str) -> Self {
        let dm = if user == "root" {
            dm
        } else {
            let mut engine = main::new_engine(dm.clone());
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
            dm.divide(Some(PermissionPair {
                writer: writer_set,
                reader: reader_set,
            }))
        };
        main::new_engine(dm)
    }

    pub fn get_gloabl(&self) -> Arc<dyn AsDataManager> {
        self.dm.get_global()
    }

    pub fn divide(&self) -> Self {
        Self {
            dm: Arc::new(temp::TempDataManager::new(self.dm.get_global())),
        }
    }

    pub async fn divide_with_user(&self, user: &str) -> Self {
        Self::new(self.dm.get_global(), user).await
    }

    pub fn entry_2_tree(script_str: &str, next_v_json: &json::JsonValue) -> ScriptTree {
        let mut next_v = Vec::with_capacity(next_v_json.len());
        for (sub_script_str, sub_next_v_json) in next_v_json.entries() {
            next_v.push(Self::entry_2_tree(sub_script_str, sub_next_v_json));
        }
        let (script, name) = match script_str.rfind('\n') {
            Some(pos) => (
                script_str[0..pos].to_string(),
                script_str[pos + 1..].to_string(),
            ),
            None => (script_str.to_string(), script_str.to_string()),
        };
        ScriptTree {
            script,
            name,
            next_v,
        }
    }

    pub fn tree_2_entry(script_tree: &ScriptTree) -> json::JsonValue {
        main::tree_2_entry(script_tree)
    }

    pub async fn set_func(name: &str, func_op: Option<Box<dyn func::AsFunc>>) {
        main::set_func(name, func_op).await
    }

    pub async fn execute(&mut self, script_tree: &json::JsonValue) -> io::Result<json::JsonValue> {
        let (script_str, next_v_json) = script_tree.entries().next().unwrap();
        let script_tree = Self::entry_2_tree(script_str, next_v_json);
        self.execute1(&script_tree).await
    }

    pub async fn execute1(&mut self, script_tree: &ScriptTree) -> io::Result<json::JsonValue> {
        main::execute1(self, script_tree).await
    }

    pub async fn execute2(&mut self, script_tree: &ScriptTree1) -> io::Result<json::JsonValue> {
        main::execute2(self, script_tree).await
    }

    /// Reset temp.
    pub async fn reset(&mut self) -> io::Result<()> {
        self.dm.reset().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{data::MemDataManager, util::Path};

    use super::{EdgeEngine, ScriptTree1};

    #[test]
    fn test() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut engine = EdgeEngine::new(Arc::new(MemDataManager::new(None)), "root").await;
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
            engine.reset().await.unwrap();
            let rs = engine
                .get_gloabl()
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
            let mut engine = EdgeEngine::new(Arc::new(MemDataManager::new(None)), "root").await;

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
                engine1.reset().await.unwrap();
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
}

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::{io, sync::Arc};

use crate::{data::AsDataManager, func, util::Path};

mod dep {
    use std::{
        collections::HashMap,
        io,
        sync::{Arc, Mutex},
    };

    use tokio::sync::RwLock;

    use super::{
        inc::{Inc, IncValue},
        main, ScriptTree, ScriptTree1,
    };
    use crate::{data::AsDataManager, func, util::Path};

    static mut EDGE_ENGINE_FUNC_MAP_OP: Option<RwLock<HashMap<String, Box<dyn func::AsFunc>>>> =
        None;
    static mut EDGE_ENGINE_FUNC_MAP_OP_LOCK: Mutex<()> = Mutex::new(());

    pub fn get_inc_v(
        dm: Arc<dyn AsDataManager>,
        function: &str,
    ) -> impl std::future::Future<Output = io::Result<Vec<Inc>>> + Send + '_ {
        async move {
            let output_v = dm
                .get(&Path::from_str(&format!("{function}->inc->output")))
                .await?;
            let function_v = dm
                .get(&Path::from_str(&format!("{function}->inc->function")))
                .await?;
            let input_v = dm
                .get(&Path::from_str(&format!("{function}->inc->input")))
                .await?;
            let input1_v = dm
                .get(&Path::from_str(&format!("{function}->inc->input1")))
                .await?;
            let mut inc_v = Vec::with_capacity(output_v.len());
            for i in 0..output_v.len() {
                inc_v.push(Inc {
                    output: IncValue::from_str(&output_v[i]),
                    function: IncValue::from_str(&function_v[i]),
                    input: IncValue::from_str(&input_v[i]),
                    input1: IncValue::from_str(&input1_v[i]),
                });
            }
            Ok(inc_v)
        }
    }

    pub fn gen_value() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    pub async fn get_value_v(dm: Arc<dyn AsDataManager>, iv: &IncValue) -> io::Result<Vec<String>> {
        match iv {
            IncValue::Addr(addr) => dm.get(&Path::from_str(addr)).await,
            IncValue::Definition(definition) => main::sync_definition(dm, definition).await,
            IncValue::Value(name) => Ok(vec![name.clone()]),
        }
    }

    pub async fn get_path_anyway(
        dm: Arc<dyn AsDataManager>,
        iv: &IncValue,
    ) -> io::Result<Path> {
        match iv {
            IncValue::Addr(addr) => Ok(Path::from_str(addr)),
            IncValue::Definition(definition) => {
                let path = Path::from_str(&format!("{definition}->cache"));
                if dm.get(&path).await?.is_empty() {
                    let rs = main::sync_definition(dm.clone(), definition).await?;
                    dm.set(&path, rs).await?;
                }
                Ok(path)
            }
            IncValue::Value(value) => Ok(Path {
                root: value.clone(),
                step_v: vec![],
            }),
        }
    }

    #[async_recursion::async_recursion]
    pub async fn invoke_inc(dm: Arc<dyn AsDataManager>, inc: &Inc) -> io::Result<()> {
        log::debug!("invoke_inc: {:?}", inc);
        let output = Path::from_str(inc.output.as_str());
        if output.step_v.is_empty() {
            return Ok(());
        }
        let func_name_v = get_value_v(dm.clone(), &inc.function).await?;
        if func_name_v.is_empty() {
            return Err(io::Error::other(format!(
                "no funtion: {}\nat invoke_inc",
                inc.function.as_str()
            )));
        }
        let func_mp = unsafe { EDGE_ENGINE_FUNC_MAP_OP.as_ref().unwrap().read().await };
        match func_mp.get(&func_name_v[0]) {
            Some(func) => {
                let input = get_path_anyway(dm.clone(), &inc.input).await?;
                let input1 = get_path_anyway(dm.clone(), &inc.input1).await?;
                func.invoke(dm.clone(), output.clone(), input, input1)
                    .await?;
            }
            None => {
                if func_name_v[0] == "func" {
                    // Return the names of all funtions.
                    let mut rs = Vec::with_capacity(func_mp.len());
                    for (name, _) in &*func_mp {
                        rs.push(name.clone());
                    }
                    dm.set(&output, rs).await?;
                } else {
                    let input = get_path_anyway(dm.clone(), &inc.input).await?;
                    let input1 = get_path_anyway(dm.clone(), &inc.input1).await?;
                    let input_item_v = dm.get(&input).await?;
                    let input1_item_v = dm.get(&input1).await?;
                    let inc_v = get_inc_v(dm.clone(), &func_name_v[0]).await?;
                    let new_root = gen_value();
                    dm.set(
                        &Path::from_str(&format!("{new_root}->$:input")),
                        input_item_v,
                    )
                    .await?;
                    dm.set(
                        &Path::from_str(&format!("{new_root}->$:input1")),
                        input1_item_v,
                    )
                    .await?;
                    log::debug!("inc_v.len(): {}", inc_v.len());
                    invoke_inc_v(dm.clone(), &new_root, &inc_v).await?;
                    let rs = dm
                        .get(&Path::from_str(&format!("{new_root}->$:output")))
                        .await?;
                    dm.set(&output, rs).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn set_func(name: &str, func_op: Option<Box<dyn func::AsFunc>>) {
        lazy_mp();
        let mut w_mp = unsafe { EDGE_ENGINE_FUNC_MAP_OP.as_ref().unwrap().write() }.await;
        match func_op {
            Some(func) => w_mp.insert(name.to_string(), func),
            None => w_mp.remove(name),
        };
    }

    #[async_recursion::async_recursion]
    pub async fn inner_execute(
        dm: Arc<dyn AsDataManager>,
        input: &str,
        script_tree: &ScriptTree,
        out_tree: &mut json::JsonValue,
    ) -> io::Result<()> {
        let root = gen_value();
        dm.append(
            &Path::from_str(&format!("{root}->$:input")),
            vec![input.to_string()],
        )
        .await?;
        let inc_v = parse_script(&script_tree.script)?;
        let rs = invoke_inc_v(dm.clone(), &root, &inc_v).await?;
        if script_tree.next_v.is_empty() {
            let _ = out_tree.insert(&script_tree.name, rs);
            return Ok(());
        }
        let mut cur = json::object! {};
        for next_tree in &script_tree.next_v {
            // fork
            for input in &rs {
                let mut sub_out_tree = json::object! {};
                inner_execute(dm.clone(), input, next_tree, &mut sub_out_tree).await?;
                merge(&mut cur, &mut sub_out_tree);
            }
        }
        let _ = out_tree.insert(&script_tree.name, cur);
        Ok(())
    }

    #[async_recursion::async_recursion]
    pub async fn inner_execute1(
        dm: Arc<dyn AsDataManager>,
        input: &str,
        script_tree: &ScriptTree1,
        out_tree: &mut json::JsonValue,
    ) -> io::Result<()> {
        let root = gen_value();
        dm.append(
            &Path::from_str(&format!("{root}->$:input")),
            vec![input.to_string()],
        )
        .await?;
        let inc_v = parse_script1(&script_tree.script)?;
        let rs = invoke_inc_v(dm.clone(), &root, &inc_v).await?;
        if script_tree.next_v.is_empty() {
            let _ = out_tree.insert(&script_tree.name, rs);
            return Ok(());
        }
        let mut cur = json::object! {};
        for next_tree in &script_tree.next_v {
            // fork
            for input in &rs {
                let mut sub_out_tree = json::object! {};
                inner_execute(dm.clone(), input, next_tree, &mut sub_out_tree).await?;
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
                        output: IncValue::from_str(word_v[0].trim()),
                        function: IncValue::from_str(word_v[2].trim()),
                        input: IncValue::from_str(word_v[3].trim()),
                        input1: IncValue::from_str(word_v[4].trim()),
                    });
                } else if word_v[1] == "+=" {
                    inc_v.push(Inc {
                        output: IncValue::from_str("$->$:temp"),
                        function: IncValue::from_str(word_v[2].trim()),
                        input: IncValue::from_str(word_v[3].trim()),
                        input1: IncValue::from_str(word_v[4].trim()),
                    });
                    inc_v.push(Inc {
                        output: IncValue::from_str(word_v[0].trim()),
                        function: IncValue::from_str("append"),
                        input: IncValue::from_str(word_v[0].trim()),
                        input1: IncValue::from_str("$->$:temp"),
                    });
                } else {
                    return Err(io::Error::other("when parse_script:\n\tunknown operator"));
                }
                continue;
            }
            inc_v.push(Inc {
                output: IncValue::from_str(word_v[0].trim()),
                function: IncValue::from_str(word_v[1].trim()),
                input: IncValue::from_str(word_v[2].trim()),
                input1: IncValue::from_str(word_v[3].trim()),
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
                        output: IncValue::from_str(word_v[0].trim()),
                        function: IncValue::from_str(word_v[2].trim()),
                        input: IncValue::from_str(word_v[3].trim()),
                        input1: IncValue::from_str(word_v[4].trim()),
                    });
                } else if word_v[1] == "+=" {
                    inc_v.push(Inc {
                        output: IncValue::from_str("$->$:temp"),
                        function: IncValue::from_str(word_v[2].trim()),
                        input: IncValue::from_str(word_v[3].trim()),
                        input1: IncValue::from_str(word_v[4].trim()),
                    });
                    inc_v.push(Inc {
                        output: IncValue::from_str(word_v[0].trim()),
                        function: IncValue::from_str("append"),
                        input: IncValue::from_str(word_v[0].trim()),
                        input1: IncValue::from_str("$->$:temp"),
                    });
                } else {
                    return Err(io::Error::other("when parse_script:\n\tunknown operator"));
                }
                continue;
            }
            inc_v.push(Inc {
                output: IncValue::from_str(word_v[0].trim()),
                function: IncValue::from_str(word_v[1].trim()),
                input: IncValue::from_str(word_v[2].trim()),
                input1: IncValue::from_str(word_v[3].trim()),
            });
        }
        Ok(inc_v)
    }

    pub fn invoke_inc_v<'a>(
        dm: Arc<dyn AsDataManager>,
        root: &'a str,
        inc_v: &'a Vec<Inc>,
    ) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send + 'a {
        async move {
            log::debug!("inc_v.len(): {}", inc_v.len());
            for inc in inc_v {
                let inc = Inc {
                    output: unwrap_value(root, inc.output.clone()),
                    function: unwrap_value(root, inc.function.clone()),
                    input: unwrap_value(root, inc.input.clone()),
                    input1: unwrap_value(root, inc.input1.clone()),
                };
                invoke_inc(dm.clone(), &inc).await?;
            }
            dm.get(&Path::from_str(&format!("{root}->$:output"))).await
        }
    }

    pub fn lazy_mp() {
        let lk = unsafe { EDGE_ENGINE_FUNC_MAP_OP_LOCK.lock().unwrap() };
        if unsafe { EDGE_ENGINE_FUNC_MAP_OP.is_none() } {
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
            unsafe { EDGE_ENGINE_FUNC_MAP_OP = Some(RwLock::new(func_mp)) };
        }
        drop(lk);
    }

    pub fn unwrap_value(root: &str, iv: IncValue) -> IncValue {
        match &iv {
            IncValue::Addr(addr) => {
                if addr.starts_with("$->") {
                    IncValue::Addr(format!("{root}{}", &addr[1..]))
                } else {
                    iv
                }
            }
            IncValue::Value(value) => {
                if value == "_" {
                    return IncValue::Value(String::new());
                } else if value == "?" {
                    return IncValue::Value(gen_value());
                } else if value == "$?" {
                    return IncValue::Value(format!("${}", gen_value()));
                } else {
                    iv
                }
            }
            _ => iv,
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

    use crate::{data::AsDataManager, func, util::Path};

    use super::{temp, EdgeEngine, ScriptTree, ScriptTree1};

    pub fn new_edge_engine(dm: Arc<dyn AsDataManager>) -> EdgeEngine {
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
        super::dep::inner_execute(this.dm.clone(), "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
    }

    #[cfg(test)]
    mod test_execute1 {
        use std::sync::Arc;

        use crate::{
            data::{CacheDataManager, MemDataManager},
            engine::{main, EdgeEngine, ScriptTree},
        };

        #[test]
        fn test() {
            let task = async {
                let dm = Arc::new(CacheDataManager::new(Arc::new(MemDataManager::new(None))));
                let mut edge_engine = EdgeEngine::new(dm, "root").await;
                let rs = main::execute1(
                    &mut edge_engine,
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
                edge_engine.commit().await.unwrap();
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
                let dm = Arc::new(CacheDataManager::new(Arc::new(MemDataManager::new(None))));
                let mut edge_engine = EdgeEngine::new(dm.clone(), "root").await;
                let rs = main::execute1(
                    &mut edge_engine,
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
                let dm = Arc::new(CacheDataManager::new(Arc::new(MemDataManager::new(None))));
                let mut edge_engine = EdgeEngine::new(dm, "root").await;
                let rs = main::execute1(
                    &mut edge_engine,
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
                let dm = Arc::new(CacheDataManager::new(Arc::new(MemDataManager::new(None))));
                let mut edge_engine = EdgeEngine::new(dm, "root").await;
                let rs = main::execute1(
                    &mut edge_engine,
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
                let global = Arc::new(MemDataManager::new(None));
                let dm = Arc::new(CacheDataManager::new(global));

                let mut edge_engine = EdgeEngine::new(dm, "root").await;
                main::execute1(
                    &mut edge_engine,
                    &ScriptTree {
                        script: ["root->name = edge _"].join("\n"),
                        name: "".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                edge_engine.commit().await.unwrap();

                let rs = main::execute1(
                    &mut edge_engine,
                    &ScriptTree {
                        script: ["test->name = edge _", "$->$:output = edge<-name _"].join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    },
                )
                .await
                .unwrap();
                edge_engine.commit().await.unwrap();

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
                let global = Arc::new(MemDataManager::new(None));
                let dm = Arc::new(CacheDataManager::new(global));
                let mut edge_engine = EdgeEngine::new(dm.clone(), "root").await;
                main::execute1(
                    &mut edge_engine,
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
                edge_engine.commit().await.unwrap();
                main::execute1(
                    &mut edge_engine,
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
                edge_engine.commit().await.unwrap();
                let rs = main::execute1(
                    &mut edge_engine,
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
                let global = Arc::new(MemDataManager::new(None));
                let dm = Arc::new(CacheDataManager::new(global));
                let mut edge_engine = EdgeEngine::new(dm.clone(), "root").await;
                main::execute1(
                    &mut edge_engine,
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
                edge_engine.commit().await.unwrap();
                let rs = main::execute1(
                    &mut edge_engine,
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
                edge_engine.commit().await.unwrap();
                let rs = main::execute1(
                    &mut edge_engine,
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
        super::dep::inner_execute1(this.dm.clone(), "", &script_tree, &mut out_tree).await?;
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

    /// From definition synchronization values
    pub async fn sync_definition(
        dm: Arc<dyn AsDataManager>,
        definition: &str,
    ) -> io::Result<Vec<String>> {
        if definition.starts_with("$$") {
            return Ok(vec![definition[1..].to_string()]);
        }
        let inc_v = super::dep::get_inc_v(dm.clone(), &format!("{definition}->function")).await?;
        let new_root = super::dep::gen_value();
        log::debug!("inc_v.len(): {}", inc_v.len());
        dm.set(
            &Path::from_str(&format!("{new_root}->$:input")),
            vec![definition.to_string()],
        )
        .await?;
        super::dep::invoke_inc_v(dm.clone(), &new_root, &inc_v).await?;
        dm.get(&Path::from_str(&format!("{new_root}->$:output")))
            .await
    }
}

mod temp;

pub mod inc;

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
    pub next_v: Vec<ScriptTree>,
}

pub struct EdgeEngine {
    dm: Arc<temp::TempDataManager>,
}

impl EdgeEngine {
    /// New edge engine
    /// # Parameters
    /// - dm: data manager in root
    /// - writer: writer
    pub async fn new(dm: Arc<dyn AsDataManager>, writer: &str) -> Self {
        let dm = if writer == "root" {
            dm
        } else {
            let paper_v = dm
                .get(&Path::from_str(&format!("{writer}->paper->name")))
                .await
                .unwrap();
            let mut paper_set = paper_v.into_iter().collect::<HashSet<String>>();
            paper_set.insert("$".to_string());
            paper_set.insert("".to_string());
            dm.divide(Some(paper_set))
        };
        main::new_edge_engine(dm)
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

    pub async fn commit(&mut self) -> io::Result<()> {
        self.dm.commit().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::data::MemDataManager;

    use super::EdgeEngine;

    #[test]
    fn test() {
        let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        rt.block_on(async move {
            let dm = Arc::new(MemDataManager::new(None));
            let mut edge_engine = EdgeEngine::new(dm, "root").await;
            let rs = edge_engine
                .execute2(&super::ScriptTree1 {
                    script: vec![
                        "$->$:inc = ? _".to_string(),
                        "$->$:inc->output = '$->$:output' _".to_string(),
                        "$->$:inc->function = + _".to_string(),
                        "$->$:inc->input = 1 _".to_string(),
                        "$->$:inc->input1 = 1 _".to_string(),
                        "$test->function = ? _".to_string(),
                        "$test->function->inc = $->$:inc _".to_string(),
                        "$->$:output = $test _".to_string(),
                    ],
                    name: "rs".to_string(),
                    next_v: vec![],
                })
                .await
                .unwrap();
            assert_eq!(rs["rs"][0], "2")
        });
    }
}

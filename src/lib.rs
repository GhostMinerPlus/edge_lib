mod inc;

use serde::{Deserialize, Serialize};
use std::io;

use crate::data::AsDataManager;

// Public
pub mod data;
pub mod err;
pub mod mem_table;
pub mod util;

#[derive(Clone, Deserialize, Debug)]
pub struct Inc {
    pub output: String,
    pub operator: String,
    pub function: String,
    pub input: String,
    pub input1: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScriptTree {
    pub script: String,
    pub name: String,
    pub next_v: Vec<ScriptTree>,
}

pub trait AsEdgeEngine {
    /// Deprecated
    fn execute(
        &mut self,
        _: &json::JsonValue,
    ) -> impl std::future::Future<Output = io::Result<json::JsonValue>> + Send {
        async { todo!() }
    }

    fn execute1(
        &mut self,
        script_tree: &ScriptTree,
    ) -> impl std::future::Future<Output = io::Result<json::JsonValue>> + Send;

    fn commit(&mut self) -> impl std::future::Future<Output = io::Result<()>> + Send;

    fn decompose(
        &mut self,
        target_script: &str,
    ) -> impl std::future::Future<Output = err::Result<ScriptTree>> + Send {
        async { todo!() }
    }
}

pub struct EdgeEngine {
    dm: Box<dyn AsDataManager>,
}

impl EdgeEngine {
    pub fn new(dm: Box<dyn AsDataManager>) -> Self {
        Self { dm }
    }

    fn is_mutable(path: &str) -> bool {
        path.contains("->") || path.contains("<-")
    }

    async fn decompose_inc(&mut self, target_inc: &Inc) -> err::Result<ScriptTree> {
        let flag_v = [
            Self::is_mutable(&target_inc.input),
            Self::is_mutable(&target_inc.input1),
        ];
        return Err(err::Error::Question(format!(
            "how to decompose '{} {} {}'?",
            target_inc.function, flag_v[0], flag_v[1]
        )));
    }
}

impl AsEdgeEngine for EdgeEngine {
    async fn execute(&mut self, script_tree: &json::JsonValue) -> io::Result<json::JsonValue> {
        let (script_str, next_v_json) = script_tree.entries().next().unwrap();
        let script_tree = util::entry_2_tree(script_str, next_v_json);
        self.execute1(&script_tree).await
    }

    async fn execute1(&mut self, script_tree: &ScriptTree) -> io::Result<json::JsonValue> {
        let mut out_tree = json::object! {};
        util::execute(&mut self.dm, "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
    }

    async fn commit(&mut self) -> io::Result<()> {
        self.dm.commit().await
    }

    async fn decompose(&mut self, target_script: &str) -> err::Result<ScriptTree> {
        let target_inc_v = util::parse_script(target_script).map_err(err::map_io_err)?;
        if target_inc_v.len() == 1 {
            return self.decompose_inc(&target_inc_v[0]).await;
        }
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{data::DataManager, ScriptTree};

    use super::{AsEdgeEngine, EdgeEngine};

    #[test]
    fn test() {
        let task = async {
            let dm = DataManager::new();
            let mut edge_engine = EdgeEngine::new(Box::new(dm));
            let rs = edge_engine
                .execute1(&ScriptTree {
                    script: [
                        "$->$left = new 100 100",
                        "$->$right = new 100 100",
                        "$->$output = + $->$left $->$right",
                    ]
                    .join("\n"),
                    name: "root".to_string(),
                    next_v: vec![ScriptTree {
                        script: format!("$->$output = rand $->$input _"),
                        name: "then".to_string(),
                        next_v: vec![],
                    }],
                })
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
    fn test_if() {
        let task = async {
            let dm = DataManager::new();
            let mut edge_engine = EdgeEngine::new(Box::new(dm));
            let rs = edge_engine
                .execute1(&ScriptTree {
                    script: [
                        "$->$server_exists = inner root->web_server huiwen<-name",
                        "$->$web_server = if $->$server_exists ?",
                        "$->$output = = $->$web_server _",
                    ]
                    .join("\n"),
                    name: "info".to_string(),
                    next_v: vec![],
                })
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
            let dm = DataManager::new();
            let mut edge_engine = EdgeEngine::new(Box::new(dm));
            let rs = edge_engine
                .execute1(&ScriptTree {
                    script: ["$->$output = + '1 ' 1"].join("\n"),
                    name: "result".to_string(),
                    next_v: vec![],
                })
                .await
                .unwrap();
            assert!(rs["result"][0].as_str() == Some("2"));
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
            let dm = DataManager::new();

            let mut edge_engine = EdgeEngine::new(Box::new(dm));
            edge_engine
                .execute1(&ScriptTree {
                    script: ["root->name = = edge _"].join("\n"),
                    name: "".to_string(),
                    next_v: vec![],
                })
                .await
                .unwrap();
            edge_engine.commit().await.unwrap();

            let rs = edge_engine
                .execute1(&ScriptTree {
                    script: ["test->name = = edge _", "$->$output = = edge<-name _"].join("\n"),
                    name: "result".to_string(),
                    next_v: vec![],
                })
                .await
                .unwrap();
            edge_engine.commit().await.unwrap();

            assert!(rs["result"].len() == 2);
        };
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(task);
    }
}

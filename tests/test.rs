use std::sync::Arc;

use edge_lib::{
    data::{AsDataManager, Auth, MemDataManager, RecDataManager},
    util::Path,
    EdgeEngine, ScriptTree,
};

#[test]
fn test_listener() {
    let task = async {
        let dm = RecDataManager::new(Arc::new(MemDataManager::new()));

        let mut edge_engine = EdgeEngine::new(dm.divide(Auth {
            uid: "root".to_string(),
            gid: "root".to_string(),
            gid_v: Vec::new(),
        }));
        edge_engine
            .execute1(&ScriptTree {
                script: [
                    "name->listener = = ? _",
                    "name->listener->target = = 'root->name_cnt' _",
                    "name->listener->inc = = ? _",
                    "name->listener->inc->output = = '$->$output' _",
                    "name->listener->inc->operator = = '=' _",
                    "name->listener->inc->function = = 'count' _",
                    "name->listener->inc->input = = 'test<-name' _",
                    "name->listener->inc->input1 = = '_' _",
                ]
                .join("\n"),
                name: "".to_string(),
                next_v: vec![],
            })
            .await
            .unwrap();
        edge_engine.commit().await.unwrap();

        let listener_v = dm.get(&Path::from_str("name->listener")).await.unwrap();
        let target_v = dm
            .get(&Path::from_str(&format!("{}->target", listener_v[0])))
            .await
            .unwrap();
        assert_eq!(target_v[0], "'root->name_cnt'");

        let rs = edge_engine
            .execute1(&ScriptTree {
                script: ["test->name = = test _", "$->$output = = root->name_cnt _"].join("\n"),
                name: "result".to_string(),
                next_v: vec![],
            })
            .await
            .unwrap();
        edge_engine.commit().await.unwrap();

        assert_eq!(rs["result"][0].as_str().unwrap(), "1");
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(task);
}

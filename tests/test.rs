use edge_lib::{data::{AsDataManager, DataManager}, AsEdgeEngine, EdgeEngine, ScriptTree};

#[test]
fn test_cache() {
    let task = async {
        let mut dm = DataManager::new();

        let mut edge_engine = EdgeEngine::new(dm.divide());
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

        let listener_v = dm.get_target_v("name", "listener").await.unwrap();
        let target_v = dm.get_target_v(&listener_v[0], "target").await.unwrap();
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

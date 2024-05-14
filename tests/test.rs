use edge_lib::{data::DataManager, AsEdgeEngine, EdgeEngine};

#[test]
fn test_decompose() {
    let task = async {
        let dm = DataManager::new();
        let mut edge_engine = EdgeEngine::new(Box::new(dm));
        match edge_engine.decompose("$->$output = == a->b c->d").await {
            Ok(_) => todo!(),
            Err(e) => match e {
                edge_lib::err::Error::Other(_) => todo!(),
                edge_lib::err::Error::Question(question) => {
                    assert_eq!(question, "how to decompose '== true true'?");
                }
            }
        }
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(task);
}

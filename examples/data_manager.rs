use edge_lib::{
    data::{AsDataManager, MemDataManager},
    util::Path,
};

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dm = MemDataManager::new(None);
        dm.set(
            &Path::from_str("root->name"),
            vec!["data_manager".to_string()],
        )
        .await
        .unwrap();
        dm.commit().await.unwrap();
        let name_v = dm.get(&Path::from_str("root->name")).await.unwrap();
        assert_eq!(name_v.len(), 1);
        assert_eq!(name_v[0], "data_manager");
    });
}

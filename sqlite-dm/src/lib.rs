use sqlx::{Pool, Sqlite};
use std::{future, io, pin::Pin, sync::Arc};

use edge_lib::{
    data::{AsDataManager, Auth},
    util::Path,
};

mod dao;

const INIT_SQL: &str = "CREATE TABLE IF NOT EXISTS edge_t (
    id integer PRIMARY KEY,
    source varchar(500),
    paper varchar(100),
    code varchar(100),
    target varchar(500)
);
CREATE INDEX IF NOT EXISTS edge_t_source_paper_code ON edge_t (source, paper, code);
CREATE INDEX IF NOT EXISTS edge_t_target_paper_code ON edge_t (target, paper, code);";

#[derive(Clone)]
pub struct SqliteDataManager {
    pool: Pool<Sqlite>,
    auth: Auth,
}

impl SqliteDataManager {
    pub fn new(pool: Pool<Sqlite>, auth: Auth) -> Self {
        Self { pool, auth }
    }

    pub async fn init(&self) {
        sqlx::query(INIT_SQL).execute(&self.pool).await.unwrap();
    }
}

impl AsDataManager for SqliteDataManager {
    fn get_auth(&self) -> &Auth {
        &self.auth
    }

    fn divide(&self, auth: Auth) -> Arc<dyn AsDataManager> {
        Arc::new(Self {
            auth,
            pool: self.pool.clone(),
        })
    }

    fn append<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            if let Some(auth) = &self.auth {
                if !auth.writer.contains(&step.paper) {
                    return Err(io::Error::other("permision denied"));
                }
            }
            let root_v = self.get(&path).await?;
            for source in &root_v {
                dao::insert_edge(self.pool.clone(), source, &step.paper, &step.code, &item_v)
                    .await?;
            }
            Ok(())
        })
    }

    fn set<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        let mut path = path.clone();
        Box::pin(async move {
            let step = path.step_v.pop().unwrap();
            if let Some(auth) = &self.auth {
                if !auth.writer.contains(&step.paper) {
                    return Err(io::Error::other("permision denied"));
                }
            }
            let root_v = self.get(&path).await?;
            for source in &root_v {
                dao::delete_edge_with_source_code(
                    self.pool.clone(),
                    &step.paper,
                    source,
                    &step.code,
                )
                .await?;
            }
            for source in &root_v {
                dao::insert_edge(self.pool.clone(), source, &step.paper, &step.code, &item_v)
                    .await?;
            }
            Ok(())
        })
    }

    fn get<'a, 'a1, 'f>(
        &'a self,
        path: &'a1 Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            if let Some(root) = &path.root_op {
                return Box::pin(future::ready(Ok(vec![root.clone()])));
            } else {
                return Box::pin(future::ready(Ok(vec![])));
            }
        }
        let path = path.clone();
        Box::pin(async move {
            if let Some(auth) = &self.auth {
                for step in &path.step_v {
                    if !auth.writer.contains(&step.paper) && !auth.reader.contains(&step.paper) {
                        return Err(io::Error::other("permision denied"));
                    }
                }
            }
            dao::get(self.pool.clone(), &path).await
        })
    }

    fn clear<'a, 'f>(
        &'a self,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'f>>
    where
        'a: 'f,
    {
        Box::pin(async move {
            match &self.auth {
                Some(auth) => {
                    for paper in &auth.writer {
                        let _ = dao::clear_paper(self.pool.clone(), paper).await;
                    }
                    Ok(())
                }
                None => dao::clear(self.pool.clone()).await,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use edge_lib::{
        data::TempDataManager,
        engine::{EdgeEngine, ScriptTree1},
    };
    use sqlx::sqlite::SqliteConnectOptions;

    use super::*;

    #[test]
    fn test_root_type() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool =
                sqlx::SqlitePool::connect_with(SqliteConnectOptions::new().filename("test.db"))
                    .await
                    .unwrap();
            let dm = Arc::new(SqliteDataManager::new(pool, None));
            dm.init().await;
            let mut engine = EdgeEngine::new(Arc::new(TempDataManager::new(dm)), "root").await;
            engine
                .execute2(&ScriptTree1 {
                    script: vec!["root->type = user _".to_string()],
                    name: "rs".to_string(),
                    next_v: vec![],
                })
                .await
                .unwrap();
            engine.reset();

            let rs = engine
                .get_dm()
                .get(&Path::from_str("root->type"))
                .await
                .unwrap();
            assert_eq!(rs[0], "user")
        })
    }
}

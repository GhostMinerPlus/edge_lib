use sqlx::{Pool, Sqlite};
use std::{future, pin::Pin};

use edge_lib::{
    err,
    util::{
        data::{AsDataManager, Auth, Fu},
        Path,
    },
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

    fn append<'a, 'a1, 'f>(
        &'a mut self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn Fu<Output = err::Result<()>> + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        Box::pin(async move {
            let mut path = path.clone();
            let step = path.step_v.pop().unwrap();
            if let Some(auth) = &self.auth {
                if !auth.writer.contains(&step.paper) {
                    return Err(err::Error::new(
                        err::ErrorKind::PermissionDenied,
                        format!("{}", step.paper),
                    ));
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
        &'a mut self,
        path: &'a1 Path,
        item_v: Vec<String>,
    ) -> Pin<Box<dyn Fu<Output = err::Result<()>> + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(())));
        }
        Box::pin(async move {
            let mut path = path.clone();
            let step = path.step_v.pop().unwrap();
            if let Some(auth) = &self.auth {
                if !auth.writer.contains(&step.paper) {
                    return Err(err::Error::new(
                        err::ErrorKind::PermissionDenied,
                        format!("{}", step.paper),
                    ));
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
    ) -> Pin<Box<dyn Fu<Output = err::Result<Vec<String>>> + 'f>>
    where
        'a: 'f,
        'a1: 'f,
    {
        if path.step_v.is_empty() {
            return Box::pin(future::ready(Ok(path.root_v.clone())));
        }
        let path = path.clone();
        Box::pin(async move {
            if let Some(auth) = &self.auth {
                for step in &path.step_v {
                    if !auth.writer.contains(&step.paper) && !auth.reader.contains(&step.paper) {
                        return Err(err::Error::new(
                            err::ErrorKind::PermissionDenied,
                            format!("{}", step.paper),
                        ));
                    }
                }
            }
            dao::get(self.pool.clone(), &path).await
        })
    }

    fn get_code_v<'a, 'a1, 'a2, 'f>(
        &'a self,
        root: &'a1 str,
        space: &'a2 str,
    ) -> Pin<Box<dyn Fu<Output = err::Result<Vec<String>>> + 'f>>
    where
        'a: 'f,
        'a1: 'f,
        'a2: 'f,
    {
        Box::pin(async move { dao::get_code_v(self.pool.clone(), root, space).await })
    }
}

#[cfg(test)]
mod tests {
    use edge_lib::util::{
        data::AsDataManager,
        engine::{AsEdgeEngine, EdgeEngine},
        Path,
    };
    use sqlx::sqlite::SqliteConnectOptions;

    use crate::SqliteDataManager;

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
            let mut global = SqliteDataManager::new(pool, None);
            global.init().await;
            let mut dm = EdgeEngine::new(&mut global);
            dm.execute_script(&vec!["root->type = user _".to_string()])
                .await
                .unwrap();

            let rs = dm.get(&Path::from_str("root->type")).await.unwrap();
            assert_eq!(rs[0], "user")
        })
    }
}

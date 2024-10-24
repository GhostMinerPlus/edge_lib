use edge_lib::{err, util::Path};
use sqlx::{Pool, Row, Sqlite};

mod main {
    use edge_lib::{err, util::Step};
    use sqlx::{Pool, Sqlite};

    pub async fn delete_edge_with_source_code(
        pool: Pool<Sqlite>,
        source: &str,
        paper: &str,
        code: &str,
    ) -> err::Result<()> {
        sqlx::query("delete from edge_t where source = ? and paper = ? and code = ?")
            .bind(source)
            .bind(paper)
            .bind(code)
            .execute(&pool)
            .await
            .map_err(|e| {
                log::error!("{e}\nat delete_edge_with_source_code");

                moon_err::Error::new(
                    err::ErrorKind::RuntimeError,
                    e.to_string(),
                    format!("at delete_edge_with_source_code"),
                )
            })?;
        Ok(())
    }

    pub fn gen_sql_stm(first_step: &Step, step_v: &[Step]) -> String {
        let sql = if first_step.arrow == "->" {
            format!(
            "select v_{}.root from (select target as root, id from edge_t where source=? and paper=? and code=?) v_0",
            step_v.len(),
       )
        } else {
            format!(
            "select v_{}.root from (select source as root, id from edge_t where target=? and paper=? and code=?) v_0",
            step_v.len(),
       )
        };
        let mut root = format!("v_0");
        let mut no = 0;
        let join_v = step_v.iter().map(|step| {
            let p_root = root.clone();
            no += 1;
            root = format!("v_{no}");
            if step.arrow == "->" {
                format!(
                    "join (select target as root, source, id from edge_t where paper=? and code=?) v_{no} on v_{no}.source = {p_root}.root",
               )
            } else {
                format!(
                    "join (select source as root, target, id from edge_t where paper=? and code=?) v_{no} on v_{no}.source = {p_root}.root",
               )
            }
        }).reduce(|acc, item| {
            format!("{acc}\n{item}")
        }).unwrap_or_default();
        format!("{sql}\n{join_v} order by v_{}.id", step_v.len())
    }

    #[cfg(test)]
    mod test_gen_sql {
        use edge_lib::util::Step;

        #[test]
        fn test_gen_sql() {
            let sql = super::gen_sql_stm(
                &Step {
                    arrow: "->".to_string(),
                    code: "code".to_string(),
                    paper: "".to_string(),
                },
                &vec![Step {
                    arrow: "->".to_string(),
                    code: "code".to_string(),
                    paper: "".to_string(),
                }],
            );
            println!("{sql}")
        }
    }
}

pub async fn insert_edge(
    pool: Pool<Sqlite>,
    source: &str,
    paper: &str,
    code: &str,
    target_v: &Vec<String>,
) -> err::Result<()> {
    if target_v.is_empty() {
        return Ok(());
    }
    log::info!("commit target_v: {}", target_v.len());
    let value_v = target_v
        .iter()
        .map(|_| format!("(?,?,?,?)"))
        .reduce(|acc, item| {
            if acc.is_empty() {
                item
            } else {
                format!("{acc},{item}")
            }
        })
        .unwrap();

    let sql = format!("insert into edge_t (source,paper,code,target) values {value_v}");
    let mut statement = sqlx::query(&sql);
    for target in target_v {
        statement = statement.bind(source).bind(paper).bind(code).bind(target);
    }
    statement.execute(&pool).await.map_err(|e| {
        log::error!("{e}\nat insert_edge");

        moon_err::Error::new(
            err::ErrorKind::Other(format!("SqlxError")),
            e.to_string(),
            format!("at insert_edge"),
        )
    })?;
    Ok(())
}

pub async fn get(pool: Pool<Sqlite>, path: &Path) -> err::Result<Vec<String>> {
    let first_step = &path.step_v[0];
    let sql = main::gen_sql_stm(first_step, &path.step_v[1..]);
    let mut arr = Vec::new();

    for root in &path.root_v {
        let mut stm = sqlx::query(&sql).bind(root);
        for step in &path.step_v {
            stm = stm.bind(&step.paper).bind(&step.code);
        }
        let rs = stm.fetch_all(&pool).await.map_err(|e| {
            log::error!("{e}\n at get");

            moon_err::Error::new(err::ErrorKind::Other(format!("SqlxError")), e.to_string(), format!("at get"))
        })?;
        for row in rs {
            arr.push(row.get(0));
        }
    }

    Ok(arr)
}

pub async fn delete_edge_with_source_code(
    pool: Pool<Sqlite>,
    paper: &str,
    source: &str,
    code: &str,
) -> err::Result<()> {
    main::delete_edge_with_source_code(pool, source, paper, code).await
}

pub async fn get_code_v(pool: Pool<Sqlite>, root: &str, paper: &str) -> err::Result<Vec<String>> {
    Ok(
        sqlx::query("select code from edge_t where source = ? and paper = ?")
            .bind(root)
            .bind(paper)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                log::error!("{e}\n at get_code_v");

                moon_err::Error::new(err::ErrorKind::Other(format!("SqlxError")), e.to_string(), format!("get_code_v"))
            })?
            .iter()
            .map(|row| row.get(0))
            .collect(),
    )
}

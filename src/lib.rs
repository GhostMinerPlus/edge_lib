mod inc;

use serde::{Deserialize, Serialize};
use std::io;

use crate::data::AsDataManager;

#[async_recursion::async_recursion]
async fn get_all_by_path(
    dm: &mut Box<dyn AsDataManager>,
    mut path: Path,
) -> io::Result<Vec<String>> {
    if path.step_v.is_empty() {
        if path.root.is_empty() {
            return Ok(Vec::new());
        } else {
            return Ok(vec![path.root.clone()]);
        }
    }
    let root = path.root.clone();
    let step = path.step_v.remove(0);
    let curr_v = if step.arrow == "->" {
        dm.get_target_v(&root, &step.code).await?
    } else {
        dm.get_source_v(&step.code, &root).await?
    };
    let mut rs = Vec::new();
    for root in curr_v {
        rs.append(
            &mut get_all_by_path(
                dm,
                Path {
                    root,
                    step_v: path.step_v.clone(),
                },
            )
            .await?,
        );
    }
    Ok(rs)
}

fn gen_root() -> String {
    format!("${}", uuid::Uuid::new_v4().to_string())
}

#[async_recursion::async_recursion]
async fn on_asigned(dm: &mut Box<dyn AsDataManager>, code: &str) -> io::Result<()> {
    let listener_v = dm.get_target_v(code, "listener").await?;
    for listener in &listener_v {
        let target = escape_word(&dm.get_target(listener, "target").await?);
        if target.is_empty() {
            continue;
        }
        let inc_v = dump_inc_v(dm, listener)
            .await?
            .into_iter()
            .map(|mut inc| {
                *inc.output.as_mut() = inc.output.as_str().replace("$->$output", &target);
                inc
            })
            .collect::<Vec<Inc>>();
        let new_root = gen_root();
        asign(
            dm,
            &format!("{new_root}->$input"),
            "=",
            vec![code.to_string()],
        )
        .await?;
        for inc in &inc_v {
            let inc = unwrap_inc(dm, &new_root, inc).await?;
            invoke_inc(dm, &inc).await?;
        }
    }

    Ok(())
}

async fn asign(
    dm: &mut Box<dyn AsDataManager>,
    output: &str,
    operator: &str,
    item_v: Vec<String>,
) -> io::Result<()> {
    let mut output_path = Path::from_str(output);
    let last_step = match output_path.step_v.pop() {
        Some(step) => step,
        None => {
            let e = io::Error::other("invalid path");
            log::error!("{e}: {output}");
            return Err(io::Error::other(e));
        }
    };
    if last_step.arrow == "<-" {
        return Err(io::Error::other("not allow to asign parents by '<-'"));
    }
    let root_v = get_all_by_path(dm, output_path.clone()).await?;

    if operator == "=" {
        for source in &root_v {
            dm.set_target_v(source, &last_step.code, &item_v).await?;
        }
    } else {
        for source in &root_v {
            dm.append_target_v(source, &last_step.code, &item_v).await?;
        }
    }
    if output_path.root.starts_with('$') || data::is_temp(&last_step.code) {
        return Ok(());
    }
    on_asigned(dm, &last_step.code).await
}

async fn unwrap_value(root: &str, value: &str) -> io::Result<String> {
    if value.starts_with('\'') {
        Ok(value.to_string())
    } else if value.starts_with("$<-") {
        Ok(format!("{root}{}", &value[1..]))
    } else if value.starts_with("$->") {
        Ok(format!("{root}{}", &value[1..]))
    } else if value == "?" {
        Ok(uuid::Uuid::new_v4().to_string())
    } else if value == "$" {
        Ok(root.to_string())
    } else if value == "_" {
        Ok("".to_string())
    } else {
        Ok(value.to_string())
    }
}

fn escape_word(word: &str) -> String {
    let mut word = word.replace("\\'", "'");
    if word.starts_with('\'') && word.ends_with('\'') {
        word = word[1..word.len() - 1].to_string();
    }
    word
}

async fn dump_inc_v(dm: &mut Box<dyn AsDataManager>, function: &str) -> io::Result<Vec<Inc>> {
    let output_v = get_all_by_path(dm, Path::from_str(&format!("{function}->inc->output"))).await?;
    let operator_v =
        get_all_by_path(dm, Path::from_str(&format!("{function}->inc->operator"))).await?;
    let function_v =
        get_all_by_path(dm, Path::from_str(&format!("{function}->inc->function"))).await?;
    let input_v = get_all_by_path(dm, Path::from_str(&format!("{function}->inc->input"))).await?;
    let input1_v = get_all_by_path(dm, Path::from_str(&format!("{function}->inc->input1"))).await?;
    let mut inc_v = Vec::with_capacity(output_v.len());
    for i in 0..output_v.len() {
        inc_v.push(Inc {
            output: IncValue::from_string(escape_word(&output_v[i])),
            operator: IncValue::from_string(escape_word(&operator_v[i])),
            function: IncValue::from_string(escape_word(&function_v[i])),
            input: IncValue::from_string(escape_word(&input_v[i])),
            input1: IncValue::from_string(escape_word(&input1_v[i])),
        });
    }
    Ok(inc_v)
}

#[async_recursion::async_recursion]
async fn invoke_inc(dm: &mut Box<dyn AsDataManager>, inc: &Inc) -> io::Result<()> {
    log::debug!("invoke_inc: {:?}", inc);
    let input_item_v = get_all_by_path(dm, Path::from_str(inc.input.as_str())).await?;
    let input1_item_v = get_all_by_path(dm, Path::from_str(inc.input1.as_str())).await?;
    let rs = match inc.function.as_str() {
        //
        "new" => inc::new(dm, input_item_v, input1_item_v).await?,
        "line" => inc::line(dm, input_item_v, input1_item_v).await?,
        "rand" => inc::rand(dm, input_item_v, input1_item_v).await?,
        //
        "append" => inc::append(dm, input_item_v, input1_item_v).await?,
        "distinct" => inc::distinct(dm, input_item_v, input1_item_v).await?,
        "left" => inc::left(dm, input_item_v, input1_item_v).await?,
        "inner" => inc::inner(dm, input_item_v, input1_item_v).await?,
        "if" => inc::if_(dm, input_item_v, input1_item_v).await?,
        //
        "+" => inc::add(dm, input_item_v, input1_item_v).await?,
        "-" => inc::minus(dm, input_item_v, input1_item_v).await?,
        "*" => inc::mul(dm, input_item_v, input1_item_v).await?,
        "/" => inc::div(dm, input_item_v, input1_item_v).await?,
        "%" => inc::rest(dm, input_item_v, input1_item_v).await?,
        //
        "==" => inc::equal(dm, input_item_v, input1_item_v).await?,
        "!=" => inc::not_equal(dm, input_item_v, input1_item_v).await?,
        ">" => inc::greater(dm, input_item_v, input1_item_v).await?,
        "<" => inc::smaller(dm, input_item_v, input1_item_v).await?,
        //
        "sort" => inc::sort(dm, input_item_v, input1_item_v).await?,
        //
        "count" => inc::count(dm, input_item_v, input1_item_v).await?,
        "sum" => inc::sum(dm, input_item_v, input1_item_v).await?,
        //
        "=" => inc::set(dm, input_item_v, input1_item_v).await?,
        _ => {
            let inc_v = dump_inc_v(dm, inc.function.as_str()).await?;
            let new_root = gen_root();
            asign(dm, &format!("{new_root}->$input"), "=", input_item_v).await?;
            asign(dm, &format!("{new_root}->$input1"), "=", input1_item_v).await?;
            log::debug!("inc_v.len(): {}", inc_v.len());
            for inc in &inc_v {
                let inc = unwrap_inc(dm, &new_root, inc).await?;
                invoke_inc(dm, &inc).await?;
            }
            get_all_by_path(dm, Path::from_str(&format!("{new_root}->$output"))).await?
        }
    };
    asign(dm, inc.output.as_str(), inc.operator.as_str(), rs).await
}

async fn get_one(dm: &mut Box<dyn AsDataManager>, root: &str, id: &str) -> io::Result<String> {
    let path = unwrap_value(root, id).await?;
    let id_v = get_all_by_path(dm, Path::from_str(&path)).await?;
    if id_v.len() != 1 {
        return Err(io::Error::new(io::ErrorKind::NotFound, "need 1 but not"));
    }
    Ok(id_v[0].clone())
}

async fn unwrap_inc(dm: &mut Box<dyn AsDataManager>, root: &str, inc: &Inc) -> io::Result<Inc> {
    let inc = Inc {
        output: IncValue::from_str(&unwrap_value(root, inc.output.as_str()).await?),
        operator: IncValue::from_str(&get_one(dm, root, inc.operator.as_str()).await?),
        function: IncValue::from_str(&get_one(dm, root, inc.function.as_str()).await?),
        input: IncValue::from_str(&unwrap_value(root, inc.input.as_str()).await?),
        input1: IncValue::from_str(&unwrap_value(root, inc.input1.as_str()).await?),
    };
    Ok(inc)
}

fn find_close_quotation(path: &str) -> usize {
    let pos = path.find('\'').unwrap();
    if pos == 0 {
        return 0;
    }
    if &path[pos - 1..pos] == "\\" {
        return pos + 1 + find_close_quotation(&path[pos + 1..]);
    }
    pos
}

fn find_arrrow_in_block(path: &str, pos: usize) -> usize {
    let a_pos = find_arrrow_in_pure(&path[0..pos]);
    if a_pos < pos {
        return a_pos;
    }
    let c_pos = pos + 1 + find_close_quotation(&path[pos + 1..]);
    c_pos + 1 + find_arrrow(&path[c_pos + 1..])
}

fn find_arrrow_in_pure(path: &str) -> usize {
    let p = path.find("->");
    let q = path.find("<-");
    if p.is_none() && q.is_none() {
        path.len()
    } else {
        if p.is_some() && q.is_some() {
            let p = p.unwrap();
            let q = q.unwrap();
            std::cmp::min(p, q)
        } else if p.is_some() {
            p.unwrap()
        } else {
            q.unwrap()
        }
    }
}

fn find_arrrow(path: &str) -> usize {
    if let Some(pos) = path.find('\'') {
        return find_arrrow_in_block(path, pos);
    }
    find_arrrow_in_pure(path)
}

async fn invoke_inc_v(
    dm: &mut Box<dyn AsDataManager>,
    root: &str,
    inc_v: &Vec<Inc>,
) -> io::Result<Vec<String>> {
    log::debug!("inc_v.len(): {}", inc_v.len());
    for inc in inc_v {
        let inc = unwrap_inc(dm, &root, inc).await?;
        invoke_inc(dm, &inc).await?;
    }
    get_all_by_path(dm, Path::from_str(&format!("{root}->$output"))).await
}

fn merge(p_tree: &mut json::JsonValue, s_tree: &mut json::JsonValue) {
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

#[async_recursion::async_recursion]
async fn execute(
    dm: &mut Box<dyn AsDataManager>,
    input: &str,
    script_tree: &ScriptTree,
    out_tree: &mut json::JsonValue,
) -> io::Result<()> {
    let root = gen_root();
    asign(
        dm,
        &format!("{root}->$input"),
        "+=",
        vec![input.to_string()],
    )
    .await?;
    let inc_v = parse_script(&script_tree.script)?;
    let rs = invoke_inc_v(dm, &root, &inc_v).await?;
    if script_tree.next_v.is_empty() {
        let _ = out_tree.insert(&script_tree.name, rs);
        return Ok(());
    }
    let mut cur = json::object! {};
    for next_tree in &script_tree.next_v {
        // fork
        for input in &rs {
            let mut sub_out_tree = json::object! {};
            execute(dm, input, next_tree, &mut sub_out_tree).await?;
            merge(&mut cur, &mut sub_out_tree);
        }
    }
    let _ = out_tree.insert(&script_tree.name, cur);
    Ok(())
}

fn split_line(line: &str) -> Vec<String> {
    let part_v: Vec<&str> = line.split(' ').collect();
    if part_v.len() <= 5 {
        return part_v.into_iter().map(|s| s.to_string()).collect();
    }

    let mut word_v = Vec::with_capacity(5);
    let mut entered = false;
    for part in part_v {
        if entered {
            *word_v.last_mut().unwrap() = format!("{} {part}", word_v.last().unwrap());
            if part.ends_with('\'') && !part.ends_with("\\'") {
                entered = false;
            }
        } else {
            word_v.push(part.to_string());
            if part.starts_with('\'') {
                entered = true;
            }
        }
    }

    return word_v;
}

fn parse_script(script: &str) -> io::Result<Vec<Inc>> {
    let mut inc_v = Vec::new();
    for line in script.lines() {
        if line.is_empty() {
            continue;
        }

        let word_v = split_line(line);
        if word_v.len() != 5 {
            return Err(io::Error::other(
                "when parse_script:\n\tmore than 5 words in a line",
            ));
        }
        inc_v.push(Inc {
            output: IncValue::from_str(word_v[0].trim()),
            operator: IncValue::from_str(word_v[1].trim()),
            function: IncValue::from_str(word_v[2].trim()),
            input: IncValue::from_str(word_v[3].trim()),
            input1: IncValue::from_str(word_v[4].trim()),
        });
    }
    Ok(inc_v)
}

#[derive(Clone)]
struct Step {
    arrow: String,
    code: String,
}

#[derive(Clone)]
struct Path {
    root: String,
    step_v: Vec<Step>,
}

impl Path {
    fn from_str(path: &str) -> Self {
        if path.is_empty() {
            return Path {
                root: String::new(),
                step_v: Vec::new(),
            };
        }
        log::debug!("Path::from_str: {path}");
        if path.starts_with('\'') && path.ends_with('\'') {
            return Self {
                root: path.to_string(),
                step_v: Vec::new(),
            };
        }
        let mut s = find_arrrow(path);

        let root = path[0..s].to_string();
        if s == path.len() {
            return Self {
                root,
                step_v: Vec::new(),
            };
        }
        let mut tail = &path[s..];
        let mut step_v = Vec::new();
        loop {
            s = find_arrrow(&tail[2..]) + 2;
            step_v.push(Step {
                arrow: tail[0..2].to_string(),
                code: tail[2..s].to_string(),
            });
            if s == tail.len() {
                break;
            }
            tail = &tail[s..];
        }
        Self { root, step_v }
    }
}

#[derive(Clone, Debug)]
pub enum IncValue {
    Addr(String),
    Value(String),
}

impl IncValue {
    pub fn as_mut(&mut self) -> &mut String {
        match self {
            IncValue::Addr(addr) => addr,
            IncValue::Value(value) => value,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            IncValue::Addr(addr) => addr,
            IncValue::Value(value) => value,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            IncValue::Addr(addr) => addr.clone(),
            IncValue::Value(value) => value.clone(),
        }
    }

    pub fn from_str(s: &str) -> Self {
        Self::from_string(s.to_string())
    }

    pub fn from_string(s: String) -> Self {
        if s.starts_with('\'') && s.ends_with('\'') {
            return Self::Value(s);
        }
        if s.contains("->") || s.contains("<-") {
            return Self::Addr(s);
        }
        return Self::Value(s);
    }
}

#[derive(Clone, Debug)]
struct Inc {
    pub output: IncValue,
    pub operator: IncValue,
    pub function: IncValue,
    pub input: IncValue,
    pub input1: IncValue,
}

// Public
pub mod data;
pub mod err;
pub mod mem_table;

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
        async { Err(io::Error::other("deprecated")) }
    }

    fn execute1(
        &mut self,
        script_tree: &ScriptTree,
    ) -> impl std::future::Future<Output = io::Result<json::JsonValue>> + Send;

    fn commit(&mut self) -> impl std::future::Future<Output = io::Result<()>> + Send;
}

pub struct EdgeEngine {
    dm: Box<dyn AsDataManager>,
}

impl EdgeEngine {
    pub fn new(dm: Box<dyn AsDataManager>) -> Self {
        Self { dm }
    }

    fn entry_2_tree(script_str: &str, next_v_json: &json::JsonValue) -> ScriptTree {
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
}

impl AsEdgeEngine for EdgeEngine {
    async fn execute(&mut self, script_tree: &json::JsonValue) -> io::Result<json::JsonValue> {
        let (script_str, next_v_json) = script_tree.entries().next().unwrap();
        let script_tree = Self::entry_2_tree(script_str, next_v_json);
        self.execute1(&script_tree).await
    }

    async fn execute1(&mut self, script_tree: &ScriptTree) -> io::Result<json::JsonValue> {
        let mut out_tree = json::object! {};
        execute(&mut self.dm, "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
    }

    async fn commit(&mut self) -> io::Result<()> {
        self.dm.commit().await
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
                    script: ["$->$output = = '1 ' _"].join("\n"),
                    name: "result".to_string(),
                    next_v: vec![],
                })
                .await
                .unwrap();
            assert!(rs["result"][0].as_str() == Some("'1 '"));
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

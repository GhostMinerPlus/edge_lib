use serde::{Deserialize, Serialize};
use std::{fmt::Display, io, sync::Arc};

use crate::data::AsDataManager;

pub mod data;
pub mod err;
pub mod func;
pub mod mem_table;
pub mod util;

#[derive(Clone, Debug)]
pub struct Inc {
    pub output: IncValue,
    pub function: IncValue,
    pub input: IncValue,
    pub input1: IncValue,
}

pub enum PathType {
    Pure,
    Temp,
    Mixed,
}

pub enum PathPart {
    Pure(Path),
    Temp(Path),
    EntirePure,
    EntireTemp,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Step {
    pub arrow: String,
    pub code: String,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Path {
    pub root: String,
    pub step_v: Vec<Step>,
}

impl Path {
    pub fn from_str(path: &str) -> Self {
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
        let mut s = Self::find_arrrow(path).unwrap_or(path.len());

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
            s = Self::find_arrrow(&tail[2..]).unwrap_or(tail.len());
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

    pub fn to_string(&self) -> String {
        let mut s = self.root.clone();
        for step in &self.step_v {
            s = format!("{s}{}{}", step.arrow, step.code);
        }
        s
    }

    pub fn is_temp(&self) -> bool {
        if self.step_v.is_empty() {
            return false;
        }
        self.step_v.last().unwrap().code.starts_with('$')
    }

    pub fn path_type(&self) -> PathType {
        let mut cnt = 0;
        for i in 0..self.step_v.len() {
            if self.step_v[i].code.starts_with('$') {
                cnt += 1;
            }
        }
        if cnt == 0 {
            PathType::Pure
        } else if cnt == self.step_v.len() {
            PathType::Temp
        } else {
            PathType::Mixed
        }
    }

    pub fn first_part(&self) -> PathPart {
        if self.step_v.is_empty() {
            return PathPart::EntirePure;
        }
        let first_step = &self.step_v[0];
        if first_step.code.starts_with('$') {
            let mut end = 1;
            for i in 1..self.step_v.len() {
                if !self.step_v[i].code.starts_with('$') {
                    break;
                }
                end += 1;
            }
            if end == self.step_v.len() {
                return PathPart::EntireTemp;
            }
            PathPart::Temp(Path {
                root: self.root.clone(),
                step_v: self.step_v[0..end].to_vec(),
            })
        } else {
            let mut end = 1;
            for i in 1..self.step_v.len() {
                if self.step_v[i].code.starts_with('$') {
                    break;
                }
                end += 1;
            }
            if end == self.step_v.len() {
                return PathPart::EntirePure;
            }
            PathPart::Pure(Path {
                root: self.root.clone(),
                step_v: self.step_v[0..end].to_vec(),
            })
        }
    }

    fn find_quotation(path: &str) -> Option<usize> {
        let pos = path.find('\'')?;
        if pos == 0 {
            return Some(0);
        }
        if &path[pos - 1..pos] == "\\" {
            return Some(pos + 1 + Self::find_quotation(&path[pos + 1..])?);
        }
        Some(pos)
    }

    fn find_arrrow_in_block(path: &str, pos: usize) -> Option<usize> {
        match Self::find_arrrow_in_pure(&path[0..pos]) {
            Some(a_pos) => Some(a_pos),
            None => {
                let c_pos = pos + 1 + Self::find_quotation(&path[pos + 1..])?;
                match Self::find_arrrow(&path[c_pos + 1..]) {
                    Some(a_pos) => Some(c_pos + 1 + a_pos),
                    None => None,
                }
            }
        }
    }

    fn find_arrrow_in_pure(path: &str) -> Option<usize> {
        let p = path.find("->");
        let q = path.find("<-");
        if p.is_none() && q.is_none() {
            None
        } else {
            Some(if p.is_some() && q.is_some() {
                let p = p.unwrap();
                let q = q.unwrap();
                std::cmp::min(p, q)
            } else if p.is_some() {
                p.unwrap()
            } else {
                q.unwrap()
            })
        }
    }

    fn find_arrrow(path: &str) -> Option<usize> {
        if let Some(pos) = Self::find_quotation(path) {
            return Self::find_arrrow_in_block(path, pos);
        }
        Self::find_arrrow_in_pure(path)
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
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
        if s.starts_with('\'') && s.ends_with('\'') && !s.ends_with("\\'") {
            return Self::Value(s);
        }
        if s.contains("->") || s.contains("<-") {
            return Self::Addr(s);
        }
        return Self::Value(s);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScriptTree {
    pub script: String,
    pub name: String,
    pub next_v: Vec<ScriptTree>,
}

pub struct EdgeEngine {
    dm: Arc<dyn AsDataManager>,
}

impl EdgeEngine {
    pub fn new(dm: Arc<dyn AsDataManager>) -> Self {
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
        main::execute1::<dep::Dep>(self, script_tree).await
    }

    pub async fn commit(&mut self) -> io::Result<()> {
        self.dm.commit().await
    }

    /// return x
    ///
    /// ->name
    ///
    /// ->group
    pub async fn resolve_func(&self, inc_v: &[Inc], z: &str) -> io::Result<Vec<String>> {
        main::resolve_func::<dep::Dep>(self, inc_v, z).await
    }
}

mod main {
    use std::{
        collections::HashMap,
        io,
        sync::{Arc, Mutex},
    };

    use tokio::sync::RwLock;

    use crate::{
        data::AsDataManager, dep::AsDep, func, util, EdgeEngine, Inc, IncValue, Path, ScriptTree,
    };

    static mut EDGE_ENGINE_FUNC_MAP_OP: Option<RwLock<HashMap<String, Box<dyn func::AsFunc>>>> =
        None;
    static mut EDGE_ENGINE_FUNC_MAP_OP_LOCK: Mutex<()> = Mutex::new(());

    pub fn new_edge_engine(dm: Arc<dyn AsDataManager>) -> EdgeEngine {
        lazy_mp();
        EdgeEngine { dm }
    }

    pub async fn execute1<D: AsDep>(
        this: &mut EdgeEngine,
        script_tree: &ScriptTree,
    ) -> io::Result<json::JsonValue> {
        let mut out_tree = json::object! {};
        inner_execute::<D>(this, "", &script_tree, &mut out_tree).await?;
        Ok(out_tree)
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
    pub async fn resolve_func<D: AsDep>(
        this: &EdgeEngine,
        inc_v: &[Inc],
        z: &str,
    ) -> io::Result<Vec<String>> {
        let dm = this.dm.clone();
        if inc_v.is_empty() {
            return Err(io::Error::other("empty inc_v"));
        }
        if inc_v.len() == 1 {
            resolve_inc::<D>(this, &inc_v[0], z).await
        } else {
            let x_v = this.resolve_func(&inc_v[inc_v.len() - 1..], z).await?;
            // 递归裂解
            let mut c_x_v = Vec::with_capacity(x_v.len());
            for x in x_v {
                let name = &dm.get(&Path::from_str(&format!("{x}->name"))).await?[0];
                if let Some(pos) = find_setter(inc_v, name) {
                    let x_v2 = this.resolve_func(&inc_v[0..pos + 1], &x).await?;
                    c_x_v.extend(x_v2);
                } else {
                    c_x_v.push(x);
                }
            }
            Ok(c_x_v)
        }
    }

    pub fn tree_2_entry(script_tree: &ScriptTree) -> json::JsonValue {
        let script = format!("{}\n{}", script_tree.script, script_tree.name);
        let value = tree_2_entry(script_tree);
        let mut json = json::object! {};
        let _ = json.insert(&script, value);
        json
    }

    /// 解析函数为参数方程组，并返回所有依赖变量
    ///
    /// x_i = f_i(t', z)
    ///
    /// index
    ///
    /// ->x 依赖变量
    ///
    /// ->f 函数
    ///
    /// ->t 参数个数
    async fn resolve_func_by_index(this: &EdgeEngine, func: &str) -> io::Result<Option<String>> {
        let mut rs = this
            .dm
            .get(&Path::from_str(&format!("{func}->index")))
            .await?;
        Ok(rs.pop())
    }

    /// return group
    ///
    /// ->index
    ///
    /// ->x->name
    ///
    /// ->x->group
    ///
    /// ->input
    ///
    /// ->input1
    ///
    /// ->z
    async fn resolve_inc<D: AsDep>(
        this: &EdgeEngine,
        inc: &Inc,
        z: &str,
    ) -> io::Result<Vec<String>> {
        let index_op = resolve_func_by_index(this, inc.function.as_str()).await?;
        if index_op.is_none() {
            return Err(io::Error::other(format!(
                "no index:\n\rwhen resolve {}",
                inc.function.as_str()
            )));
        }
        let index = index_op.unwrap();
        let group = D::gen_value();
        this.dm
            .set(
                &Path::from_str(&format!("{group}->index")),
                vec![index.clone()],
            )
            .await?;
        this.dm
            .set(&Path::from_str(&format!("{group}->z")), vec![z.to_string()])
            .await?;

        let x_v = this.dm.get(&Path::from_str(&format!("{index}->x"))).await?;
        let mut r_x_v = Vec::with_capacity(x_v.len());
        for x in &x_v {
            let r_x = D::gen_value();
            if x == "$->$input" {
                this.dm
                    .set(
                        &Path::from_str(&format!("{r_x}->name")),
                        vec![inc.input.as_str().to_string()],
                    )
                    .await?;
                this.dm
                    .set(
                        &Path::from_str(&format!("{group}->input")),
                        vec![inc.input.as_str().to_string()],
                    )
                    .await?;
            } else if x == "$->$input1" {
                this.dm
                    .set(
                        &Path::from_str(&format!("{r_x}->name")),
                        vec![inc.input1.as_str().to_string()],
                    )
                    .await?;
                this.dm
                    .set(
                        &Path::from_str(&format!("{group}->input1")),
                        vec![inc.input1.as_str().to_string()],
                    )
                    .await?;
            } else {
                this.dm
                    .set(&Path::from_str(&format!("{r_x}->name")), vec![x.clone()])
                    .await?;
            }
            this.dm
                .set(
                    &Path::from_str(&format!("{r_x}->group")),
                    vec![group.clone()],
                )
                .await?;
            r_x_v.push(r_x);
        }
        Ok(r_x_v)
    }

    #[async_recursion::async_recursion]
    async fn inner_execute<D: AsDep>(
        this: &EdgeEngine,
        input: &str,
        script_tree: &ScriptTree,
        out_tree: &mut json::JsonValue,
    ) -> io::Result<()> {
        let root = D::gen_root();
        this.dm
            .append(
                &Path::from_str(&format!("{root}->$input")),
                vec![input.to_string()],
            )
            .await?;
        let inc_v = parse_script(&script_tree.script)?;
        let rs = invoke_inc_v::<D>(this, &root, &inc_v).await?;
        if script_tree.next_v.is_empty() {
            let _ = out_tree.insert(&script_tree.name, rs);
            return Ok(());
        }
        let mut cur = json::object! {};
        for next_tree in &script_tree.next_v {
            // fork
            for input in &rs {
                let mut sub_out_tree = json::object! {};
                inner_execute::<D>(this, input, next_tree, &mut sub_out_tree).await?;
                merge(&mut cur, &mut sub_out_tree);
            }
        }
        let _ = out_tree.insert(&script_tree.name, cur);
        Ok(())
    }

    fn find_setter(inc_v: &[Inc], x: &str) -> Option<usize> {
        if inc_v.is_empty() {
            return None;
        }

        let mut i = inc_v.len() - 1;
        loop {
            let inc = &inc_v[i];
            if inc.output.as_str() == x {
                return Some(i);
            }
            if i == 0 {
                break;
            }
            i -= 1;
        }
        None
    }

    async fn get_inc_v(dm: Arc<dyn AsDataManager>, function: &str) -> io::Result<Vec<Inc>> {
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
                output: IncValue::from_string(util::escape_word(&output_v[i])),
                function: IncValue::from_string(util::escape_word(&function_v[i])),
                input: IncValue::from_string(util::escape_word(&input_v[i])),
                input1: IncValue::from_string(util::escape_word(&input1_v[i])),
            });
        }
        Ok(inc_v)
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

    fn parse_script(script: &str) -> io::Result<Vec<Inc>> {
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
                        output: IncValue::from_str("$->$temp"),
                        function: IncValue::from_str(word_v[2].trim()),
                        input: IncValue::from_str(word_v[3].trim()),
                        input1: IncValue::from_str(word_v[4].trim()),
                    });
                    inc_v.push(Inc {
                        output: IncValue::from_str(word_v[0].trim()),
                        function: IncValue::from_str("append"),
                        input: IncValue::from_str(word_v[0].trim()),
                        input1: IncValue::from_str("$->$temp"),
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

    async fn get_one<D: AsDep>(this: &EdgeEngine, root: &str, value: &str) -> io::Result<String> {
        let path = D::unwrap_value(root, value)?;
        let id_v = this.dm.get(&Path::from_str(&path)).await?;
        if id_v.len() != 1 {
            return Err(io::Error::new(io::ErrorKind::NotFound, "need 1 but not"));
        }
        Ok(id_v[0].clone())
    }

    async fn unwrap_inc<D: AsDep>(this: &EdgeEngine, root: &str, inc: &Inc) -> io::Result<Inc> {
        let inc = Inc {
            output: IncValue::from_str(&D::unwrap_value(root, inc.output.as_str())?),
            function: IncValue::from_str(&get_one::<D>(this, root, inc.function.as_str()).await?),
            input: IncValue::from_str(&D::unwrap_value(root, inc.input.as_str())?),
            input1: IncValue::from_str(&D::unwrap_value(root, inc.input1.as_str())?),
        };
        Ok(inc)
    }

    async fn invoke_inc_v<D: AsDep>(
        this: &EdgeEngine,
        root: &str,
        inc_v: &Vec<Inc>,
    ) -> io::Result<Vec<String>> {
        log::debug!("inc_v.len(): {}", inc_v.len());
        for inc in inc_v {
            let inc = unwrap_inc::<D>(this, &root, inc).await?;
            invoke_inc::<D>(this, &inc).await?;
        }
        this.dm
            .get(&Path::from_str(&format!("{root}->$output")))
            .await
    }

    #[async_recursion::async_recursion]
    async fn invoke_inc<D: AsDep>(this: &EdgeEngine, inc: &Inc) -> io::Result<()> {
        log::debug!("invoke_inc: {:?}", inc);
        let output = Path::from_str(inc.output.as_str());
        if output.step_v.is_empty() {
            return Ok(());
        }
        let input = Path::from_str(inc.input.as_str());
        let input1 = Path::from_str(inc.input1.as_str());
        let func_mp = unsafe { EDGE_ENGINE_FUNC_MAP_OP.as_ref().unwrap().read().await };
        match func_mp.get(inc.function.as_str()) {
            Some(func) => {
                func.invoke(this.dm.clone(), output.clone(), input, input1)
                    .await?;
            }
            None => {
                if inc.function.as_str() == "$func" {
                    let mut rs = Vec::with_capacity(func_mp.len());
                    for (name, _) in &*func_mp {
                        rs.push(name.clone());
                    }
                    this.dm.set(&output, rs).await?;
                } else if inc.function.as_str() == "$resolve" {
                    let input_item_v = this.dm.get(&input).await?;
                    if input_item_v.is_empty() {
                        return Err(io::Error::other("when $resolve:\n\rno input"));
                    }
                    let inc_v = get_inc_v(this.dm.clone(), &input_item_v[0]).await?;
                    if inc_v.is_empty() {
                        return Err(io::Error::other("empty inc_v"));
                    }
                    let rs = this.resolve_func(&inc_v, inc.input1.as_str()).await?;
                    this.dm.set(&output, rs).await?;
                } else {
                    let input_item_v = this.dm.get(&input).await?;
                    let input1_item_v = this.dm.get(&input1).await?;
                    let inc_v = get_inc_v(this.dm.clone(), inc.function.as_str()).await?;
                    let new_root = D::gen_root();
                    this.dm
                        .set(
                            &Path::from_str(&format!("{new_root}->$input")),
                            input_item_v,
                        )
                        .await?;
                    this.dm
                        .set(
                            &Path::from_str(&format!("{new_root}->$input1")),
                            input1_item_v,
                        )
                        .await?;
                    log::debug!("inc_v.len(): {}", inc_v.len());
                    invoke_inc_v::<D>(this, &new_root, &inc_v).await?;
                    let rs = this
                        .dm
                        .get(&Path::from_str(&format!("{new_root}->$output")))
                        .await?;
                    this.dm.set(&output, rs).await?;
                }
            }
        }
        if output.is_temp() {
            return Ok(());
        }
        on_asigned::<D>(this, &output.step_v.last().unwrap().code).await
    }

    #[async_recursion::async_recursion]
    async fn on_asigned<D: AsDep>(this: &EdgeEngine, code: &str) -> io::Result<()> {
        let listener_v = this
            .dm
            .get(&Path::from_str(&format!("{code}->listener")))
            .await?;
        for listener in &listener_v {
            let target_v = this
                .dm
                .get(&Path::from_str(&format!("{listener}->target")))
                .await?;
            if target_v.is_empty() {
                continue;
            }
            let target = util::escape_word(&target_v[0]);
            let inc_v = get_inc_v(this.dm.clone(), listener)
                .await?
                .into_iter()
                .map(|mut inc| {
                    *inc.output.as_mut() = inc.output.as_str().replace("$->$output", &target);
                    inc
                })
                .collect::<Vec<Inc>>();
            let new_root = D::gen_root();
            this.dm
                .set(
                    &Path::from_str(&format!("{new_root}->$input")),
                    vec![code.to_string()],
                )
                .await?;
            invoke_inc_v::<D>(this, &new_root, &inc_v).await?;
        }

        Ok(())
    }

    fn lazy_mp() {
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
            unsafe { EDGE_ENGINE_FUNC_MAP_OP = Some(RwLock::new(func_mp)) };
        }
        drop(lk);
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use crate::{
            data::{MemDataManager, RecDataManager},
            main::EDGE_ENGINE_FUNC_MAP_OP,
            EdgeEngine, ScriptTree,
        };

        #[test]
        fn test() {
            let task = async {
                let dm = RecDataManager::new(Arc::new(MemDataManager::new()));
                let mut edge_engine = EdgeEngine::new(Arc::new(dm));
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
                let dm = RecDataManager::new(Arc::new(MemDataManager::new()));
                let mut edge_engine = EdgeEngine::new(Arc::new(dm));
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
                let dm = RecDataManager::new(Arc::new(MemDataManager::new()));
                let mut edge_engine = EdgeEngine::new(Arc::new(dm));
                let rs = edge_engine
                    .execute1(&ScriptTree {
                        script: ["$->$output = = '1\\s' _"].join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    })
                    .await
                    .unwrap();
                assert!(rs["result"][0].as_str() == Some("'1\\s'"));
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(task);
        }

        #[test]
        fn test_resolve() {
            let task = async {
                let dm = RecDataManager::new(Arc::new(MemDataManager::new()));

                let mut edge_engine = EdgeEngine::new(Arc::new(dm));
                let rs = edge_engine
                    .execute1(&ScriptTree {
                        script: [
                            "test->inc = ? _",
                            "test->inc->output = '$->$output' _",
                            "test->inc->function = '+' _",
                            "test->inc->input = '1' _",
                            "test->inc->input1 = '1' _",
                            "$->$output $resolve test 2",
                        ]
                        .join("\n"),
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

        #[test]
        fn test_cache() {
            let task = async {
                let dm = RecDataManager::new(Arc::new(MemDataManager::new()));

                let mut edge_engine = EdgeEngine::new(Arc::new(dm));
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

        #[test]
        fn test_func() {
            let task = async {
                let dm = RecDataManager::new(Arc::new(MemDataManager::new()));
                let mut edge_engine = EdgeEngine::new(Arc::new(dm));
                let r_mp = unsafe { EDGE_ENGINE_FUNC_MAP_OP.as_ref().unwrap().read() }.await;
                let sz = r_mp.len();
                drop(r_mp);
                let rs = edge_engine
                    .execute1(&ScriptTree {
                        script: ["$->$output = $func _ _"].join("\n"),
                        name: "result".to_string(),
                        next_v: vec![],
                    })
                    .await
                    .unwrap();
                assert_eq!(rs["result"].len(), sz);
            };
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(task);
        }
    }
}

mod dep {
    use std::io;

    pub struct Dep {}

    impl AsDep for Dep {}

    pub trait AsDep {
        fn gen_root() -> String {
            format!("${}", Self::gen_value())
        }

        fn gen_value() -> String {
            uuid::Uuid::new_v4().to_string()
        }

        fn unwrap_value(root: &str, value: &str) -> io::Result<String> {
            if value.starts_with("$<-") {
                Ok(format!("{root}{}", &value[1..]))
            } else if value.starts_with("$->") {
                Ok(format!("{root}{}", &value[1..]))
            } else if value == "?" {
                Ok(Self::gen_value())
            } else if value == "$" {
                Ok(root.to_string())
            } else if value == "_" {
                Ok("".to_string())
            } else {
                Ok(value.to_string())
            }
        }
    }
}

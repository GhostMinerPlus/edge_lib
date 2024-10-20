mod main {
    use crate::util;

    use super::{Path, PathPart, PathType, Step};

    pub fn fmt(this: &Path, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", to_string(this))
    }

    pub fn contains(this: &Path, paper: &str, code: &str) -> bool {
        for step in &this.step_v {
            if step.paper == paper && step.code == code {
                return true;
            }
        }
        false
    }

    pub fn from_str(path: &str) -> Path {
        if path == "_" {
            return Path {
                root_v: Vec::new(),
                step_v: Vec::new(),
            };
        }
        if path.is_empty() {
            return Path {
                root_v: vec![String::new()],
                step_v: Vec::new(),
            };
        }

        let s = find_arrrow(path).unwrap_or(path.len());
        let root_v = path[0..s]
            .split(',')
            .map(|root| util::escape_word(root))
            .collect();
        let mut tail = &path[s..];
        let mut step_v = Vec::new();
        while !tail.is_empty() {
            let s = match find_arrrow(&tail[2..]) {
                Some(s) => s + 2,
                None => tail.len(),
            };
            let (paper, code) = {
                let pair = tail[2..s].split(':').collect::<Vec<&str>>();
                if pair.len() >= 2 {
                    (pair[0].to_string(), pair[1].to_string())
                } else if pair.len() == 1 {
                    (String::new(), pair[0].to_string())
                } else {
                    (String::new(), String::new())
                }
            };
            step_v.push(Step {
                arrow: tail[0..2].to_string(),
                paper,
                code,
            });
            tail = &tail[s..];
        }
        Path { root_v, step_v }
    }

    #[cfg(test)]
    mod test_from_str {
        #[test]
        fn should_from_str() {
            let path = super::from_str("51aae06c-65e9-468a-83b5-041fd52b37fc->$:proxy->path");
            assert_eq!(path.step_v.len(), 2);
        }
    }

    pub fn to_string(this: &Path) -> String {
        if !this.root_v.is_empty() {
            let mut s = this
                .root_v
                .iter()
                .map(|root| util::unescape_word(root))
                .reduce(|acc, item| format!("{acc},{item}"))
                .unwrap();
            for step in &this.step_v {
                s = format!("{s}{}{}:{}", step.arrow, step.paper, step.code);
            }
            s
        } else {
            "_".to_string()
        }
    }

    pub fn path_type(this: &Path) -> PathType {
        let mut cnt = 0;
        for i in 0..this.step_v.len() {
            if this.step_v[i].paper == "$" {
                cnt += 1;
            }
        }
        if cnt == 0 {
            PathType::Pure
        } else if cnt == this.step_v.len() {
            PathType::Temp
        } else {
            PathType::Mixed
        }
    }

    pub fn first_part(this: &Path) -> PathPart {
        if this.step_v.is_empty() {
            return PathPart::EntirePure;
        }
        let first_step = &this.step_v[0];
        if first_step.paper == "$" {
            let mut end = 1;
            for i in 1..this.step_v.len() {
                if this.step_v[i].paper != "$" {
                    break;
                }
                end += 1;
            }
            if end == this.step_v.len() {
                return PathPart::EntireTemp;
            }
            PathPart::Temp(Path {
                root_v: this.root_v.clone(),
                step_v: this.step_v[0..end].to_vec(),
            })
        } else {
            let mut end = 1;
            for i in 1..this.step_v.len() {
                if this.step_v[i].paper == "$" {
                    break;
                }
                end += 1;
            }
            if end == this.step_v.len() {
                return PathPart::EntirePure;
            }
            PathPart::Pure(Path {
                root_v: this.root_v.clone(),
                step_v: this.step_v[0..end].to_vec(),
            })
        }
    }

    fn find_quotation(path: &str) -> Option<usize> {
        let pos = path.find('\'')?;

        if pos == 0 {
            return Some(0);
        }

        if path[0..pos].ends_with("\\'") {
            return Some(pos + 1 + find_quotation(&path[pos + 1..])?);
        }

        Some(pos)
    }

    fn find_arrrow_in_block(path: &str, pos: usize) -> Option<usize> {
        match find_arrrow_in_pure(&path[0..pos]) {
            Some(a_pos) => Some(a_pos),
            None => {
                let c_pos = pos + 1 + find_quotation(&path[pos + 1..])?;
                match find_arrrow(&path[c_pos + 1..]) {
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
        if let Some(pos) = find_quotation(path) {
            return find_arrrow_in_block(path, pos);
        }
        find_arrrow_in_pure(path)
    }
}

pub(crate) mod func;

pub mod data;
pub mod engine;
pub mod mem_table;

use std::pin::Pin;

use data::{AsDataManager, Fu};

use crate::err;

pub(crate) fn dump<'a1, 'a2, 'a3, 'f, DM>(
    dm: &'a1 DM,
    root: &'a2 str,
    space: &'a3 str,
) -> Pin<Box<impl Fu<Output = err::Result<json::JsonValue>> + 'f>>
where
    'a1: 'f,
    'a2: 'f,
    'a3: 'f,
    DM: AsDataManager + ?Sized,
{
    Box::pin(async move {
        let code_v = dm.get_code_v(root, space).await?;

        if code_v.is_empty() {
            return Ok(json::JsonValue::String(root.to_string()));
        }

        let mut rj = json::object! {};

        for code in &code_v {
            let mut rj_item_v = json::array![];

            let paper_code = format!("{space}:{code}");

            let sub_root_v = dm
                .get(&Path::from_str(&format!("{root}->{paper_code}")))
                .await?;

            for sub_root in &sub_root_v {
                rj_item_v.push(dump(dm, sub_root, space).await?).unwrap();
            }

            rj.insert(&paper_code, rj_item_v).unwrap();
        }

        Ok(rj)
    })
}

pub fn escape_word(mut word: &str) -> String {
    if word.starts_with('\'') && word.ends_with('\'') {
        word = &word[1..word.len() - 1];
    }

    let mut rs = String::new();
    let mut pos = 0;
    while pos < word.len() {
        pos += match word[pos..].find('\\') {
            Some(offset) => {
                let ch = &word[pos + offset + 1..pos + offset + 2];
                let ch = match ch {
                    "n" => "\n",
                    "t" => "\t",
                    "s" => " ",
                    _ => ch,
                };
                rs = format!("{rs}{}{ch}", &word[pos..pos + offset]);
                offset + 2
            }
            None => {
                rs = format!("{rs}{}", &word[pos..]);
                break;
            }
        };
    }
    rs
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

#[derive(Clone, Eq, Hash, PartialEq, Debug)]
pub struct Step {
    pub arrow: String,
    pub paper: String,
    pub code: String,
}

/// root->paper:code, root->paper:code, root->paper:code
#[derive(Clone, Eq, Hash, PartialEq, Debug)]
pub struct Path {
    pub root_v: Vec<String>,
    pub step_v: Vec<Step>,
}

impl Path {
    pub fn from_str(path: &str) -> Self {
        main::from_str(path)
    }

    pub fn to_string(&self) -> String {
        main::to_string(self)
    }

    pub fn is_temp(&self) -> bool {
        if self.step_v.is_empty() {
            return false;
        }
        self.step_v.last().unwrap().paper == "$"
    }

    pub fn path_type(&self) -> PathType {
        main::path_type(self)
    }

    pub fn first_part(&self) -> PathPart {
        main::first_part(self)
    }

    /// step_v 中是否包含 paper:code
    pub fn contains(&self, paper: &str, code: &str) -> bool {
        main::contains(self, paper, code)
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        main::fmt(self, f)
    }
}

pub fn unescape_word(word: &str) -> String {
    let common_s = word
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
        .replace("\'", "\\'");
    format!("'{}'", common_s.replace(" ", "\\s"))
}

pub fn rs_2_str(rs: &[String]) -> String {
    let mut acc = String::new();

    if rs.is_empty() {
        return acc;
    }

    for i in 0..rs.len() - 1 {
        let item = &rs[i];

        acc = if item.ends_with("\\c") {
            format!("{acc}{}", &item[0..item.len() - 2])
        } else {
            format!("{acc}{item}\n")
        }
    }

    let item = rs.last().unwrap();

    acc = if item.ends_with("\\c") {
        format!("{acc}{}", &item[0..item.len() - 2])
    } else {
        format!("{acc}{item}")
    };

    acc
}

pub fn str_2_rs(s: &str) -> Vec<String> {
    let mut rs = Vec::new();

    for line in s.lines() {
        if line.len() > 500 {
            let mut start = 0;

            loop {
                let end = start + 500;

                if end >= line.len() {
                    rs.push(line[start..].to_string());

                    break;
                }

                rs.push(format!("{}\\c", &line[start..end]));

                start = end;
            }
        } else {
            rs.push(line.to_string());
        }
    }

    rs
}

pub fn gen_value() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use crate::util::escape_word;

    use super::Path;

    #[test]
    fn test_root_v() {
        let path = Path::from_str("'$->$:output\\s+\\s1\\s1','$->$:output\\s+=\\s$->$:output\\s1'");
        assert_eq!(path.root_v.len(), 2)
    }

    #[test]
    fn test_escape_word() {
        let rs = escape_word("\\wo\\nrd");
        assert_eq!(rs, "wo\nrd");
    }
}

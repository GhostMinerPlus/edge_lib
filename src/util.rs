pub fn escape_word(word: &str) -> String {
    main::escape_word(word)
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
        main::from_str(path)
    }

    pub fn to_string(&self) -> String {
        main::to_string(self)
    }

    pub fn is_temp(&self) -> bool {
        main::is_temp(self)
    }

    pub fn path_type(&self) -> PathType {
        main::path_type(self)
    }

    pub fn first_part(&self) -> PathPart {
        main::first_part(self)
    }

    /// step_v 中是否包含 code
    pub fn contains(&self, code: &str) -> bool {
        main::contains(self, code)
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        main::fmt(self, f)
    }
}

mod main {
    use super::{Path, PathPart, PathType, Step};

    pub fn fmt(this: &Path, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", to_string(this))
    }

    pub fn contains(this: &Path, code: &str) -> bool {
        for step in &this.step_v {
            if step.code == code {
                return true;
            }
        }
        false
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

    #[cfg(test)]
    mod test_escape_word {
        use super::escape_word;

        #[test]
        fn test() {
            let rs = escape_word("\\wo\\nrd");
            assert_eq!(rs, "wo\nrd");
        }
    }

    pub fn from_str(path: &str) -> Path {
        if path.is_empty() {
            return Path {
                root: String::new(),
                step_v: Vec::new(),
            };
        }
        log::debug!("Path::from_str: {path}");
        if path.starts_with('\'') && path.ends_with('\'') {
            return Path {
                root: path.to_string(),
                step_v: Vec::new(),
            };
        }
        let s = find_arrrow(path).unwrap_or(path.len());
        let root = path[0..s].to_string();
        let mut tail = &path[s..];
        let mut step_v = Vec::new();
        while !tail.is_empty() {
            let s = match find_arrrow(&tail[2..]) {
                Some(s) => s + 2,
                None => tail.len(),
            };
            step_v.push(Step {
                arrow: tail[0..2].to_string(),
                code: tail[2..s].to_string(),
            });
            tail = &tail[s..];
        }
        Path { root, step_v }
    }

    #[cfg(test)]
    mod test_from_str {
        #[test]
        fn should_from_str() {
            let path = super::from_str("$51aae06c-65e9-468a-83b5-041fd52b37fc->$proxy->path");
            assert_eq!(path.step_v.len(), 2);
        }
    }

    pub fn to_string(this: &Path) -> String {
        let mut s = this.root.clone();
        for step in &this.step_v {
            s = format!("{s}{}{}", step.arrow, step.code);
        }
        s
    }

    pub fn is_temp(this: &Path) -> bool {
        if this.step_v.is_empty() {
            return false;
        }
        this.step_v.last().unwrap().code.starts_with('$')
    }

    pub fn path_type(this: &Path) -> PathType {
        let mut cnt = 0;
        for i in 0..this.step_v.len() {
            if this.step_v[i].code.starts_with('$') {
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
        if first_step.code.starts_with('$') {
            let mut end = 1;
            for i in 1..this.step_v.len() {
                if !this.step_v[i].code.starts_with('$') {
                    break;
                }
                end += 1;
            }
            if end == this.step_v.len() {
                return PathPart::EntireTemp;
            }
            PathPart::Temp(Path {
                root: this.root.clone(),
                step_v: this.step_v[0..end].to_vec(),
            })
        } else {
            let mut end = 1;
            for i in 1..this.step_v.len() {
                if this.step_v[i].code.starts_with('$') {
                    break;
                }
                end += 1;
            }
            if end == this.step_v.len() {
                return PathPart::EntirePure;
            }
            PathPart::Pure(Path {
                root: this.root.clone(),
                step_v: this.step_v[0..end].to_vec(),
            })
        }
    }

    fn find_quotation(path: &str) -> Option<usize> {
        let pos = path.find('\'')?;
        if pos == 0 {
            return Some(0);
        }
        if &path[pos - 1..pos] == "\\" {
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

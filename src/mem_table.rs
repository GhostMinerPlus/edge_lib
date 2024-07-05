use std::collections::{BTreeMap, BTreeSet};

use crate::data::Auth;

fn insert(mp: &mut BTreeMap<(String, String), BTreeSet<u64>>, k: (String, String), v: u64) {
    if let Some(uuid_v) = mp.get_mut(&k) {
        uuid_v.insert(v);
    } else {
        let mut set = BTreeSet::new();
        set.insert(v);
        mp.insert(k, set);
    }
}

fn next_id(id: &mut u64) -> u64 {
    let new_id = *id;
    *id += 1;
    new_id
}

// Public
#[derive(Clone)]
pub struct Edge {
    pub source: String,
    pub code: String,
    pub target: String,
    pub paper: String,
    pub pen: String,
}

#[derive(Clone)]
pub struct MemTable {
    id: u64,
    edge_mp: BTreeMap<u64, Edge>,
    inx_source_code: BTreeMap<(String, String), BTreeSet<u64>>,
    inx_code_target: BTreeMap<(String, String), BTreeSet<u64>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            id: 0,
            edge_mp: BTreeMap::new(),
            inx_source_code: BTreeMap::new(),
            inx_code_target: BTreeMap::new(),
        }
    }

    pub fn insert_edge(&mut self, auth: &Auth, source: &str, code: &str, target: &str) -> u64 {
        let uuid = next_id(&mut self.id);
        let edge = match auth.clone() {
            Auth::Writer(paper, pen) => Edge {
                source: source.to_string(),
                code: code.to_string(),
                target: target.to_string(),
                paper,
                pen,
            },
            Auth::Printer(pen) => Edge {
                source: source.to_string(),
                code: code.to_string(),
                target: target.to_string(),
                paper: pen.clone(),
                pen,
            },
        };
        self.edge_mp.insert(uuid, edge);
        insert(
            &mut self.inx_source_code,
            (source.to_string(), code.to_string()),
            uuid,
        );
        insert(
            &mut self.inx_code_target,
            (code.to_string(), target.to_string()),
            uuid,
        );
        uuid
    }

    pub fn get_target_v(&mut self, auth: &Auth, source: &str, code: &str) -> Vec<String> {
        if let Some(uuid_v) = self
            .inx_source_code
            .get(&(source.to_string(), code.to_string()))
        {
            let mut arr = Vec::with_capacity(uuid_v.len());
            if auth.is_root() {
                for uuid in uuid_v {
                    let edge = &self.edge_mp[uuid];
                    arr.push(edge.target.clone());
                }
            } else {
                for uuid in uuid_v {
                    let edge = &self.edge_mp[uuid];
                    if main::check_common_auth(auth, edge) {
                        arr.push(edge.target.clone());
                    }
                }
            }
            arr
        } else {
            Vec::new()
        }
    }

    pub fn get_source_v(&mut self, auth: &Auth, code: &str, target: &str) -> Vec<String> {
        if let Some(uuid_v) = self
            .inx_code_target
            .get(&(code.to_string(), target.to_string()))
        {
            let mut arr = Vec::with_capacity(uuid_v.len());
            if auth.is_root() {
                for uuid in uuid_v {
                    let edge = &self.edge_mp[uuid];
                    arr.push(edge.source.clone());
                }
            } else {
                for uuid in uuid_v {
                    let edge = &self.edge_mp[uuid];
                    if main::check_common_auth(auth, edge) {
                        arr.push(edge.source.clone());
                    }
                }
            }
            arr
        } else {
            Vec::new()
        }
    }

    pub fn delete_edge_with_source_code(&mut self, auth: &Auth, source: &str, code: &str) {
        if let Some(uuid_v) = self
            .inx_source_code
            .remove(&(source.to_string(), code.to_string()))
        {
            if auth.is_root() {
                for uuid in &uuid_v {
                    let edge = self.edge_mp.remove(uuid).unwrap();
                    self.inx_code_target
                        .get_mut(&(edge.code, edge.target))
                        .unwrap()
                        .remove(uuid);
                }
            } else {
                let mut rest_set = BTreeSet::new();
                for uuid in &uuid_v {
                    if !main::check_common_auth(auth, &self.edge_mp[uuid]) {
                        rest_set.insert(*uuid);
                        continue;
                    }
                    let edge = self.edge_mp.remove(uuid).unwrap();
                    self.inx_code_target
                        .get_mut(&(edge.code.clone(), edge.target.clone()))
                        .unwrap()
                        .remove(uuid);
                }
                if !rest_set.is_empty() {
                    self.inx_source_code
                        .insert((source.to_string(), code.to_string()), rest_set);
                }
            }
        }
    }

    pub fn clear(&mut self, auth: &Auth) {
        if auth.is_root() {
            self.id = 0;
            self.edge_mp.clear();
            self.inx_source_code.clear();
            self.inx_code_target.clear();
        } else {
            let mut new_mp = BTreeMap::new();
            for (uuid, edge) in &self.edge_mp {
                if !main::check_common_auth(auth, edge) {
                    new_mp.insert(*uuid, edge.clone());
                } else {
                    self.inx_source_code
                        .get_mut(&(edge.source.clone(), edge.code.clone()))
                        .unwrap()
                        .remove(uuid);
                    self.inx_code_target
                        .get_mut(&(edge.code.clone(), edge.target.clone()))
                        .unwrap()
                        .remove(uuid);
                }
            }
            self.edge_mp = new_mp;
        }
    }
}

mod main {
    use crate::data::Auth;

    use super::Edge;

    pub fn check_common_auth(auth: &Auth, edge: &Edge) -> bool {
        match auth {
            Auth::Writer(paper, _) => edge.paper == *paper,
            Auth::Printer(pen) => edge.pen == *pen,
        }
    }
}

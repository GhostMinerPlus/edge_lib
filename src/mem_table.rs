use std::{collections::BTreeMap, mem::take};

fn insert(mp: &mut BTreeMap<(String, String), Vec<u64>>, k: (String, String), v: u64) {
    if let Some(uuid_v) = mp.get_mut(&k) {
        uuid_v.push(v);
    } else {
        mp.insert(k, vec![v]);
    }
}

fn next_id(id: &mut u64) -> u64 {
    *id = *id + 1;
    *id
}

// Public
#[derive(Clone)]
pub struct Edge {
    pub source: String,
    pub code: String,
    pub target: String,
    is_saved: bool,
}

#[derive(Clone)]
pub struct MemTable {
    id: u64,
    edge_mp: BTreeMap<u64, Edge>,
    inx_source_code: BTreeMap<(String, String), Vec<u64>>,
    inx_code_target: BTreeMap<(String, String), Vec<u64>>,
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

    pub fn insert_edge(&mut self, source: &str, code: &str, target: &str) -> u64 {
        let uuid = next_id(&mut self.id);
        let edge = Edge {
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
            is_saved: false,
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

    pub fn insert_temp_edge(&mut self, source: &str, code: &str, target: &str) -> u64 {
        let uuid = next_id(&mut self.id);
        let edge = Edge {
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
            is_saved: true,
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

    pub fn get_target(&self, source: &str, code: &str) -> Option<String> {
        match self
            .inx_source_code
            .get(&(source.to_string(), code.to_string()))
        {
            Some(uuid_v) => {
                let edge = &self.edge_mp[uuid_v.last().unwrap()];
                Some(edge.target.clone())
            }
            None => None,
        }
    }

    pub fn get_target_v_unchecked(&mut self, source: &str, code: &str) -> Vec<String> {
        if let Some(uuid_v) = self
            .inx_source_code
            .get(&(source.to_string(), code.to_string()))
        {
            let mut arr = Vec::with_capacity(uuid_v.len());
            for uuid in uuid_v {
                arr.push(self.edge_mp[uuid].target.clone());
            }
            arr
        } else {
            Vec::new()
        }
    }

    pub fn get_source_v_unchecked(&mut self, code: &str, target: &str) -> Vec<String> {
        if let Some(uuid_v) = self
            .inx_code_target
            .get(&(code.to_string(), target.to_string()))
        {
            let mut arr = Vec::with_capacity(uuid_v.len());
            for uuid in uuid_v {
                arr.push(self.edge_mp[uuid].source.clone());
            }
            arr
        } else {
            Vec::new()
        }
    }

    pub fn get_source(&self, code: &str, target: &str) -> Option<String> {
        match self
            .inx_code_target
            .get(&(code.to_string(), target.to_string()))
        {
            Some(uuid_v) => Some(self.edge_mp[uuid_v.last().unwrap()].source.clone()),
            None => None,
        }
    }

    pub fn take(&mut self) -> BTreeMap<u64, Edge> {
        self.id = 0;
        self.inx_source_code.clear();
        self.inx_code_target.clear();
        take(&mut self.edge_mp)
            .into_iter()
            .filter(|(_, edge)| !edge.is_saved)
            .collect()
    }

    pub fn delete_edge_with_source_code(&mut self, source: &str, code: &str) {
        if let Some(uuid_v) = self
            .inx_source_code
            .remove(&(source.to_string(), code.to_string()))
        {
            for uuid in &uuid_v {
                let edge = self.edge_mp.remove(uuid).unwrap();
                self.inx_code_target.remove(&(edge.code, edge.target));
            }
        }
    }

    pub fn delete_edge_with_code_target(&mut self, code: &str, target: &str) {
        if let Some(uuid_v) = self
            .inx_code_target
            .remove(&(code.to_string(), target.to_string()))
        {
            for uuid in &uuid_v {
                let edge = self.edge_mp.remove(uuid).unwrap();
                self.inx_source_code.remove(&(edge.source, edge.code));
            }
        }
    }

    pub fn delete_saved_edge_with_source_code(&mut self, source: &str, code: &str) {
        if let Some(uuid_v) = self
            .inx_source_code
            .get_mut(&(source.to_string(), code.to_string()))
        {
            let mut new_uuid_v = Vec::new();
            for uuid in &*uuid_v {
                if self.edge_mp[uuid].is_saved {
                    let edge = self.edge_mp.remove(uuid).unwrap();
                    self.inx_code_target.remove(&(edge.code, edge.target));
                } else {
                    new_uuid_v.push(*uuid);
                }
            }
            *uuid_v = new_uuid_v;
        }
    }

    pub fn delete_saved_edge_with_code_target(&mut self, code: &str, target: &str) {
        if let Some(uuid_v) = self
            .inx_code_target
            .get_mut(&(code.to_string(), target.to_string()))
        {
            let mut new_uuid_v = Vec::new();
            for uuid in &*uuid_v {
                if self.edge_mp[&uuid].is_saved {
                    let edge = self.edge_mp.remove(&uuid).unwrap();
                    self.inx_source_code.remove(&(edge.source, edge.code));
                } else {
                    new_uuid_v.push(*uuid);
                }
            }
            *uuid_v = new_uuid_v;
        }
    }
}

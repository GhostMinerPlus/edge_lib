use std::
    collections::{BTreeMap, BTreeSet}
;

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
    *id = *id + 1;
    *id
}

// Public
#[derive(Clone)]
pub struct Edge {
    pub source: String,
    pub code: String,
    pub target: String,
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

    pub fn insert_edge(&mut self, source: &str, code: &str, target: &str) -> u64 {
        let uuid = next_id(&mut self.id);
        let edge = Edge {
            source: source.to_string(),
            code: code.to_string(),
            target: target.to_string(),
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

    pub fn get_target_v(&mut self, source: &str, code: &str) -> Vec<String> {
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

    pub fn get_source_v(&mut self, code: &str, target: &str) -> Vec<String> {
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

    pub fn delete_edge_with_source_code(&mut self, source: &str, code: &str) {
        if let Some(uuid_v) = self
            .inx_source_code
            .remove(&(source.to_string(), code.to_string()))
        {
            for uuid in &uuid_v {
                let edge = self.edge_mp.remove(uuid).unwrap();
                self.inx_code_target
                    .get_mut(&(edge.code, edge.target))
                    .unwrap()
                    .remove(uuid);
            }
        }
    }
}

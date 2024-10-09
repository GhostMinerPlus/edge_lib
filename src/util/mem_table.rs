use std::collections::{BTreeMap, BTreeSet};

fn next_id(id: &mut u64) -> u64 {
    let new_id = *id;
    *id += 1;
    new_id
}

// Public
#[derive(Clone)]
pub struct Edge {
    pub source: String,
    pub paper: String,
    pub code: String,
    pub target: String,
}

#[derive(Clone)]
pub struct MemTable {
    id: u64,
    edge_mp: BTreeMap<u64, Edge>,
    inx_source_code: BTreeMap<(String, (String, String)), BTreeSet<u64>>,
    inx_code_target: BTreeMap<((String, String), String), BTreeSet<u64>>,
    inx_paper: BTreeMap<String, BTreeSet<u64>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            id: 0,
            edge_mp: BTreeMap::new(),
            inx_source_code: BTreeMap::new(),
            inx_code_target: BTreeMap::new(),
            inx_paper: BTreeMap::new(),
        }
    }

    pub fn insert_edge(&mut self, source: &str, paper: &str, code: &str, target: &str) -> u64 {
        log::debug!("insert edge: {source}->{code}->{target}");
        let uuid = next_id(&mut self.id);
        let edge = Edge {
            source: source.to_string(),
            paper: paper.to_string(),
            code: code.to_string(),
            target: target.to_string(),
        };
        let source_code_k = (edge.source.clone(), (edge.paper.clone(), edge.code.clone()));
        match self.inx_source_code.get_mut(&source_code_k) {
            Some(set) => {
                set.insert(uuid);
            }
            None => {
                let mut set = BTreeSet::new();
                set.insert(uuid);
                self.inx_source_code.insert(source_code_k.clone(), set);
            }
        }
        let code_target_k = ((edge.paper.clone(), edge.code.clone()), edge.target.clone());
        match self.inx_code_target.get_mut(&code_target_k) {
            Some(set) => {
                set.insert(uuid);
            }
            None => {
                let mut set = BTreeSet::new();
                set.insert(uuid);
                self.inx_code_target.insert(code_target_k.clone(), set);
            }
        }
        match self.inx_paper.get_mut(&edge.paper) {
            Some(set) => {
                set.insert(uuid);
            }
            None => {
                let mut set = BTreeSet::new();
                set.insert(uuid);
                self.inx_paper.insert(edge.paper.clone(), set);
            }
        }
        self.edge_mp.insert(uuid, edge);
        uuid
    }

    pub fn get_target_v(&self, source: &str, paper: &str, code: &str) -> Vec<String> {
        if let Some(uuid_v) = self
            .inx_source_code
            .get(&(source.to_string(), (paper.to_string(), code.to_string())))
        {
            let mut arr = Vec::with_capacity(uuid_v.len());
            for uuid in uuid_v {
                let edge = &self.edge_mp[uuid];
                arr.push(edge.target.clone());
            }
            arr
        } else {
            Vec::new()
        }
    }

    pub fn get_source_v(&self, paper: &str, code: &str, target: &str) -> Vec<String> {
        if let Some(uuid_v) = self
            .inx_code_target
            .get(&((paper.to_string(), code.to_string()), target.to_string()))
        {
            let mut arr = Vec::with_capacity(uuid_v.len());
            for uuid in uuid_v {
                let edge = &self.edge_mp[uuid];
                arr.push(edge.source.clone());
            }
            arr
        } else {
            Vec::new()
        }
    }

    pub fn delete_edge_with_source_code(&mut self, source: &str, paper: &str, code: &str) {
        if let Some(uuid_v) = self
            .inx_source_code
            .remove(&(source.to_string(), (paper.to_string(), code.to_string())))
        {
            for uuid in &uuid_v {
                let edge = self.edge_mp.remove(uuid).unwrap();
                self.inx_code_target
                    .get_mut(&((edge.paper, edge.code), edge.target))
                    .unwrap()
                    .remove(uuid);
                if let Some(uuid_v) = self.inx_paper.get_mut(paper) {
                    uuid_v.remove(uuid);
                }
            }
        }
    }

    pub fn clear_paper(&mut self, paper: &str) {
        if let Some(uuid_v) = self.inx_paper.remove(paper) {
            for uuid in &uuid_v {
                let edge = self.edge_mp.remove(uuid).unwrap();
                self.inx_source_code
                    .get_mut(&(edge.source.clone(), (edge.paper.clone(), edge.code.clone())))
                    .unwrap()
                    .remove(uuid);
                self.inx_code_target
                    .get_mut(&((edge.paper, edge.code), edge.target))
                    .unwrap()
                    .remove(uuid);
            }
        }
    }

    pub fn clear(&mut self) {
        self.id = 0;
        self.edge_mp.clear();
        self.inx_source_code.clear();
        self.inx_code_target.clear();
        self.inx_paper.clear();
    }

    pub fn get_code_v(&self, root: &str, space: &str) -> Vec<String> {
        if let Some(id_v) = self.inx_paper.get(space) {
            return id_v
                .iter()
                .filter(|id| {
                    let edge = &self.edge_mp[id];
                    edge.source == root
                })
                .map(|id| {
                    let edge = &self.edge_mp[id];
                    edge.code.clone()
                })
                .collect();
        }
        Vec::new()
    }
}

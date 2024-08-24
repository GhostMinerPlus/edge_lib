use crate::util::escape_word;

#[derive(Clone, Debug)]
pub struct Inc {
    pub output: IncValue,
    pub function: IncValue,
    pub input: IncValue,
    pub input1: IncValue,
}

#[derive(Clone, Debug)]
pub enum IncValue {
    /// -> <-
    Addr(String),
    /// $xxx
    Definition(String),
    /// Normal
    Value(String),
}

impl IncValue {
    pub fn as_mut(&mut self) -> &mut String {
        match self {
            IncValue::Addr(addr) => addr,
            IncValue::Definition(definition) => definition,
            IncValue::Value(value) => value,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            IncValue::Addr(addr) => addr,
            IncValue::Definition(definition) => definition,
            IncValue::Value(value) => value,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            IncValue::Addr(addr) => addr.clone(),
            IncValue::Definition(definition) => definition.clone(),
            IncValue::Value(value) => value.clone(),
        }
    }

    pub fn from_str(s: &str) -> Self {
        if s.starts_with('\'') && s.ends_with('\'') && !s.ends_with("\\'") {
            return Self::Value(escape_word(s));
        }
        if s.contains("->") || s.contains("<-") {
            return Self::Addr(s.to_string());
        }
        if s.starts_with('$') {
            Self::Definition(s.to_string())
        } else {
            Self::Value(s.to_string())
        }
    }

    pub fn from_string(s: String) -> Self {
        if s.starts_with('\'') && s.ends_with('\'') && !s.ends_with("\\'") {
            return Self::Value(escape_word(&s));
        }
        if s.contains("->") || s.contains("<-") {
            return Self::Addr(s);
        }
        if s.starts_with('$') {
            Self::Definition(s)
        } else {
            Self::Value(s)
        }
    }
}

use std::{cmp::min, collections::HashSet, io, pin::Pin, sync::Arc};

use rand::random;

use crate::data::AsDataManager;

// Public
pub trait AsFunc: Send + Sync {
    fn invoke(
        &self,
        dm: Arc<dyn AsDataManager>,
        input_item_v: Vec<String>,
        input1_item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>>;
}

impl<F, R> AsFunc for F
where
    F: Fn(Arc<dyn AsDataManager>, Vec<String>, Vec<String>) -> R,
    F: Send + Sync,
    R: std::future::Future<Output = io::Result<Vec<String>>> + Send + 'static,
{
    fn invoke(
        &self,
        dm: Arc<dyn AsDataManager>,
        input_item_v: Vec<String>,
        input1_item_v: Vec<String>,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<Vec<String>>> + Send>> {
        Box::pin(self(dm, input_item_v, input1_item_v))
    }
}

pub fn append(
    _: Arc<dyn AsDataManager>,
    mut input_item_v: Vec<String>,
    mut input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        input_item_v.append(&mut input1_item_v);
        Ok(input_item_v)
    }
}

pub fn distinct(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let mut set: HashSet<String> = HashSet::new();
        Ok(input_item_v
            .into_iter()
            .filter(|item| set.insert(item.clone()))
            .collect())
    }
}

pub fn left(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let mut set = HashSet::new();
        set.extend(input1_item_v);

        Ok(input_item_v
            .into_iter()
            .filter(|item| !set.contains(item))
            .collect())
    }
}

pub fn inner(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let mut set = HashSet::new();
        set.extend(input1_item_v);

        Ok(input_item_v
            .into_iter()
            .filter(|item| set.contains(item))
            .collect())
    }
}

pub fn if_(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        if input_item_v.is_empty() {
            Ok(input1_item_v)
        } else {
            Ok(input_item_v)
        }
    }
}

pub fn set(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move { Ok(input_item_v) }
}

pub fn add(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            let left = input_item_v[i].parse::<f64>();
            if left.is_err() {
                continue;
            }
            let right = input1_item_v[i].parse::<f64>();
            if right.is_err() {
                continue;
            }
            let r: f64 = left.unwrap() + right.unwrap();
            output_item_v.push(r.to_string());
        }
        Ok(output_item_v)
    }
}

pub fn minus(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            let left = input_item_v[i].parse::<f64>();
            if left.is_err() {
                continue;
            }
            let right = input1_item_v[i].parse::<f64>();
            if right.is_err() {
                continue;
            }
            let r: f64 = left.unwrap() - right.unwrap();
            output_item_v.push(r.to_string());
        }
        Ok(output_item_v)
    }
}

pub fn mul(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            let left = input_item_v[i].parse::<f64>();
            if left.is_err() {
                continue;
            }
            let right = input1_item_v[i].parse::<f64>();
            if right.is_err() {
                continue;
            }
            let r: f64 = left.unwrap() * right.unwrap();
            output_item_v.push(r.to_string());
        }
        Ok(output_item_v)
    }
}

pub fn div(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            let left = input_item_v[i].parse::<f64>();
            if left.is_err() {
                continue;
            }
            let right = input1_item_v[i].parse::<f64>();
            if right.is_err() {
                continue;
            }
            let r: f64 = left.unwrap() / right.unwrap();
            output_item_v.push(r.to_string());
        }
        Ok(output_item_v)
    }
}

pub fn rest(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            let left = input_item_v[i].parse::<i64>();
            if left.is_err() {
                continue;
            }
            let right = input1_item_v[i].parse::<i64>();
            if right.is_err() {
                continue;
            }
            let r: i64 = left.unwrap() % right.unwrap();
            output_item_v.push(r.to_string());
        }
        Ok(output_item_v)
    }
}

pub fn equal(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            if input_item_v[i] == input1_item_v[i] {
                output_item_v.push(input_item_v[i].clone());
            }
        }
        Ok(output_item_v)
    }
}

pub fn not_equal(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            if input_item_v[i] != input1_item_v[i] {
                output_item_v.push(input_item_v[i].clone());
            }
        }
        Ok(output_item_v)
    }
}

pub fn greater(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            let left = input_item_v[i].parse::<f64>();
            if left.is_err() {
                continue;
            }
            let right = input1_item_v[i].parse::<f64>();
            if right.is_err() {
                continue;
            }
            if left.unwrap() > right.unwrap() {
                output_item_v.push(input_item_v[i].clone());
            }
        }
        Ok(output_item_v)
    }
}

pub fn smaller(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            let left = input_item_v[i].parse::<f64>();
            if left.is_err() {
                continue;
            }
            let right = input1_item_v[i].parse::<f64>();
            if right.is_err() {
                continue;
            }
            if left.unwrap() < right.unwrap() {
                output_item_v.push(input_item_v[i].clone());
            }
        }
        Ok(output_item_v)
    }
}

pub fn new(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        if min(input_item_v.len(), input1_item_v.len()) != 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "need 1 but not"));
        }
        let sz = input_item_v[0]
            .parse::<i64>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut output_item_v = Vec::with_capacity(sz as usize);
        for _ in 0..sz {
            output_item_v.push(input1_item_v[0].clone());
        }
        Ok(output_item_v)
    }
}

pub fn line(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        if input_item_v.len() != 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "need 1 but not"));
        }
        let sz = input_item_v[0]
            .parse::<i64>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut output_item_v = Vec::with_capacity(sz as usize);
        for i in 0..sz {
            output_item_v.push(i.to_string());
        }
        Ok(output_item_v)
    }
}

pub fn rand(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        if input_item_v.len() != 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "need 1 but not"));
        }
        let sz = input_item_v[0]
            .parse::<i64>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut output_item_v = Vec::with_capacity(sz as usize);
        for _ in 0..sz {
            let r = random::<f64>();
            output_item_v.push(r.to_string());
        }
        Ok(output_item_v)
    }
}

pub fn count(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let mut output_item_v = Vec::new();
        output_item_v.push(input_item_v.len().to_string());
        Ok(output_item_v)
    }
}

pub fn sum(
    _: Arc<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move {
        let mut output_item_v = Vec::new();
        let mut r = 0.0;
        for input_item in &input_item_v {
            r += input_item.parse::<f64>().unwrap();
        }
        output_item_v.push(r.to_string());
        Ok(output_item_v)
    }
}

pub fn divide(
    _: Arc<dyn AsDataManager>,
    _: Vec<String>,
    _: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move { todo!() }
}

pub fn agent(
    _: Arc<dyn AsDataManager>,
    _: Vec<String>,
    _: Vec<String>,
) -> impl std::future::Future<Output = io::Result<Vec<String>>> + Send {
    async move { todo!() }
}

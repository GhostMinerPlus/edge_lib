use std::{
    cmp::min,
    collections::HashSet,
    io,
    pin::Pin,
    sync::Arc,
};

use rand::random;

use crate::{data::AsDataManager, Path};

// Public
pub trait AsFunc: Send + Sync {
    fn invoke(
        &self,
        dm: Arc<dyn AsDataManager>,
        output: Path,
        input: Path,
        input1: Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;
}

impl<F, R> AsFunc for F
where
    F: Fn(Arc<dyn AsDataManager>, Path, Path, Path) -> R,
    F: Send + Sync,
    R: std::future::Future<Output = io::Result<()>> + Send + 'static,
{
    fn invoke(
        &self,
        dm: Arc<dyn AsDataManager>,
        output: Path,
        input: Path,
        input1: Path,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>> {
        Box::pin(self(dm, output, input, input1))
    }
}

pub fn append(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let mut input1_item_v = dm.get(&input1).await?;
        if output == input {
            dm.append(&output, input1_item_v).await
        } else {
            let mut input_item_v = dm.get(&input).await?;
            input_item_v.append(&mut input1_item_v);
            dm.set(&output, input_item_v).await
        }
    }
}

pub fn distinct(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    _: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let mut set: HashSet<String> = HashSet::new();
        let input_item_v = dm.get(&input).await?;
        dm.set(
            &output,
            input_item_v
                .into_iter()
                .filter(|item| set.insert(item.clone()))
                .collect(),
        )
        .await
    }
}

pub fn left(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
        let mut set = HashSet::new();
        set.extend(input1_item_v);

        dm.set(
            &output,
            input_item_v
                .into_iter()
                .filter(|item| !set.contains(item))
                .collect(),
        )
        .await
    }
}

pub fn inner(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
        let mut set = HashSet::new();
        set.extend(input1_item_v);

        dm.set(
            &output,
            input_item_v
                .into_iter()
                .filter(|item| set.contains(item))
                .collect(),
        )
        .await
    }
}

pub fn if_(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
        let rs = if input_item_v.is_empty() {
            input1_item_v
        } else {
            input_item_v
        };
        dm.set(&output, rs).await
    }
}

pub fn set(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    _: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        dm.set(&output, input_item_v).await
    }
}

pub fn add(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn minus(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn mul(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn div(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn rest(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn equal(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            if input_item_v[i] == input1_item_v[i] {
                output_item_v.push(input_item_v[i].clone());
            }
        }
        dm.set(&output, output_item_v).await
    }
}

pub fn not_equal(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
        let sz = min(input_item_v.len(), input1_item_v.len());
        let mut output_item_v = Vec::with_capacity(sz);
        for i in 0..sz {
            if input_item_v[i] != input1_item_v[i] {
                output_item_v.push(input_item_v[i].clone());
            }
        }
        dm.set(&output, output_item_v).await
    }
}

pub fn greater(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn smaller(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn new(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let input1_item_v = dm.get(&input1).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn line(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    _: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn rand(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    _: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
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
        dm.set(&output, output_item_v).await
    }
}

pub fn count(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    _: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let mut output_item_v = Vec::new();
        output_item_v.push(input_item_v.len().to_string());
        dm.set(&output, output_item_v).await
    }
}

pub fn sum(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    _: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        let mut output_item_v = Vec::new();
        let mut r = 0.0;
        for input_item in &input_item_v {
            r += input_item.parse::<f64>().unwrap();
        }
        output_item_v.push(r.to_string());
        dm.set(&output, output_item_v).await
    }
}

pub fn slice(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    input1: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let input_item_v = dm.get(&input).await?;
        if input_item_v.is_empty() {
            return Err(io::Error::other("when $slice:\n\rno input"));
        }
        let input1_item_v = dm.get(&input1).await?;
        if input1_item_v.len() < 2 {
            return Err(io::Error::other("when $slice:\n\rno input1"));
        }
        let start = input1_item_v[0]
            .parse::<usize>()
            .map_err(|e| io::Error::other(e))?;
        let end = input1_item_v[1]
            .parse::<usize>()
            .map_err(|e| io::Error::other(e))?;
        dm.set(&output, input_item_v[start..end].to_vec()).await
    }
}

pub fn sort(
    dm: Arc<dyn AsDataManager>,
    output: Path,
    input: Path,
    _: Path,
) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        let mut input_item_v = dm.get(&input).await?;
        input_item_v.sort();
        dm.set(&output, input_item_v).await
    }
}

use std::{cmp::min, collections::HashSet, io};

use rand::random;

use crate::{data::AsDataManager, util::Path};

pub async fn append(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
    let mut input1_item_v = dm.get(&input1).await?;
    if output == input {
        dm.append(&output, input1_item_v).await
    } else {
        let mut input_item_v = dm.get(&input).await?;
        input_item_v.append(&mut input1_item_v);
        dm.set(&output, input_item_v).await
    }
}

pub async fn distinct(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    _: Path,
) -> io::Result<()> {
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

pub async fn left(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn inner(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn if_(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
    let input_item_v = dm.get(&input).await?;
    let input1_item_v = dm.get(&input1).await?;
    let rs = if input_item_v.is_empty() {
        input1_item_v
    } else {
        input_item_v
    };
    dm.set(&output, rs).await
}

pub async fn if_0(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
    let input_item_v = dm.get(&input).await?;
    let input1_item_v = dm.get(&input1).await?;
    let rs = if !input_item_v.is_empty() {
        vec![]
    } else {
        input1_item_v
    };
    dm.set(&output, rs).await
}

pub async fn if_1(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
    let input_item_v = dm.get(&input).await?;
    let input1_item_v = dm.get(&input1).await?;
    let rs = if input_item_v.is_empty() {
        input_item_v
    } else {
        input1_item_v
    };
    dm.set(&output, rs).await
}

pub async fn set(dm: &dyn AsDataManager, output: Path, input: Path, _: Path) -> io::Result<()> {
    let input_item_v = dm.get(&input).await?;
    dm.set(&output, input_item_v).await
}

pub async fn add(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn minus(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn mul(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn div(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn rest(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn equal(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn not_equal(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn greater(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn smaller(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn new(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
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

pub async fn line(dm: &dyn AsDataManager, output: Path, input: Path, _: Path) -> io::Result<()> {
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

pub async fn rand(dm: &dyn AsDataManager, output: Path, input: Path, _: Path) -> io::Result<()> {
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

pub async fn count(dm: &dyn AsDataManager, output: Path, input: Path, _: Path) -> io::Result<()> {
    let input_item_v = dm.get(&input).await?;
    let mut output_item_v = Vec::new();
    output_item_v.push(input_item_v.len().to_string());
    dm.set(&output, output_item_v).await
}

pub async fn sum(dm: &dyn AsDataManager, output: Path, input: Path, _: Path) -> io::Result<()> {
    let input_item_v = dm.get(&input).await?;
    let mut output_item_v = Vec::new();
    let mut r = 0.0;
    for input_item in &input_item_v {
        r += input_item.parse::<f64>().unwrap();
    }
    output_item_v.push(r.to_string());
    dm.set(&output, output_item_v).await
}

pub async fn slice(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    input1: Path,
) -> io::Result<()> {
    let input_item_v = dm.get(&input).await?;
    if input_item_v.is_empty() {
        return Err(io::Error::other("no input\nwhen slice"));
    }
    let input1_item_v = dm.get(&input1).await?;
    if input1_item_v.len() < 2 {
        return Err(io::Error::other("no input1\nwhen slice"));
    }
    let start = input1_item_v[0]
        .parse::<usize>()
        .map_err(|e| io::Error::other(e))?;
    let end = input1_item_v[1]
        .parse::<usize>()
        .map_err(|e| io::Error::other(e))?;
    dm.set(&output, input_item_v[start..end].to_vec()).await
}

pub async fn sort(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    order: Path,
) -> io::Result<()> {
    let input_item_v = dm.get(&input).await?;
    let order_v = dm.get(&order).await?;
    if input_item_v.len() != order_v.len() {
        return Err(io::Error::other("not the same length\nwhen sort"));
    }
    let mut temp = input_item_v
        .into_iter()
        .enumerate()
        .map(|(i, item)| (item, order_v[i].parse().unwrap()))
        .collect::<Vec<(String, f64)>>();
    temp.sort_by(|p, q| {
        if p.1 == q.1 {
            std::cmp::Ordering::Equal
        } else if p.1 > q.1 {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        }
    });
    dm.set(&output, temp.into_iter().map(|(item, _)| item).collect())
        .await
}

pub async fn sort_s(
    dm: &dyn AsDataManager,
    output: Path,
    input: Path,
    order: Path,
) -> io::Result<()> {
    let input_item_v = dm.get(&input).await?;
    let order_v = dm.get(&order).await?;
    if input_item_v.len() != order_v.len() {
        return Err(io::Error::other("not the same length\nwhen sort"));
    }
    let mut temp = input_item_v
        .into_iter()
        .enumerate()
        .map(|(i, item)| (item, &order_v[i]))
        .collect::<Vec<(String, &String)>>();
    temp.sort_by(|p, q| p.1.cmp(q.1));
    dm.set(&output, temp.into_iter().map(|(item, _)| item).collect())
        .await
}

use std::{cmp::min, collections::HashSet, io};

use rand::random;

use crate::data::AsDataManager;

// Public
pub async fn append(
    _: &mut Box<dyn AsDataManager>,
    mut input_item_v: Vec<String>,
    mut input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    input_item_v.append(&mut input1_item_v);
    Ok(input_item_v)
}

pub async fn distinct(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> io::Result<Vec<String>> {
    let mut set: HashSet<String> = HashSet::new();
    Ok(input_item_v
        .into_iter()
        .filter(|item| set.insert(item.clone()))
        .collect())
}

pub async fn left(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    let mut set = HashSet::new();
    set.extend(input1_item_v);

    Ok(input_item_v
        .into_iter()
        .filter(|item| !set.contains(item))
        .collect())
}

pub async fn inner(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    let mut set = HashSet::new();
    set.extend(input1_item_v);

    Ok(input_item_v
        .into_iter()
        .filter(|item| set.contains(item))
        .collect())
}

pub async fn if_(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    if input_item_v.is_empty() {
        Ok(input1_item_v)
    } else {
        Ok(input_item_v)
    }
}

pub async fn set(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> io::Result<Vec<String>> {
    Ok(input_item_v)
}

// pub async fn sort(
//     dm: &mut Box<dyn AsDataManager>,
//     input_item_v: Vec<String>,
//     _: Vec<String>,
// ) -> io::Result<Vec<String>> {
//     let mut temp_item_v = Vec::with_capacity(input_item_v.len());
//     for input_item in &input_item_v {
//         let no = dm.get_target(input_item, "$no").await?;
//         temp_item_v.push((input_item.clone(), no));
//     }
//     temp_item_v.sort_by(|p, q| p.1.cmp(&q.1));
//     let output_item_v = temp_item_v.into_iter().map(|item| item.0).collect();
//     Ok(output_item_v)
// }

pub async fn add(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn minus(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn mul(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn div(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn rest(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn equal(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    let sz = min(input_item_v.len(), input1_item_v.len());
    let mut output_item_v = Vec::with_capacity(sz);
    for i in 0..sz {
        if input_item_v[i] == input1_item_v[i] {
            output_item_v.push(input_item_v[i].clone());
        }
    }
    Ok(output_item_v)
}

pub async fn not_equal(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
    let sz = min(input_item_v.len(), input1_item_v.len());
    let mut output_item_v = Vec::with_capacity(sz);
    for i in 0..sz {
        if input_item_v[i] != input1_item_v[i] {
            output_item_v.push(input_item_v[i].clone());
        }
    }
    Ok(output_item_v)
}

pub async fn greater(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn smaller(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn new(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    input1_item_v: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn line(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn rand(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> io::Result<Vec<String>> {
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

pub async fn count(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> io::Result<Vec<String>> {
    let mut output_item_v = Vec::new();
    output_item_v.push(input_item_v.len().to_string());
    Ok(output_item_v)
}

pub async fn sum(
    _: &mut Box<dyn AsDataManager>,
    input_item_v: Vec<String>,
    _: Vec<String>,
) -> io::Result<Vec<String>> {
    let mut output_item_v = Vec::new();
    let mut r = 0.0;
    for input_item in &input_item_v {
        r += input_item.parse::<f64>().unwrap();
    }
    output_item_v.push(r.to_string());
    Ok(output_item_v)
}

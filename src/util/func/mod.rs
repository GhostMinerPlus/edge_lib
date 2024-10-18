use std::{cmp::min, collections::HashSet, future::Future, pin::Pin};

use rand::random;

use crate::{err, util::Path};

use super::data::AsDataManager;

mod inner {
    use std::collections::HashSet;

    pub fn inner(input_item_v: Vec<String>, input1_item_v: Vec<String>) -> Vec<String> {
        let mut set = HashSet::new();
        set.extend(input1_item_v);

        input_item_v
            .into_iter()
            .filter(|item| set.contains(item))
            .collect()
    }
}

pub async fn append<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let mut input1_item_v = dm.get(input1).await?;
    if output == input {
        dm.append(output, input1_item_v).await
    } else {
        let mut input_item_v = dm.get(input).await?;
        input_item_v.append(&mut input1_item_v);
        dm.set(output, input_item_v).await
    }
}

#[allow(unused)]
pub async fn distinct<DM>(
    dm: &mut DM,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let mut set: HashSet<String> = HashSet::new();
    let input_item_v = dm.get(input).await?;
    dm.set(
        output,
        input_item_v
            .into_iter()
            .filter(|item| set.insert(item.clone()))
            .collect(),
    )
    .await
}

pub async fn left<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
    let mut set = HashSet::new();
    set.extend(input1_item_v);

    dm.set(
        output,
        input_item_v
            .into_iter()
            .filter(|item| !set.contains(item))
            .collect(),
    )
    .await
}

pub async fn inner<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;

    dm.set(output, inner::inner(input_item_v, input1_item_v))
        .await
}

pub async fn if_<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
    let rs = if input_item_v.is_empty() {
        input1_item_v
    } else {
        input_item_v
    };
    dm.set(output, rs).await
}

pub async fn if_0<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
    let rs = if !input_item_v.is_empty() {
        vec![]
    } else {
        input1_item_v
    };
    dm.set(output, rs).await
}

pub async fn if_1<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
    let rs = if input_item_v.is_empty() {
        input_item_v
    } else {
        input1_item_v
    };
    dm.set(output, rs).await
}

#[allow(unused)]
pub async fn set<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    dm.set(output, input_item_v).await
}

pub async fn add<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
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
    dm.set(output, output_item_v).await
}

pub async fn minus<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
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
    dm.set(output, output_item_v).await
}

pub async fn mul<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
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
    dm.set(output, output_item_v).await
}

pub async fn div<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
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
    dm.set(output, output_item_v).await
}

pub async fn rest<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
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
    dm.set(output, output_item_v).await
}

pub async fn equal<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
    let sz = min(input_item_v.len(), input1_item_v.len());
    let mut output_item_v = Vec::with_capacity(sz);
    for i in 0..sz {
        if input_item_v[i] == input1_item_v[i] {
            output_item_v.push(input_item_v[i].clone());
        }
    }
    dm.set(output, output_item_v).await
}

pub async fn not_equal<DM>(
    dm: &mut DM,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
    let sz = min(input_item_v.len(), input1_item_v.len());
    let mut output_item_v = Vec::with_capacity(sz);
    for i in 0..sz {
        if input_item_v[i] != input1_item_v[i] {
            output_item_v.push(input_item_v[i].clone());
        }
    }
    dm.set(output, output_item_v).await
}

pub async fn greater<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
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
    dm.set(output, output_item_v).await
}

pub async fn smaller<DM>(dm: &mut DM, output: &Path, input: &Path, input1: &Path) -> err::Result<()>
where
    DM: AsDataManager + Sync + Send + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
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
    dm.set(output, output_item_v).await
}

pub async fn new(
    dm: &mut dyn AsDataManager,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()> {
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
    if min(input_item_v.len(), input1_item_v.len()) != 1 {
        return Err(err::Error::new(
            err::ErrorKind::Other,
            format!("need 1 but not"),
        ));
    }
    let sz = input_item_v[0]
        .parse::<i64>()
        .map_err(|e| err::Error::new(err::ErrorKind::Other, e.to_string()))?;
    let mut output_item_v = Vec::with_capacity(sz as usize);
    for _ in 0..sz {
        output_item_v.push(input1_item_v[0].clone());
    }
    dm.set(output, output_item_v).await
}

#[allow(unused)]
pub async fn line(
    dm: &mut dyn AsDataManager,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()> {
    let input_item_v = dm.get(input).await?;
    if input_item_v.len() != 1 {
        return Err(err::Error::new(
            err::ErrorKind::Other,
            format!("need 1 but not"),
        ));
    }
    let sz = input_item_v[0]
        .parse::<i64>()
        .map_err(|e| err::Error::new(err::ErrorKind::Other, e.to_string()))?;
    let mut output_item_v = Vec::with_capacity(sz as usize);
    for i in 0..sz {
        output_item_v.push(i.to_string());
    }
    dm.set(output, output_item_v).await
}

#[allow(unused)]
pub async fn rand(
    dm: &mut dyn AsDataManager,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()> {
    let input_item_v = dm.get(input).await?;
    if input_item_v.len() != 1 {
        return Err(err::Error::new(
            err::ErrorKind::Other,
            format!("need 1 but not, when checking",),
        ));
    }
    let sz = input_item_v[0]
        .parse::<i64>()
        .map_err(|e| err::Error::new(err::ErrorKind::Other, e.to_string()))?;
    let mut output_item_v = Vec::with_capacity(sz as usize);
    for _ in 0..sz {
        let r = random::<f64>();
        output_item_v.push(r.to_string());
    }
    dm.set(output, output_item_v).await
}

#[allow(unused)]
pub async fn count(
    dm: &mut dyn AsDataManager,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()> {
    let input_item_v = dm.get(input).await?;
    let mut output_item_v = Vec::new();
    output_item_v.push(input_item_v.len().to_string());
    dm.set(output, output_item_v).await
}

#[allow(unused)]
pub async fn sum(
    dm: &mut dyn AsDataManager,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()> {
    let input_item_v = dm.get(input).await?;
    let mut output_item_v = Vec::new();
    let mut r = 0.0;
    for input_item in input_item_v {
        r += input_item.parse::<f64>().unwrap();
    }
    output_item_v.push(r.to_string());
    dm.set(output, output_item_v).await
}

pub async fn slice(
    dm: &mut dyn AsDataManager,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()> {
    let input_item_v = dm.get(input).await?;
    if input_item_v.is_empty() {
        return Err(err::Error::new(
            err::ErrorKind::Other,
            "no input\nwhen slice".to_string(),
        ));
    }
    let input1_item_v = dm.get(input1).await?;
    if input1_item_v.len() < 2 {
        return Err(err::Error::new(
            err::ErrorKind::Other,
            "no input1\nwhen slice".to_string(),
        ));
    }
    let start = input1_item_v[0]
        .parse::<usize>()
        .map_err(|e| err::Error::new(err::ErrorKind::Other, e.to_string()))?;
    let end = input1_item_v[1]
        .parse::<usize>()
        .map_err(|e| err::Error::new(err::ErrorKind::Other, e.to_string()))?;
    dm.set(output, input_item_v[start..end].to_vec()).await
}

pub async fn sort(
    dm: &mut dyn AsDataManager,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()> {
    let input_item_v = dm.get(input).await?;
    let order_v = dm.get(input1).await?;
    if input_item_v.len() != order_v.len() {
        return Err(err::Error::new(
            err::ErrorKind::Other,
            "not the same length\nwhen sort".to_string(),
        ));
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
    dm.set(output, temp.into_iter().map(|(item, _)| item).collect())
        .await
}

pub async fn sort_s(
    dm: &mut dyn AsDataManager,
    output: &Path,
    input: &Path,
    input1: &Path,
) -> err::Result<()> {
    let input_item_v = dm.get(input).await?;
    let order_v = dm.get(input1).await?;
    if input_item_v.len() != order_v.len() {
        return Err(err::Error::new(
            err::ErrorKind::Other,
            "not the same length\nwhen sort".to_string(),
        ));
    }
    let mut temp = input_item_v
        .into_iter()
        .enumerate()
        .map(|(i, item)| (item, &order_v[i]))
        .collect::<Vec<(String, &String)>>();
    temp.sort_by(|p, q| p.1.cmp(q.1));
    dm.set(output, temp.into_iter().map(|(item, _)| item).collect())
        .await
}

pub fn dump<'a1, 'a2, 'a3, 'a4, 'f>(
    dm: &'a1 mut dyn AsDataManager,
    output: &'a2 Path,
    input: &'a3 Path,
    input1: &'a4 Path,
) -> Pin<Box<dyn Future<Output = err::Result<()>> + Send + 'f>>
where
    'a1: 'f,
    'a2: 'f,
    'a3: 'f,
    'a4: 'f,
{
    Box::pin(async move {
        // root
        let root_v = dm.get(input).await?;
        // type name
        let space_v = dm.get(input1).await?;

        // rj
        let mut rj = json::array![];
        if space_v.is_empty() {
            for root in root_v {
                rj.push(json::JsonValue::String(root)).unwrap();
            }
        } else {
            for root in &root_v {
                rj.push(crate::util::dump(dm, root, &space_v[0]).await?)
                    .unwrap();
            }
        }

        // rs
        let rs = crate::util::str_2_rs(&rj.to_string());

        // set
        dm.set(output, rs).await
    })
}

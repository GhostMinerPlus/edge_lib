use std::{cmp::min, collections::HashSet, io};

use rand::random;

use crate::util::{
    data::{AsDataManager, AsTempDataManager},
    Path,
};

mod inner {
    use std::{collections::HashSet, future::Future, io, pin::Pin};

    use crate::util::{
        data::{AsDataManager, AsTempDataManager},
        Path,
    };

    pub fn inner(input_item_v: Vec<String>, input1_item_v: Vec<String>) -> Vec<String> {
        let mut set = HashSet::new();
        set.extend(input1_item_v);

        input_item_v
            .into_iter()
            .filter(|item| set.contains(item))
            .collect()
    }

    pub fn dump<'a1, 'a2, 'a3, 'a4, 'f, DM>(
        dm: &'a1 DM,
        root: &'a2 str,
        space: &'a3 str,
    ) -> Pin<Box<impl Future<Output = io::Result<json::JsonValue>> + 'f>>
    where
        DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
        'a1: 'f,
        'a2: 'f,
        'a3: 'f,
        'a4: 'f,
    {
        Box::pin(async move {
            let code_v = dm.get_code_v(root, space).await?;

            if code_v.is_empty() {
                return Ok(json::JsonValue::String(root.to_string()));
            }

            let mut rj = json::object! {};

            for code in &code_v {
                let mut rj_item_v = json::array![];

                let sub_root_v = dm
                    .get(&Path::from_str(&format!("{root}->{space}:{code}")))
                    .await?;

                for sub_root in &sub_root_v {
                    rj_item_v.push(dump(dm, sub_root, space).await?).unwrap();
                }

                rj.insert(code, rj_item_v).unwrap();
            }

            Ok(rj)
        })
    }
}

pub async fn append<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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
pub async fn distinct<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn left<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn inner<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;

    dm.set(output, inner::inner(input_item_v, input1_item_v))
        .await
}

pub async fn if_<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn if_0<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn if_1<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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
pub async fn set<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    dm.set(output, input_item_v).await
}

pub async fn add<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn minus<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn mul<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn div<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn rest<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn equal<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn not_equal<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn greater<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn smaller<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
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

pub async fn new<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let input1_item_v = dm.get(input1).await?;
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
    dm.set(output, output_item_v).await
}

#[allow(unused)]
pub async fn line<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
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
    dm.set(output, output_item_v).await
}

#[allow(unused)]
pub async fn rand<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    if input_item_v.len() != 1 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "need 1 but not, when checking"));
    }
    let sz = input_item_v[0]
        .parse::<i64>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut output_item_v = Vec::with_capacity(sz as usize);
    for _ in 0..sz {
        let r = random::<f64>();
        output_item_v.push(r.to_string());
    }
    dm.set(output, output_item_v).await
}

#[allow(unused)]
pub async fn count<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let mut output_item_v = Vec::new();
    output_item_v.push(input_item_v.len().to_string());
    dm.set(output, output_item_v).await
}

#[allow(unused)]
pub async fn sum<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let mut output_item_v = Vec::new();
    let mut r = 0.0;
    for input_item in input_item_v {
        r += input_item.parse::<f64>().unwrap();
    }
    output_item_v.push(r.to_string());
    dm.set(output, output_item_v).await
}

pub async fn slice<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    if input_item_v.is_empty() {
        return Err(io::Error::other("no input\nwhen slice"));
    }
    let input1_item_v = dm.get(input1).await?;
    if input1_item_v.len() < 2 {
        return Err(io::Error::other("no input1\nwhen slice"));
    }
    let start = input1_item_v[0]
        .parse::<usize>()
        .map_err(|e| io::Error::other(e))?;
    let end = input1_item_v[1]
        .parse::<usize>()
        .map_err(|e| io::Error::other(e))?;
    dm.set(output, input_item_v[start..end].to_vec()).await
}

pub async fn sort<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let order_v = dm.get(input1).await?;
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
    dm.set(output, temp.into_iter().map(|(item, _)| item).collect())
        .await
}

pub async fn sort_s<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let input_item_v = dm.get(input).await?;
    let order_v = dm.get(input1).await?;
    if input_item_v.len() != order_v.len() {
        return Err(io::Error::other("not the same length\nwhen sort"));
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

pub async fn dump<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
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
            rj.push(inner::dump(dm, root, &space_v[0]).await?).unwrap();
        }
    }

    // rs
    let rs = crate::util::str_2_rs(&rj.to_string());

    // set
    dm.set(output, rs).await
}

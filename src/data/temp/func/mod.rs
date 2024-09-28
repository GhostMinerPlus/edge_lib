use std::io;

use crate::util::Path;

use super::{AsDataManager, AsTempDataManager};

pub async fn dump<DM>(dm: &DM, output: &Path, input: &Path, input1: &Path) -> io::Result<()>
where
    DM: AsTempDataManager + Sync + Send + 'static + ?Sized,
{
    let root_v = dm.get(input).await?;
    let path_v = dm.get(input1).await?;
    // ## rj
    let rj = json::array![];

    // # rs
    let mut rs = Vec::new();
    for line in rj.to_string().lines() {
        if line.len() > 500 {
            let mut start = 0;
            loop {
                let end = start + 500;
                if end >= line.len() {
                    rs.push(line[start..].to_string());
                    break;
                }
                rs.push(format!("{}\\c", &line[start..end]));
                start = end;
            }
        } else {
            rs.push(line.to_string());
        }
    }
    // set
    dm.set(output, rs).await
}

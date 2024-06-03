use deltalake::kernel::{models::Add, Action::Add as ActionAdd};
use deltalake::operations::transaction::FinalizedCommit;
use deltalake::operations::transaction::CommitBuilder;
use deltalake::protocol::DeltaOperation;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;

use deltalake::{open_table, DeltaTable};

#[tokio::main]
async fn main() {
    let table_location = "./my_table";

    let table = open_table(&table_location).await.unwrap();

    let new_files = get_new_files(&table_location).await;
    let new_add : Vec<_>= new_files.iter().map(create_add).map(ActionAdd).collect();

    let finalized_commit = commit_new_files(new_add, table).await;
    match finalized_commit {
        Some(x) => println!("Version # : {:?}", x.version),
        _ => println!("No new files")
    }

    println!("Complete");
}

async fn get_new_files(table_location: &str) -> Vec<PathBuf> {
    let t = open_table(table_location).await.unwrap();
    let existing = t.get_file_uris().unwrap().collect::<Vec<String>>().join("");

    let mut parquet_files = vec![];

    for path in fs::read_dir(table_location).unwrap() {
        let path = path.unwrap().path();
        if let Some("parquet") = path.extension().and_then(OsStr::to_str) {
            parquet_files.push(path.to_owned());
        }
    }
    parquet_files
        .into_iter()
        .filter(|x| !existing.contains(x.file_name().unwrap().to_str().unwrap()))
        .collect::<Vec<_>>()
}

fn create_add(file: &PathBuf) -> Add {
    let file_name = file.file_name().unwrap().to_str().unwrap().to_string();
    let file_size = file.metadata().unwrap().len();
    let file_modified_metadata = file
        .metadata()
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    Add {
        path: file_name,
        partition_values: HashMap::new(),
        size: file_size as i64,
        modification_time: file_modified_metadata as i64,
        data_change: true,
        stats: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
        stats_parsed: None,
    }
}


async fn commit_new_files(a: Vec<deltalake::kernel::Action>, table: DeltaTable)-> Option<FinalizedCommit> {
    if a.len() == 0 {
        return None
    }
    let temp_thing = table.clone();
    let table_reference = temp_thing.state.unwrap();
    let c = CommitBuilder::default().with_actions(a);
    let log_it = table.log_store();
    let pre_commit = c.build(
        Some(&table_reference),
        log_it,
        DeltaOperation::Update { predicate: None },
    );
     Some(pre_commit.expect("Finalize Fail").await.unwrap())
}

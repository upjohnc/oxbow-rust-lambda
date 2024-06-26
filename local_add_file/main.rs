use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use core::panic;
use deltalake::arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef};
use deltalake::kernel::{models::Add, Action::Add as ActionAdd, Schema, StructField};
use deltalake::operations::transaction::{CommitBuilder, FinalizedCommit};
use deltalake::parquet::arrow::{
    async_reader::ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use deltalake::protocol::DeltaOperation;
use deltalake::storage::object_store::local::LocalFileSystem;
use deltalake::{open_table, DeltaOps, DeltaTable, DeltaTableError, ObjectMeta, Path as DtlPath};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, DirEntry};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

#[tokio::main]
async fn main() {
    let table_location = "./my_table";
    enum TableOutput {
        FinalizedCommit(FinalizedCommit),
        DeltaTable(DeltaTable),
    }

    let table: TableOutput = match open_table(&table_location).await {
        Ok(the_table) => {
            let new_files = get_new_files(&table_location).await;
            let new_add: Vec<_> = new_files.iter().map(create_add).map(ActionAdd).collect();

            let finalized_commit = commit_new_files(new_add, the_table).await;
            TableOutput::FinalizedCommit(finalized_commit.expect("Commit should work"))
        }
        Err(DeltaTableError::InvalidTableLocation(_)) | Err(DeltaTableError::NotATable(_)) => {
            let columns = get_columns_schema(PathBuf::from(table_location)).await;
            let new_files = get_all_files(&table_location);
            let actions: Vec<_> = new_files.iter().map(create_add).map(ActionAdd).collect();
            let output = DeltaOps::try_from_uri(&table_location)
                .await
                .unwrap()
                .create()
                .with_columns(columns)
                .with_actions(actions)
                .await
                .unwrap();
            TableOutput::DeltaTable(output)
        }
        Err(err) => panic!("{:?}", err),
    };

    let table_version = match table {
        TableOutput::FinalizedCommit(x) => x.version,
        TableOutput::DeltaTable(y) => y.version(),
    };
    println!("Version # : {}", table_version);
    println!("Complete");
}

fn get_all_files(table_location: &str) -> Vec<PathBuf> {
    let mut parquet_files = vec![];
    for path in fs::read_dir(table_location).unwrap() {
        let path = path.unwrap().path();
        if let Some("parquet") = path.extension().and_then(OsStr::to_str) {
            parquet_files.push(path.to_owned());
        }
    }
    parquet_files
}

async fn get_new_files(table_location: &str) -> Vec<PathBuf> {
    let t = open_table(table_location).await.unwrap();
    let existing = t.get_file_uris().unwrap().collect::<Vec<String>>().join("");

    let parquet_files = get_all_files(table_location);

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

async fn commit_new_files(
    a: Vec<deltalake::kernel::Action>,
    table: DeltaTable,
) -> Option<FinalizedCommit> {
    if a.len() == 0 {
        return None;
    }
    let temp_table = table.clone();
    let table_reference = temp_table.state.unwrap();
    let commit_builder = CommitBuilder::default().with_actions(a);
    let log_it = table.log_store();
    let pre_commit = commit_builder.build(
        Some(&table_reference),
        log_it,
        DeltaOperation::Update { predicate: None },
    );
    Some(pre_commit.expect("Finalize Fail").await.unwrap())
}

#[derive(Clone, Debug)]
struct PathMeta {
    file_path: PathBuf,
    size: u64,
}

impl From<DirEntry> for PathMeta {
    fn from(x: DirEntry) -> Self {
        Self {
            file_path: x.path(),
            size: x.metadata().unwrap().len(),
        }
    }
}

// todo: change name
fn my_thing(dir: PathBuf) -> Option<Vec<PathMeta>> {
    let result_list_dir = fs::read_dir(dir.as_path())
        .unwrap()
        .into_iter()
        .map(|x| PathMeta::from(x.unwrap()))
        .collect::<Vec<_>>();
    if result_list_dir.len() == 0 {
        return None;
    }

    Some(result_list_dir)
}
fn find_smallest_file(files: Vec<PathMeta>, dir: PathBuf) -> Option<(DtlPath, PathMeta)> {
    if files.len() == 0 {
        return None;
    }
    let mut smallest = files[0].clone();
    for file in files {
        if file.size < smallest.size {
            smallest = file.clone();
        }
    }
    let file_name = match smallest.file_path.strip_prefix(dir.as_path()) {
        Ok(x) => x.to_str().unwrap(),
        Err(_) => smallest.file_path.as_path().to_str().unwrap(),
    };
    Some((DtlPath::parse(file_name).unwrap(), smallest))
}

async fn get_schema(dir_prefix: PathBuf) -> SchemaRef {
    let files = my_thing(dir_prefix.clone()).expect("Expect files in the directory");
    let (file_path, file_meta) =
        find_smallest_file(files, dir_prefix.clone()).expect("Expect parsed path");
    let local_file =
        Arc::new(LocalFileSystem::new_with_prefix(dir_prefix.clone()).expect("Local System thing"));

    let d = NaiveDate::from_ymd_opt(2015, 6, 3).unwrap();
    let t = NaiveTime::from_hms_milli_opt(12, 34, 56, 789).unwrap();
    let ndt = NaiveDateTime::new(d, t);
    let dt = DateTime::from_naive_utc_and_offset(ndt, Utc);

    let file_size = file_meta.size;
    let object_meta = ObjectMeta {
        location: file_path,
        last_modified: dt,
        size: file_size as usize,
        e_tag: None,
        version: None,
    };
    let parquet_object = ParquetObjectReader::new(local_file, object_meta);
    ParquetRecordBatchStreamBuilder::new(parquet_object)
        .await
        .unwrap()
        .build()
        .unwrap()
        .schema()
        .clone()
}

fn coerce_field(
    field: deltalake::arrow::datatypes::FieldRef,
) -> deltalake::arrow::datatypes::FieldRef {
    match field.data_type() {
        deltalake::arrow::datatypes::DataType::Timestamp(unit, tz) => match unit {
            deltalake::arrow::datatypes::TimeUnit::Millisecond => {
                println!("I have been asked to create a table with a Timestamp(millis) column ({}) that I cannot handle. Cowardly setting the Delta schema to pretend it is a Timestamp(micros)", field.name());
                let field = deltalake::arrow::datatypes::Field::new(
                    field.name(),
                    deltalake::arrow::datatypes::DataType::Timestamp(
                        deltalake::arrow::datatypes::TimeUnit::Microsecond,
                        tz.clone(),
                    ),
                    field.is_nullable(),
                );
                return Arc::new(field);
            }
            _ => {}
        },
        _ => {}
    };
    field.clone()
}

async fn get_columns_schema(table_prefix: PathBuf) -> Vec<StructField> {
    let arrow_schema_1 = get_schema(table_prefix).await;

    let mut conversions: Vec<Arc<Field>> = vec![];

    for field in arrow_schema_1.fields().iter() {
        conversions.push(coerce_field(field.clone()));
    }

    let arrow_schema = ArrowSchema::new_with_metadata(conversions, arrow_schema_1.metadata.clone());

    let schema = Schema::try_from(&arrow_schema);

    let columns = schema.unwrap().fields().clone();
    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_extra::{copy_items, dir::CopyOptions};
    use std::fs::canonicalize;
    use std::path::Path;

    #[tokio::test]
    async fn test_get_schema() {
        // todo : add test
        assert_eq!(1, 1);
    }

    #[tokio::test]
    async fn test_get_all_files() {
        let dir_base = "tests/data_local_file";
        let table_path = "parquet_files";
        let path = canonicalize(&format!("../{}/{}", dir_base, table_path))
            .expect("Failed to canonicalize");
        let dir = tempfile::tempdir().expect("Failed to create temp dir");

        let options = CopyOptions::new();
        let _ = copy_items(&[path.as_path()], dir.path(), &options).expect("Failed to copy items");

        let result = get_all_files(dir.path().join(Path::new(table_path)).to_str().unwrap());

        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_get_new_files() {
        let dir_base = "tests/data_local_file";
        let table_path = "parquet_files_with_table";
        let path = canonicalize(&format!("../{}/{}", dir_base, table_path))
            .expect("Failed to canonicalize");
        let dir = tempfile::tempdir().expect("Failed to create temp dir");

        let options = CopyOptions::new();
        let table_path = Path::new(table_path);
        let _ = copy_items(&[path.as_path()], dir.path(), &options);

        let result = get_new_files(&dir.path().join(table_path).to_str().unwrap()).await;

        assert_eq!(1, result.len());
    }

    #[test]
    fn test_path_meta_from() {
        let result_list_dir = fs::read_dir(Path::new("../tests/fs_file"))
            .unwrap()
            .into_iter()
            .map(|x| PathMeta::from(x.unwrap()))
            .collect::<Vec<_>>();

        assert_eq!(1, result_list_dir.len());

        let test_element = &result_list_dir[0];
        assert_eq!(test_element.size, 1149);
        assert_eq!(
            test_element.file_path,
            PathBuf::from("../tests/fs_file/feed_1.parquet")
        );
    }

    #[test]
    fn test_path_meta_from_dir() {
        let result_list_dir = my_thing(PathBuf::from("../tests/fs_file")).unwrap();
        assert_eq!(1, result_list_dir.len());

        let test_element = &result_list_dir[0];
        assert_eq!(test_element.size, 1149);
        assert_eq!(
            test_element.file_path,
            PathBuf::from("../tests/fs_file/feed_1.parquet")
        );
    }

    #[test]
    fn test_smallest_file() {
        let dir = PathBuf::from("../tests/fs_smallest");
        let files = my_thing(dir.clone()).unwrap();
        let (_, result_file) = find_smallest_file(files, dir).unwrap();
        assert_eq!(
            result_file.file_path,
            PathBuf::from("../tests/fs_smallest/small.txt")
        );
    }
}

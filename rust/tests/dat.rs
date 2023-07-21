use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionContext};
use deltalake::open_table;
use serde::Deserialize;
use std::path::Path;
use std::sync::Arc;
pub type TestResult = Result<(), Box<dyn std::error::Error + 'static>>;
use std::sync::Once;

static INIT: Once = Once::new();

fn initialize() {
    INIT.call_once(setup::run);
}

pub mod setup {
    //! Build script for DAT
    use std::fs::File;
    use std::io::{BufReader, BufWriter, Write};
    use std::path::Path;

    use flate2::read::GzDecoder;
    use tar::Archive;

    const DAT_EXISTS_FILE_CHECK: &str = "tests/data/dat/v0.0.2/.done";
    const VERSION: &str = "0.0.2";
    pub const OUTPUT_FOLDER: &str = "tests/data/dat/v0.0.2";

    pub fn run() {
        if dat_exists() {
            return;
        }

        let tarball_data = download_dat_files();
        extract_tarball(tarball_data);
        write_done_file();
    }

    fn dat_exists() -> bool {
        Path::new(DAT_EXISTS_FILE_CHECK).exists()
    }

    fn download_dat_files() -> Vec<u8> {
        let tarball_url = format!(
        "https://github.com/delta-incubator/dat/releases/download/v{version}/deltalake-dat-v{version}.tar.gz",
        version = VERSION
    );

        let response = ureq::get(&tarball_url).call().unwrap();
        let mut tarball_data: Vec<u8> = Vec::new();
        response
            .into_reader()
            .read_to_end(&mut tarball_data)
            .unwrap();

        tarball_data
    }

    fn extract_tarball(tarball_data: Vec<u8>) {
        let tarball = GzDecoder::new(BufReader::new(&tarball_data[..]));
        let mut archive = Archive::new(tarball);
        std::fs::create_dir_all(OUTPUT_FOLDER).expect("Failed to create output directory");
        archive
            .unpack(OUTPUT_FOLDER)
            .expect("Failed to unpack tarball");
    }

    fn write_done_file() {
        let mut done_file = BufWriter::new(
            File::create(DAT_EXISTS_FILE_CHECK).expect("Failed to create .done file"),
        );
        write!(done_file, "done").expect("Failed to write .done file");
    }
}

/// Utility for comparing a delta table
/// with a dataframe.
async fn deltaeq(ctx: &SessionContext, delta_ctx_name: &str, expected: DataFrame) -> bool {
    let delta_df = ctx.table(delta_ctx_name).await.unwrap();
    let delta_df_count = delta_df.clone().count().await.unwrap();
    let counts_eq = delta_df_count == expected.clone().count().await.unwrap();
    if counts_eq {
        let intersecting_table = &delta_df.intersect(expected).unwrap();
        intersecting_table.clone().count().await.unwrap() == delta_df_count
    } else {
        false
    }
}
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct TableVersionMetadata {
    version: i64,
    properties: serde_json::Value,
    min_reader_version: i64,
    min_writer_version: i64,
}

#[macro_export]
macro_rules! dat_test {
    ($( $test_name:ident $test:literal),*) => {
        $(
#[tokio::test]
async fn $test_name() -> TestResult {
    initialize();
    let test_case = Path::new($test);
    let root = &format!("{output_folder}/out/reader_tests/generated", output_folder=setup::OUTPUT_FOLDER);
    let root = Path::new(root).join(test_case);
    let actual_path = root.join(Path::new("delta"));
    let expected_path_root = root.join(Path::new("expected"));
    let actual = open_table(&actual_path.to_str().unwrap()).await?;
    let max_verison = actual.version();
    for version in (0..=max_verison).rev() {
        let ctx: SessionContext = SessionContext::new();
        let vstring = match max_verison == version {
            true => "latest".to_owned(),
            false => "v".to_owned() + &version.to_string(),
        };

        let expected_path = expected_path_root
            .join(Path::new(&vstring))
            .join(Path::new("table_content"));
        if !expected_path.exists(){
            continue;
        }
        let expected_metadata_path = expected_path_root.join(Path::new(&vstring)).join(Path::new("table_version_metadata.json"));
        let expected_metadata_rdr = std::fs::File::open(expected_metadata_path.to_str().unwrap())?;

        let expected_metadata: TableVersionMetadata = serde_json::from_reader(expected_metadata_rdr)?;
        let expected = ctx.read_parquet(expected_path.to_str().unwrap(), ParquetReadOptions::default()).await?;
        let mut actual = open_table(&actual_path.to_str().unwrap()).await?;
        if actual.version() != version{
            actual.load_version(version).await?;
        }
        assert!(expected_metadata.version == actual.version());
        ctx.register_table("actual", Arc::new(actual))?;
        assert!(deltaeq(&ctx, "actual", expected).await);
    }
    Ok(())
}

        )*
    }
}

dat_test!(
    test_all_primitive_types "all_primitive_types",
    test_basic_append "basic_append",
    test_basic_partitioned "basic_partitioned",
    // test_multi_partitioned "multi_partitioned",
    // test_multi_partitioned_2 "multi_partitioned_2",
    // test_nested_types "nested_types",
    test_no_replay "no_replay",
    // test_no_stats "no_stats",
    test_stats_as_struct "stats_as_struct",
    test_with_checkpoint "with_checkpoint",
    test_with_schema_change "with_schema_change"
);

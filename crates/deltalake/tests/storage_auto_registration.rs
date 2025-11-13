//! Smoke tests ensuring storage backends register their URI schemes automatically when the
//! corresponding feature is enabled.

use deltalake::{table::builder::DeltaTableBuilder, DeltaResult};
use url::Url;

#[allow(dead_code)]
fn builder_for(uri: &str) -> DeltaResult<DeltaTableBuilder> {
    let url = Url::parse(uri).expect("static test URI must parse");
    DeltaTableBuilder::from_uri(url)
}

#[cfg(feature = "gcs")]
mod gcs {
    use super::*;

    #[test]
    fn recognizes_basic_gs_uri() {
        builder_for("gs://test-bucket/table")
            .expect("gs:// scheme should be recognized when the gcs feature is enabled");
    }

    #[test]
    fn accepts_nested_paths() {
        builder_for("gs://bucket/nested/path")
            .expect("nested gs:// paths should also be recognized");
    }
}

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
mod s3 {
    use super::*;

    #[test]
    fn recognizes_s3_uri() {
        builder_for("s3://bucket/table").expect("s3:// scheme should be registered");
    }

    #[test]
    fn recognizes_s3a_uri() {
        builder_for("s3a://bucket/table").expect("s3a:// scheme should be registered");
    }
}

#[cfg(feature = "azure")]
mod azure {
    use super::*;

    #[test]
    fn recognizes_azure_abfss_uri() {
        builder_for("abfss://container@account.dfs.core.windows.net/table")
            .expect("abfss:// scheme should be registered");
    }
}

#[cfg(feature = "hdfs")]
mod hdfs {
    use super::*;

    #[test]
    fn recognizes_hdfs_uri() {
        builder_for("hdfs://namenode:8020/path/to/table")
            .expect("hdfs:// scheme should be registered");
    }
}

#[cfg(feature = "lakefs")]
mod lakefs {
    use super::*;

    #[test]
    fn recognizes_lakefs_uri() {
        builder_for("lakefs://repo/branch/table").expect("lakefs:// scheme should be registered");
    }
}

#[cfg(feature = "unity-experimental")]
mod unity {
    use super::*;

    #[test]
    fn recognizes_unity_uri() {
        builder_for("uc://main.catalog/schema.table").expect("uc:// scheme should be registered");
    }
}

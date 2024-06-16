/**
 * The deltalake crate is currently just a meta-package shim for deltalake-core
 */
pub use deltalake_core::*;

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
pub use deltalake_aws as aws;
#[cfg(feature = "azure")]
pub use deltalake_azure as azure;
#[cfg(feature = "gcs")]
pub use deltalake_gcp as gcp;
#[cfg(feature = "hdfs")]
pub use deltalake_hdfs as hdfs;

/**
 * The deltalake crate is currently just a meta-package shim for deltalake-core
 */
pub use deltalake_core::*;

#[cfg(not(any(feature = "rustls", feature = "native-tls")))]
compile_error!("You must enable at least one of the features: `rustls` or `native-tls`.");

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
pub use deltalake_aws as aws;
#[cfg(feature = "azure")]
pub use deltalake_azure as azure;
#[cfg(feature = "unity-experimental")]
pub use deltalake_catalog_unity as unity_catalog;
#[cfg(feature = "gcs")]
pub use deltalake_gcp as gcp;
#[cfg(feature = "hdfs")]
pub use deltalake_hdfs as hdfs;
#[cfg(feature = "lakefs")]
pub use deltalake_lakefs as lakefs;

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
mod __deltalake_auto_register_s3 {
    #[ctor::ctor]
    fn register() {
        crate::aws::register_handlers(None);
    }
}

#[cfg(feature = "azure")]
mod __deltalake_auto_register_azure {
    #[ctor::ctor]
    fn register() {
        crate::azure::register_handlers(None);
    }
}

#[cfg(feature = "gcs")]
mod __deltalake_auto_register_gcs {
    #[ctor::ctor]
    fn register() {
        crate::gcp::register_handlers(None);
    }
}

#[cfg(feature = "hdfs")]
mod __deltalake_auto_register_hdfs {
    #[ctor::ctor]
    fn register() {
        crate::hdfs::register_handlers(None);
    }
}

#[cfg(feature = "lakefs")]
mod __deltalake_auto_register_lakefs {
    #[ctor::ctor]
    fn register() {
        crate::lakefs::register_handlers(None);
    }
}

#[cfg(feature = "unity-experimental")]
mod __deltalake_auto_register_unity {
    #[ctor::ctor]
    fn register() {
        crate::unity_catalog::register_handlers(None);
    }
}

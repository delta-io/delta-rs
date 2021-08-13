use std::fmt;

/// Struct describing an object stored in GCS.
#[derive(Debug)]
pub struct GCSObject<'a> {
    /// The bucket where the object is stored.
    pub bucket: tame_gcs::BucketName<'a>,
    /// The path of the object within the bucket.
    pub path: tame_gcs::ObjectName<'a>,
}

impl<'a> GCSObject<'a> {
    //// Create a new GCSObject from a bucket and path.
    pub(crate) fn new(bucket: &'a str, path: &'a str) -> Self {
        // We do not validate the input strings here
        // as it is expected that they are correctly parsed and validated a level up in the
        // storage module
        GCSObject {
            bucket: tame_gcs::BucketName::non_validated(bucket),
            path: tame_gcs::ObjectName::non_validated(path),
        }
    }
}

impl<'a> fmt::Display for GCSObject<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "gs://{}/{}", self.bucket, self.path)
    }
}

impl<'a> AsRef<tame_gcs::BucketName<'a>> for GCSObject<'a> {
    fn as_ref(&self) -> &tame_gcs::BucketName<'a> {
        &self.bucket
    }
}

impl<'a> AsRef<tame_gcs::ObjectName<'a>> for GCSObject<'a> {
    fn as_ref(&self) -> &tame_gcs::ObjectName<'a> {
        &self.path
    }
}

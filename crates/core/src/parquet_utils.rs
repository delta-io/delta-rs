use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub(crate) fn default_writer_properties(compression: Compression) -> WriterProperties {
    WriterProperties::builder()
        .set_created_by(format!("delta-rs version {}", crate::crate_version()))
        .set_compression(compression)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::schema::types::ColumnPath;

    #[test]
    fn default_writer_properties_sets_created_by_and_compression() {
        let writer_properties = default_writer_properties(Compression::SNAPPY);

        assert_eq!(
            writer_properties.created_by(),
            format!("delta-rs version {}", crate::crate_version())
        );
        assert_eq!(
            writer_properties.compression(&ColumnPath::from("id")),
            Compression::SNAPPY
        );
    }
}

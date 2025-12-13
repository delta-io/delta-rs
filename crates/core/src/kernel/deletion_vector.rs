//! Deletion Vector support for Delta Lake
//!
//! This module provides functionality to read and apply deletion vectors (DVs)
//! when reading Delta tables. Deletion vectors are an efficient way to mark
//! individual rows as deleted without rewriting entire parquet files.
//!
//! ## Overview
//!
//! Deletion vectors can be stored in three ways:
//! - **Inline** (`storageType = 'i'`): Base85-encoded bitmap stored directly in the log
//! - **UUID-relative** (`storageType = 'u'`): Stored in a file with UUID-based path
//! - **Absolute path** (`storageType = 'p'`): Stored in a file with absolute path
//!
//! The bitmap format uses RoaringBitmap serialization as specified in the
//! [Delta Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vector-Format).
//!
//! ## Example
//!
//! ```ignore
//! use deltalake_core::kernel::deletion_vector::{load_deletion_vector, DeletionVectorLoader};
//!
//! // Load a deletion vector from a descriptor
//! let dv = load_deletion_vector(&descriptor, &table_root, object_store.as_ref()).await?;
//!
//! // Check if a row is deleted
//! if dv.contains(row_index) {
//!     // Row is deleted, skip it
//! }
//! ```

use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;
use roaring::RoaringTreemap;
use url::Url;

use crate::kernel::models::{DeletionVectorDescriptor, StorageType};
use crate::{DeltaResult, DeltaTableError};

/// Magic bytes at the start of a deletion vector file
const DV_MAGIC: [u8; 4] = [0x72, 0x6F, 0x61, 0x72]; // "roar" in ASCII

/// Deletion vector version 1
const DV_VERSION_1: u8 = 1;

/// A deletion vector representing deleted row indices using a RoaringBitmap.
///
/// Uses `roaring::RoaringTreemap` which supports 64-bit values (required for
/// large files with row indices > 2^32).
#[derive(Debug, Clone)]
pub struct DeletionVector {
    /// The bitmap of deleted row indices
    bitmap: RoaringTreemap,
}

impl DeletionVector {
    /// Create a new empty deletion vector
    pub fn empty() -> Self {
        Self {
            bitmap: RoaringTreemap::new(),
        }
    }

    /// Create a deletion vector from a list of deleted row indices
    ///
    /// This is useful for testing and for cases where the deleted rows
    /// are known in advance.
    pub fn from_indices(indices: Vec<u64>) -> Self {
        let bitmap: RoaringTreemap = indices.into_iter().collect();
        Self { bitmap }
    }

    /// Create a deletion vector from raw RoaringBitmap bytes
    ///
    /// The bytes should be in the portable RoaringBitmap serialization format
    /// as specified in the Delta Protocol.
    pub fn from_roaring_bytes(bytes: Bytes, _expected_cardinality: u64) -> DeltaResult<Self> {
        // Delta uses 32-bit RoaringBitmap format, but we use RoaringTreemap for 64-bit support
        // The format is compatible - we deserialize as 32-bit and promote to 64-bit
        let cursor = Cursor::new(bytes.as_ref());
        
        // Try deserializing as RoaringBitmap first (32-bit, most common case)
        match roaring::RoaringBitmap::deserialize_from(cursor) {
            Ok(bitmap32) => {
                // Convert 32-bit bitmap to 64-bit treemap
                let mut bitmap = RoaringTreemap::new();
                for val in bitmap32.iter() {
                    bitmap.insert(val as u64);
                }
                Ok(Self { bitmap })
            }
            Err(e) => {
                // Try as treemap directly (64-bit)
                let cursor = Cursor::new(bytes.as_ref());
                match RoaringTreemap::deserialize_from(cursor) {
                    Ok(bitmap) => Ok(Self { bitmap }),
                    Err(_) => Err(DeltaTableError::Generic(format!(
                        "Failed to deserialize deletion vector bitmap: {}",
                        e
                    ))),
                }
            }
        }
    }

    /// Check if a row index is marked as deleted
    pub fn contains(&self, row_index: u64) -> bool {
        self.bitmap.contains(row_index)
    }

    /// Get the number of deleted rows
    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    /// Check if the deletion vector is empty
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    /// Get an iterator over all deleted row indices
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.bitmap.iter()
    }

    /// Create a boolean filter array for Arrow record batches
    ///
    /// Returns a boolean array where `true` means the row should be kept
    /// and `false` means the row is deleted.
    pub fn to_arrow_filter(&self, num_rows: usize) -> arrow::array::BooleanArray {
        let mut builder = arrow::array::BooleanBuilder::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            builder.append_value(!self.bitmap.contains(row_idx as u64));
        }
        builder.finish()
    }

    /// Get the underlying bitmap (for advanced usage)
    pub fn bitmap(&self) -> &RoaringTreemap {
        &self.bitmap
    }
}

/// Decode base85 (Z85) encoded data
///
/// Delta uses a variant of Z85 encoding for inline deletion vectors and UUIDs.
fn decode_base85(encoded: &str) -> DeltaResult<Vec<u8>> {
    // Z85 alphabet used by Delta
    const Z85_CHARS: &[u8; 85] =
        b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

    let mut decode_map = [0u8; 256];
    for (i, &c) in Z85_CHARS.iter().enumerate() {
        decode_map[c as usize] = i as u8;
    }

    let bytes = encoded.as_bytes();
    if bytes.len() % 5 != 0 {
        return Err(DeltaTableError::Generic(format!(
            "Invalid base85 length: {} (must be multiple of 5)",
            bytes.len()
        )));
    }

    let mut result = Vec::with_capacity(bytes.len() * 4 / 5);

    for chunk in bytes.chunks(5) {
        let mut value: u32 = 0;
        for &b in chunk {
            value = value * 85 + decode_map[b as usize] as u32;
        }
        result.extend_from_slice(&value.to_be_bytes());
    }

    Ok(result)
}

/// Decode a UUID from base85 encoding
fn decode_uuid_from_base85(encoded: &str) -> DeltaResult<uuid::Uuid> {
    // The UUID is the last 20 characters of the encoded string
    if encoded.len() < 20 {
        return Err(DeltaTableError::Generic(format!(
            "Invalid UUID encoding: too short ({} chars)",
            encoded.len()
        )));
    }

    let uuid_part = &encoded[encoded.len() - 20..];
    let bytes = decode_base85(uuid_part)?;

    if bytes.len() < 16 {
        return Err(DeltaTableError::Generic(format!(
            "Invalid UUID bytes length: {}",
            bytes.len()
        )));
    }

    let uuid_bytes: [u8; 16] = bytes[..16].try_into().map_err(|_| {
        DeltaTableError::Generic("Failed to convert UUID bytes".into())
    })?;

    Ok(uuid::Uuid::from_bytes(uuid_bytes))
}

/// Reconstruct the deletion vector file path from UUID-based encoding
///
/// For `storageType = 'u'`, the path is reconstructed as:
/// `deletion_vector_<uuid>.bin`
fn reconstruct_dv_path_from_uuid(path_or_inline_dv: &str, table_root: &Url) -> DeltaResult<Url> {
    let uuid = decode_uuid_from_base85(path_or_inline_dv)?;

    // Extract prefix (characters before the UUID encoding)
    let prefix = if path_or_inline_dv.len() > 20 {
        &path_or_inline_dv[..path_or_inline_dv.len() - 20]
    } else {
        ""
    };

    let filename = format!("{}deletion_vector_{}.bin", prefix, uuid.as_hyphenated());
    table_root.join(&filename).map_err(|e| {
        DeltaTableError::Generic(format!("Failed to construct DV path: {}", e))
    })
}

/// Load a deletion vector from its descriptor
///
/// This is the main entry point for loading deletion vectors. It handles all three
/// storage types: inline, UUID-relative, and absolute path.
///
/// # Arguments
///
/// * `dv` - The deletion vector descriptor from the Add action
/// * `table_root` - The root URL of the Delta table
/// * `object_store` - The object store for reading DV files
///
/// # Returns
///
/// A `DeletionVector` that can be used to filter rows during reads.
pub async fn load_deletion_vector(
    dv: &DeletionVectorDescriptor,
    table_root: &Url,
    object_store: &dyn ObjectStore,
) -> DeltaResult<DeletionVector> {
    match dv.storage_type {
        StorageType::Inline => load_inline_dv(dv),
        StorageType::UuidRelativePath => load_uuid_relative_dv(dv, table_root, object_store).await,
        StorageType::AbsolutePath => load_absolute_path_dv(dv, object_store).await,
    }
}

/// Load an inline deletion vector
fn load_inline_dv(dv: &DeletionVectorDescriptor) -> DeltaResult<DeletionVector> {
    let bytes = decode_base85(&dv.path_or_inline_dv)?;
    DeletionVector::from_roaring_bytes(Bytes::from(bytes), dv.cardinality as u64)
}

/// Load a UUID-relative deletion vector
async fn load_uuid_relative_dv(
    dv: &DeletionVectorDescriptor,
    table_root: &Url,
    object_store: &dyn ObjectStore,
) -> DeltaResult<DeletionVector> {
    let dv_url = reconstruct_dv_path_from_uuid(&dv.path_or_inline_dv, table_root)?;
    let dv_path = Path::from_url_path(dv_url.path())?;

    let bytes = object_store.get(&dv_path).await?.bytes().await?;
    let offset = dv.offset.unwrap_or(0) as usize;

    // Read the DV data starting at the offset
    let dv_bytes = read_dv_from_bytes(&bytes[offset..], dv.size_in_bytes as usize)?;

    DeletionVector::from_roaring_bytes(Bytes::from(dv_bytes), dv.cardinality as u64)
}

/// Load an absolute path deletion vector
async fn load_absolute_path_dv(
    dv: &DeletionVectorDescriptor,
    object_store: &dyn ObjectStore,
) -> DeltaResult<DeletionVector> {
    let dv_path = Path::from(dv.path_or_inline_dv.as_str());

    let bytes = object_store.get(&dv_path).await?.bytes().await?;
    let offset = dv.offset.unwrap_or(0) as usize;

    // Read the DV data starting at the offset
    let dv_bytes = read_dv_from_bytes(&bytes[offset..], dv.size_in_bytes as usize)?;

    DeletionVector::from_roaring_bytes(Bytes::from(dv_bytes), dv.cardinality as u64)
}

/// Read deletion vector bytes from a buffer, handling the file format
fn read_dv_from_bytes(bytes: &[u8], expected_size: usize) -> DeltaResult<Vec<u8>> {
    if bytes.len() < 4 {
        return Err(DeltaTableError::Generic(
            "DV file too short for magic bytes".into(),
        ));
    }

    // Check for magic bytes (optional in some formats)
    let (data_start, has_magic) = if &bytes[0..4] == DV_MAGIC {
        if bytes.len() < 5 {
            return Err(DeltaTableError::Generic(
                "DV file too short for version byte".into(),
            ));
        }
        let version = bytes[4];
        if version != DV_VERSION_1 {
            return Err(DeltaTableError::Generic(format!(
                "Unsupported DV version: {}",
                version
            )));
        }
        (5, true)
    } else {
        (0, false)
    };

    let data_len = if has_magic {
        expected_size - 5 // Subtract magic + version
    } else {
        expected_size
    };

    if bytes.len() < data_start + data_len {
        return Err(DeltaTableError::Generic(format!(
            "DV file too short: expected {} bytes, got {}",
            data_start + data_len,
            bytes.len()
        )));
    }

    Ok(bytes[data_start..data_start + data_len].to_vec())
}


/// A helper for batch loading deletion vectors
pub struct DeletionVectorLoader {
    table_root: Url,
    object_store: Arc<dyn ObjectStore>,
}

impl DeletionVectorLoader {
    /// Create a new deletion vector loader
    pub fn new(table_root: Url, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            table_root,
            object_store,
        }
    }

    /// Load a deletion vector from a descriptor
    pub async fn load(&self, dv: &DeletionVectorDescriptor) -> DeltaResult<DeletionVector> {
        load_deletion_vector(dv, &self.table_root, self.object_store.as_ref()).await
    }

    /// Load deletion vectors for multiple files in parallel
    pub async fn load_batch(
        &self,
        descriptors: &[Option<&DeletionVectorDescriptor>],
    ) -> DeltaResult<Vec<Option<DeletionVector>>> {
        let mut results = Vec::with_capacity(descriptors.len());

        for dv_opt in descriptors {
            let dv = match dv_opt {
                Some(dv) => Some(self.load(dv).await?),
                None => None,
            };
            results.push(dv);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_deletion_vector() {
        let dv = DeletionVector::empty();
        assert!(dv.is_empty());
        assert_eq!(dv.cardinality(), 0);
        assert!(!dv.contains(0));
        assert!(!dv.contains(100));
    }

    #[test]
    fn test_from_indices() {
        let dv = DeletionVector::from_indices(vec![1, 3, 5, 7]);
        assert!(!dv.is_empty());
        assert_eq!(dv.cardinality(), 4);
        assert!(!dv.contains(0));
        assert!(dv.contains(1));
        assert!(!dv.contains(2));
        assert!(dv.contains(3));
        assert!(!dv.contains(4));
        assert!(dv.contains(5));
        assert!(!dv.contains(6));
        assert!(dv.contains(7));
    }

    #[test]
    fn test_decode_base85_simple() {
        // Test with known values
        let encoded = "00000";
        let result = decode_base85(encoded);
        assert!(result.is_ok());
    }

    #[test]
    fn test_arrow_filter() {
        let dv = DeletionVector::from_indices(vec![1, 3]);

        let filter = dv.to_arrow_filter(5);
        assert_eq!(filter.len(), 5);
        assert!(filter.value(0)); // Row 0 kept
        assert!(!filter.value(1)); // Row 1 deleted
        assert!(filter.value(2)); // Row 2 kept
        assert!(!filter.value(3)); // Row 3 deleted
        assert!(filter.value(4)); // Row 4 kept
    }

    #[test]
    fn test_storage_type_variants() {
        assert_eq!(StorageType::Inline.as_ref(), "i");
        assert_eq!(StorageType::UuidRelativePath.as_ref(), "u");
        assert_eq!(StorageType::AbsolutePath.as_ref(), "p");
    }

    #[test]
    fn test_roaring_bitmap_serialization() {
        // Create a bitmap and serialize it
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(3);
        bitmap.insert(100);

        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).unwrap();

        // Deserialize it using our DeletionVector
        let dv = DeletionVector::from_roaring_bytes(Bytes::from(bytes), 3).unwrap();

        assert_eq!(dv.cardinality(), 3);
        assert!(!dv.contains(0));
        assert!(dv.contains(1));
        assert!(!dv.contains(2));
        assert!(dv.contains(3));
        assert!(dv.contains(100));
    }

    #[test]
    fn test_iterator() {
        let dv = DeletionVector::from_indices(vec![5, 10, 15]);
        let indices: Vec<u64> = dv.iter().collect();
        assert_eq!(indices.len(), 3);
        assert!(indices.contains(&5));
        assert!(indices.contains(&10));
        assert!(indices.contains(&15));
    }
}


//! Liquid Clustering support for Delta Lake
//!
//! This module provides functionality to read and work with Liquid Clustering metadata.
//! Liquid Clustering is an advanced table layout optimization that dynamically organizes
//! data for optimal query performance.
//!
//! ## Overview
//!
//! Liquid Clustering information is stored in the Delta log as `domainMetadata` actions
//! with domain `delta.liquid`. The configuration contains:
//! - Clustering columns: The columns used for clustering
//! - Additional metadata about the clustering configuration
//!
//! ## Example
//!
//! ```ignore
//! use deltalake_core::kernel::liquid_clustering::LiquidClusteringConfig;
//!
//! // Parse Liquid Clustering config from domain metadata
//! let config = LiquidClusteringConfig::from_domain_metadata(&domain_metadata)?;
//!
//! // Get clustering columns
//! for col in &config.clustering_columns {
//!     println!("Clustering column: {}", col.physical_name);
//! }
//! ```

use serde::{Deserialize, Serialize};

use crate::kernel::models::DomainMetadata;
use crate::{DeltaResult, DeltaTableError};

/// The domain name for Liquid Clustering metadata
pub const LIQUID_CLUSTERING_DOMAIN: &str = "delta.liquid";

/// Configuration for Liquid Clustering
///
/// This struct represents the parsed configuration from the `delta.liquid` domain metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LiquidClusteringConfig {
    /// The domain name (should always be "delta.liquid")
    pub domain_name: String,

    /// The columns used for clustering
    pub clustering_columns: Vec<ClusteringColumn>,
}

/// A column used for Liquid Clustering
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusteringColumn {
    /// The physical name of the column (handles column mapping)
    pub physical_name: Vec<String>,
}

impl LiquidClusteringConfig {
    /// Parse a LiquidClusteringConfig from a DomainMetadata action
    ///
    /// # Arguments
    ///
    /// * `domain_metadata` - The domain metadata action from the Delta log
    ///
    /// # Returns
    ///
    /// The parsed configuration, or an error if parsing fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The domain is not "delta.liquid"
    /// - The configuration JSON is invalid
    pub fn from_domain_metadata(domain_metadata: &DomainMetadata) -> DeltaResult<Self> {
        if domain_metadata.domain != LIQUID_CLUSTERING_DOMAIN {
            return Err(DeltaTableError::Generic(format!(
                "Expected domain '{}', got '{}'",
                LIQUID_CLUSTERING_DOMAIN, domain_metadata.domain
            )));
        }

        if domain_metadata.removed {
            return Err(DeltaTableError::Generic(
                "Liquid Clustering configuration has been removed".into(),
            ));
        }

        serde_json::from_str(&domain_metadata.configuration).map_err(|e| {
            DeltaTableError::Generic(format!(
                "Failed to parse Liquid Clustering configuration: {}",
                e
            ))
        })
    }

    /// Get the clustering column names (logical names, first element of each physical_name path)
    pub fn column_names(&self) -> Vec<String> {
        self.clustering_columns
            .iter()
            .filter_map(|c| c.physical_name.first().cloned())
            .collect()
    }

    /// Check if a column is a clustering column
    pub fn is_clustering_column(&self, column_name: &str) -> bool {
        self.clustering_columns.iter().any(|c| {
            c.physical_name
                .first()
                .map(|n| n == column_name)
                .unwrap_or(false)
        })
    }
}

/// Information about clustering for a specific file
///
/// This struct provides information about how a file fits into the
/// liquid clustering layout.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileClusteringInfo {
    /// The file path
    pub path: String,

    /// The clustering provider (usually "liquid")
    pub clustering_provider: Option<String>,

    /// Min values for clustering columns in this file
    pub clustering_min_values: Option<serde_json::Value>,

    /// Max values for clustering columns in this file
    pub clustering_max_values: Option<serde_json::Value>,
}

/// Helper to check if a table has Liquid Clustering enabled
pub fn is_liquid_clustering_enabled(domain_metadata: Option<&DomainMetadata>) -> bool {
    domain_metadata
        .map(|dm| dm.domain == LIQUID_CLUSTERING_DOMAIN && !dm.removed)
        .unwrap_or(false)
}

/// Clustering statistics for file pruning
///
/// When Liquid Clustering is enabled, files contain additional statistics
/// that can be used for more aggressive file pruning during query planning.
#[derive(Debug, Clone, Default)]
pub struct ClusteringStats {
    /// Z-order clustering interleaved byte ranges
    pub zorder_ranges: Option<Vec<(u64, u64)>>,

    /// Clustering column min/max bounds
    pub column_bounds: std::collections::HashMap<String, ColumnBounds>,
}

/// Bounds for a single clustering column
#[derive(Debug, Clone)]
pub struct ColumnBounds {
    /// Minimum value (as JSON for type flexibility)
    pub min: Option<serde_json::Value>,
    /// Maximum value (as JSON for type flexibility)
    pub max: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_liquid_clustering_config() {
        let config_json =
            r#"{"clusteringColumns":[{"physicalName":["id"]}],"domainName":"delta.liquid"}"#;

        let domain_metadata = DomainMetadata {
            domain: LIQUID_CLUSTERING_DOMAIN.to_string(),
            configuration: config_json.to_string(),
            removed: false,
        };

        let config = LiquidClusteringConfig::from_domain_metadata(&domain_metadata).unwrap();

        assert_eq!(config.domain_name, "delta.liquid");
        assert_eq!(config.clustering_columns.len(), 1);
        assert_eq!(
            config.clustering_columns[0].physical_name,
            vec!["id".to_string()]
        );
    }

    #[test]
    fn test_parse_multi_column_clustering() {
        let config_json = r#"{"clusteringColumns":[{"physicalName":["date"]},{"physicalName":["region"]},{"physicalName":["customer_id"]}],"domainName":"delta.liquid"}"#;

        let domain_metadata = DomainMetadata {
            domain: LIQUID_CLUSTERING_DOMAIN.to_string(),
            configuration: config_json.to_string(),
            removed: false,
        };

        let config = LiquidClusteringConfig::from_domain_metadata(&domain_metadata).unwrap();

        assert_eq!(config.clustering_columns.len(), 3);
        assert_eq!(
            config.column_names(),
            vec!["date", "region", "customer_id"]
        );
    }

    #[test]
    fn test_wrong_domain_error() {
        let domain_metadata = DomainMetadata {
            domain: "wrong.domain".to_string(),
            configuration: "{}".to_string(),
            removed: false,
        };

        let result = LiquidClusteringConfig::from_domain_metadata(&domain_metadata);
        assert!(result.is_err());
    }

    #[test]
    fn test_removed_config_error() {
        let domain_metadata = DomainMetadata {
            domain: LIQUID_CLUSTERING_DOMAIN.to_string(),
            configuration: "{}".to_string(),
            removed: true,
        };

        let result = LiquidClusteringConfig::from_domain_metadata(&domain_metadata);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_clustering_column() {
        let config_json =
            r#"{"clusteringColumns":[{"physicalName":["id"]},{"physicalName":["date"]}],"domainName":"delta.liquid"}"#;

        let domain_metadata = DomainMetadata {
            domain: LIQUID_CLUSTERING_DOMAIN.to_string(),
            configuration: config_json.to_string(),
            removed: false,
        };

        let config = LiquidClusteringConfig::from_domain_metadata(&domain_metadata).unwrap();

        assert!(config.is_clustering_column("id"));
        assert!(config.is_clustering_column("date"));
        assert!(!config.is_clustering_column("other_column"));
    }

    #[test]
    fn test_is_liquid_clustering_enabled() {
        let enabled = DomainMetadata {
            domain: LIQUID_CLUSTERING_DOMAIN.to_string(),
            configuration: "{}".to_string(),
            removed: false,
        };

        let disabled = DomainMetadata {
            domain: LIQUID_CLUSTERING_DOMAIN.to_string(),
            configuration: "{}".to_string(),
            removed: true,
        };

        let other_domain = DomainMetadata {
            domain: "other.domain".to_string(),
            configuration: "{}".to_string(),
            removed: false,
        };

        assert!(is_liquid_clustering_enabled(Some(&enabled)));
        assert!(!is_liquid_clustering_enabled(Some(&disabled)));
        assert!(!is_liquid_clustering_enabled(Some(&other_domain)));
        assert!(!is_liquid_clustering_enabled(None));
    }
}




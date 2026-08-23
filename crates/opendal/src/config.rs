//! Helpers for mapping delta storage options onto OpenDAL operator config.

use std::collections::HashMap;

/// Storage-option prefix whose entries are forwarded to OpenDAL.
///
/// A storage option `opendal.<key> = <value>` is passed to
/// [`opendal::Operator::via_iter`] as `<key> = <value>`. This keeps delta's own
/// reserved option keys (retry, runtime, limit, certificate, …) out of the
/// OpenDAL config and lets users reach any documented service key without a
/// per-service allow-list.
pub const OPENDAL_PREFIX: &str = "opendal.";

/// Collect the `opendal.<key>` storage options into OpenDAL config pairs,
/// stripping the prefix. The result is sorted for deterministic behavior.
pub fn collect_prefixed(raw: &HashMap<String, String>, prefix: &str) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = raw
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(prefix)
                .filter(|rest| !rest.is_empty())
                .map(|rest| (rest.to_string(), v.clone()))
        })
        .collect();
    pairs.sort();
    pairs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_and_strips_prefix() {
        let raw = HashMap::from([
            (
                "opendal.endpoint".to_string(),
                "http://localhost".to_string(),
            ),
            ("opendal.region".to_string(), "us-east-1".to_string()),
            ("max_retries".to_string(), "3".to_string()),
            ("opendal.".to_string(), "ignored-empty-key".to_string()),
        ]);
        let pairs = collect_prefixed(&raw, OPENDAL_PREFIX);
        assert_eq!(
            pairs,
            vec![
                ("endpoint".to_string(), "http://localhost".to_string()),
                ("region".to_string(), "us-east-1".to_string()),
            ]
        );
    }
}

//! Utility functions for converting time formats.
#![allow(unused)]

use arrow::temporal_conversions;
use parquet::basic::TimeUnit;

/// Convert an ISO-8601/RFC3339 timestamp string to a numeric microsecond epoch representation.
/// Stats strings are written with millisecond precision as described by the delta protocol.
pub fn timestamp_micros_from_stats_string(s: &str) -> Result<i64, chrono::format::ParseError> {
    chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.timestamp_millis() * 1000)
}

/// Convert the timestamp to a ISO-8601 style format suitable for JSON statistics.
pub fn timestamp_to_delta_stats_string(n: i64, time_unit: &TimeUnit) -> Option<String> {
    let dt = match time_unit {
        TimeUnit::MILLIS(_) => temporal_conversions::timestamp_ms_to_datetime(n),
        TimeUnit::MICROS(_) => temporal_conversions::timestamp_us_to_datetime(n),
        TimeUnit::NANOS(_) => temporal_conversions::timestamp_ns_to_datetime(n),
    }?;

    Some(format!("{}", dt.format("%Y-%m-%dT%H:%M:%S%.3fZ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::format::{MicroSeconds, MilliSeconds, NanoSeconds, TimeUnit};

    #[test]
    fn test_timestamp_to_delta_stats_string() {
        let s =
            timestamp_to_delta_stats_string(1628685199541, &TimeUnit::MILLIS(MilliSeconds::new()))
                .unwrap();
        assert_eq!("2021-08-11T12:33:19.541Z".to_string(), s);
        let s = timestamp_to_delta_stats_string(
            1628685199541000,
            &TimeUnit::MICROS(MicroSeconds::new()),
        )
        .unwrap();
        assert_eq!("2021-08-11T12:33:19.541Z".to_string(), s);
        let s = timestamp_to_delta_stats_string(
            1628685199541000000,
            &TimeUnit::NANOS(NanoSeconds::new()),
        )
        .unwrap();
        assert_eq!("2021-08-11T12:33:19.541Z".to_string(), s);
    }

    #[test]
    fn test_timestamp_micros_from_stats_string() {
        let us = timestamp_micros_from_stats_string("2021-08-11T12:33:19.541Z").unwrap();
        assert_eq!(1628685199541000i64, us);
    }
}

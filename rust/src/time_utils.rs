//! Utility functions for converting time formats.
#![allow(unused)]

#[cfg(feature = "arrow")]
use arrow::temporal_conversions;
#[cfg(feature = "parquet")]
use parquet::basic::TimeUnit;
#[cfg(feature = "parquet2")]
use parquet2::schema::types::TimeUnit;

// vendored from arrow-rs and arrow2 so we don't need to depend on arrow2 when the parquet2 feature
// is enabled.
#[cfg(not(feature = "arrow"))]
mod temporal_conversions {
    use chrono::NaiveDateTime;

    /// Number of milliseconds in a second
    pub const MILLISECONDS: i64 = 1_000;
    /// Number of microseconds in a second
    pub const MICROSECONDS: i64 = 1_000_000;
    /// Number of nanoseconds in a second
    pub const NANOSECONDS: i64 = 1_000_000_000;

    /// converts a `i64` representing a `timestamp(ms)` to [`NaiveDateTime`]
    #[inline]
    pub fn timestamp_ms_to_datetime(v: i64) -> Option<NaiveDateTime> {
        let (sec, milli_sec) = split_second(v, MILLISECONDS);

        NaiveDateTime::from_timestamp_opt(
            // extract seconds from milliseconds
            sec,
            // discard extracted seconds and convert milliseconds to nanoseconds
            milli_sec * MICROSECONDS as u32,
        )
    }

    /// converts a `i64` representing a `timestamp(us)` to [`NaiveDateTime`]
    #[inline]
    pub fn timestamp_us_to_datetime(v: i64) -> Option<NaiveDateTime> {
        let (sec, micro_sec) = split_second(v, MICROSECONDS);

        NaiveDateTime::from_timestamp_opt(
            // extract seconds from microseconds
            sec,
            // discard extracted seconds and convert microseconds to nanoseconds
            micro_sec * MILLISECONDS as u32,
        )
    }

    /// converts a `i64` representing a `timestamp(ns)` to [`NaiveDateTime`]
    #[inline]
    pub fn timestamp_ns_to_datetime(v: i64) -> Option<NaiveDateTime> {
        let (sec, nano_sec) = split_second(v, NANOSECONDS);

        NaiveDateTime::from_timestamp_opt(
            // extract seconds from nanoseconds
            sec, // discard extracted seconds
            nano_sec,
        )
    }

    ///
    #[inline]
    pub(crate) fn split_second(v: i64, base: i64) -> (i64, u32) {
        if v < 0 {
            let v = -v;
            let mut seconds = v / base;
            let mut part = v % base;

            if part > 0 {
                seconds += 1;
                part = base - part;
            }
            (-seconds, part as u32)
        } else {
            (v / base, (v % base) as u32)
        }
    }
}

/// Convert an ISO-8601/RFC3339 timestamp string to a numeric microsecond epoch representation.
/// Stats strings are written with millisecond precision as described by the delta protocol.
pub fn timestamp_micros_from_stats_string(s: &str) -> Result<i64, chrono::format::ParseError> {
    chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.timestamp_millis() * 1000)
}

/// Convert the timestamp to a ISO-8601 style format suitable for JSON statistics.
#[cfg(feature = "parquet")]
pub fn timestamp_to_delta_stats_string(n: i64, time_unit: &TimeUnit) -> Option<String> {
    let dt = match time_unit {
        TimeUnit::MILLIS(_) => temporal_conversions::timestamp_ms_to_datetime(n),
        TimeUnit::MICROS(_) => temporal_conversions::timestamp_us_to_datetime(n),
        TimeUnit::NANOS(_) => temporal_conversions::timestamp_ns_to_datetime(n),
    }?;

    Some(format!("{}", dt.format("%Y-%m-%dT%H:%M:%S%.3fZ")))
}

/// Convert the timestamp to a ISO-8601 style format suitable for JSON statistics.
#[cfg(feature = "parquet2")]
pub fn timestamp_to_delta_stats_string(n: i64, time_unit: &TimeUnit) -> Option<String> {
    let dt = match time_unit {
        TimeUnit::Milliseconds => temporal_conversions::timestamp_ms_to_datetime(n),
        TimeUnit::Microseconds => temporal_conversions::timestamp_us_to_datetime(n),
        TimeUnit::Nanoseconds => temporal_conversions::timestamp_ns_to_datetime(n),
    }?;

    Some(format!("{}", dt.format("%Y-%m-%dT%H:%M:%S%.3fZ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(feature = "parquet2"))]
    use parquet::format::{MicroSeconds, MilliSeconds, NanoSeconds, TimeUnit};

    #[cfg(not(feature = "parquet2"))]
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

    #[cfg(feature = "parquet2")]
    #[test]
    fn test_timestamp_to_delta_stats_string() {
        let s = timestamp_to_delta_stats_string(1628685199541, &TimeUnit::Milliseconds).unwrap();
        assert_eq!("2021-08-11T12:33:19.541Z".to_string(), s);
        let s = timestamp_to_delta_stats_string(1628685199541000, &TimeUnit::Microseconds).unwrap();
        assert_eq!("2021-08-11T12:33:19.541Z".to_string(), s);
        let s =
            timestamp_to_delta_stats_string(1628685199541000000, &TimeUnit::Nanoseconds).unwrap();
        assert_eq!("2021-08-11T12:33:19.541Z".to_string(), s);
    }

    #[test]
    fn test_timestamp_micros_from_stats_string() {
        let us = timestamp_micros_from_stats_string("2021-08-11T12:33:19.541Z").unwrap();
        assert_eq!(1628685199541000i64, us);
    }
}

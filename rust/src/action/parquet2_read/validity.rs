//! Parquet deserialization for row validity

use parquet2::encoding::hybrid_rle::HybridRleDecoder;

/// Iterator that returns row index for rows that are not null
pub struct ValidityRowIndexIter<'a> {
    row_idx: usize,
    max_def_level: u32,
    validity_iter: HybridRleDecoder<'a>,
}

impl<'a> ValidityRowIndexIter<'a> {
    /// Create parquet primitive value reader
    pub fn new(max_def_level: i16, validity_iter: HybridRleDecoder<'a>) -> Self {
        Self {
            max_def_level: max_def_level as u32,
            validity_iter,
            row_idx: 0,
        }
    }
}

impl<'a> Iterator for ValidityRowIndexIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(def_lvl) = self.validity_iter.next() {
            if def_lvl == self.max_def_level {
                let row_idx = self.row_idx;
                self.row_idx += 1;
                return Some(row_idx);
            } else {
                self.row_idx += 1;
                continue;
            }
        }
        None
    }
}

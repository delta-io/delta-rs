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
        for def_lvl in self.validity_iter.by_ref() {
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

/// Iterator that returns row index for leaf repeated rows that are not null
///
/// For example, used in List type where each index contains multiple values.
#[allow(dead_code)]
pub struct ValidityRepeatedRowIndexIter<'a> {
    row_idx: usize,
    max_def_level: u32,
    max_rep_level: u32,
    repeat_count: usize,
    lvl_iter: std::iter::Zip<HybridRleDecoder<'a>, HybridRleDecoder<'a>>,
}

impl<'a> ValidityRepeatedRowIndexIter<'a> {
    /// Create parquet primitive value reader
    pub fn new(
        max_rep_level: i16,
        rep_iter: HybridRleDecoder<'a>,
        max_def_level: i16,
        validity_iter: HybridRleDecoder<'a>,
    ) -> Self {
        Self {
            lvl_iter: rep_iter.zip(validity_iter),
            max_rep_level: max_rep_level as u32,
            max_def_level: max_def_level as u32,
            row_idx: 0,
            repeat_count: 0,
        }
    }
}

impl<'a> Iterator for ValidityRepeatedRowIndexIter<'a> {
    // (index, item_count)
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        for (rep_lvl, def_lvl) in self.lvl_iter.by_ref() {
            if def_lvl == self.max_def_level {
                if rep_lvl == 0 {
                    match self.repeat_count {
                        0 => self.repeat_count = 1,
                        item_count => {
                            // reached start of next batch
                            // return current batch
                            let row_idx = self.row_idx;
                            self.row_idx += 1;
                            self.repeat_count = 1;
                            return Some((row_idx, item_count));
                        }
                    }
                } else {
                    // accumulate count for current batch
                    self.repeat_count += 1;
                }
            } else {
                if self.repeat_count >= 1 {
                    let row_idx = self.row_idx;
                    let item_count = self.repeat_count;
                    self.row_idx += 1;
                    // set to 0 becauze def_lvl not at max def level
                    self.repeat_count = 0;
                    return Some((row_idx, item_count));
                } else {
                    self.row_idx += 1;
                    continue;
                }
            }
        }

        if self.repeat_count >= 1 {
            let item_count = self.repeat_count;
            // set repeat count to 0 so we can end the iteration
            self.repeat_count = 0;
            Some((self.row_idx, item_count))
        } else {
            None
        }
    }
}

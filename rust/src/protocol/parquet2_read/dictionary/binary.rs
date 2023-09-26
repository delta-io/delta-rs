use parquet2::encoding::get_length;
use parquet2::error::Error;

#[derive(Debug)]
pub struct BinaryPageDict<'a> {
    values: Vec<&'a [u8]>,
}

impl<'a> BinaryPageDict<'a> {
    pub fn new(values: Vec<&'a [u8]>) -> Self {
        Self { values }
    }

    #[inline]
    pub fn value(&self, index: usize) -> Result<&[u8], Error> {
        self.values
            .get(index)
            .map(|v| *v)
            .ok_or_else(|| Error::OutOfSpec("invalid index".to_string()))
    }
}

fn read_plain<'a>(bytes: &'a [u8], length: usize) -> Result<Vec<&'a [u8]>, Error> {
    let mut bytes = bytes;
    let mut values = Vec::new();

    for _ in 0..length {
        let slot_length = get_length(bytes).unwrap();
        bytes = &bytes[4..];

        if slot_length > bytes.len() {
            return Err(Error::OutOfSpec(
                "The string on a dictionary page has a length that is out of bounds".to_string(),
            ));
        }
        let (result, remaining) = bytes.split_at(slot_length);

        values.push(result);
        bytes = remaining;
    }

    Ok(values)
}

pub fn read<'a>(buf: &'a [u8], num_values: usize) -> Result<BinaryPageDict<'a>, Error> {
    let values = read_plain(buf, num_values)?;
    Ok(BinaryPageDict::new(values))
}

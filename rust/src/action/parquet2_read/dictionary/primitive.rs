use parquet2::error::{Error, Result};
use parquet2::types::{decode, NativeType};

pub fn read<T: NativeType>(buf: &[u8], num_values: usize) -> Result<Vec<T>> {
    let size_of = std::mem::size_of::<T>();

    let typed_size = num_values.wrapping_mul(size_of);

    let values = buf.get(..typed_size).ok_or_else(|| {
        Error::OutOfSpec(
            "The number of values declared in the dict page does not match the length of the page"
                .to_string(),
        )
    })?;

    let values = values.chunks_exact(size_of).map(decode::<T>).collect();

    Ok(values)
}

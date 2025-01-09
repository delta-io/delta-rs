//! Utitlies for reading JSON files and handling JSON data.

use std::io::{BufRead, BufReader, Cursor};
use std::task::Poll;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_json::{reader::Decoder, ReaderBuilder};
use arrow_schema::SchemaRef as ArrowSchemaRef;
use arrow_select::concat::concat_batches;
use bytes::{Buf, Bytes};
use futures::{ready, Stream, StreamExt};
use itertools::Itertools;
use object_store::Result as ObjectStoreResult;

use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

#[inline]
pub(crate) fn get_reader(data: &[u8]) -> BufReader<Cursor<&[u8]>> {
    BufReader::new(Cursor::new(data))
}

#[inline]
pub(crate) fn get_decoder(
    schema: ArrowSchemaRef,
    config: &DeltaTableConfig,
) -> DeltaResult<Decoder> {
    Ok(ReaderBuilder::new(schema)
        .with_batch_size(config.log_batch_size)
        .build_decoder()?)
}

// Raw arrow implementation of the json parsing. Separate from the public function for testing.
//
// NOTE: This code is really inefficient because arrow lacks the native capability to perform robust
// StringArray -> StructArray JSON parsing. See https://github.com/apache/arrow-rs/issues/6522. If
// that shortcoming gets fixed upstream, this method can simplify or hopefully even disappear.
//
// NOTE: this function is hoisted from delta-kernel-rs to support transitioning to kernel.
fn parse_json_impl(
    json_strings: &StringArray,
    schema: ArrowSchemaRef,
) -> Result<RecordBatch, delta_kernel::Error> {
    if json_strings.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Use batch size of 1 to force one record per string input
    let mut decoder = ReaderBuilder::new(schema.clone())
        .with_batch_size(1)
        .build_decoder()?;
    let parse_one = |json_string: Option<&str>| -> Result<RecordBatch, delta_kernel::Error> {
        let mut reader = BufReader::new(json_string.unwrap_or("{}").as_bytes());
        let buf = reader.fill_buf()?;
        let read = buf.len();
        if !(decoder.decode(buf)? == read) {
            return Err(delta_kernel::Error::missing_data("Incomplete JSON string"));
        }
        let Some(batch) = decoder.flush()? else {
            return Err(delta_kernel::Error::missing_data("Expected data"));
        };
        if !(batch.num_rows() == 1) {
            return Err(delta_kernel::Error::generic("Expected one row"));
        }
        Ok(batch)
    };
    let output: Vec<_> = json_strings.iter().map(parse_one).try_collect()?;
    Ok(concat_batches(&schema, output.iter())?)
}

/// Parse an array of JSON strings into a record batch.
///
/// Null values in the input array are preseverd in the output record batch.
pub(crate) fn parse_json(
    json_strings: &StringArray,
    output_schema: ArrowSchemaRef,
) -> DeltaResult<RecordBatch> {
    Ok(parse_json_impl(json_strings, output_schema)?)
}

/// Decode a stream of bytes into a stream of record batches.
pub(crate) fn decode_stream<S: Stream<Item = ObjectStoreResult<Bytes>> + Unpin>(
    mut decoder: Decoder,
    mut input: S,
) -> impl Stream<Item = Result<RecordBatch, DeltaTableError>> {
    let mut buffered = Bytes::new();
    futures::stream::poll_fn(move |cx| {
        loop {
            if buffered.is_empty() {
                buffered = match ready!(input.poll_next_unpin(cx)) {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                    None => break,
                };
            }
            let decoded = match decoder.decode(buffered.as_ref()) {
                Ok(decoded) => decoded,
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            };
            let read = buffered.len();
            buffered.advance(decoded);
            if decoded != read {
                break;
            }
        }

        Poll::Ready(decoder.flush().map_err(DeltaTableError::from).transpose())
    })
}

/// Decode data provided by a reader into an iterator of record batches.
pub(crate) fn decode_reader<'a, R: BufRead + 'a>(
    decoder: &'a mut Decoder,
    mut reader: R,
) -> impl Iterator<Item = Result<RecordBatch, DeltaTableError>> + 'a {
    let mut next = move || {
        loop {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break; // Input exhausted
            }
            let read = buf.len();
            let decoded = decoder.decode(buf)?;

            reader.consume(decoded);
            if decoded != read {
                break; // Read batch size
            }
        }
        decoder.flush()
    };
    std::iter::from_fn(move || next().map_err(DeltaTableError::from).transpose())
}

#[cfg(test)]
mod tests {
    use crate::kernel::arrow::json::parse_json;
    use crate::DeltaTableConfig;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn json_to_struct() {
        let json_strings = StringArray::from(vec![
            Some(r#"{"a": 1, "b": "foo"}"#),
            Some(r#"{"a": 2, "b": "bar"}"#),
            None,
            Some(r#"{"a": 3, "b": "baz"}"#),
        ]);
        let struct_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let config = DeltaTableConfig::default();
        let result = parse_json(&json_strings, struct_schema.clone()).unwrap();
        let expected = RecordBatch::try_new(
            struct_schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(3)])),
                Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("bar"),
                    None,
                    Some("baz"),
                ])),
            ],
        )
        .unwrap();
        assert_eq!(result, expected);
    }
}

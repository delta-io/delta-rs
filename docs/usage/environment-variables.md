# Environment variables

`deltalake` reads several `DELTARS_*` environment variables that tune how
much work a write (or vacuum) does at once. They are tuning knobs, not API
parameters: there is no `write_deltalake` argument or `WriterProperties`
field for them, and an unknown or misspelled name is silently ignored, so
the default applies.

All of them except `DELTARS_MAX_IN_FLIGHT_UPLOAD_BYTES` are read once per
process and cached. Set them before the first write in the process; changing
one after a write has started has no effect on that process.
`DELTARS_MAX_IN_FLIGHT_UPLOAD_BYTES` is the exception: it is read when each
write starts.

## Bounding memory when the object store is slow

When a data file reaches its target size, the writer uploads it in the background and starts the next file at once. Each pending upload holds that file's bytes until the store accepts them. A slow store would let them pile up, so `deltalake` caps the bytes they may hold.

The cap is a ceiling, not a reservation. A store that keeps up never reaches it. Once uploads do fall behind, the write waits for one to land before it starts another file, and that backpressure reaches the data source.

By default the cap is a quarter of the memory the process may use — the container limit if there is one, otherwise total system memory — held between 4 and 32 times `target_file_size`. Below 4, uploads run one at a time. Above 32 they do not go faster, because how many keep a store busy depends on its request concurrency, and stores saturate well below 32. At the default 100 MiB `target_file_size` the upper bound is 3.2 GiB, so the memory share only lowers the cap under about 13 GiB of RAM. If the lower bound raises the share instead, `deltalake` warns that the target file size is large for the memory available.

Set `DELTARS_MAX_IN_FLIGHT_UPLOAD_BYTES` to a byte count to choose the cap yourself, or to `-1` to remove it. `deltalake` reads it when a write starts, so it can differ between writes. Keep it above a few times `target_file_size`: a larger file takes the whole cap and still uploads, but then uploads run one at a time.

One cap covers one write call. Every writer in it shares the cap, including the change data feed writer and all partition writers, so more partition values do not raise the cap. Concurrent writes each get their own.

The cap covers uploads only. A write also keeps one file open per partition value it meets, and each open file buffers its current row group outside the cap. Writing across many partition values can therefore use several times the cap: measured against a fast store with a 64 MiB cap, peak memory was 22 MiB for 1 partition value and 460 MiB for 512, none of it pending uploads. Write fewer partition values per call, or lower `max_row_group_size` in `WriterProperties`, to bound that part.

``` python
import os

os.environ["DELTARS_MAX_IN_FLIGHT_UPLOAD_BYTES"] = str(256 * 1024 * 1024)  # 256 MiB

from deltalake import write_deltalake

write_deltalake("s3://bucket/my_table", data, mode="append")
```

## Tuning writer concurrency with environment variables

| Variable | What it bounds | Default |
| --- | --- | --- |
| `DELTARS_MAX_CONCURRENT_WRITERS` | Concurrent writer tasks in a write. Each writer holds open multipart uploads and in-memory buffers, so more writers means higher memory and file descriptor usage. | Number of available CPU cores, clamped to 1-128 |
| `DELTARS_WRITER_BATCH_CHANNEL_SIZE` | Record batches that may be in flight between the producer and the writers (channel backpressure). | 10 |
| `DELTARS_MAX_CONCURRENCY_TASKS` | Multipart upload parallelism within a single file. | 10 |
| `DELTARS_UPLOAD_PART_SIZE` | Multipart upload part size in bytes. Values below 5 MiB fall back to 5 MiB, and values above 5 GiB are capped at 5 GiB, matching S3 and GCS part limits. | 5 MiB |
| `DELTARS_ROLL_ON_ROW_GROUP_BOUNDARY` | Whether files roll on row group boundaries. Set to `1`, `true`, or `yes` (case-insensitive) to enable. | `false` |
| `DELTARS_VACUUM_LIST_CONCURRENCY` | Concurrent LIST calls during vacuum. This one applies to vacuum, not writes; per-operation control is available through `VacuumBuilder::with_scan_concurrency`. | 10 |

The three concurrency knobs bound different stages, so they do not substitute for
each other: `DELTARS_MAX_CONCURRENT_WRITERS` caps how many writer tasks run at
once, `DELTARS_WRITER_BATCH_CHANNEL_SIZE` caps how many record batches wait
between the producer and those writers, and `DELTARS_MAX_CONCURRENCY_TASKS` caps
the multipart upload parallelism inside one file. If peak memory during a write
stays high after tuning the upload-bytes cap, lowering
`DELTARS_MAX_CONCURRENT_WRITERS` is the next knob to try, since each writer
holds its own buffers outside that cap.

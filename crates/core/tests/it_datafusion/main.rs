#![cfg(feature = "datafusion")]

mod fs_common;

mod checkpoint_writer;
mod command_filesystem_check;
mod command_merge;
mod command_optimize;
mod command_restore;
mod command_vacuum;
mod commit_info_format;
mod datafusion_dat;
mod datafusion_table_provider;
mod file_selection_bench_bridge;
mod integration;
mod integration_checkpoint;
mod integration_datafusion;
mod read_delta_log_test;
mod read_delta_partitions_test;

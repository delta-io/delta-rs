use chrono::Duration;
use clap::{App, AppSettings, Arg};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let matches = App::new("Delta table inspector")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Utility to help inspect Delta tables")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            App::new("info")
                .about("dump table metadata info")
                .setting(AppSettings::ArgRequiredElseHelp)
                .args(&[Arg::new("uri").help("Table URI").required(true)]),
        )
        .subcommand(
            App::new("files")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("output list of files for a given version, default to latest")
                .args(&[
                    Arg::new("uri").help("Table URI").required(true),
                    Arg::new("full_uri")
                        .help("Display files in full URI")
                        .takes_value(false)
                        .long("full-uri")
                        .short('f'),
                    Arg::new("version")
                        .takes_value(true)
                        .long("version")
                        .short('v')
                        .help("specify table version"),
                ]),
        )
        .subcommand(
            App::new("vacuum")
                .about("vacuum table")
                .setting(AppSettings::ArgRequiredElseHelp)
                .args(&[
                    Arg::new("uri").help("Table URI").required(true),
                    Arg::new("retention_hours")
                        .help("Retention hours for vacuum")
                        .takes_value(true)
                        .long("retention-hours")
                        .short('r'),
                    Arg::new("no_dry_run")
                        .help("Perform an actual vacuum instead dry run")
                        .takes_value(false)
                        .long("no-dry-run")
                        .short('n'),
                ]),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("files", files_matches)) => {
            let table_uri = files_matches.value_of("uri").unwrap();

            let table = match files_matches.value_of_t::<i64>("version") {
                Ok(v) => deltalake::open_table_with_version(table_uri, v).await?,
                Err(clap::Error {
                    kind: clap::ErrorKind::ArgumentNotFound,
                    ..
                }) => deltalake::open_table(table_uri).await?,
                Err(e) => e.exit(),
            };

            if files_matches.is_present("full_uri") {
                table.get_file_uris()?.for_each(|f| println!("{f}"));
            } else {
                table
                    .snapshot()?
                    .file_paths_iter()
                    .for_each(|f| println!("{f}"));
            };
        }
        Some(("info", info_matches)) => {
            let table_uri = info_matches.value_of("uri").unwrap();
            let table = deltalake::open_table(table_uri).await?;
            println!("{table}");
        }
        Some(("vacuum", vacuum_matches)) => {
            let dry_run = !vacuum_matches.is_present("no_dry_run");
            let table_uri = vacuum_matches.value_of("uri").unwrap();
            let table = deltalake::open_table(table_uri).await?;
            let retention = vacuum_matches
                .value_of("retention_hours")
                .map(|s| s.parse::<i64>().unwrap())
                .unwrap();
            let (_table, metrics) = deltalake::operations::DeltaOps(table)
                .vacuum()
                .with_retention_period(Duration::hours(retention))
                .with_dry_run(dry_run)
                .await?;

            if dry_run {
                println!("Files to deleted: {metrics:#?}");
            } else {
                println!("Files deleted: {metrics:#?}");
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}

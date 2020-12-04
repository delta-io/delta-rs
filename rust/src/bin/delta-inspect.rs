extern crate anyhow;
extern crate deltalake;

use clap::{App, AppSettings, Arg};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let matches = App::new("Delta table inspector")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Utility to help inspect Delta talebs")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            App::new("info")
                .about("dump table metadata info")
                .setting(AppSettings::ArgRequiredElseHelp)
                .args(&[Arg::new("path").about("Table path").required(true)]),
        )
        .subcommand(
            App::new("files")
                .setting(AppSettings::ArgRequiredElseHelp)
                .about("output list of files for a given version, defalt to latest")
                .args(&[
                    Arg::new("path").about("Table path").required(true),
                    Arg::new("full_path")
                        .about("Display files in full path")
                        .takes_value(false)
                        .long("full-path")
                        .short('f'),
                    Arg::new("version")
                        .takes_value(true)
                        .long("version")
                        .short('v')
                        .about("specify table version"),
                ]),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("files", files_matches)) => {
            let table_path = files_matches.value_of("path").unwrap();

            let table = match files_matches.value_of_t::<i64>("version") {
                Ok(v) => deltalake::open_table_with_version(table_path, v).await?,
                Err(clap::Error {
                    kind: clap::ErrorKind::ArgumentNotFound,
                    ..
                }) => deltalake::open_table(table_path).await?,
                Err(e) => e.exit(),
            };

            let files = if files_matches.is_present("full_path") {
                table.get_file_paths()
            } else {
                table.get_files().clone()
            };

            files.iter().for_each(|f| println!("{}", f));
        }
        Some(("info", info_matches)) => {
            let table_path = info_matches.value_of("path").unwrap();
            let table = deltalake::open_table(table_path).await?;
            println!("{}", table);
        }
        _ => unreachable!(),
    }

    Ok(())
}

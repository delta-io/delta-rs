use anyhow;
use delta;
use std::env;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("USAGE: {} TABLE_PATH", args[0]);
        std::process::exit(1);
    }
    let table_path = &args[1];

    let table = delta::open_table(table_path)?;

    println!("{}", table);
    println!("{:#?}", table.get_files());

    Ok(())
}

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use ledger_rs_core::{
    core::{BeanFileStorage, LedgerParserState, get_contents, new_beaninput},
    parse::parse_file,
};

#[derive(Parser)]
#[command(version, about, long_about=None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Readall { filepath: PathBuf },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Readall { filepath } => readall(filepath),
    }
}

fn readall(f: PathBuf) {
    let mut state = LedgerParserState::new();

    state.insert(f);

    let mut storage = BeanFileStorage::new();

    while !state.all_files_read() {
        let mut temp: Vec<(u32, PathBuf, String)> = vec![];

        for (p, (n, _)) in &state.input_files {
            let s = get_contents(p.as_path().as_ref());
            match s {
                Ok(b) => {
                    temp.push((*n, p.clone().to_path_buf(), b));
                }
                Err(x) => println!("{:?}", x),
            }
        }

        for (n, p, b) in temp {
            storage.add(b, n);
            let input = storage.get_ref(n);
            state.set_current_file_no(n);
            let mut beaninput = new_beaninput(input, &mut state);
            let _ = parse_file(&mut beaninput).unwrap();
            state.set_read(p);
        }
    }

    println!("{:?}", state);
    println!("{:?}", storage.file_contents.len());
}

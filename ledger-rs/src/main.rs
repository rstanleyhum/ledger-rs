use std::path::PathBuf;

use clap::{Parser, Subcommand};

use ledger_rs_core::{
    core::{LedgerParserState, get_contents, new_beaninput},
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

    state.insert(f.clone());

    let (input, _) = get_contents(f.as_path()).unwrap();
    let mut beaninput = new_beaninput(&input, &mut state);
    parse_file(&mut beaninput).unwrap();

    // state.transactions.iter().for_each(|x| println!("{:?}", x));
    // state.postings.iter().for_each(|x| println!("{:?}", x));
    // state.includes.iter().for_each(|x| println!("{:?}", x));
    // state
    //     .informationals
    //     .iter()
    //     .for_each(|x| println!("{:?}", x));

    //let array = state.try_transactions().unwrap();
    //println!("{:?}", array);

    state.verify();

    //state.write_parquets();
}

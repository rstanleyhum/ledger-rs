use std::path::PathBuf;

use clap::{Parser, Subcommand};

use ledger_rs_core::{
    core::{LedgerParserState, Statement, get_contents, new_beaninput},
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

    let mut all_statements: Vec<Statement> = vec![];

    state.insert(f.clone());

    let input = get_contents(f.as_path()).unwrap();
    let mut beaninput = new_beaninput(&input, &mut state);
    let mut s = parse_file(&mut beaninput).unwrap();
    all_statements.append(&mut s);

    println!("{:?}", state);

    all_statements.iter().for_each(|x| println!("{:?}", x));
}

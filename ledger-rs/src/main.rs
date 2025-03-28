use std::path::PathBuf;

use clap::{Parser, Subcommand};

use ledger_rs_core::{
    parse::{new_beaninput, parse_file},
    state::{LedgerParserState, get_contents},
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
    state.verify();
    state.accounts(
        "tc_commodity_final",
        "tc_quantity_final",
        "./tc_balances.csv",
    );
    state.accounts(
        "cp_commodity_final",
        "cp_quantity_final",
        "./cp_balances.csv",
    );
}

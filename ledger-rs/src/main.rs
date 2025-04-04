use std::path::PathBuf;

use clap::{Parser, Subcommand};

use ledger_rs_core::{parse::parse_filename, state::LedgerState};

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
    let mut state = LedgerState::new();

    state.insert(f.clone());
    parse_filename(f, &mut state);
    state.verify().unwrap();

    println!("tc_balances\n{}", state.tc_balances().unwrap());
    println!("cp_balances\n{}", state.cp_balances().unwrap());
}

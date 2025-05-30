use std::{path::PathBuf, str::FromStr};

use chrono::NaiveDate;
use clap::{Parser, Subcommand};

use ledger_rs_core::{parse::parse_filename, state::ledgerstate::LedgerState};
use ledger_rs_csv::{
    rj_cdn::{compile_holdings, process_activites},
    rj_cdn_closed::process_closed_acct_trans,
    rj_symbols::load_symbols,
    rj_usa::process_us_transaction,
};
use ledger_rs_qfx::qfx::parse_qfx_file;

#[derive(Parser)]
#[command(version, about, long_about=None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Bean {
        filepath: PathBuf,
    },
    RjUsa {
        filepath: PathBuf,
        acct: String,
        owner: String,
        currency: String,
    },
    RjCdnClosed {
        filepath: PathBuf,
        acct: String,
        owner: String,
        currency: String,
        commodity_f: PathBuf,
    },
    RjCdnActivities {
        filepath: PathBuf,
        acct: String,
        owner: String,
        currency: String,
        symbol_f: PathBuf,
    },
    RjCdnHoldings {
        filepath: PathBuf,
        bkdate_string: String,
        currency: String,
    },
    RjSymbols {
        symbol_f: PathBuf,
    },
    Qfx {
        symbols_f: PathBuf,
        filepath: PathBuf,
        bean_filepath: Option<PathBuf>,
        encoding: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Bean { filepath } => bean(filepath).await,
        Command::RjUsa {
            filepath,
            acct,
            owner,
            currency,
        } => rj_usa(filepath, acct.as_str(), owner.as_str(), currency.as_str()).await,
        Command::RjCdnClosed {
            filepath,
            acct,
            owner,
            currency,
            commodity_f,
        } => {
            rj_cdn_closed(
                filepath,
                acct.as_str(),
                owner.as_str(),
                currency.as_str(),
                commodity_f,
            )
            .await
        }
        Command::RjCdnActivities {
            filepath,
            acct,
            owner,
            currency,
            symbol_f,
        } => {
            rj_cdn_activites(
                filepath,
                acct.as_str(),
                owner.as_str(),
                currency.as_str(),
                symbol_f,
            )
            .await
        }
        Command::RjCdnHoldings {
            filepath,
            bkdate_string,
            currency,
        } => {
            rj_cdn_holdings(
                filepath,
                NaiveDate::from_str(&bkdate_string).unwrap(),
                currency.as_str(),
            )
            .await
        }
        Command::RjSymbols { symbol_f } => rj_symbols(symbol_f),
        Command::Qfx {
            symbols_f,
            filepath,
            bean_filepath,
            encoding,
        } => read_qfx(filepath, encoding, symbols_f, bean_filepath).await,
    }
}

async fn bean(f: PathBuf) {
    let mut state = LedgerState::new();

    state.insert(f.clone());
    parse_filename(f, &mut state);
    state.verify().await.unwrap();
    println!("tc_balances\n");
    state.tc_balances().await.unwrap().show().await.unwrap();
    println!("cp_balances\n");
    state.cp_balances().await.unwrap().show().await.unwrap();

    state.write_transactions().await.unwrap();
    state.write_verifications().await.unwrap();
}

async fn rj_usa(f: PathBuf, acct: &str, owner: &str, currency: &str) {
    let mut state = LedgerState::new();

    process_us_transaction(f.to_str().unwrap(), acct, owner, currency, &mut state).unwrap();

    println!("transactions: {}", state.transactions.len());
    println!("postings: {}", state.postings.len());
    println!("balances: {}", state.verifications.len());
    println!("\n");
    state.verify().await.unwrap();
    state.write_transactions().await.unwrap();
}

async fn rj_cdn_closed(f: PathBuf, acct: &str, owner: &str, currency: &str, commodity_f: PathBuf) {
    let mut state = LedgerState::new();

    process_closed_acct_trans(
        f.to_str().unwrap(),
        acct,
        owner,
        currency,
        commodity_f.to_str().unwrap(),
        &mut state,
    )
    .unwrap();

    println!("transactions: {}", state.transactions.len());
    println!("postings: {}", state.postings.len());
    println!("balances: {}", state.verifications.len());
    println!("\n");
    state.verify().await.unwrap();
    state.write_transactions().await.unwrap();
}

async fn rj_cdn_activites(
    f: PathBuf,
    acct: &str,
    owner: &str,
    currency: &str,
    commodity_f: PathBuf,
) {
    let mut state = LedgerState::new();

    process_activites(
        f.to_str().unwrap(),
        acct,
        owner,
        currency,
        commodity_f.to_str().unwrap(),
        &mut state,
    )
    .unwrap();

    println!("transactions: {}", state.transactions.len());
    println!("postings: {}", state.postings.len());
    println!("balances: {}", state.verifications.len());
    println!("\n");
    state.verify().await.unwrap();
    state.write_transactions().await.unwrap();
}

async fn rj_cdn_holdings(f: PathBuf, bkdate: NaiveDate, currency: &str) {
    let mut state = LedgerState::new();

    compile_holdings(f.to_str().unwrap(), bkdate, currency, &mut state).unwrap();

    println!("transactions: {}", state.transactions.len());
    println!("postings: {}", state.postings.len());
    println!("balances: {}", state.verifications.len());
    println!("\n");
    state.verify().await.unwrap();
    state.write_transactions().await.unwrap();
}

fn rj_symbols(f: PathBuf) {
    let result = load_symbols(String::from(f.to_str().unwrap())).unwrap();
    println!("{:?}", result);
}

async fn read_qfx(f: PathBuf, e: Option<String>, symbols_f: PathBuf, b: Option<PathBuf>) {
    let mut state = LedgerState::new();

    let _ = parse_qfx_file(f, e, symbols_f, &mut state);

    println!("transactions: {}", state.transactions.len());
    println!("postings: {}", state.postings.len());
    println!("balances: {}", state.verifications.len());
    println!("\n");
    state.verify().await.unwrap();
    state.write_transactions().await.unwrap();
    state.write_verifications().await.unwrap();

    if b.is_none() {
        return;
    }

    let mut b_state = LedgerState::new();

    let b_path = b.unwrap();

    b_state.insert(b_path.clone());
    parse_filename(b_path.clone(), &mut b_state);
    b_state.verify().await.unwrap();

    state.compare_postings(&b_state).await.unwrap();
}

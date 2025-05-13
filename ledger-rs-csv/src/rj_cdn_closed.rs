use std::{
    fs::{File, OpenOptions},
    io::{Error, Write},
    sync::atomic::Ordering,
};

use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::Deserialize;

use ledger_rs_core::{
    core::{HeaderParams, PostingParams},
    state::ledgerstate::LedgerState,
};

use crate::{
    rj_common::{acct_cash, acct_dividend, acct_fees, acct_gainloss, acct_securities, acct_todo},
    rj_core::{InterPost, Position},
    rj_decimal::{self, reverse_sign},
};

#[derive(Debug, Deserialize)]
struct ClosedAcctTransRecord {
    #[serde(rename = "Trade Date")]
    _traded: String,
    #[serde(rename = "Process Date")]
    _processed: String,
    #[serde(rename = "Settle Date")]
    settled: String,
    #[serde(rename = "Tran")]
    tran_type: ClosedTranType,
    #[serde(rename = "Description")]
    description: String,

    /// Symbol: security symbol
    #[serde(rename = "Symbol")]
    symbol: String,

    /// Quantity: quantity of security in symbol units: negative is sell => scale at 3
    #[serde(rename = "Quantity", with = "rj_decimal")]
    quantity: Decimal,

    /// Cost: cost of security in currency units: has same sign as Quantity => scale at 2
    #[serde(rename = "Cost", with = "rj_decimal")]
    cost: Decimal,

    /// Price: price = cost/quantity
    #[serde(rename = "Price", with = "rj_decimal")]
    _price: Decimal,
    #[serde(rename = "Proc Date Value")]
    _proc_date_value: String,

    /// Amount: change in cash position: positive decrease in asset => scale at 2
    #[serde(rename = "Amount", with = "rj_decimal")]
    amount: Decimal,
}

impl ClosedAcctTransRecord {
    fn get_cash_position(&self, currency: &str) -> Position {
        let mut amt = self.amount.clone();
        amt.rescale(3);
        let amt = reverse_sign(&amt);
        (amt, String::from(currency))
    }

    fn get_sec_position(&self) -> Position {
        let sec = if self.symbol == "" {
            String::from("UNKNOWNSEC")
        } else {
            self.symbol.clone()
        };
        let mut q = self.quantity.clone();
        q.rescale(3);
        (q, sec)
    }

    fn get_cost(&self, currency: &str) -> Position {
        let mut cost = self.cost.clone();
        cost.rescale(3);
        cost.set_sign_positive(true);
        (cost, String::from(currency))
    }

    fn transfer(&self, currency: &str, cash: &str, sec: &str, todo: &str) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        if self.symbol == "" {
            let cash_p = self.get_cash_position(currency);
            res.push((String::from(cash), Some(cash_p), None));
        } else {
            let sec_p = self.get_sec_position();
            let cost_p = self.get_cost(currency);
            res.push((String::from(sec), Some(sec_p), Some(cost_p)));
        }
        res.push((String::from(todo), None, None));
        res
    }

    fn buy(&self, currency: &str, cash: &str, sec: &str) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        if self.symbol != "" {
            let cash_p = self.get_cash_position(currency);
            let sec_p = self.get_sec_position();
            let cost_p = self.get_cost(currency);

            res.push((String::from(sec), Some(sec_p), Some(cost_p)));
            res.push((String::from(cash), Some(cash_p), None));
        }
        res
    }

    fn cash_transaction(&self, currency: &str, cash: &str, acct: &str) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        let cash_p = self.get_cash_position(currency);

        res.push((String::from(cash), Some(cash_p), None));
        res.push((String::from(acct), None, None));

        res
    }

    fn dividend(&self, currency: &str, cash: &str, sec: &str, acct: &str) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        if self.amount == Decimal::from(0) {
            if self.cost != Decimal::from(0) {
                let sec_p = self.get_sec_position();
                let cost_p = self.get_cost(currency);
                res.push((String::from(sec), Some(sec_p), Some(cost_p)));
                res.push((String::from(acct), None, None));
            }
        } else {
            let cash_p = self.get_cash_position(currency);
            res.push((String::from(cash), Some(cash_p), None));
            res.push((String::from(acct), None, None));
        }
        res
    }

    fn sell(&self, currency: &str, cash: &str, sec: &str, gl: &str) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        if self.symbol != "" {
            let cash_p = self.get_cash_position(currency);
            let sec_p = self.get_sec_position();
            res.push((String::from(sec), Some(sec_p), None));
            res.push((String::from(cash), Some(cash_p), None));
            res.push((String::from(gl), None, None));
        }
        res
    }

    fn store_closed_transaction(
        &self,
        commodity: &mut File,
        acct: &str,
        owner: &str,
        currency: &str,
        state: &mut LedgerState,
    ) -> Result<(), Error> {
        let cash = acct_cash!(owner, acct);
        let sec = acct_securities!(owner, acct);
        let todo = acct_todo!(owner);
        let dividend_acct = acct_dividend!(owner);
        let fees = acct_fees!(owner);
        let gl = acct_gainloss!(owner);

        let description = self.description.clone();
        let bkdate = NaiveDate::parse_from_str(&self.settled, "%Y-%m-%d").unwrap();
        let t_type = &self.tran_type;
        let narration = format!("{t_type} - {description}").trim().to_string();
        let symbol = self.symbol.trim().to_string();

        let posts = match &self.tran_type {
            ClosedTranType::ATI => self.transfer(currency, &cash, &sec, &todo), // Account Transfer In
            ClosedTranType::BUY => self.buy(currency, &cash, &sec),             // Buy
            ClosedTranType::CN => self.cash_transaction(currency, &cash, &todo), // RRSP Contribution
            ClosedTranType::CR => self.cash_transaction(currency, &cash, &todo), // Cash Receipt
            ClosedTranType::DVR => self.dividend(currency, &cash, &sec, &dividend_acct), // Reinvested Dividend
            ClosedTranType::DVU => self.dividend("USD", &cash, &sec, &dividend_acct), // US Cash Dividend
            ClosedTranType::EFT => self.cash_transaction(currency, &cash, &todo), // Electronic Funds Transfer
            ClosedTranType::GST => self.cash_transaction(currency, &cash, &fees), // Goods and Services Tax
            ClosedTranType::QST => self.cash_transaction(currency, &cash, &fees), // Quebec Sales Tax
            ClosedTranType::SEL => self.sell(currency, &cash, &sec, &gl),         // Sell
            ClosedTranType::TFE => self.cash_transaction(currency, &cash, &fees), // Sec Tfr Costs
            ClosedTranType::TSF => self.transfer(currency, &cash, &sec, &todo), // Internal Transfer
            ClosedTranType::VFN => self.cash_transaction(currency, &cash, &fees), // Virdian Fees Non-Registered
            ClosedTranType::VFR => self.cash_transaction(currency, &cash, &fees), // Virdian Fees Registered
        };

        let posno = state.line_count.fetch_add(1, Ordering::SeqCst);
        if posts.len() == 0 {
            // Store errors in parsing file
        } else {
            let transno = posno.clone();
            let th = HeaderParams {
                statement_no: transno,
                file_no: 0u32,
                start: 0u32,
                end: 0u32,
                date: bkdate,
                narration: narration,
                tags: None,
            };
            state.transactions.push(th);

            posts
                .into_iter()
                .map(|(acct, cp, tc)| {
                    let posno = state.line_count.fetch_add(1, Ordering::SeqCst);
                    let (cp_quantity, cp_commodity) = match cp {
                        None => (None, None),
                        Some((q, c)) => (Some(q), Some(c)),
                    };
                    let (tc_quantity, tc_commodity) = match tc {
                        None => (None, None),
                        Some((q, c)) => (Some(q), Some(c)),
                    };
                    PostingParams {
                        statement_no: posno,
                        transaction_no: transno,
                        file_no: 0u32,
                        start: 0u32,
                        end: 0u32,
                        account: acct,
                        cp_quantity: cp_quantity,
                        cp_commodity: cp_commodity,
                        tc_quantity: tc_quantity,
                        tc_commodity: tc_commodity,
                    }
                })
                .for_each(|x| state.postings.push(x));
        }

        if symbol != "" {
            write!(commodity, "{}", format!("{symbol},{description}\n"))?;
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize)]
enum ClosedTranType {
    ATI, // Account Transfer In
    BUY, // Buy
    CN,  // RRSP Contribution
    CR,  // Cash Receipt
    DVR, // Reinvested Dividend
    DVU, // US Cash Dividend
    EFT, // Electronic Funds Transfer
    GST, // Goods and Services Tax
    QST, // Quebec Sales Tax
    SEL, // Sell
    TFE, // Sec Tfr Costs
    TSF, // Internal Transfer
    VFN, // Virdian Fees Non-Registered
    VFR, // Virdian Fees Registered
}

impl std::fmt::Display for ClosedTranType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::ATI => "ATI",
            Self::BUY => "BUY",
            Self::CN => "CN",
            Self::CR => "CR",
            Self::DVR => "DVR",
            Self::DVU => "DVU",
            Self::EFT => "EFT",
            Self::GST => "GST",
            Self::QST => "QST",
            Self::SEL => "SEL",
            Self::TFE => "TFE",
            Self::TSF => "TSF",
            Self::VFN => "VFN",
            Self::VFR => "VFR",
        };
        write!(f, "{}", s)
    }
}

pub fn process_closed_acct_trans(
    filepath: &str,
    acct: &str,
    owner: &str,
    currency: &str,
    commodity_filepath: &str,
    state: &mut LedgerState,
) -> Result<(), Error> {
    let mut commodity_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(commodity_filepath)?;
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b',')
        .quoting(true)
        .from_path(filepath)
        .unwrap();
    for result in rdr.deserialize::<ClosedAcctTransRecord>() {
        match result {
            Ok(t) => {
                t.store_closed_transaction(&mut commodity_file, acct, owner, currency, state)?;
            }
            Err(e) => {
                println!("{:?}\n", e);
            }
        }
    }
    Ok(())
}

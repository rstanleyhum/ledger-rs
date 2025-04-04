use std::{io::Error, sync::atomic::Ordering};

use chrono::NaiveDate;
use ledger_rs_core::{
    core::{HeaderParams, PostingParams},
    state::LedgerState,
};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    rj_common::{
        acct_cash, acct_dividend, acct_fees, acct_foreigntax, acct_gainloss, acct_longtermcapgains,
        acct_securities, acct_shorttermcapgains, acct_todo,
    },
    rj_core::{InterPost, Position},
    rj_decimal,
};

#[derive(Debug, Deserialize)]
struct USTransactionRecord {
    #[serde(rename = "Account #")]
    _acct_num: String,
    #[serde(rename = "Account Nickname/Title")]
    _title: String,
    #[serde(rename = "Date")]
    _date: String,
    #[serde(rename = "Trade Date")]
    _trade_date: String,

    /// Symbol: security symbol
    #[serde(rename = "Security ID")]
    symbol: String,
    #[serde(rename = "CUSIP")]
    _cusip: String,
    #[serde(rename = "Description")]
    description: String,

    /// Amount: amount in currency with positive increasing cash assets
    #[serde(rename = "Net Amount", with = "rj_decimal")]
    amount: Decimal,
    #[serde(rename = "Net Amount in Local Currency")]
    _local_amount: String,

    /// Date in mm-dd-yyyy format
    #[serde(rename = "Settlement Date")]
    settled: String,

    /// Quantity: quantity of security in symbol units. it is always positive
    ///   is a String type because there are both X.XX, "X,XXX.XX" values and the csv reader doesn't like both
    ///   quoted and non-quoted values in the same field
    #[serde(rename = "Quantity")]
    quantity: String,

    #[serde(rename = "Price (Native)")]
    _price: String,
    #[serde(rename = "Principal")]
    _principal: String,
    #[serde(rename = "Principal in Local Currency")]
    _local_principal: String,
    #[serde(rename = "Commission/Fees")]
    _fees: String,
    #[serde(rename = "Account Type")]
    _acct_type: String,
    #[serde(rename = "Balance Type")]
    _balance_type: String,
    #[serde(rename = "Details")]
    details: String,
    #[serde(rename = "Payee")]
    _payee: String,
    #[serde(rename = "Paid for (Name)")]
    _paid: String,
    #[serde(rename = "Request Reason")]
    _reason: String,
}

impl USTransactionRecord {
    fn get_cash_position(&self, currency: &str) -> Position {
        let mut amt = self.amount.clone();
        amt.rescale(3);
        (amt, String::from(currency))
    }

    fn get_sec_position(&self) -> Position {
        let mut sec = if self.symbol == "" {
            String::from("UNKNOWNSEC")
        } else {
            self.symbol.clone()
        };
        let q = self.quantity.clone();
        let q = q.replace("$", "");
        let q = q.replace(",", "");
        let mut q = match Decimal::from_str_exact(&q) {
            Ok(x) => x,
            Err(_) => {
                let mut new_sec = "Error".to_string();
                new_sec.push_str(&self.quantity.clone());
                new_sec.push_str(&" ");
                new_sec.push_str(&sec);
                sec = new_sec.to_owned();
                Decimal::from(0)
            }
        };
        q.rescale(3);
        (q, sec)
    }

    fn get_cost(&self, currency: &str) -> Position {
        let (mut q, s) = self.get_cash_position(currency);
        q.set_sign_positive(true);
        (q, s)
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

    fn sell(&self, currency: &str, cash: &str, sec: &str, gl: &str) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        if self.symbol != "" {
            let cash_p = self.get_cash_position(currency);
            let (mut sec_q, sec_s) = self.get_sec_position();
            sec_q.set_sign_negative(true);
            let sec_p = (sec_q, sec_s);
            res.push((String::from(sec), Some(sec_p), None));
            res.push((String::from(cash), Some(cash_p), None));
            res.push((String::from(gl), None, None));
        }
        res
    }

    fn store_us_transaction(
        &self,
        acct: &str,
        owner: &str,
        currency: &str,
        state: &mut LedgerState,
    ) -> Result<(), Error> {
        let description = &self.description;

        let cash = acct_cash!(owner, acct);
        let sec = acct_securities!(owner, acct);
        let todo = acct_todo!(owner);
        let dividend_acct = acct_dividend!(owner);
        let fees = acct_fees!(owner);

        let foreigntaxes = acct_foreigntax!(owner);

        let longtermcapgains = acct_longtermcapgains!(owner);
        let shorttermcapgains = acct_shorttermcapgains!(owner);
        let gl = acct_gainloss!(owner);

        let mut posts = if description.starts_with("ADVISORY FEES")
            || description.starts_with("ASSET BASED FEE")
            || description.starts_with("MAINTENANCE FEE")
        {
            self.cash_transaction(currency, &cash, &fees)
        } else if description.starts_with("BUY ") {
            self.buy(currency, &cash, &sec)
        } else if description.starts_with("CASH DIVIDEND RECEIVED") {
            self.cash_transaction(currency, &cash, &dividend_acct)
        } else if description.starts_with("CASH IN LIEU OF FRACTIONALSHARE RECEIVED") {
            self.cash_transaction(currency, &cash, &dividend_acct)
        } else if description.starts_with("FOREIGN SECURITY DIVIDEND RECEIVED") {
            self.cash_transaction(currency, &cash, &dividend_acct)
        } else if description.starts_with("FOREIGN TAX WITHHELD AT   THE SOURCE") {
            self.cash_transaction(currency, &cash, &foreigntaxes)
        } else if description.starts_with("LONG TERM CAPITAL GAIN    DISTRIBUTION") {
            self.cash_transaction(currency, &cash, &longtermcapgains)
        } else if description.starts_with("REINVEST CASH INCOME") {
            self.buy(currency, &cash, &sec)
        } else if description.starts_with("ROLLOVER CONTRIBUTION") {
            self.cash_transaction(currency, &cash, &todo)
        } else if description.starts_with("SELL ") {
            self.sell(currency, &cash, &sec, &gl)
        } else if description.starts_with("SHORT TERM CAPITAL GAIN   DISTRIBUTION") {
            self.cash_transaction(currency, &cash, &shorttermcapgains)
        } else if description.starts_with("STOCK SPIN-OFF RECEIVED") {
            self.buy(currency, &cash, &sec)
        } else if description.starts_with("STOCK SPLIT RECEIVED") {
            self.buy(currency, &cash, &sec)
        } else if description.starts_with("YOUR ASSET TRANSFERRED") {
            self.transfer(currency, &cash, &sec, &todo)
        } else {
            Vec::<InterPost>::new()
        };

        let bkdate = match NaiveDate::parse_from_str(&self.settled, "%m-%d-%Y") {
            Ok(x) => x,
            Err(_) => {
                posts = Vec::<InterPost>::new();
                NaiveDate::MIN
            }
        };

        let posno = state.line_count.fetch_add(1, Ordering::SeqCst);
        if posts.len() == 0 {
            // storage
            //     .lineerrors
            //     .borrow_mut()
            //     .append_lineerror(posno, 0, 0, format!("{:?}", self));
        } else {
            let details = &self.details;
            let narration = format!("{description}-{details}").trim().to_string();

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

        Ok(())
    }
}

pub fn process_us_transaction(
    filepath: &str,
    acct: &str,
    owner: &str,
    currency: &str,
    state: &mut LedgerState,
) -> Result<(), Error> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b',')
        .quoting(true)
        .from_path(filepath)?;
    for result in rdr.deserialize::<USTransactionRecord>() {
        match result {
            Ok(t) => {
                t.store_us_transaction(acct, owner, currency, state)?;
            }
            Err(e) => {
                println!("{:?}\n", e);
            }
        }
    }
    Ok(())
}

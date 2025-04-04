use std::{io::Error, sync::atomic::Ordering};

use chrono::NaiveDate;
use ledger_rs_core::{
    core::{BALANCE_ACTION, HeaderParams, PostingParams, VerificationParams},
    state::LedgerState,
};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    rj_common::{
        acct_capgains, acct_cash, acct_distribution, acct_dividend, acct_fees, acct_foreigntax,
        acct_gainloss, acct_interest, acct_securities, acct_todo,
    },
    rj_core::{InterPost, Position},
    rj_decimal::{self, reverse_sign},
    rj_symbols::{SymbolsMap, load_symbols},
};

#[derive(Debug, Deserialize)]
struct TransRecord {
    #[serde(rename = "Processed")]
    _processed: String,

    /// Date in YYYY-MM-DD format
    #[serde(rename = "Settled")]
    settled: String,
    #[serde(rename = "Tran Types")]
    tran_types: TranType,

    /// Description with symbol look up
    #[serde(rename = "Description")]
    description: String,
    #[serde(rename = "Price", with = "rj_decimal")]
    price: Decimal,

    /// Quantity: units of symbol (negative is decrease in sec)
    #[serde(rename = "Quantity", with = "rj_decimal")]
    quantity: Decimal,

    /// amount in currency (negative is decrease in cash)
    #[serde(rename = "Amount", with = "rj_decimal")]
    amount: Decimal,
}

impl TransRecord {
    fn get_cash_position(&self, currency: &str) -> Position {
        let mut amt = self.amount.clone();
        amt.rescale(3);
        (amt, String::from(currency))
    }

    fn get_sec_position(&self, symbols: &SymbolsMap) -> Position {
        let sec = match symbols.get(&self.description) {
            Some(s) => s.clone().to_string(),
            None => "UNKNOENSEC".to_string(),
        };
        let mut q = self.quantity.clone();
        q.rescale(3);
        (q, sec)
    }

    fn get_cost(&self, currency: &str) -> Position {
        let price = self.price.clone();
        let quantity = self.quantity.clone();
        let mut cost = price * quantity;

        cost.rescale(3);
        cost.set_sign_positive(true);
        (cost, String::from(currency))
    }

    fn transfer(
        &self,
        currency: &str,
        symbols: &SymbolsMap,
        cash: &str,
        sec: &str,
        todo: &str,
    ) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        if self.amount != Decimal::ZERO {
            let cash_p = self.get_cash_position(currency);
            res.push((String::from(cash), Some(cash_p), None));
        }

        if self.quantity != Decimal::ZERO {
            let sec_p = self.get_sec_position(symbols);
            let cost_p = self.get_cost(currency);
            res.push((String::from(sec), Some(sec_p), Some(cost_p)));
        }

        res.push((String::from(todo), None, None));
        res
    }

    fn buy(&self, currency: &str, symbols: &SymbolsMap, cash: &str, sec: &str) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        let cash_p = self.get_cash_position(currency);
        let sec_p = self.get_sec_position(symbols);
        let (mut c_q, c_s) = self.get_cash_position(currency);
        c_q.set_sign_positive(true);
        let cost_p = (c_q, c_s);

        res.push((String::from(sec), Some(sec_p), Some(cost_p)));
        res.push((String::from(cash), Some(cash_p), None));
        res
    }

    fn cash_transaction(&self, currency: &str, cash: &str, acct: &str) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        let cash_p = self.get_cash_position(currency);

        res.push((String::from(cash), Some(cash_p), None));
        res.push((String::from(acct), None, None));

        res
    }

    fn sell(
        &self,
        currency: &str,
        symbols: &SymbolsMap,
        cash: &str,
        sec: &str,
        gl: &str,
    ) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        let cash_p = self.get_cash_position(currency);
        let sec_p: (Decimal, String) = self.get_sec_position(symbols);
        res.push((String::from(sec), Some(sec_p), None));
        res.push((String::from(cash), Some(cash_p), None));
        res.push((String::from(gl), None, None));
        res
    }

    fn reinvestment(
        &self,
        currency: &str,
        symbols: &SymbolsMap,
        sec: &str,
        acct: &str,
    ) -> Vec<InterPost> {
        let mut res: Vec<InterPost> = Vec::new();

        let sec_p: (Decimal, String) = self.get_sec_position(symbols);
        let cost_p = self.get_cost(currency);
        let rev_cost_p = cost_p.clone();
        let (q, s) = rev_cost_p;
        let q = reverse_sign(&q);
        let rev_cost_p = (q, s);
        res.push((String::from(sec), Some(sec_p), Some(cost_p)));
        res.push((String::from(acct), Some(rev_cost_p), None));
        res
    }

    fn store_transaction(
        &self,
        acct: &str,
        owner: &str,
        currency: &str,
        symbols: &SymbolsMap,
        state: &mut LedgerState,
    ) -> Result<(), Error> {
        let cash = acct_cash!(owner, acct);
        let sec = acct_securities!(owner, acct);
        let todo = acct_todo!(owner);
        let dividend_acct = acct_dividend!(owner);
        let fees = acct_fees!(owner);

        let capgains = acct_capgains!(owner);
        let distribution = acct_distribution!(owner);
        let foreigntax = acct_foreigntax!(owner);
        let gl = acct_gainloss!(owner);
        let interest = acct_interest!(owner);

        let description = self.description.clone();
        let bkdate = NaiveDate::parse_from_str(&self.settled, "%Y-%m-%d").unwrap();
        let narration = format!("{}:{description}", self.tran_types);

        let posts = match self.tran_types {
            TranType::Buy => self.buy(currency, symbols, &cash, &sec),
            TranType::CanadianCashDividend => {
                self.cash_transaction(currency, &cash, &dividend_acct)
            }
            TranType::CashReceipt => self.cash_transaction(currency, &cash, &todo),
            TranType::Distribution => self.cash_transaction(currency, &cash, &distribution),
            TranType::ExpiringRightsAndWarrants
            | TranType::MFNotionalDistribution
            | TranType::MandatoryExchange
            | TranType::Sell => self.sell(currency, symbols, &cash, &sec, &gl),
            TranType::ForeignDividend => self.cash_transaction(currency, &cash, &dividend_acct),
            TranType::ForeignNonResidentTax => self.cash_transaction(currency, &cash, &foreigntax),
            TranType::ForeignTransfer => self.cash_transaction(currency, &cash, &todo),
            TranType::GoodsAndServicesTax => self.cash_transaction(currency, &cash, &fees),
            TranType::InternalTransfer => self.transfer(currency, symbols, &cash, &sec, &todo),
            TranType::MutualFundDividend => self.cash_transaction(currency, &cash, &dividend_acct),
            TranType::OtherManagedAcctFeeRegistered => {
                self.cash_transaction(currency, &cash, &fees)
            }
            TranType::OtherManagedAcctFees => self.cash_transaction(currency, &cash, &fees),
            TranType::PartnersFeeNonRegistered => self.cash_transaction(currency, &cash, &fees),
            TranType::QuebecSalesTax => self.cash_transaction(currency, &cash, &fees),
            TranType::RRSPContribution => self.cash_transaction(currency, &cash, &todo),
            TranType::ReinvestmentDividend => {
                self.reinvestment(currency, symbols, &sec, &dividend_acct)
            }
            TranType::SecTfrCosts => self.cash_transaction(currency, &cash, &fees),
            TranType::SpinOffForeign => self.buy(currency, symbols, &cash, &sec),
            TranType::SpinOffUS => self.buy(currency, symbols, &cash, &sec),
            TranType::StockSplit => self.buy(currency, symbols, &cash, &sec),
            TranType::USCashDividend => self.cash_transaction("USD", &cash, &dividend_acct),
            TranType::USSourceLongTermGains => self.cash_transaction(currency, &cash, &capgains),
            TranType::ViridanFeesNonRegistered => self.cash_transaction(currency, &cash, &fees),
            TranType::MonthlyInterest => self.cash_transaction(currency, &cash, &interest),
            TranType::MFReturnOfCapital => Vec::new(),
        };

        let posno = state.line_count.fetch_add(1, Ordering::SeqCst);
        if posts.len() == 0 {
            // storage
            //     .lineerrors
            //     .borrow_mut()
            //     .append_lineerror(posno, 0, 0, format!("{:?}", self));
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

        Ok(())
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
enum TranType {
    #[serde(rename = "BUY")]
    Buy,
    #[serde(rename = "CASH RECEIPT")]
    CashReceipt,
    #[serde(rename = "CDN CASH DIVIDEND")]
    CanadianCashDividend,
    #[serde(rename = "DISTRIBUTION")]
    Distribution,
    #[serde(rename = "EXPIRING RIGHTS/WARRANTS")]
    ExpiringRightsAndWarrants,
    #[serde(rename = "FOREIGN DIVIDEND")]
    ForeignDividend,
    #[serde(rename = "FOREIGN NON RESIDENT TAX")]
    ForeignNonResidentTax,
    #[serde(rename = "FOREIGN TRANSFER")]
    ForeignTransfer,
    #[serde(rename = "GOODS & SERVICES TAX")]
    GoodsAndServicesTax,
    #[serde(rename = "INTERNAL TRANSFER")]
    InternalTransfer,
    #[serde(rename = "MANDATORY EXCHANGE")]
    MandatoryExchange,
    #[serde(rename = "MF NOTIONAL DISTRIBUTION")]
    MFNotionalDistribution,
    #[serde(rename = "MUTUAL FUND DIVIDEND")]
    MutualFundDividend,
    #[serde(rename = "OTHER MANAGED ACCT FEES")]
    OtherManagedAcctFees,
    #[serde(rename = "OTHER MGD ACCT FEE RGSRTD")]
    OtherManagedAcctFeeRegistered,
    #[serde(rename = "PARTNERS FEE NON REG'D")]
    PartnersFeeNonRegistered,
    #[serde(rename = "QUEBEC SALES TAX")]
    QuebecSalesTax,
    #[serde(rename = "REINVESTED DIVIDEND")]
    ReinvestmentDividend,
    #[serde(rename = "RRSP CONTRIBUTION")]
    RRSPContribution,
    #[serde(rename = "SEC TFR COSTS")]
    SecTfrCosts,
    #[serde(rename = "SELL")]
    Sell,
    #[serde(rename = "SPIN OFF (FOREIGN)")]
    SpinOffForeign,
    #[serde(rename = "SPIN OFF (US)")]
    SpinOffUS,
    #[serde(rename = "STOCK SPLIT")]
    StockSplit,
    #[serde(rename = "US CASH DIVIDEND")]
    USCashDividend,
    #[serde(rename = "US SOURCE LONG TERM GAINS")]
    USSourceLongTermGains,
    #[serde(rename = "VIRIDIAN FEES NON REGISTD")]
    ViridanFeesNonRegistered,
    #[serde(rename = "MONTHLY INTEREST")]
    MonthlyInterest,
    #[serde(rename = "MF RETURN OF CAPITAL")]
    MFReturnOfCapital,
}

impl std::fmt::Display for TranType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Buy => "Buy",
            Self::CanadianCashDividend => "CanadianCashDividend",
            Self::CashReceipt => "CashReceipt",
            Self::Distribution => "Distribution",
            Self::ExpiringRightsAndWarrants => "ExpiringRightsAndWarrants",
            Self::ForeignDividend => "ForeignDivident",
            Self::ForeignNonResidentTax => "ForeignNonResidentTax",
            Self::ForeignTransfer => "ForeignTransfer",
            Self::GoodsAndServicesTax => "GoodsAndServicesTax",
            Self::InternalTransfer => "InternalTransfer",
            Self::MFNotionalDistribution => "MFNotionalDistribution",
            Self::MandatoryExchange => "MandatoryExchange",
            Self::MutualFundDividend => "MutualFundDivident",
            Self::OtherManagedAcctFeeRegistered => "OtherManagedAcctFeeRegistered",
            Self::OtherManagedAcctFees => "OtherManagedAcctFees",
            Self::PartnersFeeNonRegistered => "PartnersFeeNonRegistered",
            Self::QuebecSalesTax => "QuebecSalesTax",
            Self::RRSPContribution => "RRSPContribution",
            Self::ReinvestmentDividend => "ReinvestmentDividend",
            Self::SecTfrCosts => "SecTfrCosts",
            Self::Sell => "Sell",
            Self::SpinOffForeign => "SpinOffForeign",
            Self::SpinOffUS => "SpinOffUS",
            Self::StockSplit => "StockSplit",
            Self::USCashDividend => "USCashDividend",
            Self::USSourceLongTermGains => "USSourceLongTermGains",
            Self::ViridanFeesNonRegistered => "ViridanFeesNonRegistered",
            Self::MonthlyInterest => "MonthlyInterest",
            Self::MFReturnOfCapital => "MFReturnOfCapital",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Deserialize)]
struct HoldingRecord {
    #[serde(rename = "Client Name")]
    client_name: String,
    #[serde(rename = "Client Id")]
    _client_id: String,
    #[serde(rename = "Account Nickname")]
    _account_nickname: String,

    #[serde(rename = "Account Number")]
    account_number: String,
    #[serde(rename = "Asset Category")]
    _asset_category: String,
    #[serde(rename = "Industry")]
    _industry: String,
    #[serde(rename = "Symbol")]
    symbol: String,
    #[serde(rename = "Holding")]
    holding: String,
    #[serde(rename = "Quantity", with = "rj_decimal")]
    quantity: Decimal,
    #[serde(rename = "Price")]
    _price: String,
    #[serde(rename = "Fund")]
    fund: String,
    #[serde(rename = "Average Cost")]
    _average_cost: String,
    #[serde(rename = "Book Value", with = "rj_decimal")]
    _book_value: Decimal,
    #[serde(rename = "Market Value")]
    _market_value: String,
    #[serde(rename = "Accrued Interest")]
    _accrued_interest: String,
    #[serde(rename = "G/L")]
    _gain_loss: String,
    #[serde(rename = "G/L (%)")]
    _gain_loss_percent: String,
    #[serde(rename = "Percentage of Assets")]
    _percent_assets: String,
}

impl HoldingRecord {
    fn store_balance(
        &self,
        bkdate: NaiveDate,
        currency: &str,
        state: &mut LedgerState,
    ) -> Result<(), Error> {
        let owner = match self.client_name.as_str() {
            "ROBERT HUM" => "Stan",
            "JESSICA DUBY" => "Jess",
            "ROBERT/JESSICA HUM/DUBY" => "Joint",
            _ => "UNKNOWN",
        };
        let acct = self.account_number.as_str();

        let cash = acct_cash!(owner, acct);
        let sec = acct_securities!(&owner, acct);

        let posno = state.line_count.fetch_add(1, Ordering::SeqCst);

        let v = if self.holding == "CASH" {
            let cp_s = if self.fund != "" {
                self.fund.clone()
            } else {
                currency.to_string()
            };

            VerificationParams {
                statement_no: posno,
                file_no: 0u32,
                start: 0u32,
                end: 0u32,
                date: bkdate,
                action: BALANCE_ACTION,
                account: cash,
                quantity: Some(self.quantity),
                commodity: Some(cp_s),
            }
        } else {
            let cp_s = self.symbol.clone();
            VerificationParams {
                statement_no: posno,
                file_no: 0u32,
                start: 0u32,
                end: 0u32,
                date: bkdate,
                action: BALANCE_ACTION,
                account: sec,
                quantity: Some(self.quantity),
                commodity: Some(cp_s),
            }
            // TODO: add book value
        };

        state.verifications.push(v);

        Ok(())
    }
}

pub fn process_activites(
    filepath: &str,
    acct: &str,
    owner: &str,
    currency: &str,
    symbol_filepath: &str,
    state: &mut LedgerState,
) -> Result<(), Error> {
    let symbols = load_symbols(symbol_filepath.to_string())?;
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b',')
        .quoting(true)
        .from_path(filepath)?;
    for result in rdr.deserialize::<TransRecord>() {
        match result {
            Ok(t) => {
                t.store_transaction(acct, owner, currency, &symbols, state)?;
            }
            Err(e) => {
                println!("{:?}\n", e);
            }
        }
    }
    Ok(())
}

pub fn compile_holdings(
    filepath: &str,
    bkdate: NaiveDate,
    currency: &str,
    state: &mut LedgerState,
) -> Result<(), Error> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b',')
        .quoting(true)
        .from_path(filepath)?;

    for result in rdr.deserialize::<HoldingRecord>() {
        match result {
            Ok(t) => {
                t.store_balance(bkdate, currency, state)?;
            }
            Err(e) => {
                println!("{:?}\n", e);
            }
        }
    }
    Ok(())
}

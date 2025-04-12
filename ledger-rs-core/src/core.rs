use arrow_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};

use chrono::NaiveDate;

use rust_decimal::Decimal;

pub const ASSETS_BASE: &str = "Assets";
pub const LIABILITIES_BASE: &str = "Liabilities";
pub const EQUITY_BASE: &str = "Equity";
pub const INCOME_BASE: &str = "Income";
pub const EXPENSES_BASE: &str = "Expenses";

pub const OPEN_SYMBOL: &str = "open";
pub const CLOSE_SYMBOL: &str = "close";
pub const BALANCE_SYMBOL: &str = "balance";
pub const EVENT_SYMBOL: &str = "event";
pub const OPTION_SYMBOL: &str = "option";
pub const INCLUDE_SYMBOL: &str = "include";
pub const CUSTOM_SYMBOL: &str = "custom";

pub const DATE_FORMAT: &str = "%Y-%m-%d";
pub const ACCOUNT: &str = "account";
pub const FINAL_CP_COMMODITY: &str = "cp_commodity_final";
pub const FINAL_CP_QUANTITY: &str = "cp_quantity_final";
pub const FINAL_TC_COMMODITY: &str = "tc_commodity_final";
pub const FINAL_TC_QUANTITY: &str = "tc_quantity_final";

pub const CP_COMMODITY: &str = "cp_commodity";
pub const CP_QUANTITY: &str = "cp_quantity";
pub const FILE_NO: &str = "file_no";
pub const LENGTH: &str = "length";
pub const START: &str = "start";
pub const STATEMENT_NO: &str = "statement_no";
pub const TC_COMMODITY: &str = "tc_commodity";
pub const TC_COMMODITY_RIGHT: &str = "tc_commodity_right";
pub const TC_QUANTITY: &str = "tc_quantity";
pub const TOTALS: &str = "totals";
pub const TRANSACTION_NO: &str = "transaction_no";
pub const ACCOUNT_SEP: &str = ":";
pub const PRECISION: usize = 38;
pub const SCALE: usize = 2;

pub const TOTAL: &str = "total";
pub const ACCOUNT_RIGHT: &str = "account_right";
pub const MATCH: &str = "match";
pub const DATE: &str = "date";
pub const COST_SEP: &str = "@@";
pub const TRANSACTION_FLAG: &str = "*";
pub const TAGS: &str = "tags";

pub const NARRATION: &str = "narration";

pub const OPEN_ACTION: u32 = 0;
pub const BALANCE_ACTION: u32 = 1;
pub const CLOSE_ACTION: u32 = 2;
pub const EVENT_ACTION: u32 = 3;
pub const OPTION_ACTION: u32 = 4;
pub const CUSTOM_ACTION: u32 = 5;

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
pub struct IncludeParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]

pub struct VerificationParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub date: NaiveDate,
    pub action: u32, // Open, Balance, CLose
    pub account: String,
    pub quantity: Option<Decimal>,
    pub commodity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]

pub struct HeaderParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub date: NaiveDate,
    pub narration: String,
    pub tags: Option<String>,
}

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]

pub struct PostingParams {
    pub statement_no: u32,
    pub transaction_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub account: String,
    pub cp_quantity: Option<Decimal>,
    pub cp_commodity: Option<String>,
    pub tc_quantity: Option<Decimal>,
    pub tc_commodity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
pub struct InfoParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub date: Option<NaiveDate>,
    pub action: u32, // Event, Option, Custom
    pub attribute: Option<String>,
    pub value: String,
}

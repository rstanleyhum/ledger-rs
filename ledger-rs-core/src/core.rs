use arrow_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};

use chrono::NaiveDate;

use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowSerialize, ArrowDeserialize)]
pub struct IncludeParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub path: String,
}

pub const OPEN_ACTION: u32 = 0;
pub const BALANCE_ACTION: u32 = 1;
pub const CLOSE_ACTION: u32 = 2;

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
    pub tags: Option<Vec<String>>,
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

pub const EVENT_ACTION: u32 = 3;
pub const OPTION_ACTION: u32 = 4;
pub const CUSTOM_ACTION: u32 = 5;

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

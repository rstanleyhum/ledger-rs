use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    string::String,
};

use anyhow::Result;
use chrono::NaiveDate;
use encoding_rs::{Encoding, WINDOWS_1252};
use encoding_rs_io::DecodeReaderBytesBuilder;
use ledger_rs_core::{
    core::{BALANCE_ACTION, HeaderParams, PostingParams, VerificationParams},
    state::LedgerState,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};

use crate::symbols::load_accounts;

#[derive(Debug)]
pub struct InterTrans {
    pub date: NaiveDate,
    pub narration: String,
    pub account: String,
    pub quantity: Decimal,
    pub commodity: String,
}

#[derive(Debug)]
pub struct InterBalance {
    pub date: NaiveDate,
    pub account: String,
    pub quantity: Decimal,
    pub commodity: String,
}

#[derive(Debug)]
pub struct QfxImportState {
    pub transactions: Vec<InterTrans>,
    pub balances: Vec<InterBalance>,
}

impl QfxImportState {
    pub fn new() -> Self {
        Self {
            transactions: vec![],
            balances: vec![],
        }
    }

    fn append_transaction(
        &mut self,
        date: NaiveDate,
        narration: String,
        account: String,
        quantity: Decimal,
        commodity: String,
    ) {
        self.transactions.push(InterTrans {
            date,
            narration,
            account,
            quantity,
            commodity,
        });
    }

    fn append_balance(
        &mut self,
        date: NaiveDate,
        account: String,
        quantity: Decimal,
        commodity: String,
    ) {
        self.balances.push(InterBalance {
            date,
            account,
            quantity,
            commodity,
        })
    }
}
///
/// OFXHeader is not used. It is here for documentation purposes. The importer doesn't use any of the fields
///
#[derive(Debug, Deserialize)]
pub struct _OFXHeader {
    #[serde(rename = "OFXHEADER")]
    pub ofx_header: String,
    #[serde(rename = "DATA")]
    pub data: String,
    #[serde(rename = "VERSION")]
    pub version: String,
    #[serde(rename = "SECURITY")]
    pub security: String,
    #[serde(rename = "ENCODING")]
    pub encoding: String,
    #[serde(rename = "CHARSET")]
    pub charset: String,
    #[serde(rename = "COMPRESSION")]
    pub compression: String,
    #[serde(rename = "OLDFILEUID")]
    pub old_file_uid: String,
    #[serde(rename = "NEWFILEUID")]
    pub new_file_uid: String,
    #[serde(skip)]
    pub ofx_data: String,
}

#[derive(Debug, Deserialize)]
pub struct OFX {
    #[serde(rename = "signonmsgsrsv1")]
    _signonmsgsrsv1: SIGNONMSGSRSV1,
    bankmsgsrsv1: Option<BANKMSGSRSV1>,
    creditcardmsgsrsv1: Option<CREDITCARDMSGSRSV1>,
}

impl OFX {
    pub fn to_bk(&self, state: &mut QfxImportState) -> Result<()> {
        if let Some(b) = &self.bankmsgsrsv1 {
            b.to_bk(state)?;
        }
        if let Some(c) = &self.creditcardmsgsrsv1 {
            c.to_bk(state)?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct SIGNONMSGSRSV1 {
    #[serde(rename = "sonrs")]
    _sonrs: SONRS,
}

#[derive(Debug, Deserialize)]
struct SONRS {
    #[serde(rename = "status")]
    _status: STATUS,
    #[serde(rename = "dtserver", skip)]
    _dtserver: String,
    #[serde(rename = "language", skip)]
    _language: String,
    #[serde(rename = "intu.bid", skip)]
    _intu_bid: String,
}

#[derive(Debug, Deserialize)]
struct STATUS {
    #[serde(rename = "code", skip)]
    _code: String,
    #[serde(rename = "severity", skip)]
    _severity: String,
    #[serde(rename = "message")]
    _message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BANKMSGSRSV1 {
    #[serde(rename = "stmttrnrs")]
    stmttrnrs: Vec<STMTTRNRS>,
}

impl BANKMSGSRSV1 {
    fn to_bk(&self, state: &mut QfxImportState) -> Result<()> {
        for x in self.stmttrnrs.iter() {
            x.to_bk(state)?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct STMTTRNRS {
    #[serde(rename = "trnuid", skip)]
    _trnuid: String,
    #[serde(rename = "status")]
    _status: STATUS,
    stmtrs: STMTRS,
}

impl STMTTRNRS {
    fn to_bk(&self, state: &mut QfxImportState) -> Result<()> {
        self.stmtrs.to_bk(state)
    }
}

#[derive(Debug, Deserialize)]
struct STMTRS {
    curdef: String,
    bankacctfrom: BANKACCTFROM,
    banktranlist: BANKTRANLIST,
    ledgerbal: LEDGERBAL,
    #[serde(rename = "availbal")]
    _availbal: AVAILBAL,
}

impl STMTRS {
    fn to_bk(&self, state: &mut QfxImportState) -> Result<()> {
        let acctid = self.bankacctfrom.get_acctid();
        let currency = self.curdef.clone();
        self.banktranlist
            .to_bk(state, acctid.clone(), currency.clone())?;
        self.ledgerbal
            .to_bk(state, acctid.clone(), currency.clone())?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct BANKACCTFROM {
    #[serde(rename = "bankid", skip)]
    _bankid: String,
    acctid: String,
    #[serde(rename = "accttype", skip)]
    _accttype: String,
}

impl BANKACCTFROM {
    fn get_acctid(&self) -> String {
        format!("{}", self.acctid)
    }
}

#[derive(Debug, Deserialize)]
struct BANKTRANLIST {
    #[serde(rename = "dtstart", skip)]
    _dtstart: String,
    #[serde(rename = "dtend", skip)]
    _dtend: String,
    #[serde(rename = "stmttrn")]
    stmtrn_list: Vec<STMTTRN>,
}

impl BANKTRANLIST {
    fn to_bk(&self, state: &mut QfxImportState, acctid: String, currency: String) -> Result<()> {
        for x in self.stmtrn_list.iter() {
            x.to_bk(state, acctid.clone(), currency.clone())?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct STMTTRN {
    #[serde(rename = "trntype", skip)]
    _trntype: String,
    #[serde(deserialize_with = "from_qfx_datetime")]
    dtposted: NaiveDate,
    #[serde(deserialize_with = "from_qfx_decimal")]
    trnamt: Decimal,
    #[serde(rename = "fitid", skip)]
    _fitid: String,
    name: Option<String>,
    memo: Option<String>,
}

impl STMTTRN {
    fn to_bk(&self, state: &mut QfxImportState, acctid: String, currency: String) -> Result<()> {
        let dt = self.dtposted.clone();
        let amt = self.trnamt.clone();
        let narration = match (&self.name, &self.memo) {
            (Some(n), Some(m)) => {
                format!("{n} / {m}")
            }
            (Some(n), None) => n.clone(),
            (None, Some(m)) => m.clone(),
            (None, None) => "PROBLEM".to_string(),
        };
        state.append_transaction(dt, narration, acctid, amt, currency);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct LEDGERBAL {
    #[serde(deserialize_with = "from_qfx_decimal")]
    balamt: Decimal,
    #[serde(deserialize_with = "from_qfx_datetime")]
    dtasof: NaiveDate,
}

impl LEDGERBAL {
    fn to_bk(&self, state: &mut QfxImportState, acctid: String, currency: String) -> Result<()> {
        let dt = self.dtasof.clone();
        let amt = self.balamt.clone();
        state.append_balance(dt, acctid, amt, currency);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct AVAILBAL {
    #[serde(rename = "balamt", skip)]
    _balamt: String,
    #[serde(rename = "dtasof", skip)]
    _dtasof: String,
}

#[derive(Debug, Deserialize)]
struct CREDITCARDMSGSRSV1 {
    ccstmttrnrs: CCSTMTTRNRS,
}

impl CREDITCARDMSGSRSV1 {
    fn to_bk(&self, state: &mut QfxImportState) -> Result<()> {
        self.ccstmttrnrs.to_bk(state)
    }
}

#[derive(Debug, Deserialize)]
struct CCSTMTTRNRS {
    #[serde(rename = "trnuid", skip)]
    _trnuid: String,
    #[serde(rename = "status")]
    _status: STATUS,
    ccstmtrs: CCSTMTRS,
}

impl CCSTMTTRNRS {
    fn to_bk(&self, state: &mut QfxImportState) -> Result<()> {
        self.ccstmtrs.to_bk(state)
    }
}

#[derive(Debug, Deserialize)]
struct CCSTMTRS {
    curdef: String,
    ccacctfrom: CCACCTFROM,
    banktranlist: BANKTRANLIST,
    ledgerbal: LEDGERBAL,
    #[serde(rename = "availbal")]
    _availbal: AVAILBAL,
}

impl CCSTMTRS {
    fn to_bk(&self, state: &mut QfxImportState) -> Result<()> {
        let acct = self.ccacctfrom.get_acctid();
        let currency = self.curdef.clone();
        self.banktranlist
            .to_bk(state, acct.clone(), currency.clone())?;
        self.ledgerbal
            .to_bk(state, acct.clone(), currency.clone())?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CCACCTFROM {
    acctid: String,
}

impl CCACCTFROM {
    fn get_acctid(&self) -> String {
        format!("{}", self.acctid)
    }
}

pub fn get_ofx_data(filename: &PathBuf, e: Option<&'static Encoding>) -> Result<String> {
    let rdr = File::open(filename)?;
    let reader = BufReader::new(DecodeReaderBytesBuilder::new().encoding(e).build(rdr));
    let mut in_ofx_data = false;
    let mut ofx_data_vec = Vec::<String>::new();
    for line in reader.lines() {
        let l = line.unwrap();
        if !in_ofx_data & l.trim().starts_with("<OFX>") {
            in_ofx_data = true;
            ofx_data_vec.push(l.to_string());
        } else if in_ofx_data {
            ofx_data_vec.push(l.to_string());
        } else {
            continue;
        }
    }

    let result = ofx_data_vec.join("\n");
    Ok(result)
}

pub fn process_qfx(filename: &PathBuf, encoding: Option<&'static Encoding>) -> Result<OFX> {
    let input = get_ofx_data(&filename, encoding)?;
    let sgml = sgmlish::Parser::builder()
        .lowercase_names()
        .trim_whitespace(true)
        .expand_entities(|entity| match entity {
            "lt" => Some("<"),
            "gt" => Some(">"),
            "amp" => Some("&"),
            "nbsp" => Some(" "),
            _ => None,
        })
        .parse(&input)?;
    let sgml = sgmlish::transforms::normalize_end_tags(sgml)?;
    let ofx_data = sgmlish::from_fragment::<OFX>(sgml)?;
    Ok(ofx_data)
}

pub fn parse_qfx_file(
    filename: PathBuf,
    encoding: Option<String>,
    symbols_f: PathBuf,
    state: &mut LedgerState,
) -> Result<()> {
    let symbols = load_accounts(String::from(symbols_f.to_str().unwrap())).unwrap();
    let e = match encoding {
        Some(e_string) => {
            if e_string == "1252" {
                Some(WINDOWS_1252)
            } else {
                None
            }
        }
        None => None,
    };
    let mut import_state = QfxImportState::new();
    let ofx_data = process_qfx(&filename, e)?;
    ofx_data.to_bk(&mut import_state)?;

    let mut count = 1;
    import_state.transactions.iter().for_each(|t| {
        let acct = match symbols.get(&t.account) {
            Some(n) => n.clone(),
            None => t.account.clone(),
        };
        state.transactions.push(HeaderParams {
            statement_no: count,
            file_no: 0u32,
            start: 0u32,
            end: 0u32,
            date: t.date,
            narration: t.narration.clone(),
            tags: None,
        });
        state.postings.push(PostingParams {
            statement_no: count,
            transaction_no: count,
            file_no: 0u32,
            start: 0u32,
            end: 0u32,
            account: acct,
            cp_quantity: Some(t.quantity),
            cp_commodity: Some(t.commodity.clone()),
            tc_quantity: Some(t.quantity),
            tc_commodity: Some(t.commodity.clone()),
        });
        count = count + 1;
    });
    import_state.balances.iter().for_each(|t| {
        let acct = match symbols.get(&t.account) {
            Some(n) => n.clone(),
            None => t.account.clone(),
        };
        state.verifications.push(VerificationParams {
            statement_no: count,
            file_no: 0u32,
            start: 0u32,
            end: 0u32,
            date: t.date,
            action: BALANCE_ACTION,
            account: acct,
            quantity: Some(t.quantity),
            commodity: Some(t.commodity.clone()),
        });
        count = count + 1;
    });

    Ok(())
}

const QFX_DATE_FORMAT: &'static str = "%Y%m%d";

fn from_qfx_datetime<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveDate::parse_from_str(&s[0..8], QFX_DATE_FORMAT).map_err(serde::de::Error::custom)
}

fn from_qfx_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Decimal::from_str_exact(&s).map_err(serde::de::Error::custom)
}

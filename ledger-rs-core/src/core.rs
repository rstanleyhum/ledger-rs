use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{Error, Read},
    ops::Range,
    path::{Path, PathBuf},
};

use chrono::NaiveDate;
use rust_decimal::Decimal;
use winnow::{LocatingSlice, Stateful, Str};

#[derive(PartialEq, Debug)]
pub struct OpenParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub date: NaiveDate,
    pub account: String,
}

#[derive(PartialEq, Debug)]
pub struct CloseParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub date: NaiveDate,
    pub account: String,
}

#[derive(PartialEq, Debug)]
pub struct BalanceParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub date: NaiveDate,
    pub account: String,
    pub position: Decimal,
    pub commodity: String,
}

#[derive(PartialEq, Debug)]
pub struct IncludeParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub path: String,
    pub statements: Vec<Statement>,
}

#[derive(PartialEq, Debug)]
pub struct HeaderParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub date: NaiveDate,
    pub narration: String,
    pub tags: Option<Vec<String>>,
}

#[derive(PartialEq, Debug)]
pub struct PostingParams {
    pub statement_no: u32,
    pub file_no: u32,
    pub start: u32,
    pub end: u32,
    pub account: String,
    pub cp_q: Option<Decimal>,
    pub cp_c: Option<String>,
    pub tc_q: Option<Decimal>,
    pub tc_c: Option<String>,
}

#[derive(PartialEq, Debug)]
pub struct TransactionParams {
    pub header: HeaderParams,
    pub postings: Vec<PostingParams>,
}

#[derive(PartialEq, Debug)]
pub enum Statement {
    Open((OpenParams, Range<usize>)),
    Close((CloseParams, Range<usize>)),
    Balance((BalanceParams, Range<usize>)),
    Include((IncludeParams, Range<usize>)),
    Transaction((TransactionParams, Range<usize>)),
    Event(Range<usize>),
    Option(Range<usize>),
    Custom(Range<usize>),
    Comment(Range<usize>),
    Empty(Range<usize>),
    Other(Range<usize>),
}

pub type BeanInput<'b> = Stateful<LocatingSlice<Str<'b>>, &'b mut LedgerParserState>;

pub fn new_beaninput<'s>(s: &'s str, state: &'s mut LedgerParserState) -> BeanInput<'s> {
    Stateful {
        input: LocatingSlice::new(s),
        state: state,
    }
}

#[derive(Debug)]
pub struct LedgerParserState {
    pub input_files: HashMap<PathBuf, u32>,
    current_file_no: Vec<u32>,
    previous_position: HashMap<u32, u32>,
    statement_no: u32,
}

impl LedgerParserState {
    pub fn new() -> Self {
        Self {
            input_files: HashMap::new(),
            current_file_no: vec![],
            previous_position: HashMap::new(),
            statement_no: 0,
        }
    }

    pub fn insert(&mut self, f: PathBuf) {
        if !self.input_files.contains_key(&f) {
            let n = self.input_files.len();
            self.input_files.insert(f, n as u32);
            self.current_file_no.push(n as u32);
            self.previous_position.insert(n as u32, 0);
        }
    }

    pub fn statement_no(&mut self, r_start: u32) -> u32 {
        let prev = self
            .previous_position
            .get(&self.get_file_no().unwrap())
            .unwrap();
        self.statement_no = self.statement_no + r_start - *prev;
        self.previous_position
            .insert(self.get_file_no().unwrap(), r_start);
        self.statement_no
    }

    pub fn get_file_no(&self) -> Option<u32> {
        let current = self.current_file_no.len();
        if current == 0 {
            None
        } else {
            Some(self.current_file_no[current - 1])
        }
    }

    pub fn finished(&mut self) {
        self.current_file_no.pop();
    }
}

pub fn get_contents(f: &Path) -> Result<String, Error> {
    let mut s = String::new();
    let mut infile = OpenOptions::new().read(true).open(f).unwrap();
    let _ = infile.read_to_string(&mut s).unwrap();
    Ok(s)
}

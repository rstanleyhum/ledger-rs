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
    pub input_files: HashMap<PathBuf, (u32, bool)>,
    pub current_file_no: u32,
    pub current_position: u32,
}

impl LedgerParserState {
    pub fn new() -> Self {
        Self {
            input_files: HashMap::new(),
            current_file_no: 0,
            current_position: 0,
        }
    }

    pub fn insert(&mut self, f: PathBuf) {
        if !self.input_files.contains_key(&f) {
            let n = self.input_files.len();
            self.input_files.insert(f, (n as u32, false));
        }
    }

    pub fn set_read(&mut self, f: PathBuf) {
        match self.input_files.get(&f) {
            Some((n, _)) => {
                self.input_files.insert(f, (*n, true));
            }
            None => {}
        }
    }

    pub fn all_files_read(&self) -> bool {
        for (_, (_, d)) in &self.input_files {
            if !d {
                return false;
            }
        }
        true
    }

    pub fn set_current_file_no(&mut self, n: u32) {
        self.current_file_no = n;
    }

    pub fn increment_pos(&mut self) {
        self.current_position += 1;
    }
}

pub struct BeanFileStorage {
    pub file_contents: HashMap<u32, String>,
}

impl BeanFileStorage {
    pub fn new() -> Self {
        Self {
            file_contents: HashMap::new(),
        }
    }

    pub fn add(&mut self, c: String, f: u32) {
        self.file_contents.insert(f, c);
    }

    pub fn get_ref<'s>(&'s mut self, f: u32) -> &'s str {
        self.file_contents.get_mut(&f).unwrap()
    }
}

pub fn get_contents(f: &Path) -> Result<String, Error> {
    let mut s = String::new();
    let mut infile = OpenOptions::new().read(true).open(f).unwrap();
    let _ = infile.read_to_string(&mut s).unwrap();
    Ok(s)
}

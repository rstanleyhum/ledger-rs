use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Error,
    io::Read,
    path::{Path, PathBuf},
};

use arrow::array::ArrayRef;
use arrow_convert::serialize::TryIntoArrow;
use polars::frame::DataFrame;

use crate::core::{HeaderParams, IncludeParams, InfoParams, PostingParams, VerificationParams};

#[derive(Debug)]
pub struct LedgerParserState {
    pub input_files: HashMap<PathBuf, u32>,
    current_file_no: Vec<u32>,
    current_filepath: Vec<PathBuf>,
    previous_position: HashMap<u32, u32>,
    statement_no: u32,
    pub transaction_no: u32,
    pub transactions: Vec<HeaderParams>,
    pub postings: Vec<PostingParams>,
    pub verifications: Vec<VerificationParams>,
    pub includes: Vec<IncludeParams>,
    pub informationals: Vec<InfoParams>,
    pub final_df: DataFrame,
    pub errors_df: DataFrame,
}

impl LedgerParserState {
    pub fn new() -> Self {
        Self {
            input_files: HashMap::new(),
            current_file_no: vec![],
            current_filepath: vec![],
            previous_position: HashMap::new(),
            statement_no: 0,
            transaction_no: 0,
            transactions: vec![],
            postings: vec![],
            verifications: vec![],
            includes: vec![],
            informationals: vec![],
            final_df: DataFrame::empty(),
            errors_df: DataFrame::empty(),
        }
    }

    pub fn insert(&mut self, f: PathBuf) {
        if !self.input_files.contains_key(&f) {
            let n = self.input_files.len();
            self.input_files.insert(f.clone(), n as u32);
            self.current_file_no.push(n as u32);
            self.current_filepath.push(f);
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

    pub fn get_current_filepath(&self) -> Option<PathBuf> {
        let current = self.current_file_no.len();
        if current == 0 {
            None
        } else {
            Some(self.current_filepath[current - 1].clone())
        }
    }

    pub fn get_file_no(&self) -> Option<u32> {
        let current = self.current_file_no.len();
        if current == 0 {
            None
        } else {
            Some(self.current_file_no[current - 1])
        }
    }

    pub fn finished_include(&mut self, n: u32) {
        let prev = self
            .previous_position
            .get(&self.get_file_no().unwrap())
            .unwrap();
        self.statement_no = self.statement_no + n - *prev;
        self.previous_position
            .insert(*&self.get_file_no().unwrap(), n);
        self.current_file_no.pop();
    }

    pub fn try_transactions(&self) -> arrow::error::Result<ArrayRef> {
        self.transactions.try_into_arrow()
    }

    pub fn try_postings(&self) -> arrow::error::Result<ArrayRef> {
        self.postings.try_into_arrow()
    }

    pub fn try_verifications(&self) -> arrow::error::Result<ArrayRef> {
        self.verifications.try_into_arrow()
    }

    pub fn try_informationals(&self) -> arrow::error::Result<ArrayRef> {
        self.informationals.try_into_arrow()
    }

    pub fn try_includes(&self) -> arrow::error::Result<ArrayRef> {
        self.includes.try_into_arrow()
    }
}

pub fn get_contents(f: &Path) -> Result<(String, u32), Error> {
    let mut s = String::new();
    let mut infile = OpenOptions::new().read(true).open(f).unwrap();
    let n = infile.read_to_string(&mut s).unwrap();
    Ok((s, n as u32))
}

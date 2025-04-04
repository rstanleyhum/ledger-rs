use std::{collections::HashMap, path::PathBuf, sync::atomic::AtomicU32};

use polars::frame::DataFrame;

use crate::core::{HeaderParams, IncludeParams, InfoParams, PostingParams, VerificationParams};

#[derive(Debug)]
pub struct LedgerState {
    pub input_files: HashMap<PathBuf, u32>,
    current_file_no: Vec<u32>,
    current_filepath: Vec<PathBuf>,
    previous_position: HashMap<u32, u32>,
    statement_no: u32,
    pub line_count: AtomicU32,
    pub transaction_no: u32,
    pub transactions: Vec<HeaderParams>,
    pub postings: Vec<PostingParams>,
    pub verifications: Vec<VerificationParams>,
    pub includes: Vec<IncludeParams>,
    pub informationals: Vec<InfoParams>,
    pub postings_df: DataFrame,
    pub errors_df: DataFrame,
    pub accounts_df: DataFrame,
    pub commodities_df: DataFrame,
}

impl LedgerState {
    pub fn new() -> Self {
        Self {
            input_files: HashMap::new(),
            current_file_no: vec![],
            current_filepath: vec![],
            previous_position: HashMap::new(),
            statement_no: 0,
            line_count: AtomicU32::new(0),
            transaction_no: 0,
            transactions: vec![],
            postings: vec![],
            verifications: vec![],
            includes: vec![],
            informationals: vec![],
            postings_df: DataFrame::empty(),
            errors_df: DataFrame::empty(),
            accounts_df: DataFrame::empty(),
            commodities_df: DataFrame::empty(),
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
}

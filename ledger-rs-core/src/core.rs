use std::{fs::OpenOptions, io::Read, ops::Range, path::PathBuf};

use winnow::LocatingSlice;

use crate::parse;

#[derive(PartialEq, Debug)]
pub struct BeanFileParse<'s> {
    pub beanfile: BeanFile,
    pub statements: Vec<Statement<'s>>,
}

impl<'s> BeanFileParse<'s> {
    pub fn new(b: BeanFile) -> Self {
        BeanFileParse {
            beanfile: b,
            statements: vec![],
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct BeanFile {
    pub filepath: PathBuf,
    buffer: String,
    size: usize,
}

impl BeanFile {
    pub fn new(f: PathBuf) -> Self {
        let mut result = BeanFile {
            filepath: f,
            buffer: String::new(),
            size: 0,
        };
        result.read_file();
        result
    }

    fn read_file(&mut self) {
        let mut inputfile = OpenOptions::new()
            .read(true)
            .open(self.filepath.clone())
            .unwrap();
        let count = inputfile.read_to_string(&mut self.buffer).unwrap();
        self.size = count;
    }

    pub fn parse(&self) -> Vec<Statement<'_>> {
        parse::parse_file(&mut LocatingSlice::new(self.buffer.as_str())).unwrap()
    }
}

#[derive(PartialEq, Debug)]
pub struct OpenParams<'a> {
    pub date: &'a str,
    pub account: &'a str,
}

#[derive(PartialEq, Debug)]
pub struct CloseParams<'a> {
    pub date: &'a str,
    pub account: &'a str,
}

#[derive(PartialEq, Debug)]
pub struct BalanceParams<'a> {
    pub date: &'a str,
    pub account: &'a str,
    pub position: &'a str,
    pub commodity: &'a str,
}

#[derive(PartialEq, Debug)]
pub struct IncludeParams<'a> {
    pub path: &'a str,
}

#[derive(PartialEq, Debug)]
pub struct Transaction<'a> {
    pub header: &'a str,
    pub postings: Vec<&'a str>,
}

#[derive(PartialEq, Debug)]
pub enum Statement<'a> {
    Open((OpenParams<'a>, Range<usize>)),
    Close((CloseParams<'a>, Range<usize>)),
    Balance((BalanceParams<'a>, Range<usize>)),
    Include((IncludeParams<'a>, Range<usize>)),
    Transaction((Transaction<'a>, Range<usize>)),
    Event((&'a str, Range<usize>)),
    Option((&'a str, Range<usize>)),
    Custom((&'a str, Range<usize>)),
    Comment(&'a str),
    Empty(&'a str),
    Other((&'a str, Range<usize>)),
}

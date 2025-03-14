use std::{fs::OpenOptions, io::Read};

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
    directory: String,
    pub filename: String,
    buffer: String,
}

impl BeanFile {
    pub fn new(f: &str, d: &str) -> Self {
        let mut result = BeanFile {
            directory: d.to_string(),
            filename: f.to_string(),
            buffer: String::new(),
        };
        result.read_file();
        result
    }

    fn read_file(&mut self) {
        let path = format!("{}{}", self.directory, self.filename);
        let mut inputfile = OpenOptions::new().read(true).open(path).unwrap();
        let _ = inputfile.read_to_string(&mut self.buffer).unwrap();
    }

    pub fn parse(&self) -> Vec<Statement<'_>> {
        parse::parse_file(&mut self.buffer.as_str()).unwrap()
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
    Open(OpenParams<'a>),
    Close(CloseParams<'a>),
    Balance(BalanceParams<'a>),
    Include(IncludeParams<'a>),
    Transaction(Transaction<'a>),
    Event(&'a str),
    Option(&'a str),
    Custom(&'a str),
    Comment(&'a str),
    Empty(&'a str),
    Other(&'a str),
}

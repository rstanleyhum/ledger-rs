use std::{collections::HashMap, fmt, path::PathBuf, sync::atomic::AtomicU32};

use anyhow::Context;
use anyhow::Result;

use arrow::array::Date32Array;
use arrow::array::Decimal128Array;
use arrow::array::StringArray;
use arrow::array::UInt32Array;
use arrow::datatypes::Date32Type;
use arrow::datatypes::Decimal128Type;
use arrow::datatypes::DecimalType;
use datafusion::common::JoinType;
use datafusion::prelude::*;

use futures::StreamExt;
use itertools::izip;

use crate::core::ACCOUNT;
use crate::core::ACTION_COL;
use crate::core::COMMODITY;
use crate::core::DATE;
use crate::core::ERROR_NO_POSTINGS_DF;
use crate::core::FINAL_CP_COMMODITY;
use crate::core::FINAL_CP_QUANTITY;
use crate::core::FINAL_TC_COMMODITY;
use crate::core::FINAL_TC_QUANTITY;
use crate::core::NARRATION;
use crate::core::PRECISION;
use crate::core::QUANTITY;
use crate::core::SCALE;
use crate::core::STATEMENT_NO;
use crate::core::STATEMENT_NO_RIGHT;
use crate::core::TAGS;
use crate::core::TRANSACTION_NO;
use crate::core::{
    BALANCE_ACTION, BALANCE_SYMBOL, COST_SEP, HeaderParams, IncludeParams, InfoParams,
    PostingParams, TRANSACTION_FLAG, VerificationParams,
};

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
    pub transactions_df: Option<DataFrame>,
    pub postings_df: Option<DataFrame>,
    pub errors_df: Option<DataFrame>,
    pub accounts_df: Option<DataFrame>,
    pub tc_commodities_df: Option<DataFrame>,
    pub cp_commodities_df: Option<DataFrame>,
    pub verifications_df: Option<DataFrame>,
}

impl fmt::Debug for LedgerState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Ledger State: {}", self.input_files.len())
    }
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
            transactions_df: None,
            postings_df: None,
            errors_df: None,
            accounts_df: None,
            tc_commodities_df: None,
            cp_commodities_df: None,
            verifications_df: None,
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

    pub async fn write_balances(&self) -> Result<()> {
        let df = self
            .verifications_df
            .clone()
            .expect("No verifications df")
            .filter(col(ACTION_COL).eq(lit(BALANCE_ACTION)))?;

        let mut stream = df.execute_stream().await?;

        while let Some(b) = stream.next().await.transpose()? {
            let t_date = b
                .column_by_name(DATE)
                .unwrap()
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Unable to downcast date");
            let account = b
                .column_by_name(ACCOUNT)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Account unable to downcast");
            let commodity = b
                .column_by_name(COMMODITY)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Unable to downcast cp commodity col");
            let quantity = b
                .column_by_name(QUANTITY)
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("Unable to downcast decimal");

            for rec in izip!(t_date, account, commodity, quantity) {
                match rec {
                    (Some(d), Some(a), Some(c), Some(q)) => {
                        let actual_d = Date32Type::to_naive_date(d);
                        let actual_q =
                            Decimal128Type::format_decimal(q, PRECISION as u8, SCALE as i8);
                        println!("{} {} {} {} {}", actual_d, BALANCE_SYMBOL, a, actual_q, c);
                    }
                    _ => println!("Nothing"),
                };
            }
        }

        Ok(())
    }

    pub async fn write_transactions(&self) -> Result<()> {
        let transactions_df = self.transactions_df.clone().context("NO TRANSACTIONS DF")?;
        let postings_df = self.postings_df.clone().context(ERROR_NO_POSTINGS_DF)?;
        let df = transactions_df
            .join(
                postings_df.select(vec![
                    col(STATEMENT_NO).alias(STATEMENT_NO_RIGHT),
                    col(TRANSACTION_NO),
                    col(ACCOUNT),
                    col(FINAL_CP_COMMODITY),
                    col(FINAL_CP_QUANTITY),
                    col(FINAL_TC_COMMODITY),
                    col(FINAL_TC_QUANTITY),
                ])?,
                JoinType::Left,
                &[STATEMENT_NO],
                &[TRANSACTION_NO],
                None,
            )?
            .sort(vec![
                col(DATE).sort(true, false),
                col(STATEMENT_NO_RIGHT).sort(true, false),
            ])?;

        let mut stream = df.execute_stream().await?;

        let mut current_transaction_no: u32 = 0;

        while let Some(b) = stream.next().await.transpose()? {
            let narration = b
                .column_by_name(NARRATION)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Unable to downcast string array");
            let tags = b
                .column_by_name(TAGS)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Unable to downcast Tags");
            let t_date = b
                .column_by_name(DATE)
                .unwrap()
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("Unable to downcast date");
            let account = b
                .column_by_name(ACCOUNT)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Account unable to downcast");
            let transaction_no = b
                .column_by_name(TRANSACTION_NO)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("Unable to downcast transaction no col");
            let cp_commodity = b
                .column_by_name(FINAL_CP_COMMODITY)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Unable to downcast cp commodity col");
            let cp_quantity = b
                .column_by_name(FINAL_CP_QUANTITY)
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("Unable to downcast decimal");
            let tc_commodity = b
                .column_by_name(FINAL_TC_COMMODITY)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Unable to downcast tc commodity col");
            let tc_quantity = b
                .column_by_name(FINAL_TC_QUANTITY)
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("Unable to downcast decimal");

            for rec in izip!(
                transaction_no,
                t_date,
                narration,
                tags,
                account,
                cp_commodity,
                cp_quantity,
                tc_commodity,
                tc_quantity
            ) {
                match rec {
                    (
                        Some(t_no),
                        Some(d),
                        Some(n),
                        ts,
                        Some(a),
                        Some(cp_c),
                        Some(cp_q),
                        Some(tc_c),
                        Some(tc_q),
                    ) => {
                        if current_transaction_no != t_no {
                            println!();
                            let actual_d = Date32Type::to_naive_date(d);
                            match ts {
                                Some(tag_string) => {
                                    println!(
                                        "{}: {} {} \"{}\" {}",
                                        t_no, actual_d, TRANSACTION_FLAG, n, tag_string
                                    )
                                }
                                None => println!(
                                    "{}: {} {} \"{}\" ",
                                    t_no, actual_d, TRANSACTION_FLAG, n
                                ),
                            }
                            current_transaction_no = t_no;
                        }
                        let actual_cp_q =
                            Decimal128Type::format_decimal(cp_q, PRECISION as u8, SCALE as i8);
                        if cp_c == tc_c {
                            println!("  {} {} {}", a, actual_cp_q, cp_c);
                        } else {
                            let actual_tc_q =
                                Decimal128Type::format_decimal(tc_q, PRECISION as u8, SCALE as i8);
                            println!(
                                "  {} {} {} {} {} {}",
                                a, actual_cp_q, cp_c, COST_SEP, actual_tc_q, tc_c
                            );
                        }
                    }
                    _ => println!("Nothing"),
                };
            }
        }

        Ok(())
    }
}

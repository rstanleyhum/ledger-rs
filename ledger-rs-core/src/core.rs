use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::{Error, Read},
    path::{Path, PathBuf},
};

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow_convert::ArrowField;
use arrow_convert::ArrowSerialize;
use arrow_convert::{ArrowDeserialize, serialize::TryIntoArrow};

use chrono::NaiveDate;
use polars::prelude::pivot::*;
use polars::prelude::*;

use df_interchange::Interchange;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use rust_decimal::Decimal;
use winnow::{LocatingSlice, Stateful, Str};

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
    current_filepath: Vec<PathBuf>,
    previous_position: HashMap<u32, u32>,
    statement_no: u32,
    pub transaction_no: u32,
    pub transactions: Vec<HeaderParams>,
    pub postings: Vec<PostingParams>,
    pub verifications: Vec<VerificationParams>,
    pub includes: Vec<IncludeParams>,
    pub informationals: Vec<InfoParams>,
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

    pub fn verify(&self) {
        let array = self.try_postings().unwrap();
        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let batch: RecordBatch = struct_array.try_into().unwrap();

        let df_interchange = Interchange::from_arrow_54(vec![batch]).unwrap();
        let df_polars = df_interchange.to_polars_0_46().unwrap();

        let final_df = df_polars
            .clone()
            .lazy()
            .filter(col("tc_commodity").is_not_null())
            .group_by(["transaction_no", "tc_commodity"])
            .agg([(col("tc_quantity").sum() * lit(-1.0))
                .cast(DataType::Decimal(Some(38), Some(10)))
                .alias("totals")])
            .filter(col("totals").neq(0))
            .select([
                col("transaction_no"),
                col("tc_commodity"),
                col("totals"),
                col("tc_commodity")
                    .rank(
                        RankOptions {
                            method: RankMethod::Dense,
                            descending: false,
                        },
                        None,
                    )
                    .over(["transaction_no"])
                    .alias("mynum"),
            ])
            .filter(col("mynum").eq(1))
            .join(
                df_polars.clone().lazy(),
                [col("transaction_no")],
                [col("transaction_no")],
                JoinArgs::new(JoinType::Right),
            )
            .select([
                col("statement_no"),
                col("transaction_no"),
                col("file_no"),
                col("start"),
                col("account"),
                coalesce(&[col("cp_commodity"), col("tc_commodity")]).alias("cp_commodity_final"),
                coalesce(&[col("cp_quantity"), col("totals")]).alias("cp_quantity_final"),
                coalesce(&[col("tc_commodity_right"), col("tc_commodity")])
                    .alias("tc_commodity_final"),
                coalesce(&[col("tc_quantity"), col("totals")]).alias("tc_quantity_final"),
            ])
            .collect()
            .unwrap();

        let _errors_df = final_df
            .clone()
            .lazy()
            .group_by(["transaction_no", "tc_commodity_final"])
            .agg([(col("tc_quantity_final").sum())
                .cast(DataType::Decimal(Some(38), Some(10)))
                .alias("totals")])
            .filter(col("totals").neq(0).or(col("tc_commodity_final").is_null()))
            .collect()
            .unwrap();

        let _accounts_df = final_df
            .clone()
            .lazy()
            .select([col("account").value_counts(false, false, "count", false)])
            .unnest(["account"])
            .collect()
            .unwrap();

        let account_sums_df = final_df
            .clone()
            .lazy()
            .group_by([col("account"), col("tc_commodity_final")])
            .agg([
                len().alias("count"),
                col("tc_quantity_final")
                    .sum()
                    .cast(DataType::Decimal(Some(38), Some(10)))
                    .alias("total"),
            ])
            .sort(["account"], Default::default())
            .collect()
            .unwrap();

        let mut pivot_df = pivot_stable(
            &account_sums_df,
            ["tc_commodity_final"],
            Some(["account"]),
            Some(["total"]),
            true,
            None,
            None,
        )
        .unwrap();

        let mut file = std::fs::File::create("./output.json").unwrap();

        // json
        JsonWriter::new(&mut file)
            .with_json_format(JsonFormat::Json)
            .finish(&mut pivot_df)
            .unwrap();

        // // ndjson
        // JsonWriter::new(&mut file)
        //     .with_json_format(JsonFormat::JsonLines)
        //     .finish(&mut df)
        //     .unwrap();

        let _check_df = final_df
            .clone()
            .lazy()
            .filter(col("tc_commodity_final").is_null())
            .select([all()])
            .collect()
            .unwrap();

        //println!("{}", final_df);
        //println!("{}", errors_df);
        println!("{}", pivot_df);
    }

    pub fn write_parquets(&self) {
        let array = self.try_postings().unwrap();
        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let batch: RecordBatch = struct_array.try_into().unwrap();

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open("./postings.parquet")
            .unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        let array = self.try_transactions().unwrap();
        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let batch: RecordBatch = struct_array.try_into().unwrap();

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open("./transactions.parquet")
            .unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();
    }
}

pub fn get_contents(f: &Path) -> Result<(String, u32), Error> {
    let mut s = String::new();
    let mut infile = OpenOptions::new().read(true).open(f).unwrap();
    let n = infile.read_to_string(&mut s).unwrap();
    Ok((s, n as u32))
}

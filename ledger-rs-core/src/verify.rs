use std::{collections::HashMap, fs::OpenOptions};

use crate::state::LedgerParserState;
use arrow::array::{Array, RecordBatch};
use df_interchange::Interchange;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use polars::prelude::*;

impl LedgerParserState {
    pub fn verify(&mut self) {
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

        let errors_df = final_df
            .clone()
            .lazy()
            .group_by(["transaction_no", "tc_commodity_final"])
            .agg([(col("tc_quantity_final").sum())
                .cast(DataType::Decimal(Some(38), Some(10)))
                .alias("totals")])
            .filter(col("totals").neq(0).or(col("tc_commodity_final").is_null()))
            .collect()
            .unwrap();

        self.final_df = final_df;
        self.errors_df = errors_df;
    }

    pub fn accounts(&self, c_col: &str, q_col: &str, filename: &str) {
        let commodities_df = self
            .final_df
            .clone()
            .lazy()
            .select([col(c_col).unique()])
            .collect()
            .unwrap();

        let commodities: Vec<String> = commodities_df[c_col]
            .str()
            .unwrap()
            .iter()
            .filter(|x| x.is_some())
            .map(|x| x.unwrap().to_string())
            .collect::<Vec<_>>();

        let account_list_df = self
            .final_df
            .clone()
            .lazy()
            .select([col("account").unique().str().split(lit(":"))])
            .collect()
            .unwrap();

        let mut accounts_series = account_list_df["account"].clone();
        let mut done = false;
        let mut n = 1;

        while !done {
            let a = account_list_df
                .clone()
                .lazy()
                .select([col("account"), col("account").list().len().alias("length")])
                .filter(col("length").gt(n))
                .select([col("account").list().slice(lit(0), col("length") - lit(n))])
                .collect()
                .unwrap();

            let t = a
                .clone()
                .lazy()
                .select([len().alias("count")])
                .collect()
                .unwrap()
                .column("count")
                .unwrap()
                .u32()
                .unwrap()
                .get(0)
                .unwrap();

            if t == 0 {
                done = true;
            }

            n = n + 1;

            accounts_series.append_owned(a["account"].clone()).unwrap();
        }

        let a_df = DataFrame::new(vec![accounts_series]).unwrap();

        let accounts_df = a_df
            .clone()
            .lazy()
            .unique(None, UniqueKeepStrategy::Any)
            .select([col("account").list().join(lit(":"), true).alias("account")])
            .sort(
                ["account"],
                SortMultipleOptions::new().with_order_descending(true),
            )
            .collect()
            .unwrap();

        let accounts: Vec<String> = accounts_df["account"]
            .str()
            .unwrap()
            .iter()
            .filter(|x| x.is_some())
            .map(|x| x.unwrap().to_string())
            .collect::<Vec<_>>();

        let mut all_totals: HashMap<String, Vec<String>> = HashMap::new();

        for c in commodities.clone() {
            let mut totals: Vec<String> = vec![];

            for a in accounts.clone() {
                let total = self
                    .final_df
                    .clone()
                    .lazy()
                    .filter(
                        col("account")
                            .str()
                            .starts_with(lit(a.clone()))
                            .and(col(c_col).eq(lit(c.clone()))),
                    )
                    .select([col(q_col)
                        .sum()
                        .cast(DataType::Decimal(Some(38), Some(10)))
                        .alias("total")])
                    .collect()
                    .unwrap()[0]
                    .decimal()
                    .unwrap()
                    .get(0)
                    .map(|x| rust_decimal::Decimal::try_from_i128_with_scale(x, 10).unwrap())
                    .map(|x| x.to_string())
                    .unwrap();

                totals.push(total);
            }

            all_totals.insert(c, totals);
        }
        let mut cols: Vec<Column> = vec![];

        cols.push(Column::new("accounts".into(), accounts.clone()));

        for c in commodities.clone() {
            cols.push(
                Column::new(c.clone().into(), all_totals.get(&c).unwrap())
                    .cast(&DataType::Decimal(Some(38), Some(10)))
                    .unwrap(),
            );
        }

        let mut df = DataFrame::new(cols).unwrap();

        let mut file = std::fs::File::create(filename).unwrap();
        CsvWriter::new(&mut file).finish(&mut df).unwrap();
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

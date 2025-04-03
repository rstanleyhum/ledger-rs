use crate::state::LedgerParserState;
use arrow::array::{Array, RecordBatch};
use arrow_convert::serialize::TryIntoArrow;
use df_interchange::Interchange;
use polars::prelude::*;

pub const ACCOUNT: &str = "account";
pub const FINAL_CP_COMMODITY: &str = "cp_commodity_final";
pub const FINAL_CP_QUANTITY: &str = "cp_quantity_final";
pub const FINAL_TC_COMMODITY: &str = "tc_commodity_final";
pub const FINAL_TC_QUANTITY: &str = "tc_quantity_final";

const CP_COMMODITY: &str = "cp_commodity";
const CP_QUANTITY: &str = "cp_quantity";
const FILE_NO: &str = "file_no";
const LENGTH: &str = "length";
const START: &str = "start";
const STATEMENT_NO: &str = "statement_no";
const TC_COMMODITY: &str = "tc_commodity";
const TC_COMMODITY_RIGHT: &str = "tc_commodity_right";
const TC_QUANTITY: &str = "tc_quantity";
const TOTALS: &str = "totals";
const TRANSACTION_NO: &str = "transaction_no";
const ACCOUNT_SEP: &str = ":";
const PRECISION: usize = 38;
const SCALE: usize = 2;

impl LedgerParserState {
    pub fn verify(&mut self) {
        let array: Arc<dyn Array> = self.postings.try_into_arrow().unwrap();
        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let batch: RecordBatch = struct_array.try_into().unwrap();

        let df_interchange = Interchange::from_arrow_54(vec![batch]).unwrap();
        let df_postings: DataFrame = df_interchange.to_polars_0_46().unwrap();

        let df_balancing = df_postings
            .clone()
            .lazy()
            .filter(col(TC_COMMODITY).is_not_null())
            .group_by([col(TRANSACTION_NO), col(TC_COMMODITY)])
            .agg([(col(TC_QUANTITY).sum() * lit(-1.0))
                .cast(DataType::Decimal(Some(PRECISION), Some(SCALE)))
                .alias(TOTALS)])
            .filter(col(TOTALS).neq(0))
            .group_by([col(TRANSACTION_NO), col(TC_COMMODITY)])
            .agg([col(TOTALS).first()]);

        let final_postings_df = df_postings
            .clone()
            .lazy()
            .join(
                df_balancing.clone(),
                [col(TRANSACTION_NO)],
                [col(TRANSACTION_NO)],
                JoinArgs::new(JoinType::Left),
            )
            .select([
                col(STATEMENT_NO),
                col(TRANSACTION_NO),
                col(FILE_NO),
                col(START),
                col(ACCOUNT),
                coalesce(&[col(CP_COMMODITY), col(TC_COMMODITY_RIGHT)]).alias(FINAL_CP_COMMODITY),
                coalesce(&[col(CP_QUANTITY), col(TOTALS)])
                    .cast(DataType::Decimal(Some(PRECISION), Some(SCALE)))
                    .alias(FINAL_CP_QUANTITY),
                coalesce(&[col(TC_COMMODITY), col(TC_COMMODITY_RIGHT)]).alias(FINAL_TC_COMMODITY),
                coalesce(&[col(TC_QUANTITY), col(TOTALS)])
                    .cast(DataType::Decimal(Some(PRECISION), Some(SCALE)))
                    .alias(FINAL_TC_QUANTITY),
            ])
            .collect()
            .unwrap();

        let errors_df = final_postings_df
            .clone()
            .lazy()
            .group_by([TRANSACTION_NO, FINAL_TC_COMMODITY])
            .agg([(col(FINAL_TC_QUANTITY).sum())
                .cast(DataType::Decimal(Some(PRECISION), Some(SCALE)))
                .alias(TOTALS)])
            .filter(col(TOTALS).neq(0).or(col(FINAL_TC_COMMODITY).is_null()))
            .collect()
            .unwrap();

        self.postings_df = final_postings_df;
        self.errors_df = errors_df;
        self.accounts();
    }

    pub fn commodities(&mut self, c_col: &str) -> DataFrame {
        self.postings_df
            .clone()
            .lazy()
            .select([col(c_col).unique()])
            .collect()
            .unwrap()
    }

    fn accounts(&mut self) {
        let account_list_df = self
            .postings_df
            .clone()
            .lazy()
            .select([col(ACCOUNT).unique().str().split(lit(ACCOUNT_SEP))])
            .collect()
            .unwrap();

        let mut accounts_series = account_list_df[ACCOUNT].clone();
        let mut done = false;
        let mut n = 1;

        while !done {
            let a = account_list_df
                .clone()
                .lazy()
                .select([col(ACCOUNT), col(ACCOUNT).list().len().alias(LENGTH)])
                .filter(col(LENGTH).gt(n))
                .select([col(ACCOUNT).list().slice(lit(0), col(LENGTH) - lit(n))])
                .collect()
                .unwrap();

            let (rows, _) = a.shape();

            if rows == 0 {
                done = true;
            }

            n = n + 1;

            accounts_series.append_owned(a[ACCOUNT].clone()).unwrap();
        }

        let a_df = DataFrame::new(vec![accounts_series]).unwrap();

        self.accounts_df = a_df
            .clone()
            .lazy()
            .unique(None, UniqueKeepStrategy::Any)
            .select([col(ACCOUNT)
                .list()
                .join(lit(ACCOUNT_SEP), true)
                .alias(ACCOUNT)])
            .sort(
                [ACCOUNT],
                SortMultipleOptions::new().with_order_descending(true),
            )
            .collect()
            .unwrap();
    }
}

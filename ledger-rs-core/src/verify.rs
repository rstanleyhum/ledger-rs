use crate::state::LedgerParserState;
use arrow::array::{Array, RecordBatch};
use arrow_convert::serialize::TryIntoArrow;
use df_interchange::Interchange;
use polars::error::PolarsResult;
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
    pub fn verify(&mut self) -> PolarsResult<()> {
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
            .collect()?;

        let errors_df = final_postings_df
            .clone()
            .lazy()
            .group_by([TRANSACTION_NO, FINAL_TC_COMMODITY])
            .agg([(col(FINAL_TC_QUANTITY).sum())
                .cast(DataType::Decimal(Some(PRECISION), Some(SCALE)))
                .alias(TOTALS)])
            .filter(col(TOTALS).neq(0).or(col(FINAL_TC_COMMODITY).is_null()))
            .collect()?;

        self.postings_df = final_postings_df;
        self.errors_df = errors_df;
        self.accounts_df = self.get_accounts_df()?;
        Ok(())
    }

    pub fn get_commodities_df(&mut self, c_col: &str) -> PolarsResult<DataFrame> {
        self.postings_df
            .clone()
            .lazy()
            .select([col(c_col).unique()])
            .collect()
    }

    fn get_accounts_df(&mut self) -> PolarsResult<DataFrame> {
        let account_list_df = self
            .postings_df
            .clone()
            .lazy()
            .select([col(ACCOUNT).unique().str().split(lit(ACCOUNT_SEP))])
            .with_column(col(ACCOUNT).list().len().alias(LENGTH));

        let max_n = account_list_df
            .clone()
            .select([col(LENGTH).max()])
            .collect()?
            .column(LENGTH)?
            .u32()?
            .get(0);

        if let Some(max_n) = max_n {
            let mut df = account_list_df.clone().collect()?;

            for n in 1..max_n {
                let a = account_list_df
                    .clone()
                    .filter(col(LENGTH).gt(n))
                    .with_column(col(ACCOUNT).list().slice(lit(0), col(LENGTH) - lit(n)))
                    .collect()?;

                df.vstack_mut(&a)?;
            }

            df.align_chunks_par();

            df.clone()
                .lazy()
                .select([col(ACCOUNT)])
                .unique(None, UniqueKeepStrategy::Any)
                .with_column(col(ACCOUNT).list().join(lit(ACCOUNT_SEP), true))
                .sort([ACCOUNT], Default::default())
                .collect()
        } else {
            Err(PolarsError::NoData("No Accounts Found".into()))
        }
    }
}

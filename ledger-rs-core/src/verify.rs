use std::sync::Arc;

use anyhow::Result;
use anyhow::anyhow;
use arrow::array::{Array, RecordBatch, UInt64Array};
use arrow::datatypes::DataType;
use arrow_convert::serialize::TryIntoArrow;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::functions_aggregate::min_max::max;
use datafusion::functions_array::extract::array_slice;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

use crate::core::ERROR_DOWNCAST;
use crate::core::ERROR_NO_ACCOUNTS_FOUND;
use crate::core::ERROR_NO_POSTINGS_DF;
use crate::core::{
    ACCOUNT, ACCOUNT_SEP, CP_COMMODITY, CP_QUANTITY, FILE_NO, FINAL_CP_COMMODITY,
    FINAL_CP_QUANTITY, FINAL_TC_COMMODITY, FINAL_TC_QUANTITY, LENGTH, NUM, PRECISION, SCALE, START,
    STATEMENT_NO, TC_COMMODITY, TC_COMMODITY_RIGHT, TC_QUANTITY, TOTALS, TRANSACTION_NO,
    TRANSACTION_NO_RIGHT,
};
use crate::state::LedgerState;

impl LedgerState {
    pub async fn verify(&mut self) -> Result<()> {
        let ctx = SessionContext::new();
        let array: Arc<dyn Array> = self.transactions.try_into_arrow()?;
        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let batch: RecordBatch = struct_array.try_into()?;
        let df_transactions = ctx.read_batch(batch)?;
        self.transactions_df = Some(df_transactions);

        let array: Arc<dyn Array> = self.postings.try_into_arrow()?;
        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let batch: RecordBatch = struct_array.try_into()?;

        let df_postings = ctx.read_batch(batch)?;

        let row_number_window = row_number()
            .partition_by(vec![col(TRANSACTION_NO)])
            .order_by(vec![col(TC_COMMODITY).sort(true, false)])
            .build()?;

        let df_balancing = df_postings
            .clone()
            .filter(col(TC_COMMODITY).is_not_null())?
            .aggregate(
                vec![col(TRANSACTION_NO), col(TC_COMMODITY)],
                vec![
                    sum(col(TC_QUANTITY) * lit(ScalarValue::Decimal128(Some(-100), 38, 2)))
                        .alias(TOTALS),
                ],
            )?
            .filter(col(TOTALS).not_eq(lit(0)))?
            .window(vec![row_number_window.alias(NUM)])?
            .filter(col(NUM).eq(lit(1)))?
            .select(vec![col(TRANSACTION_NO), col(TC_COMMODITY), col(TOTALS)])?;

        let final_postings_df = df_postings
            .clone()
            .join(
                df_balancing.clone().select(vec![
                    col(TRANSACTION_NO).alias(TRANSACTION_NO_RIGHT),
                    col(TC_COMMODITY).alias(TC_COMMODITY_RIGHT),
                    col(TOTALS),
                ])?,
                JoinType::Left,
                &[TRANSACTION_NO],
                &[TRANSACTION_NO_RIGHT],
                None,
            )?
            .select(vec![
                col(STATEMENT_NO),
                col(TRANSACTION_NO),
                col(FILE_NO),
                col(START),
                col(ACCOUNT),
                coalesce(vec![col(CP_COMMODITY), col(TC_COMMODITY_RIGHT)])
                    .alias(FINAL_CP_COMMODITY),
                cast(
                    coalesce(vec![col(CP_QUANTITY), col(TOTALS)]),
                    DataType::Decimal128(PRECISION as u8, SCALE as i8),
                )
                .alias(FINAL_CP_QUANTITY),
                coalesce(vec![col(TC_COMMODITY), col(TC_COMMODITY_RIGHT)])
                    .alias(FINAL_TC_COMMODITY),
                cast(
                    coalesce(vec![col(TC_QUANTITY), col(TOTALS)]),
                    DataType::Decimal128(PRECISION as u8, SCALE as i8),
                )
                .alias(FINAL_TC_QUANTITY),
            ])?;

        let errors_df = final_postings_df
            .clone()
            .aggregate(
                vec![col(TRANSACTION_NO), col(FINAL_TC_COMMODITY)],
                vec![sum(col(FINAL_TC_QUANTITY)).alias(TOTALS)],
            )?
            .filter(
                col(TOTALS)
                    .not_eq(lit(0))
                    .or(col(FINAL_TC_COMMODITY).is_null()),
            )?;

        self.postings_df = Some(final_postings_df);
        self.errors_df = Some(errors_df);

        self.cp_commodities_df = Some(self.get_commodities_df(FINAL_CP_COMMODITY)?);
        self.tc_commodities_df = Some(self.get_commodities_df(FINAL_TC_COMMODITY)?);
        self.accounts_df = Some(self.get_accounts_df().await?);
        Ok(())
    }

    pub fn get_commodities_df(&mut self, c_col: &str) -> Result<DataFrame> {
        match &self.postings_df {
            Some(df) => Ok(df.clone().select(vec![col(c_col)])?.distinct()?),
            None => Err(anyhow!(ERROR_NO_POSTINGS_DF)),
        }
    }

    async fn get_accounts_df(&mut self) -> Result<DataFrame> {
        let postings_df = match &self.postings_df {
            Some(df) => df.clone().select(vec![col(ACCOUNT)])?.distinct()?,
            None => return Err(anyhow!(ERROR_NO_POSTINGS_DF)),
        };

        let account_list_df = postings_df
            .clone()
            .select([
                string_to_array(col(ACCOUNT), lit(ACCOUNT_SEP), lit(ScalarValue::Null))
                    .alias(ACCOUNT),
            ])?
            .with_column(LENGTH, array_length(col(ACCOUNT)))?;

        let results = account_list_df
            .clone()
            .aggregate(vec![], vec![max(col(LENGTH))])?
            .collect()
            .await?;

        let max_n = if let Some(batch) = results.first() {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect(ERROR_DOWNCAST);
            Some(array.value(0))
        } else {
            None
        };

        let mut df = account_list_df.clone();

        if let Some(max_n) = max_n {
            for n in 1..max_n {
                let a = account_list_df
                    .clone()
                    .filter(col(LENGTH).gt(lit(n)))?
                    .with_column(
                        ACCOUNT,
                        array_slice(col(ACCOUNT), lit(1), cast(lit(n), DataType::Int32), None),
                    )?;

                df = df.union(a)?;
            }

            df = df
                .select(vec![col(ACCOUNT)])?
                .distinct()?
                .sort(vec![col(ACCOUNT).sort(true, true)])?
                .select(vec![
                    array_to_string(col(ACCOUNT), lit(ACCOUNT_SEP)).alias(ACCOUNT),
                ])?;
        } else {
            return Err(anyhow!(ERROR_NO_ACCOUNTS_FOUND));
        }
        Ok(df)
    }
}

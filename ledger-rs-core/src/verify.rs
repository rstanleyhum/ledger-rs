use crate::core::{
    ACCOUNT, ACCOUNT_SEP, CP_COMMODITY, CP_QUANTITY, FILE_NO, FINAL_CP_COMMODITY,
    FINAL_CP_QUANTITY, FINAL_TC_COMMODITY, FINAL_TC_QUANTITY, LENGTH, PRECISION, SCALE, START,
    STATEMENT_NO, TC_COMMODITY, TC_COMMODITY_RIGHT, TC_QUANTITY, TOTALS, TRANSACTION_NO,
};
use crate::state::LedgerState;
use arrow::array::{Array, RecordBatch};
use arrow_convert::serialize::TryIntoArrow;
use df_interchange::Interchange;
use polars::error::PolarsResult;
use polars::prelude::*;

impl LedgerState {
    /**
         * calculate the sum of the postings

    sum quantity by commodity for each transaction
    -----
    select
        row_number() over (partition by transaction_no order by tc_commodity) as num,
        transaction_no,
        tc_commodity,
        -1.0*sum(tc_quantity) as tc_quantity_sum
    from
        postings
    where
        tc_commodity is not null
    group by
        transaction_no, tc_commodity
    order by
        transaction_no, tc_commodity


    find the unbalanced transactions
    ----
    select
        num,
        transaction_no,
        tc_commodity,
        tc_quantity_sum
    from
        [transactions_grouped_by_commodity]
    where
        num = 1 and tc_quantity_sum <> 0
    order by
        transaction_no


    create the final postings table

    select
        a.statement_no
        a.transaction_no,
        a.account,
        coalesce(a.cp_commodity, b.tc_commodity) as cp_commodity,
        coalesce(a.cp_quantity, b.tc_quantity_sum) as cp_commodity,
        coalesce(a.tc_commodity, b.tc_commodity) as tc_commodity,
        coalesce(a.tc_quantity, b.tc_quantity_sum) as tc_quantity
    from
        postings a
    left join
        [unbalanced transactions] b
    on
        a.transaction_no = b.transaction_no
    order by
        a.transaction_no, a.statement_no


    get the errors in the final postings
    -----
    select
        transaction_no,
        tc_commodity,
        sum(tc_quantity) as tc_quantity_sum
    from
        [final postings]
    group by
        transaction_no, tc_commodity
    having
        sum(tc_quantity) <> 0
    order by
        transaction_no, tc_commodity

    *
    */
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

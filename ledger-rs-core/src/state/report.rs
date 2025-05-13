use anyhow::Context;
use anyhow::Result;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::prelude::*;

use crate::{
    core::{
        ACCOUNT, ACCOUNT_RIGHT, ERROR_NO_ACCOUNT_DF, ERROR_NO_POSTINGS_DF, FINAL_CP_COMMODITY,
        FINAL_CP_QUANTITY, FINAL_TC_COMMODITY, FINAL_TC_QUANTITY, MATCH, RIGHT_QUALIFIER, TOTAL,
        TOTALS_ACCOUNT,
    },
    state::ledgerstate::LedgerState,
};

impl LedgerState {
    pub async fn tc_balances(&mut self) -> Result<DataFrame> {
        self.get_balances_df(FINAL_TC_COMMODITY, FINAL_TC_QUANTITY)
            .await
    }

    pub async fn cp_balances(&mut self) -> Result<DataFrame> {
        self.get_balances_df(FINAL_CP_COMMODITY, FINAL_CP_QUANTITY)
            .await
    }

    async fn get_balances_df(
        &mut self,
        commodity_col: &str,
        quantity_col: &str,
    ) -> Result<DataFrame> {
        let mut commodity_col_right = commodity_col.to_string();
        commodity_col_right.push_str(RIGHT_QUALIFIER);
        let commodity_col_right = commodity_col_right.as_str();

        let totals_df = self
            .postings_df
            .clone()
            .context(ERROR_NO_POSTINGS_DF)?
            .aggregate(
                vec![col(ACCOUNT), col(commodity_col)],
                vec![sum(col(quantity_col)).alias(TOTAL)],
            )?;

        let map_totals_df = self
            .accounts_df
            .clone()
            .context(ERROR_NO_ACCOUNT_DF)?
            .join(
                self.get_commodities_df(commodity_col)?,
                JoinType::Inner,
                &[],
                &[],
                Some(lit(1).eq(lit(1))),
            )?
            .join(
                self.accounts_df
                    .clone()
                    .context(ERROR_NO_ACCOUNT_DF)?
                    .select(vec![col(ACCOUNT).alias(ACCOUNT_RIGHT)])?,
                JoinType::Inner,
                &[],
                &[],
                Some(lit(1).eq(lit(1))),
            )?
            .with_column(MATCH, starts_with(col(ACCOUNT), col(ACCOUNT_RIGHT)))?
            .join(
                totals_df.clone().select(vec![
                    col(ACCOUNT).alias(TOTALS_ACCOUNT),
                    col(commodity_col).alias(commodity_col_right),
                    col(TOTAL),
                ])?,
                JoinType::Inner,
                &[ACCOUNT_RIGHT, commodity_col],
                &[TOTALS_ACCOUNT, commodity_col_right],
                None,
            )?
            .filter(col(MATCH).is_true())?
            .aggregate(
                vec![col(ACCOUNT), col(commodity_col)],
                vec![sum(col(TOTAL)).alias(TOTAL)],
            )?
            .sort(vec![
                col(ACCOUNT).sort(true, false),
                col(commodity_col).sort(true, false),
            ])?;

        Ok(map_totals_df)
    }
}

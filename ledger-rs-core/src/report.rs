use polars::error::PolarsResult;
use polars::frame::DataFrame;
use polars::prelude::*;

use crate::{
    state::LedgerParserState,
    verify::{
        ACCOUNT, FINAL_CP_COMMODITY, FINAL_CP_QUANTITY, FINAL_TC_COMMODITY, FINAL_TC_QUANTITY,
    },
};

const TOTAL: &str = "total";
const ACCOUNT_RIGHT: &str = "account_right";
const MATCH: &str = "match";

impl LedgerParserState {
    pub fn tc_balances(&mut self) -> PolarsResult<DataFrame> {
        self.get_balances_df(FINAL_TC_COMMODITY, FINAL_TC_QUANTITY)
    }

    pub fn cp_balances(&mut self) -> PolarsResult<DataFrame> {
        self.get_balances_df(FINAL_CP_COMMODITY, FINAL_CP_QUANTITY)
    }

    fn get_balances_df(
        &mut self,
        commodity_col: &str,
        quantity_col: &str,
    ) -> PolarsResult<DataFrame> {
        let totals_df = self
            .postings_df
            .clone()
            .lazy()
            .group_by([col(ACCOUNT), col(commodity_col)])
            .agg([col(quantity_col).sum().alias(TOTAL)]);

        let map_df = self
            .accounts_df
            .clone()
            .lazy()
            .cross_join(self.get_commodities_df(commodity_col)?.clone().lazy(), None)
            .cross_join(self.accounts_df.clone().lazy(), None)
            .with_column(
                col(ACCOUNT_RIGHT)
                    .str()
                    .starts_with(col(ACCOUNT))
                    .alias(MATCH),
            );

        map_df
            .join(
                totals_df,
                [col(ACCOUNT_RIGHT), col(commodity_col)],
                [col(ACCOUNT), col(commodity_col)],
                JoinArgs::new(JoinType::Inner),
            )
            .filter(col(MATCH).eq(true))
            .group_by([col(ACCOUNT), col(commodity_col)])
            .agg([col(TOTAL).sum()])
            .sort([ACCOUNT, commodity_col], Default::default())
            .collect()
    }
}

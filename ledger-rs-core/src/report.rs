use polars::frame::DataFrame;
use polars::prelude::*;

use crate::{
    state::LedgerParserState,
    verify::{
        ACCOUNT, FINAL_CP_COMMODITY, FINAL_CP_QUANTITY, FINAL_TC_COMMODITY, FINAL_TC_QUANTITY,
    },
};

pub const TC_TOTAL: &str = "tc_total";
pub const CP_TOTAL: &str = "cp_total";
pub const ACCOUNT_RIGHT: &str = "account_right";
pub const MATCH: &str = "match";

impl LedgerParserState {
    pub fn balances(&mut self) -> DataFrame {
        let tc_totals_df = self
            .postings_df
            .clone()
            .lazy()
            .group_by([col(ACCOUNT), col(FINAL_TC_COMMODITY)])
            .agg([col(FINAL_TC_QUANTITY).sum().alias(TC_TOTAL)]);

        let cp_totals_df = self
            .postings_df
            .clone()
            .lazy()
            .group_by([col(ACCOUNT), col(FINAL_CP_COMMODITY)])
            .agg([col(FINAL_CP_QUANTITY).sum().alias(CP_TOTAL)]);

        let tc_map = self
            .accounts_df
            .clone()
            .lazy()
            .cross_join(self.commodities(FINAL_TC_COMMODITY).clone().lazy(), None)
            .cross_join(self.accounts_df.clone().lazy(), None)
            .with_column(
                col(ACCOUNT_RIGHT)
                    .str()
                    .starts_with(col(ACCOUNT))
                    .alias(MATCH),
            )
            .collect()
            .unwrap();

        let tc_totals_df = tc_map
            .clone()
            .lazy()
            .join(
                tc_totals_df.clone().lazy(),
                [col(ACCOUNT_RIGHT), col(FINAL_TC_COMMODITY)],
                [col(ACCOUNT), col(FINAL_TC_COMMODITY)],
                JoinArgs::new(JoinType::Inner),
            )
            .filter(col(MATCH).eq(true))
            .group_by([col(ACCOUNT), col(FINAL_TC_COMMODITY)])
            .agg([col(TC_TOTAL).sum()])
            .sort([ACCOUNT], Default::default())
            .collect()
            .unwrap();

        println!("tc_totals_df\n{}", tc_totals_df);

        let cp_map = self
            .accounts_df
            .clone()
            .lazy()
            .cross_join(self.commodities(FINAL_CP_COMMODITY).clone().lazy(), None)
            .cross_join(self.accounts_df.clone().lazy(), None)
            .with_column(
                col(ACCOUNT_RIGHT)
                    .str()
                    .starts_with(col(ACCOUNT))
                    .alias(MATCH),
            )
            .collect()
            .unwrap();

        let cp_totals_df = cp_map
            .clone()
            .lazy()
            .join(
                cp_totals_df.clone().lazy(),
                [col(ACCOUNT_RIGHT), col(FINAL_CP_COMMODITY)],
                [col(ACCOUNT), col(FINAL_CP_COMMODITY)],
                JoinArgs::new(JoinType::Inner),
            )
            .filter(col(MATCH).eq(true))
            .group_by([col(ACCOUNT), col(FINAL_CP_COMMODITY)])
            .agg([col(CP_TOTAL).sum()])
            .sort([ACCOUNT], Default::default())
            .collect()
            .unwrap();

        cp_totals_df
    }
}

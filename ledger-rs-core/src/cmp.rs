use anyhow::Context;
use anyhow::Result;
use datafusion::prelude::*;

use crate::core::STATEMENT_NO;
use crate::core::STATEMENT_NO_RIGHT;
use crate::core::TRANSACTION_NO;
use crate::{
    core::{
        ACCOUNT, DATE, FINAL_CP_COMMODITY, FINAL_CP_QUANTITY, FINAL_TC_COMMODITY, FINAL_TC_QUANTITY,
    },
    state::LedgerState,
};

impl LedgerState {
    pub async fn compare_postings(&mut self, b: &LedgerState) -> Result<()> {
        let a_transactions_df = self.transactions_df.clone().context("No transactions df")?;
        let b_transactions_df = b.transactions_df.clone().context("No transactions df")?;

        let a_postings_df = self.postings_df.clone().context("No postings df")?;
        let b_postings_df = b.postings_df.clone().context("No postings df")?;

        let a_df = a_postings_df
            .join(
                a_transactions_df
                    .select(vec![col(DATE), col(STATEMENT_NO).alias(STATEMENT_NO_RIGHT)])?,
                JoinType::Left,
                &[TRANSACTION_NO],
                &[STATEMENT_NO_RIGHT],
                None,
            )?
            .select(vec![
                col(TRANSACTION_NO),
                col(DATE),
                col(ACCOUNT),
                col(FINAL_CP_COMMODITY),
                col(FINAL_CP_QUANTITY),
                col(FINAL_TC_COMMODITY),
                col(FINAL_TC_QUANTITY),
            ])?;

        a_df.clone().show().await?;

        let b_df = b_postings_df
            .join(
                b_transactions_df
                    .select(vec![col(DATE), col(STATEMENT_NO).alias(STATEMENT_NO_RIGHT)])?,
                JoinType::Left,
                &[TRANSACTION_NO],
                &[STATEMENT_NO_RIGHT],
                None,
            )?
            .select(vec![
                col(DATE),
                col(ACCOUNT),
                col(FINAL_CP_COMMODITY),
                col(FINAL_CP_QUANTITY),
                col(FINAL_TC_COMMODITY),
                col(FINAL_TC_QUANTITY),
            ])?;

        let df = a_df.join(
            b_df,
            datafusion::common::JoinType::LeftAnti,
            &[
                DATE,
                ACCOUNT,
                FINAL_CP_COMMODITY,
                FINAL_CP_QUANTITY,
                FINAL_TC_COMMODITY,
                FINAL_TC_QUANTITY,
            ],
            &[
                DATE,
                ACCOUNT,
                FINAL_CP_COMMODITY,
                FINAL_CP_QUANTITY,
                FINAL_TC_COMMODITY,
                FINAL_TC_QUANTITY,
            ],
            None,
        )?;

        df.show().await?;

        Ok(())
    }
}

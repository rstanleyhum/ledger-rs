// #[macro_export]
// macro_rules! ACCT_CASH {
//     (owner, acct) => {
//         "Assets:Investments:{owner}:{acct}:Cash"
//     };
// }
macro_rules! acct_cash {
    ($owner:expr, $acct:expr) => {
        format!("Assets:Investments:{}:{}:Cash", $owner, $acct)
    };
}

macro_rules! acct_securities {
    ($owner:expr, $acct:expr) => {
        format!("Assets:Investments:{}:{}:Securities", $owner, $acct)
    };
}

macro_rules! acct_todo {
    ($owner:expr) => {
        format!("Assets:Investments:{}:TODO", $owner)
    };
}

macro_rules! acct_capgains {
    ($owner:expr) => {
        format!("Income:Investments:{}:Taxable:CapitalGains", $owner)
    };
}

macro_rules! acct_distribution {
    ($owner:expr) => {
        format!("Income:Investments:{}:Taxable:Distribution", $owner)
    };
}

macro_rules! acct_dividend {
    ($owner:expr) => {
        format!("Income:Investments:{}:Taxable:Dividend", $owner)
    };
}

macro_rules! acct_longtermcapgains {
    ($owner:expr) => {
        format!("Income:Investments:{}:Taxable:LongTermCapitalGains", $owner)
    };
}

macro_rules! acct_shorttermcapgains {
    ($owner:expr) => {
        format!(
            "Income:Investments:{}:Taxable:ShortTermCapitalGains",
            $owner
        )
    };
}

macro_rules! acct_fees {
    ($owner:expr) => {
        format!("Expenses:Investments:{}:Fees", $owner)
    };
}

macro_rules! acct_foreigntax {
    ($owner:expr) => {
        format!("Expenses:Investments:{}:ForeignTax", $owner)
    };
}

macro_rules! acct_gainloss {
    ($owner:expr) => {
        format!("Income:Investments:{}:Taxable:GainLoss", $owner)
    };
}

macro_rules! acct_interest {
    ($owner:expr) => {
        format!("Income:Investments:{}:Taxable:Interest", $owner)
    };
}

pub(crate) use acct_capgains;
pub(crate) use acct_cash;
pub(crate) use acct_distribution;
pub(crate) use acct_dividend;
pub(crate) use acct_fees;
pub(crate) use acct_foreigntax;
pub(crate) use acct_gainloss;
pub(crate) use acct_interest;
pub(crate) use acct_longtermcapgains;
pub(crate) use acct_securities;
pub(crate) use acct_shorttermcapgains;
pub(crate) use acct_todo;

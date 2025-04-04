use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};

pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let s = s.replace("$", "");
    let s = s.replace(",", "");
    let amount = Decimal::from_str(&s).map_err(serde::de::Error::custom)?;
    Ok(amount)
}

pub fn reverse_sign(d: &Decimal) -> Decimal {
    let mut res = d.clone();
    if res.is_sign_positive() {
        res.set_sign_negative(true);
    } else {
        res.set_sign_positive(true);
    }
    res
}

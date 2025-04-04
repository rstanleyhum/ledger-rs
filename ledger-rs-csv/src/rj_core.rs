use rust_decimal::Decimal;

pub type Position = (Decimal, String);
pub type InterPost = (String, Option<Position>, Option<Position>);

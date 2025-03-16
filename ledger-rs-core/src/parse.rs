use std::ops::Range;

use winnow::LocatingSlice;
use winnow::Parser;
use winnow::Result;
use winnow::ascii::alphanumeric1;
use winnow::ascii::digit1;
use winnow::ascii::line_ending;
use winnow::ascii::space0;
use winnow::ascii::space1;
use winnow::ascii::till_line_ending;
use winnow::combinator::alt;
use winnow::combinator::delimited;
use winnow::combinator::eof;
use winnow::combinator::opt;
use winnow::combinator::preceded;
use winnow::combinator::separated;
use winnow::combinator::separated_pair;
use winnow::combinator::seq;
use winnow::stream::AsChar;
use winnow::token::literal;
use winnow::token::take_while;

use crate::core::HeaderParams;
use crate::core::PostingParams;
use crate::core::{
    BalanceParams, CloseParams, IncludeParams, OpenParams, Statement, TransactionParams,
};

fn date_string<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    seq!(_: take_while(4, |c: char| c.is_dec_digit()),
     _: '-',
     _: take_while(2, |c: char| c.is_dec_digit()),
     _: '-',
     _: take_while(2, |c: char| c.is_dec_digit())
    )
    .take()
    .parse_next(i)
}

fn base_account_name<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    alt((
        literal("Assets"),
        literal("Liabilities"),
        literal("Equity"),
        literal("Income"),
        literal("Expenses"),
    ))
    .parse_next(i)
}

fn account_name<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    take_while(1.., |c: char| c.is_alphanumeric() || c == '-').parse_next(i)
}

fn subaccount<'s>(i: &mut LocatingSlice<&'s str>) -> Result<()> {
    separated(1.., account_name, ":").parse_next(i)
}

fn full_account<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    separated_pair(base_account_name, ":", subaccount)
        .take()
        .parse_next(i)
}

fn quoted_string<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    delimited('"', take_while(1.., |c| c != '"'), '"').parse_next(i)
}

fn comment<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    preceded(';', take_while(0.., |c: char| c != '\n' && c != '\r'))
        .take()
        .parse_next(i)
}

fn tag<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    preceded('#', alphanumeric1).take().parse_next(i)
}

fn tag_list<'s>(i: &mut LocatingSlice<&'s str>) -> Result<Vec<&'s str>> {
    separated(1.., tag, " ").parse_next(i)
}

fn optional_tag_list<'s>(i: &mut LocatingSlice<&'s str>) -> Result<Vec<&'s str>> {
    let (_, r) = (space1, tag_list).parse_next(i)?;
    Ok(r)
}

fn decimal_string<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    seq!(_: opt('-'),
     _: digit1,
     _: opt(preceded('.', digit1)))
    .take()
    .parse_next(i)
}

fn commodity<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    take_while(1.., |c: char| {
        c.is_ascii_uppercase() || c.is_digit(10) || c == '_'
    })
    .parse_next(i)
}

fn commodity_position<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(&'s str, &'s str)> {
    let (_, q, _, c) = (space1, decimal_string, space1, commodity).parse_next(i)?;
    Ok((q, c))
}

fn total_cost<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(&'s str, &'s str)> {
    let (_, _, _, q, _, c) = (
        space1,
        literal("@@"),
        space1,
        decimal_string,
        space1,
        commodity,
    )
        .parse_next(i)?;
    Ok((q, c))
}

fn open_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(OpenParams<'s>, Range<usize>)> {
    seq!(OpenParams {
         date: date_string,
         _: space1,
         _: literal("open"),
         _: space1,
         account: full_account,
         _: space0,
         _: opt(comment)})
    .with_span()
    .parse_next(i)
}

fn close_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(CloseParams<'s>, Range<usize>)> {
    seq!(CloseParams {
         date: date_string,
         _: space1,
         _: literal("close"),
         _: space1,
         account: full_account,
         _: space0,
         _: opt(comment)})
    .with_span()
    .parse_next(i)
}

fn balance_statement<'s>(
    i: &mut LocatingSlice<&'s str>,
) -> Result<(BalanceParams<'s>, Range<usize>)> {
    seq!(BalanceParams {
         date: date_string,
         _: space1,
         _: literal("balance"),
         _: space1,
         account: full_account,
         _: space1,
         position: decimal_string,
         _: space1,
         commodity: commodity,
         _: space0,
         _: opt(comment)})
    .with_span()
    .parse_next(i)
}

fn include_statement<'s>(
    i: &mut LocatingSlice<&'s str>,
) -> Result<(IncludeParams<'s>, Range<usize>)> {
    seq!(IncludeParams {
         _: literal("include"),
         _: space1,
         path: quoted_string,
         _: space0,
         _: opt(comment)})
    .with_span()
    .parse_next(i)
}

fn transaction_header<'s>(i: &mut LocatingSlice<&'s str>) -> Result<HeaderParams<'s>> {
    seq!(HeaderParams {
             date: date_string,
             _: space1,
             _: literal("*"),
             _: space1,
             narration: quoted_string,
             tags: opt(optional_tag_list),
             _: space0,
             _: opt(comment)
    })
    .parse_next(i)
}

fn posting<'s>(i: &mut LocatingSlice<&'s str>) -> Result<PostingParams<'s>> {
    seq!(PostingParams {
             _: literal("  "),
             account: full_account,
             cp: opt(commodity_position),
             tc: opt(total_cost),
             _: space0,
             _: opt(comment)
    })
    .parse_next(i)
}

fn transaction_statement<'s>(
    i: &mut LocatingSlice<&'s str>,
) -> Result<(TransactionParams<'s>, Range<usize>)> {
    seq!(TransactionParams {
        header: transaction_header,
        _: line_ending,
        postings: separated(1.., posting, line_ending),
    })
    .with_span()
    .parse_next(i)
}

fn event_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(&'s str, Range<usize>)> {
    seq!(_: date_string,
         _: space1,
         _: literal("event"),
         _: space1,
         _: quoted_string,
         _: space1,
         _: quoted_string,
         _: space0,
         _: opt(comment))
    .take()
    .with_span()
    .parse_next(i)
}

fn option_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(&'s str, Range<usize>)> {
    seq!(_: literal("option"),
         _: space1,
         _: quoted_string,
         _: space1,
         _: quoted_string,
         _: space0,
         _: opt(comment))
    .take()
    .with_span()
    .parse_next(i)
}

fn custom_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(&'s str, Range<usize>)> {
    seq!(_: date_string,
         _: space1,
         _: literal("custom"),
         _: till_line_ending)
    .take()
    .with_span()
    .parse_next(i)
}

fn comment_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    seq!(_: space0,
         _: comment)
    .take()
    .parse_next(i)
}

fn empty_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    space1.take().parse_next(i)
}

fn other_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(&'s str, Range<usize>)> {
    till_line_ending.with_span().parse_next(i)
}

fn active_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<Statement<'s>> {
    alt((
        open_statement.map(Statement::Open),
        close_statement.map(Statement::Close),
        balance_statement.map(Statement::Balance),
        include_statement.map(Statement::Include),
        transaction_statement.map(Statement::Transaction),
        event_statement.map(Statement::Event),
        option_statement.map(Statement::Option),
        custom_statement.map(Statement::Custom),
        comment_statement.map(Statement::Comment),
        empty_statement.map(Statement::Empty),
        other_statement.map(Statement::Other),
    ))
    .parse_next(i)
}

fn active_statements<'s>(i: &mut LocatingSlice<&'s str>) -> Result<Vec<Statement<'s>>> {
    separated(0.., active_statement, line_ending).parse_next(i)
}

fn full_file<'s>(i: &mut LocatingSlice<&'s str>) -> Result<Vec<Statement<'s>>> {
    let (active_statements, _) = (active_statements, eof).parse_next(i)?;
    Ok(active_statements)
}

pub fn parse_file<'s>(i: &mut LocatingSlice<&'s str>) -> Result<Vec<Statement<'s>>> {
    full_file.parse_next(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_narration() {
        let mut i = LocatingSlice::new(r#""hello blag ' again""#);
        let a = quoted_string.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(""), "hello blag ' again");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_full_account() {
        let mut i = LocatingSlice::new(r#"Income:Income-Stan:Rental"#);
        let a = full_account.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(""), "Income:Income-Stan:Rental");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_comment() {
        let mut i = LocatingSlice::new(";this is a comment income:Income-Stan:Rental\n");
        let a = comment.parse_next(&mut i).unwrap();
        let e = (
            LocatingSlice::new("\n"),
            ";this is a comment income:Income-Stan:Rental",
        );
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_date_string() {
        let mut i = LocatingSlice::new("2025-02-09 *");
        let a = date_string.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(" *"), "2025-02-09");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_tag() {
        let mut i = LocatingSlice::new("#FLL ");
        let a = tag.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(" "), "#FLL");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_tag_2() {
        let mut i = LocatingSlice::new("#FLL");
        let a = tag.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(""), "#FLL");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_decimal_positive() {
        let mut i = LocatingSlice::new("123.45");
        let a = decimal_string.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(""), "123.45");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_decimal_negative() {
        let mut i = LocatingSlice::new("-123.45");
        let a = decimal_string.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(""), "-123.45");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_decimal_positive_whole() {
        let mut i = LocatingSlice::new("123");
        let a = decimal_string.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(""), "123");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_decimal_negative_whole() {
        let mut i = LocatingSlice::new("-123;");
        let a = decimal_string.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(";"), "-123");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_commodity() {
        let mut i = LocatingSlice::new("USD ");
        let a = commodity.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(" "), "USD");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_commodity_2() {
        let mut i = LocatingSlice::new("USDa ");
        let a = commodity.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new("a "), "USD");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_commodity_3() {
        let mut i = LocatingSlice::new("USD");
        let a = commodity.parse_next(&mut i).unwrap();
        let e = (LocatingSlice::new(""), "USD");
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_open_statement() {
        let mut i = LocatingSlice::new("2024-01-01 open  Expenses:Adjustment");
        let a = open_statement.parse_next(&mut i).unwrap();
        let e = (
            LocatingSlice::new(""),
            (
                OpenParams {
                    date: "2024-01-01",
                    account: "Expenses:Adjustment",
                },
                (0..40),
            ),
        );
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_close_statement() {
        let mut i = LocatingSlice::new("2024-01-01 close Expenses:Adjustment");
        let a = close_statement.parse_next(&mut i).unwrap();
        let e = (
            LocatingSlice::new(""),
            (
                CloseParams {
                    date: "2024-01-01",
                    account: "Expenses:Adjustment",
                },
                (0..30),
            ),
        );
        assert_eq!((i, a), e)
    }

    #[test]
    fn test_balance_statement() {
        let mut i = LocatingSlice::new(
            "2024-01-01 balance  Expenses:Adjustment                   0.00 CAD",
        );
        let a = balance_statement.parse_next(&mut i).unwrap();
        let e = (
            LocatingSlice::new(""),
            (
                BalanceParams {
                    date: "2024-01-01",
                    account: "Expenses:Adjustment",
                    position: "0.00",
                    commodity: "CAD",
                },
                (0..30),
            ),
        );
        assert_eq!((i, a), e)
    }
}

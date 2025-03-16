use std::ops::Range;

use chrono::NaiveDate;
use rust_decimal::Decimal;
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

fn date_string<'s>(i: &mut LocatingSlice<&'s str>) -> Result<NaiveDate> {
    seq!(_: take_while(4, |c: char| c.is_dec_digit()),
     _: '-',
     _: take_while(2, |c: char| c.is_dec_digit()),
     _: '-',
     _: take_while(2, |c: char| c.is_dec_digit())
    )
    .take()
    .try_map(|x| NaiveDate::parse_from_str(x, "%Y-%m-%d"))
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

fn full_account<'s>(i: &mut LocatingSlice<&'s str>) -> Result<String> {
    separated_pair(base_account_name, ":", subaccount)
        .take()
        .map(|x| x.to_string())
        .parse_next(i)
}

fn quoted_string<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    delimited('"', take_while(1.., |c| c != '"'), '"').parse_next(i)
}

fn narration<'s>(i: &mut LocatingSlice<&'s str>) -> Result<String> {
    quoted_string.take().map(|x| x.to_string()).parse_next(i)
}

fn comment<'s>(i: &mut LocatingSlice<&'s str>) -> Result<&'s str> {
    preceded(';', take_while(0.., |c: char| c != '\n' && c != '\r'))
        .take()
        .parse_next(i)
}

fn tag<'s>(i: &mut LocatingSlice<&'s str>) -> Result<String> {
    preceded('#', alphanumeric1)
        .take()
        .map(|x: &str| x.to_string())
        .parse_next(i)
}

fn tag_list<'s>(i: &mut LocatingSlice<&'s str>) -> Result<Vec<String>> {
    separated(1.., tag, " ").parse_next(i)
}

fn optional_tag_list<'s>(i: &mut LocatingSlice<&'s str>) -> Result<Vec<String>> {
    let (_, r) = (space1, tag_list).parse_next(i)?;
    Ok(r)
}

fn decimal_string<'s>(i: &mut LocatingSlice<&'s str>) -> Result<Decimal> {
    seq!(_: opt('-'),
     _: digit1,
     _: opt(preceded('.', digit1)))
    .take()
    .try_map(|x| Decimal::from_str_exact(x))
    .parse_next(i)
}

fn commodity<'s>(i: &mut LocatingSlice<&'s str>) -> Result<String> {
    take_while(1.., |c: char| {
        c.is_ascii_uppercase() || c.is_digit(10) || c == '_'
    })
    .take()
    .map(|x: &str| x.to_string())
    .parse_next(i)
}

fn commodity_position<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(Decimal, String)> {
    let (_, q, _, c) = (space1, decimal_string, space1, commodity).parse_next(i)?;
    Ok((q, c))
}

fn optional_commodity_position<'s>(
    i: &mut LocatingSlice<&'s str>,
) -> Result<(Option<Decimal>, Option<String>)> {
    let r = opt(commodity_position).parse_next(i)?;
    match r {
        Some((q, c)) => Ok((Some(q), Some(c))),
        None => Ok((None, None)),
    }
}

fn total_cost<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(Decimal, String)> {
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

fn optional_total_cost<'s>(
    i: &mut LocatingSlice<&'s str>,
) -> Result<(Option<Decimal>, Option<String>)> {
    let r = opt(total_cost).parse_next(i)?;
    match r {
        Some((q, c)) => Ok((Some(q), Some(c))),
        None => Ok((None, None)),
    }
}

fn open_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(OpenParams, Range<usize>)> {
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

fn close_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(CloseParams, Range<usize>)> {
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

fn balance_statement<'s>(i: &mut LocatingSlice<&'s str>) -> Result<(BalanceParams, Range<usize>)> {
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

fn transaction_header<'s>(i: &mut LocatingSlice<&'s str>) -> Result<HeaderParams> {
    let (date, _, _, _, narration, tags, _, _) = (
        date_string,
        space1,
        literal("*"),
        space1,
        narration,
        opt(optional_tag_list),
        space0,
        opt(comment),
    )
        .parse_next(i)?;
    Ok(HeaderParams {
        start: 0,
        date,
        narration,
        tags,
    })
}

fn posting<'s>(i: &mut LocatingSlice<&'s str>) -> Result<PostingParams> {
    let (_, account, (cp_q, cp_c), (tc_q, tc_c), _, _) = (
        literal("  "),
        full_account,
        optional_commodity_position,
        optional_total_cost,
        space0,
        opt(comment),
    )
        .parse_next(i)?;

    if tc_q.is_none() & tc_c.is_none() {
        Ok(PostingParams {
            start: 0,
            account,
            cp_q,
            cp_c: cp_c.clone(),
            tc_q: cp_q,
            tc_c: cp_c,
        })
    } else {
        Ok(PostingParams {
            start: 0,
            account,
            cp_q,
            cp_c,
            tc_q,
            tc_c,
        })
    }
}

fn transaction_statement<'s>(
    i: &mut LocatingSlice<&'s str>,
) -> Result<(TransactionParams, Range<usize>)> {
    let (mut t, r) = seq!(TransactionParams {
        header: transaction_header,
        _: line_ending,
        postings: separated(1.., posting, line_ending),
    })
    .with_span()
    .parse_next(i)?;
    t.header.start = r.start as u32;
    t.postings.iter_mut().for_each(|x| x.start = r.start as u32);
    Ok((t, r))
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

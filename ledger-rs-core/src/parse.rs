use std::ops::Range;
use std::path::Path;
use std::str;

use chrono::NaiveDate;
use rust_decimal::Decimal;
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

use crate::core::get_contents;
use crate::core::new_beaninput;
use crate::core::{
    BalanceParams, BeanInput, CloseParams, HeaderParams, IncludeParams, OpenParams, PostingParams,
    Statement, TransactionParams,
};

fn date_string<'s>(i: &mut BeanInput<'s>) -> Result<NaiveDate> {
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

fn base_account_name<'s>(i: &mut BeanInput<'s>) -> Result<&'s str> {
    alt((
        literal("Assets"),
        literal("Liabilities"),
        literal("Equity"),
        literal("Income"),
        literal("Expenses"),
    ))
    .parse_next(i)
}

fn account_name<'s>(i: &mut BeanInput<'s>) -> Result<&'s str> {
    take_while(1.., |c: char| c.is_alphanumeric() || c == '-').parse_next(i)
}

fn subaccount<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    separated(1.., account_name, ":").parse_next(i)
}

fn full_account<'s>(i: &mut BeanInput<'s>) -> Result<String> {
    separated_pair(base_account_name, ":", subaccount)
        .take()
        .map(|x| x.to_string())
        .parse_next(i)
}

fn quoted_string<'s>(i: &mut BeanInput<'s>) -> Result<&'s str> {
    delimited('"', take_while(1.., |c| c != '"'), '"').parse_next(i)
}

fn narration<'s>(i: &mut BeanInput<'s>) -> Result<String> {
    quoted_string.take().map(|x| x.to_string()).parse_next(i)
}

fn comment<'s>(i: &mut BeanInput<'s>) -> Result<&'s str> {
    preceded(';', take_while(0.., |c: char| c != '\n' && c != '\r'))
        .take()
        .parse_next(i)
}

fn tag<'s>(i: &mut BeanInput<'s>) -> Result<String> {
    preceded('#', alphanumeric1)
        .take()
        .map(|x: &str| x.to_string())
        .parse_next(i)
}

fn tag_list<'s>(i: &mut BeanInput<'s>) -> Result<Vec<String>> {
    separated(1.., tag, " ").parse_next(i)
}

fn opt_tag_list<'s>(i: &mut BeanInput<'s>) -> Result<Vec<String>> {
    let (_, r) = (space1, tag_list).parse_next(i)?;
    Ok(r)
}

fn decimal_string<'s>(i: &mut BeanInput<'s>) -> Result<Decimal> {
    seq!(_: opt('-'),
     _: digit1,
     _: opt(preceded('.', digit1)))
    .take()
    .try_map(|x| Decimal::from_str_exact(x))
    .parse_next(i)
}

fn commodity<'s>(i: &mut BeanInput<'s>) -> Result<String> {
    take_while(1.., |c: char| {
        c.is_ascii_uppercase() || c.is_digit(10) || c == '_'
    })
    .take()
    .map(|x: &str| x.to_string())
    .parse_next(i)
}

fn commodity_position<'s>(i: &mut BeanInput<'s>) -> Result<(Decimal, String)> {
    let (_, q, _, c) = (space1, decimal_string, space1, commodity).parse_next(i)?;
    Ok((q, c))
}

fn opt_commodity_position<'s>(i: &mut BeanInput<'s>) -> Result<(Option<Decimal>, Option<String>)> {
    let r = opt(commodity_position).parse_next(i)?;
    match r {
        Some((q, c)) => Ok((Some(q), Some(c))),
        None => Ok((None, None)),
    }
}

fn total_cost<'s>(i: &mut BeanInput<'s>) -> Result<(Decimal, String)> {
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

fn opt_total_cost<'s>(i: &mut BeanInput<'s>) -> Result<(Option<Decimal>, Option<String>)> {
    let r = opt(total_cost).parse_next(i)?;
    match r {
        Some((q, c)) => Ok((Some(q), Some(c))),
        None => Ok((None, None)),
    }
}

fn open_statement<'s>(i: &mut BeanInput<'s>) -> Result<(OpenParams, Range<usize>)> {
    let ((date, _, _, _, account, _, _), r) = (
        date_string,
        space1,
        literal("open"),
        space1,
        full_account,
        space0,
        opt(comment),
    )
        .with_span()
        .parse_next(i)?;
    Ok((
        OpenParams {
            statement_no: i.state.statement_no(r.start as u32),
            file_no: i.state.get_file_no().unwrap(),
            start: r.start as u32,
            end: r.end as u32,
            date,
            account,
        },
        r,
    ))
}

fn close_statement<'s>(i: &mut BeanInput<'s>) -> Result<(CloseParams, Range<usize>)> {
    let ((date, _, _, _, account, _, _), r) = (
        date_string,
        space1,
        literal("close"),
        space1,
        full_account,
        space0,
        opt(comment),
    )
        .with_span()
        .parse_next(i)?;
    Ok((
        CloseParams {
            statement_no: i.state.statement_no(r.start as u32),
            file_no: i.state.get_file_no().unwrap(),
            start: r.start as u32,
            end: r.end as u32,
            date,
            account,
        },
        r,
    ))
}

fn balance_statement<'s>(i: &mut BeanInput<'s>) -> Result<(BalanceParams, Range<usize>)> {
    let ((date, _, _, _, account, _, position, _, commodity, _, _), r) = (
        date_string,
        space1,
        literal("balance"),
        space1,
        full_account,
        space1,
        decimal_string,
        space1,
        commodity,
        space0,
        opt(comment),
    )
        .with_span()
        .parse_next(i)?;
    Ok((
        BalanceParams {
            statement_no: i.state.statement_no(r.start as u32),
            file_no: i.state.get_file_no().unwrap(),
            start: r.start as u32,
            end: r.end as u32,
            date,
            account,
            position,
            commodity,
        },
        r,
    ))
}

fn include_statement<'s>(i: &mut BeanInput<'s>) -> Result<(IncludeParams, Range<usize>)> {
    let ((_, _, path, _, _), r) = (
        literal("include"),
        space1,
        quoted_string,
        space0,
        opt(comment),
    )
        .with_span()
        .parse_next(i)?;
    let include_statement_no = i.state.statement_no(r.start as u32);
    let p = Path::new(path).to_path_buf();
    i.state.insert(p.clone());
    let (in_contents, total_n) = get_contents(p.as_path()).unwrap();
    let mut input = new_beaninput(&in_contents, i.state);
    parse_file(&mut input)?;
    i.state.finished_include(total_n);
    Ok((
        IncludeParams {
            statement_no: include_statement_no,
            file_no: i.state.get_file_no().unwrap(),
            start: r.start as u32,
            end: r.end as u32,
            path: path.to_string(),
        },
        r,
    ))
}

fn transaction_header<'s>(i: &mut BeanInput<'s>) -> Result<HeaderParams> {
    let ((date, _, _, _, narration, tags, _, _), r) = (
        date_string,
        space1,
        literal("*"),
        space1,
        narration,
        opt(opt_tag_list),
        space0,
        opt(comment),
    )
        .with_span()
        .parse_next(i)?;
    Ok(HeaderParams {
        statement_no: i.state.statement_no(r.start as u32),
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        date,
        narration,
        tags,
    })
}

fn posting<'s>(i: &mut BeanInput<'s>) -> Result<PostingParams> {
    let ((_, account, (cp_q, cp_c), (tc_q, tc_c), _, _), r) = (
        literal("  "),
        full_account,
        opt_commodity_position,
        opt_total_cost,
        space0,
        opt(comment),
    )
        .with_span()
        .parse_next(i)?;

    if tc_q.is_none() & tc_c.is_none() {
        Ok(PostingParams {
            statement_no: i.state.statement_no(r.start as u32),
            file_no: i.state.get_file_no().unwrap(),
            start: r.start as u32,
            end: r.end as u32,
            account,
            cp_q,
            cp_c: cp_c.clone(),
            tc_q: cp_q,
            tc_c: cp_c,
        })
    } else {
        Ok(PostingParams {
            statement_no: i.state.statement_no(r.start as u32),
            file_no: i.state.get_file_no().unwrap(),
            start: r.start as u32,
            end: r.end as u32,
            account,
            cp_q,
            cp_c,
            tc_q,
            tc_c,
        })
    }
}

fn transaction_statement<'s>(i: &mut BeanInput<'s>) -> Result<(TransactionParams, Range<usize>)> {
    seq!(TransactionParams {
        header: transaction_header,
        _: line_ending,
        postings: separated(1.., posting, line_ending),
    })
    .with_span()
    .parse_next(i)
}

fn event_statement<'s>(i: &mut BeanInput<'s>) -> Result<Range<usize>> {
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
    .span()
    .parse_next(i)
}

fn option_statement<'s>(i: &mut BeanInput<'s>) -> Result<Range<usize>> {
    seq!(_: literal("option"),
         _: space1,
         _: quoted_string,
         _: space1,
         _: quoted_string,
         _: space0,
         _: opt(comment))
    .take()
    .span()
    .parse_next(i)
}

fn custom_statement<'s>(i: &mut BeanInput<'s>) -> Result<Range<usize>> {
    seq!(_: date_string,
         _: space1,
         _: literal("custom"),
         _: till_line_ending)
    .take()
    .span()
    .parse_next(i)
}

fn comment_statement<'s>(i: &mut BeanInput<'s>) -> Result<Range<usize>> {
    seq!(_: space0,
         _: comment)
    .take()
    .span()
    .parse_next(i)
}

fn empty_statement<'s>(i: &mut BeanInput<'s>) -> Result<Range<usize>> {
    space1.take().span().parse_next(i)
}

fn other_statement<'s>(i: &mut BeanInput<'s>) -> Result<Range<usize>> {
    till_line_ending.span().parse_next(i)
}

fn active_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    let s = alt((
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
    .parse_next(i)?;
    i.state.statements.push(s);
    Ok(())
}

fn active_statements<'s>(i: &mut BeanInput<'s>) -> Result<Vec<()>> {
    separated(0.., active_statement, line_ending).parse_next(i)
}

fn full_file<'s>(i: &mut BeanInput<'s>) -> Result<Vec<()>> {
    let (active_statements, _) = (active_statements, eof).parse_next(i)?;
    Ok(active_statements)
}

pub fn parse_file<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    full_file.parse_next(i)?;
    Ok(())
}

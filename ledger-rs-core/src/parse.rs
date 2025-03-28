use std::path::Path;
use std::str;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use winnow::LocatingSlice;
use winnow::Parser;
use winnow::Result;
use winnow::Stateful;
use winnow::Str;
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

use crate::core::BALANCE_ACTION;
use crate::core::CLOSE_ACTION;
use crate::core::CUSTOM_ACTION;
use crate::core::EVENT_ACTION;
use crate::core::InfoParams;
use crate::core::OPEN_ACTION;
use crate::core::OPTION_ACTION;
use crate::core::VerificationParams;
use crate::state::LedgerParserState;
use crate::state::get_contents;

use crate::core::{HeaderParams, IncludeParams, PostingParams};

pub type BeanInput<'b> = Stateful<LocatingSlice<Str<'b>>, &'b mut LedgerParserState>;

pub fn new_beaninput<'s>(s: &'s str, state: &'s mut LedgerParserState) -> BeanInput<'s> {
    Stateful {
        input: LocatingSlice::new(s),
        state: state,
    }
}

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

fn open_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
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
    let o = VerificationParams {
        statement_no: i.state.statement_no(r.start as u32),
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        date,
        action: OPEN_ACTION,
        account,
        quantity: None,
        commodity: None,
    };
    i.state.verifications.push(o);
    Ok(())
}

fn close_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
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
    let c = VerificationParams {
        statement_no: i.state.statement_no(r.start as u32),
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        date,
        action: CLOSE_ACTION,
        account,
        quantity: None,
        commodity: None,
    };
    i.state.verifications.push(c);
    Ok(())
}

fn balance_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
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
    let b = VerificationParams {
        statement_no: i.state.statement_no(r.start as u32),
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        date,
        action: BALANCE_ACTION,
        account,
        quantity: Some(position),
        commodity: Some(commodity),
    };
    i.state.verifications.push(b);
    Ok(())
}

fn include_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
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
    let current_p = i.state.get_current_filepath().unwrap();
    let in_filepath = if p.is_absolute() {
        p.clone()
    } else {
        let parent = current_p.parent().unwrap();
        parent.join(p.as_path())
    };
    i.state.insert(in_filepath.clone());
    let (in_contents, total_n) = get_contents(in_filepath.as_path()).unwrap();
    let mut input = new_beaninput(&in_contents, i.state);
    parse_file(&mut input)?;
    i.state.finished_include(total_n);
    let s = IncludeParams {
        statement_no: include_statement_no,
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        path: path.to_string(),
    };
    i.state.includes.push(s);
    Ok(())
}

fn transaction_header<'s>(i: &mut BeanInput<'s>) -> Result<()> {
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
    let statement_no = i.state.statement_no(r.start as u32);
    i.state.transaction_no = statement_no;
    let h = HeaderParams {
        statement_no,
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        date,
        narration,
        tags,
    };
    i.state.transactions.push(h);
    Ok(())
}

fn posting<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    let ((_, account, (cp_quantity, cp_commodity), (tc_quantity, tc_commodity), _, _), r) = (
        literal("  "),
        full_account,
        opt_commodity_position,
        opt_total_cost,
        space0,
        opt(comment),
    )
        .with_span()
        .parse_next(i)?;

    let mut p = PostingParams {
        statement_no: i.state.statement_no(r.start as u32),
        transaction_no: i.state.transaction_no,
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        account,
        cp_quantity,
        cp_commodity: cp_commodity.clone(),
        tc_quantity: cp_quantity,
        tc_commodity: cp_commodity,
    };
    if !(tc_quantity.is_none() & tc_commodity.is_none()) {
        p.tc_quantity = tc_quantity;
        p.tc_commodity = tc_commodity;
    }
    i.state.postings.push(p);
    Ok(())
}

fn transaction_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    let (_, _, _): ((), &str, Vec<()>) = (
        transaction_header,
        line_ending,
        separated(1.., posting, line_ending),
    )
        .parse_next(i)?;
    Ok(())
}

fn event_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    let ((d, _, _, _, a, _, v, _, _), r) = (
        date_string,
        space1,
        literal("event"),
        space1,
        quoted_string,
        space1,
        quoted_string,
        space0,
        opt(comment),
    )
        .with_span()
        .parse_next(i)?;
    let s = InfoParams {
        statement_no: i.state.statement_no(r.start as u32),
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        date: Some(d),
        action: EVENT_ACTION,
        attribute: Some(a.to_string()),
        value: v.to_string(),
    };
    i.state.informationals.push(s);
    Ok(())
}

fn option_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    let ((_, _, a, _, v, _, _), r) = (
        literal("option"),
        space1,
        quoted_string,
        space1,
        quoted_string,
        space0,
        opt(comment),
    )
        .with_span()
        .parse_next(i)?;
    let s = InfoParams {
        statement_no: i.state.statement_no(r.start as u32),
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        date: None,
        action: OPTION_ACTION,
        attribute: Some(a.to_string()),
        value: v.to_string(),
    };
    i.state.informationals.push(s);
    Ok(())
}

fn custom_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    let ((d, _, _, v), r) = (date_string, space1, literal("custom"), till_line_ending)
        .with_span()
        .parse_next(i)?;
    let s = InfoParams {
        statement_no: i.state.statement_no(r.start as u32),
        file_no: i.state.get_file_no().unwrap(),
        start: r.start as u32,
        end: r.end as u32,
        date: Some(d),
        action: CUSTOM_ACTION,
        attribute: None,
        value: v.to_string(),
    };
    i.state.informationals.push(s);
    Ok(())
}

fn comment_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    (space0, comment).parse_next(i)?;
    Ok(())
}

fn empty_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    space1.parse_next(i)?;
    Ok(())
}

fn other_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    till_line_ending.span().parse_next(i)?;
    Ok(())
}

fn active_statement<'s>(i: &mut BeanInput<'s>) -> Result<()> {
    alt((
        open_statement,
        close_statement,
        balance_statement,
        include_statement,
        transaction_statement,
        event_statement,
        option_statement,
        custom_statement,
        comment_statement,
        empty_statement,
        other_statement,
    ))
    .parse_next(i)?;
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

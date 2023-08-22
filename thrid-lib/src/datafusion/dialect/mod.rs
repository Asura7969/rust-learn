use std::fmt;

use datafusion_sql::sqlparser::{
    ast::{SqlOption, Statement},
    dialect::Dialect,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

#[derive(Debug)]
pub struct PaimonDialect {}

impl Dialect for PaimonDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '$' || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '_' || ch == '='
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        println!("parse_statement ......");

        if parser.parse_keyword(Keyword::SELECT) {
            println!("SELECT ......");

            Some(parse_query(parser))
        } else {
            None
        }
    }
}

pub fn parse_query(_parser: &mut Parser) -> Result<Statement, ParserError> {
    todo!()
}

#[allow(dead_code)]
pub fn parse_options(parser: &mut Parser) -> Result<(), ParserError> {
    let (mut options, mut has_as, mut alias) = (vec![], false, None);

    if parser.peek_token().token != Token::EOF {
        if let Token::Word(word) = parser.peek_token().token {
            if word.keyword == Keyword::OPTIONS {
                options = parser
                    .parse_options(Keyword::OPTIONS)
                    .expect("options parser error");
            }
        };

        if parser.peek_token().token != Token::EOF {
            if let Token::Word(word) = parser.peek_token().token {
                if word.keyword == Keyword::AS {
                    has_as = true;
                    alias = Some(parser.parse_literal_string().expect("alias parser error"))
                }
            };
        }
    }

    println!(
        "has as: {}, alias: {:?}, options: {:?}",
        has_as, alias, options
    );

    Ok(())
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Options(Vec<SqlOption>);

impl fmt::Display for Options {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OPTIONS ({:?})", self.0)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    #[test]
    fn paimon_dialect_test() {
        // let sql = "select * from ods_mysql_paimon_points_5 OPTIONS('scan.snapshot-id' = '1')";
        // let dialect = PaimonDialect {};

        // let statements = Parser::parse_sql(&dialect, sql).unwrap();

        // println!("{:?}", statements);
    }
}

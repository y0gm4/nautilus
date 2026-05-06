//! Lexer for schema language.

use crate::error::{Result, SchemaError};
use crate::span::Span;
use crate::token::{Token, TokenKind};

/// Lexer for tokenizing schema source code.
pub struct Lexer<'a> {
    /// Source text being lexed.
    source: &'a str,
    /// Current byte position in source.
    pos: usize,
    /// Characters remaining (for efficient peeking).
    chars: std::str::Chars<'a>,
    /// Peeked character cache.
    peeked: Option<char>,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer for the given source.
    pub fn new(source: &'a str) -> Self {
        Self {
            source,
            pos: 0,
            chars: source.chars(),
            peeked: None,
        }
    }

    /// Get the next token.
    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();

        while self.peek() == Some('/')
            && (self.peek_n(1) == Some('/') || self.peek_n(1) == Some('*'))
        {
            self.skip_comment()?;
            self.skip_whitespace();
        }

        let start = self.pos;

        if self.is_at_end() {
            return Ok(Token::new(TokenKind::Eof, Span::new(start, start)));
        }

        let ch = self.peek().unwrap();

        if ch == '\n' {
            self.advance();
            return Ok(Token::new(TokenKind::Newline, Span::new(start, self.pos)));
        }

        if ch == '"' || ch == '\'' {
            return self.lex_string(start, ch);
        }

        if ch.is_ascii_digit() {
            return self.lex_number(start);
        }

        if ch.is_alphabetic() || ch == '_' {
            return self.lex_identifier_or_keyword(start);
        }

        if ch == '@' {
            self.advance();
            if self.peek() == Some('@') {
                self.advance();
                return Ok(Token::new(TokenKind::AtAt, Span::new(start, self.pos)));
            }
            return Ok(Token::new(TokenKind::At, Span::new(start, self.pos)));
        }

        let kind = match ch {
            '{' => {
                self.advance();
                TokenKind::LBrace
            }
            '}' => {
                self.advance();
                TokenKind::RBrace
            }
            '[' => {
                self.advance();
                TokenKind::LBracket
            }
            ']' => {
                self.advance();
                TokenKind::RBracket
            }
            '(' => {
                self.advance();
                TokenKind::LParen
            }
            ')' => {
                self.advance();
                TokenKind::RParen
            }
            ',' => {
                self.advance();
                TokenKind::Comma
            }
            ':' => {
                self.advance();
                TokenKind::Colon
            }
            '=' => {
                self.advance();
                TokenKind::Equal
            }
            '?' => {
                self.advance();
                TokenKind::Question
            }
            '!' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    TokenKind::BangEqual
                } else {
                    TokenKind::Bang
                }
            }
            '.' => {
                self.advance();
                TokenKind::Dot
            }
            '*' => {
                self.advance();
                TokenKind::Star
            }
            '+' => {
                self.advance();
                TokenKind::Plus
            }
            '-' => {
                self.advance();
                TokenKind::Minus
            }
            '<' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    TokenKind::LessEqual
                } else {
                    TokenKind::LAngle
                }
            }
            '>' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    TokenKind::GreaterEqual
                } else {
                    TokenKind::RAngle
                }
            }
            '%' => {
                self.advance();
                TokenKind::Percent
            }
            '|' => {
                self.advance();
                if self.peek() == Some('|') {
                    self.advance();
                    TokenKind::DoublePipe
                } else {
                    TokenKind::Pipe
                }
            }
            _ => {
                self.advance();
                return Err(SchemaError::UnexpectedCharacter(ch, Span::single(start)));
            }
        };

        Ok(Token::new(kind, Span::new(start, self.pos)))
    }

    /// Lex an identifier or keyword.
    fn lex_identifier_or_keyword(&mut self, start: usize) -> Result<Token> {
        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                self.advance();
            } else {
                break;
            }
        }

        let text = &self.source[start..self.pos];
        let kind = TokenKind::from_ident(text);
        Ok(Token::new(kind, Span::new(start, self.pos)))
    }

    /// Lex a string literal.
    fn lex_string(&mut self, start: usize, quote: char) -> Result<Token> {
        self.advance();

        let mut value = String::new();

        loop {
            match self.peek() {
                None | Some('\n') => {
                    return Err(SchemaError::UnterminatedString(Span::new(start, self.pos)));
                }
                Some(ch) if ch == quote => {
                    if quote == '\'' && self.peek_n(1) == Some('\'') {
                        value.push('\'');
                        self.advance();
                        self.advance();
                        continue;
                    }
                    self.advance();
                    break;
                }
                Some('\\') => {
                    self.advance();
                    match self.peek() {
                        Some(ch) => {
                            let escaped = match ch {
                                'n' => '\n',
                                't' => '\t',
                                'r' => '\r',
                                '\\' => '\\',
                                '"' if quote == '"' => '"',
                                '\'' if quote == '\'' => '\'',
                                _ => ch,
                            };
                            value.push(escaped);
                            self.advance();
                        }
                        None => {
                            return Err(SchemaError::UnterminatedString(Span::new(
                                start, self.pos,
                            )));
                        }
                    }
                }
                Some(ch) => {
                    value.push(ch);
                    self.advance();
                }
            }
        }

        Ok(Token::new(
            TokenKind::String(value),
            Span::new(start, self.pos),
        ))
    }

    /// Lex a number literal.
    fn lex_number(&mut self, start: usize) -> Result<Token> {
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                self.advance();
            } else {
                break;
            }
        }

        if self.peek() == Some('.') && self.peek_n(1).is_some_and(|ch| ch.is_ascii_digit()) {
            self.advance(); // consume '.'

            while let Some(ch) = self.peek() {
                if ch.is_ascii_digit() {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        let text = &self.source[start..self.pos];

        if text.parse::<f64>().is_err() {
            return Err(SchemaError::InvalidNumber(
                text.to_string(),
                Span::new(start, self.pos),
            ));
        }

        Ok(Token::new(
            TokenKind::Number(text.to_string()),
            Span::new(start, self.pos),
        ))
    }

    /// Skip whitespace characters (but not newlines).
    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch == ' ' || ch == '\t' || ch == '\r' {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Skip comments (single-line or block).
    fn skip_comment(&mut self) -> Result<()> {
        if self.peek() != Some('/') {
            return Ok(());
        }

        let start = self.pos;
        self.advance(); // consume first '/'

        match self.peek() {
            Some('/') => {
                self.advance(); // consume second '/'
                while let Some(ch) = self.peek() {
                    if ch == '\n' {
                        break;
                    }
                    self.advance();
                }
            }
            Some('*') => {
                self.advance(); // consume '*'

                loop {
                    match self.peek() {
                        None => {
                            return Err(SchemaError::Lexer(
                                "Unterminated block comment".to_string(),
                                Span::new(start, self.pos),
                            ));
                        }
                        Some('*') => {
                            self.advance();
                            if self.peek() == Some('/') {
                                self.advance();
                                break;
                            }
                        }
                        Some(_) => {
                            self.advance();
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Peek at the current character without consuming it.
    fn peek(&mut self) -> Option<char> {
        if self.peeked.is_none() {
            self.peeked = self.chars.next();
        }
        self.peeked
    }

    /// Peek at the nth character ahead without consuming.
    fn peek_n(&mut self, n: usize) -> Option<char> {
        if n == 0 {
            return self.peek();
        }

        let _ = self.peek();
        self.chars.clone().nth(n - 1)
    }

    /// Advance to the next character.
    fn advance(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.pos += ch.len_utf8();
        self.peeked = None;
        Some(ch)
    }

    /// Check if we've reached the end of the source.
    fn is_at_end(&mut self) -> bool {
        self.peek().is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenize(source: &str) -> Result<Vec<TokenKind>> {
        let mut lexer = Lexer::new(source);
        let mut tokens = Vec::new();

        loop {
            let token = lexer.next_token()?;
            if token.kind == TokenKind::Eof {
                break;
            }
            tokens.push(token.kind);
        }

        Ok(tokens)
    }

    #[test]
    fn test_keywords() {
        let tokens = tokenize("datasource generator model enum").unwrap();
        assert_eq!(
            tokens,
            vec![
                TokenKind::Datasource,
                TokenKind::Generator,
                TokenKind::Model,
                TokenKind::Enum
            ]
        );
    }

    #[test]
    fn test_identifiers() {
        let tokens = tokenize("User email_address _private").unwrap();
        assert_eq!(
            tokens,
            vec![
                TokenKind::Ident("User".to_string()),
                TokenKind::Ident("email_address".to_string()),
                TokenKind::Ident("_private".to_string()),
            ]
        );
    }

    #[test]
    fn test_peek_n_preserves_current_char_and_handles_utf8() {
        let mut lexer = Lexer::new("aβ");

        assert_eq!(lexer.peek_n(1), Some('β'));
        assert_eq!(lexer.peek(), Some('a'));
        lexer.advance();
        assert_eq!(lexer.peek(), Some('β'));
    }

    #[test]
    fn test_string_literals() {
        let tokens = tokenize(r#""hello" "world""#).unwrap();
        assert_eq!(
            tokens,
            vec![
                TokenKind::String("hello".to_string()),
                TokenKind::String("world".to_string()),
            ]
        );

        let tokens = tokenize("'hello' 'world'").unwrap();
        assert_eq!(
            tokens,
            vec![
                TokenKind::String("hello".to_string()),
                TokenKind::String("world".to_string()),
            ]
        );
    }

    #[test]
    fn test_string_escapes() {
        let tokens = tokenize(r#""hello \"world\"""#).unwrap();
        assert_eq!(
            tokens,
            vec![TokenKind::String("hello \"world\"".to_string())]
        );

        let tokens = tokenize(r#""line1\nline2""#).unwrap();
        assert_eq!(tokens, vec![TokenKind::String("line1\nline2".to_string())]);

        let tokens = tokenize("'O''Reilly'").unwrap();
        assert_eq!(tokens, vec![TokenKind::String("O'Reilly".to_string())]);
    }

    #[test]
    fn test_numbers() {
        let tokens = tokenize("42 3.14 100").unwrap();
        assert_eq!(
            tokens,
            vec![
                TokenKind::Number("42".to_string()),
                TokenKind::Number("3.14".to_string()),
                TokenKind::Number("100".to_string()),
            ]
        );
    }

    #[test]
    fn test_punctuation() {
        let tokens = tokenize("{ } [ ] ( ) , : = ? .").unwrap();
        assert_eq!(
            tokens,
            vec![
                TokenKind::LBrace,
                TokenKind::RBrace,
                TokenKind::LBracket,
                TokenKind::RBracket,
                TokenKind::LParen,
                TokenKind::RParen,
                TokenKind::Comma,
                TokenKind::Colon,
                TokenKind::Equal,
                TokenKind::Question,
                TokenKind::Dot,
            ]
        );
    }

    #[test]
    fn test_attributes() {
        let tokens = tokenize("@ @@ @id @@map").unwrap();
        assert_eq!(
            tokens,
            vec![
                TokenKind::At,
                TokenKind::AtAt,
                TokenKind::At,
                TokenKind::Ident("id".to_string()),
                TokenKind::AtAt,
                TokenKind::Ident("map".to_string()),
            ]
        );
    }

    #[test]
    fn test_single_line_comment() {
        let tokens = tokenize("model // this is a comment\nUser").unwrap();
        assert_eq!(
            tokens,
            vec![
                TokenKind::Model,
                TokenKind::Newline,
                TokenKind::Ident("User".to_string()),
            ]
        );
    }

    #[test]
    fn test_block_comment() {
        let tokens = tokenize("model /* comment */ User").unwrap();
        assert_eq!(
            tokens,
            vec![TokenKind::Model, TokenKind::Ident("User".to_string()),]
        );
    }

    #[test]
    fn test_multiline_block_comment() {
        let tokens = tokenize("model /* line 1\nline 2\nline 3 */ User").unwrap();
        assert_eq!(
            tokens,
            vec![TokenKind::Model, TokenKind::Ident("User".to_string()),]
        );
    }

    #[test]
    fn test_unterminated_string() {
        let result = tokenize(r#""hello"#);
        assert!(result.is_err());
        match result.unwrap_err() {
            SchemaError::UnterminatedString(_) => {}
            _ => panic!("Expected UnterminatedString error"),
        }
    }

    #[test]
    fn test_unexpected_character() {
        let result = tokenize("model #");
        assert!(result.is_err());
        match result.unwrap_err() {
            SchemaError::UnexpectedCharacter('#', _) => {}
            _ => panic!("Expected UnexpectedCharacter error"),
        }
    }

    #[test]
    fn test_newlines() {
        let tokens = tokenize("model\nUser\n").unwrap();
        assert_eq!(
            tokens,
            vec![
                TokenKind::Model,
                TokenKind::Newline,
                TokenKind::Ident("User".to_string()),
                TokenKind::Newline,
            ]
        );
    }

    #[test]
    fn test_schema_snippet() {
        let source = r#"
model User {
  id    Int    @id
  email String @unique
}
"#;
        let tokens = tokenize(source).unwrap();
        assert!(tokens.contains(&TokenKind::Model));
        assert!(tokens.contains(&TokenKind::Ident("User".to_string())));
        assert!(tokens.contains(&TokenKind::LBrace));
        assert!(tokens.contains(&TokenKind::At));
    }
}

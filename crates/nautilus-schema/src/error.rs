//! Error types for schema parsing and validation.

use crate::span::{LineIndex, Span};
use std::fmt;

/// Error type for schema operations.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaError {
    /// Lexer error with position information.
    Lexer(String, Span),
    /// Unterminated string literal.
    UnterminatedString(Span),
    /// Invalid number literal.
    InvalidNumber(String, Span),
    /// Unexpected character.
    UnexpectedCharacter(char, Span),
    /// Parser error.
    Parse(String, Span),
    /// Semantic validation error.
    Validation(String, Span),
    /// Non-blocking semantic warning.
    Warning(String, Span),
    /// Generic error without span.
    Other(String),
}

impl SchemaError {
    /// Format error with source context for display.
    pub fn format_with_source(&self, source: &str) -> String {
        let line_index = LineIndex::new(source);
        self.format_with_source_indexed(source, &line_index)
    }

    /// Format error with source context using a cached line index.
    pub fn format_with_source_indexed(&self, source: &str, line_index: &LineIndex) -> String {
        match self {
            SchemaError::Lexer(msg, span)
            | SchemaError::Parse(msg, span)
            | SchemaError::Validation(msg, span)
            | SchemaError::Warning(msg, span) => {
                let (start_pos, _) = span.to_positions_with_index(source, line_index);
                format!("{} at {}", msg, start_pos)
            }
            SchemaError::UnterminatedString(span) => {
                let (start_pos, _) = span.to_positions_with_index(source, line_index);
                format!("Unterminated string literal at {}", start_pos)
            }
            SchemaError::InvalidNumber(num, span) => {
                let (start_pos, _) = span.to_positions_with_index(source, line_index);
                format!("Invalid number '{}' at {}", num, start_pos)
            }
            SchemaError::UnexpectedCharacter(ch, span) => {
                let (start_pos, _) = span.to_positions_with_index(source, line_index);
                format!("Unexpected character '{}' at {}", ch, start_pos)
            }
            SchemaError::Other(msg) => msg.clone(),
        }
    }

    /// Format error with file path and source context.
    /// Uses the format `filepath:line:column: message` which is recognized
    /// as a clickable link by VS Code and other IDEs.
    pub fn format_with_file(&self, filepath: &str, source: &str) -> String {
        let line_index = LineIndex::new(source);
        self.format_with_file_indexed(filepath, source, &line_index)
    }

    /// Format error with file path and source context using a cached line index.
    pub fn format_with_file_indexed(
        &self,
        filepath: &str,
        source: &str,
        line_index: &LineIndex,
    ) -> String {
        match self {
            SchemaError::Lexer(msg, span)
            | SchemaError::Parse(msg, span)
            | SchemaError::Validation(msg, span)
            | SchemaError::Warning(msg, span) => {
                let (start_pos, _) = span.to_positions_with_index(source, line_index);
                format!(
                    "{}:{}:{}: {}",
                    filepath, start_pos.line, start_pos.column, msg
                )
            }
            SchemaError::UnterminatedString(span) => {
                let (start_pos, _) = span.to_positions_with_index(source, line_index);
                format!(
                    "{}:{}:{}: Unterminated string literal",
                    filepath, start_pos.line, start_pos.column
                )
            }
            SchemaError::InvalidNumber(num, span) => {
                let (start_pos, _) = span.to_positions_with_index(source, line_index);
                format!(
                    "{}:{}:{}: Invalid number '{}'",
                    filepath, start_pos.line, start_pos.column, num
                )
            }
            SchemaError::UnexpectedCharacter(ch, span) => {
                let (start_pos, _) = span.to_positions_with_index(source, line_index);
                format!(
                    "{}:{}:{}: Unexpected character '{}'",
                    filepath, start_pos.line, start_pos.column, ch
                )
            }
            SchemaError::Other(msg) => msg.clone(),
        }
    }

    /// Get the span associated with this error, if any.
    pub fn span(&self) -> Option<Span> {
        match self {
            SchemaError::Lexer(_, span)
            | SchemaError::Parse(_, span)
            | SchemaError::Validation(_, span)
            | SchemaError::Warning(_, span)
            | SchemaError::UnterminatedString(span)
            | SchemaError::InvalidNumber(_, span)
            | SchemaError::UnexpectedCharacter(_, span) => Some(*span),
            SchemaError::Other(_) => None,
        }
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaError::Lexer(msg, _) => write!(f, "Lexer error: {}", msg),
            SchemaError::UnterminatedString(_) => write!(f, "Unterminated string literal"),
            SchemaError::InvalidNumber(num, _) => write!(f, "Invalid number: {}", num),
            SchemaError::UnexpectedCharacter(ch, _) => write!(f, "Unexpected character: '{}'", ch),
            SchemaError::Parse(msg, _) => write!(f, "Parse error: {}", msg),
            SchemaError::Validation(msg, _) => write!(f, "Validation error: {}", msg),
            SchemaError::Warning(msg, _) => write!(f, "Warning: {}", msg),
            SchemaError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for SchemaError {}

/// Result type alias for schema operations.
pub type Result<T> = std::result::Result<T, SchemaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let span = Span::new(0, 5);
        let err = SchemaError::Lexer("test error".to_string(), span);
        assert_eq!(err.to_string(), "Lexer error: test error");
    }

    #[test]
    fn test_error_span() {
        let span = Span::new(10, 15);
        let err = SchemaError::UnexpectedCharacter('#', span);
        assert_eq!(err.span(), Some(span));

        let err_no_span = SchemaError::Other("generic".to_string());
        assert_eq!(err_no_span.span(), None);
    }

    #[test]
    fn test_format_with_source() {
        let source = "hello\nworld";
        let span = Span::new(6, 11); // "world"
        let err = SchemaError::Lexer("unexpected token".to_string(), span);
        let formatted = err.format_with_source(source);
        assert!(formatted.contains("2:1")); // Line 2, column 1
    }

    #[test]
    fn test_format_with_file() {
        let source = "hello\nworld";
        let span = Span::new(6, 11); // "world"
        let err = SchemaError::Lexer("unexpected token".to_string(), span);
        let formatted = err.format_with_file("schema.nautilus", source);
        assert_eq!(formatted, "schema.nautilus:2:1: unexpected token");
    }

    #[test]
    fn test_indexed_formatting_matches_plain_formatting() {
        let source = "hello\nworld";
        let span = Span::new(6, 11);
        let err = SchemaError::Lexer("unexpected token".to_string(), span);
        let index = LineIndex::new(source);
        assert_eq!(
            err.format_with_source(source),
            err.format_with_source_indexed(source, &index)
        );
        assert_eq!(
            err.format_with_file("schema.nautilus", source),
            err.format_with_file_indexed("schema.nautilus", source, &index)
        );
    }
}

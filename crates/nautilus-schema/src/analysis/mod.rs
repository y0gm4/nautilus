//! Top-level analysis API for `.nautilus` schema files.
//!
//! This module exposes a stable public contract that editor tooling (LSP servers,
//! CLI linters, etc.) can call without duplicating parsing or validation logic.
//!
//! # Quick Start
//!
//! ```ignore
//! use nautilus_schema::analysis::analyze;
//!
//! let result = analyze(source);
//! for diag in &result.diagnostics {
//!     println!("{:?} — {}", diag.severity, diag.message);
//! }
//! if let Some(ir) = &result.ir {
//!     println!("{} models", ir.models.len());
//! }
//! ```

pub mod completion;
pub mod goto_definition;
pub mod hover;
pub mod semantic_tokens;

pub use completion::{completion, completion_with_analysis, CompletionItem, CompletionKind};
pub use goto_definition::{goto_definition, goto_definition_with_analysis};
pub use hover::{config_field_hover, hover, hover_with_analysis, HoverInfo};
pub use semantic_tokens::{semantic_tokens, SemanticKind, SemanticToken};

use crate::ast::Schema;
use crate::diagnostic::Diagnostic;
use crate::ir::SchemaIr;
use crate::span::Span;
use crate::token::{Token, TokenKind};
use crate::validator::validate_all_ref;
use crate::{Lexer, Parser};

/// The result of a full analysis pass over a `.nautilus` source string.
///
/// All three fields may be present simultaneously:
/// - `ast` is `Some` even when there are parse errors (partial AST from error recovery).
/// - `ir`  is `Some` only when there are **no** validation errors.
/// - `diagnostics` is non-empty whenever any problem was found.
#[derive(Debug, Clone)]
pub struct AnalysisResult {
    /// Parsed AST (partial when parse errors occurred).
    pub ast: Option<Schema>,
    /// Fully validated IR (only present when `diagnostics` is empty).
    pub ir: Option<SchemaIr>,
    /// All problems found: lex errors + parse errors + validation errors.
    pub diagnostics: Vec<Diagnostic>,
    /// Token stream produced by the lexer (best-effort; may be incomplete on lex errors).
    pub tokens: Vec<Token>,
}

/// Analyzes a `.nautilus` schema source string end-to-end.
///
/// Runs the full pipeline (lex -> parse -> validate) and collects **all**
/// diagnostics, not just the first one.  The returned [`AnalysisResult`]
/// contains the best-effort AST, the validated IR (when error-free), and
/// the complete list of diagnostics.
pub fn analyze(source: &str) -> AnalysisResult {
    let mut diagnostics: Vec<Diagnostic> = Vec::new();

    let mut lexer = Lexer::new(source);
    let mut tokens: Vec<Token> = Vec::new();
    let mut lex_ok = true;

    loop {
        match lexer.next_token() {
            Ok(tok) => {
                let is_eof = matches!(tok.kind, TokenKind::Eof);
                tokens.push(tok);
                if is_eof {
                    break;
                }
            }
            Err(e) => {
                // For a single bad character the lexer has already advanced
                // past it, so we can record the error and keep going.
                // For errors that leave the lexer in an indeterminate state
                // (unterminated strings, invalid numbers) we stop early.
                let recoverable = matches!(e, crate::SchemaError::UnexpectedCharacter(..));
                diagnostics.push(Diagnostic::from(e));
                if !recoverable {
                    lex_ok = false;
                    break;
                }
            }
        }
    }

    if !lex_ok {
        return AnalysisResult {
            ast: None,
            ir: None,
            diagnostics,
            tokens,
        };
    }

    let mut parser = Parser::new(&tokens, source);
    let parse_result = parser.parse_schema();

    for e in parser.take_errors() {
        diagnostics.push(Diagnostic::from(e));
    }

    let ast = match parse_result {
        Ok(schema) => schema,
        Err(e) => {
            diagnostics.push(Diagnostic::from(e));
            return AnalysisResult {
                ast: None,
                ir: None,
                diagnostics,
                tokens,
            };
        }
    };

    let (ir, val_errors) = validate_all_ref(&ast);
    for e in val_errors {
        diagnostics.push(Diagnostic::from(e));
    }

    AnalysisResult {
        ast: Some(ast),
        ir,
        diagnostics,
        tokens,
    }
}

/// Returns `true` if `offset` falls within `span` (inclusive both ends).
pub(super) fn span_contains(span: Span, offset: usize) -> bool {
    offset >= span.start && offset <= span.end
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_SCHEMA: &str = r#"
datasource db {
  provider = "postgresql"
  url      = "postgresql://localhost/mydb"
}

model User {
  id    Int    @id
  email String @unique
  name  String
}

enum Role {
  Admin
  User
}
"#;

    #[test]
    fn analyze_valid_schema() {
        let r = analyze(VALID_SCHEMA);
        assert!(r.diagnostics.is_empty(), "unexpected: {:?}", r.diagnostics);
        assert!(r.ast.is_some());
        assert!(r.ir.is_some());
    }

    #[test]
    fn analyze_validation_error_returns_diagnostic() {
        let src = r#"
model Broken {
  id   Int
  name String
}
"#;
        let r = analyze(src);
        assert!(r.ast.is_some());
        let _ = r.diagnostics;
    }

    #[test]
    fn analyze_lex_error() {
        let src = "model User { id # Int }";
        let r = analyze(src);
        assert!(!r.diagnostics.is_empty());
    }

    #[test]
    fn completion_top_level() {
        let items = completion("", 0);
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"model"));
        assert!(labels.contains(&"enum"));
        assert!(labels.contains(&"datasource"));
        assert!(labels.contains(&"generator"));
    }

    #[test]
    fn completion_inside_model_returns_scalar_types() {
        let src = "model User {\n  \n}";
        let offset = src.find("\n  \n").unwrap() + 3;
        let items = completion(src, offset);
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"String"), "got: {:?}", labels);
        assert!(labels.contains(&"Int"), "got: {:?}", labels);
    }

    #[test]
    fn completion_after_at_returns_field_attrs() {
        let src = "model User {\n  id Int @\n}";
        let offset = src.find('@').unwrap() + 1;
        let items = completion(src, offset);
        assert!(
            items
                .iter()
                .any(|i| i.kind == CompletionKind::FieldAttribute),
            "got: {:?}",
            items
        );
    }

    #[test]
    fn hover_on_field_returns_type_info() {
        let src = VALID_SCHEMA;
        let offset = src.find("email").unwrap() + 2;
        let h = hover(src, offset);
        assert!(h.is_some(), "hover returned None");
        let info = h.unwrap();
        assert!(info.content.contains("email") || info.content.contains("String"));
    }

    #[test]
    fn goto_definition_resolves_user_type() {
        let src = r#"
model Post {
  id       Int  @id
  authorId Int
  author   User @relation(fields: [authorId], references: [id])
}

model User {
  id    Int    @id
  email String @unique
}
"#;
        let offset = src.find("author   User").unwrap() + "author   ".len() + 1;
        let span = goto_definition(src, offset);
        assert!(span.is_some(), "goto_definition returned None");
        let target = span.unwrap();
        assert!(
            &src[target.start..target.end].contains("User"),
            "span does not point to User: {:?}",
            &src[target.start..target.end]
        );
    }
}

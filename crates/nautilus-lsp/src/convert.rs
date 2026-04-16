//! Conversion helpers between nautilus-schema byte offsets and LSP UTF-16
//! line/character positions.

use nautilus_schema::{
    analysis::{CompletionItem, CompletionKind, HoverInfo, SemanticKind, SemanticToken},
    diagnostic::{Diagnostic, Severity},
    Span, Token, TokenKind,
};
use tower_lsp::lsp_types::{
    self, CompletionItemKind, CompletionTextEdit, DiagnosticSeverity, InsertTextFormat, Position,
    Range, SemanticToken as LspSemanticToken, TextEdit,
};

/// Convert a byte `offset` in `source` to an LSP [`Position`].
///
/// The returned position is 0-indexed (line, character).
pub fn offset_to_position(source: &str, offset: usize) -> Position {
    let safe = offset.min(source.len());
    let mut line = 0u32;
    let mut character = 0u32;

    for (idx, ch) in source.char_indices() {
        if idx >= safe {
            break;
        }
        if ch == '\n' {
            line += 1;
            character = 0;
        } else {
            character += ch.len_utf16() as u32;
        }
    }

    Position { line, character }
}

/// Convert an LSP [`Position`] to a byte offset in `source`.
///
/// Clamps to `source.len()` if the position is past the end.
pub fn position_to_offset(source: &str, pos: Position) -> usize {
    let mut current_line = 0u32;
    let mut line_start = 0usize;

    for (i, ch) in source.char_indices() {
        if current_line == pos.line {
            break;
        }
        if ch == '\n' {
            current_line += 1;
            line_start = i + ch.len_utf8();
        }
    }

    if current_line != pos.line {
        return source.len();
    }

    let mut utf16_col = 0u32;
    for (rel_idx, ch) in source[line_start..].char_indices() {
        let abs_idx = line_start + rel_idx;

        if ch == '\n' {
            return abs_idx;
        }

        let next_utf16_col = utf16_col + ch.len_utf16() as u32;
        if pos.character <= utf16_col || pos.character < next_utf16_col {
            return abs_idx;
        }
        if pos.character == next_utf16_col {
            return abs_idx + ch.len_utf8();
        }

        utf16_col = next_utf16_col;
    }

    source.len()
}

pub fn span_to_range(source: &str, span: &Span) -> Range {
    Range {
        start: offset_to_position(source, span.start),
        end: offset_to_position(source, span.end),
    }
}

pub fn nautilus_diagnostic_to_lsp(source: &str, d: &Diagnostic) -> lsp_types::Diagnostic {
    let severity = match d.severity {
        Severity::Error => DiagnosticSeverity::ERROR,
        Severity::Warning => DiagnosticSeverity::WARNING,
    };
    lsp_types::Diagnostic {
        range: span_to_range(source, &d.span),
        severity: Some(severity),
        message: d.message.clone(),
        source: Some("nautilus-schema".to_string()),
        ..Default::default()
    }
}

pub fn nautilus_completion_to_lsp(
    source: &str,
    tokens: &[Token],
    offset: usize,
    item: &CompletionItem,
) -> lsp_types::CompletionItem {
    let kind = match item.kind {
        CompletionKind::Keyword => CompletionItemKind::KEYWORD,
        CompletionKind::Type => CompletionItemKind::CLASS,
        CompletionKind::FieldAttribute => CompletionItemKind::PROPERTY,
        CompletionKind::ModelAttribute => CompletionItemKind::PROPERTY,
        CompletionKind::ModelName => CompletionItemKind::STRUCT,
        CompletionKind::EnumName => CompletionItemKind::ENUM,
        CompletionKind::FieldName => CompletionItemKind::FIELD,
    };
    let (new_text, range) = completion_text_edit(source, tokens, offset, item);
    lsp_types::CompletionItem {
        label: item.label.clone(),
        kind: Some(kind),
        detail: item.detail.clone(),
        filter_text: Some(item.label.clone()),
        text_edit: Some(CompletionTextEdit::Edit(TextEdit { range, new_text })),
        insert_text_format: if item.is_snippet {
            Some(InsertTextFormat::SNIPPET)
        } else {
            None
        },
        ..Default::default()
    }
}

fn completion_text_edit(
    source: &str,
    tokens: &[Token],
    offset: usize,
    item: &CompletionItem,
) -> (String, Range) {
    if is_quoted_string_completion(item) {
        if let Some(span) = enclosing_string_span(tokens, offset) {
            let content_start = span.start + 1;
            let content_end = span.end.saturating_sub(1);
            let new_text = item
                .insert_text
                .as_deref()
                .and_then(|text| {
                    text.strip_prefix('"')
                        .and_then(|text| text.strip_suffix('"'))
                })
                .unwrap_or(item.label.as_str())
                .to_string();
            return (
                new_text,
                Range {
                    start: offset_to_position(source, content_start),
                    end: offset_to_position(source, content_end),
                },
            );
        }
    }

    let new_text = item
        .insert_text
        .clone()
        .unwrap_or_else(|| item.label.clone());
    let (start, end) = completion_word_bounds(source, offset);
    (
        new_text,
        Range {
            start: offset_to_position(source, start),
            end: offset_to_position(source, end),
        },
    )
}

fn is_quoted_string_completion(item: &CompletionItem) -> bool {
    item.insert_text
        .as_deref()
        .is_some_and(|text| text.starts_with('"') && text.ends_with('"'))
}

fn enclosing_string_span(tokens: &[Token], offset: usize) -> Option<Span> {
    tokens.iter().find_map(|token| match &token.kind {
        TokenKind::String(_) if token.span.start < offset && offset < token.span.end => {
            Some(token.span)
        }
        _ => None,
    })
}

fn completion_word_bounds(source: &str, offset: usize) -> (usize, usize) {
    let safe = offset.min(source.len());
    let mut start = safe;
    while start > 0 {
        let ch = source[..start]
            .chars()
            .next_back()
            .expect("slice ending at a valid offset is never empty");
        if !is_completion_word_char(ch) {
            break;
        }
        start -= ch.len_utf8();
    }

    let mut end = safe;
    while end < source.len() {
        let ch = source[end..]
            .chars()
            .next()
            .expect("slice starting at a valid offset is never empty");
        if !is_completion_word_char(ch) {
            break;
        }
        end += ch.len_utf8();
    }

    (start, end)
}

fn is_completion_word_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

pub fn hover_info_to_lsp(source: &str, h: &HoverInfo) -> lsp_types::Hover {
    let range = h.span.as_ref().map(|s| span_to_range(source, s));
    lsp_types::Hover {
        contents: lsp_types::HoverContents::Markup(lsp_types::MarkupContent {
            kind: lsp_types::MarkupKind::Markdown,
            value: h.content.clone(),
        }),
        range,
    }
}

/// Encode a sorted list of [`SemanticToken`]s into the LSP delta format.
///
/// Token types legend (must match `SemanticTokensLegend` in `initialize`):
/// - `0` -> `nautilusModel`        (model reference)
/// - `1` -> `nautilusEnum`         (enum reference)
/// - `2` -> `nautilusCompositeType` (composite type reference)
pub fn semantic_tokens_to_lsp(source: &str, tokens: &[SemanticToken]) -> Vec<LspSemanticToken> {
    let mut result = Vec::with_capacity(tokens.len());
    let mut prev_line = 0u32;
    let mut prev_start = 0u32;

    for token in tokens {
        let pos = offset_to_position(source, token.span.start);
        let length = (token.span.end - token.span.start) as u32;

        let delta_line = pos.line - prev_line;
        let delta_start = if delta_line == 0 {
            pos.character - prev_start
        } else {
            pos.character
        };

        let token_type = match token.kind {
            SemanticKind::ModelRef => 0,
            SemanticKind::EnumRef => 1,
            SemanticKind::CompositeTypeRef => 2,
        };

        result.push(LspSemanticToken {
            delta_line,
            delta_start,
            length,
            token_type,
            token_modifiers_bitset: 0,
        });

        prev_line = pos.line;
        prev_start = pos.character;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_schema::analyze;

    #[test]
    fn round_trip_offset_position() {
        let source = "model User {\n  id Int\n  name String\n}";
        let pos = offset_to_position(source, 13);
        assert_eq!(
            pos,
            Position {
                line: 1,
                character: 0
            }
        );
        let back = position_to_offset(source, pos);
        assert_eq!(back, 13);
    }

    #[test]
    fn offset_to_position_at_col() {
        let source = "model User {\n  id Int\n}";
        let pos = offset_to_position(source, 5);
        assert_eq!(
            pos,
            Position {
                line: 0,
                character: 5
            }
        );
    }

    #[test]
    fn utf16_positions_handle_astral_chars() {
        let source = "model User {\n  note String @default(\"hi 😀\")\n}\n";
        let emoji_offset = source.find("😀").unwrap();
        let pos = offset_to_position(source, emoji_offset);
        assert_eq!(
            pos,
            Position {
                line: 1,
                character: 27
            }
        );
        assert_eq!(position_to_offset(source, pos), emoji_offset);
    }

    #[test]
    fn position_to_offset_clamps_past_end_line() {
        let source = "model User {\n  id Int\n}\n";
        assert_eq!(
            position_to_offset(
                source,
                Position {
                    line: 99,
                    character: 0
                }
            ),
            source.len()
        );
    }

    #[test]
    fn position_to_offset_clamps_past_end_column_to_line_end() {
        let source = "name 😀\nnext";
        let line_end = source.find('\n').unwrap();
        assert_eq!(
            position_to_offset(
                source,
                Position {
                    line: 0,
                    character: 99
                }
            ),
            line_end
        );
    }

    #[test]
    fn string_completion_replaces_the_entire_quoted_value() {
        let source = "datasource db {\n  provider = \"sq\"\n}\n";
        let quoted = source.find("\"sq\"").unwrap();
        let offset = quoted + 3;
        let analysis = analyze(source);
        let item = CompletionItem {
            label: "sqlite".to_string(),
            insert_text: Some("\"sqlite\"".to_string()),
            is_snippet: false,
            kind: CompletionKind::Keyword,
            detail: Some("SQLite database".to_string()),
        };

        let lsp_item = nautilus_completion_to_lsp(source, &analysis.tokens, offset, &item);
        let Some(CompletionTextEdit::Edit(edit)) = lsp_item.text_edit else {
            panic!("expected completion text edit");
        };

        assert_eq!(
            edit.range,
            Range {
                start: offset_to_position(source, quoted + 1),
                end: offset_to_position(source, quoted + "\"sq".len()),
            }
        );
        assert_eq!(edit.new_text, "sqlite");
        assert_eq!(lsp_item.filter_text.as_deref(), Some("sqlite"));
    }

    #[test]
    fn identifier_completion_replaces_the_current_word() {
        let source = "model User {\n  name Str\n}\n";
        let word = source.find("Str").unwrap();
        let offset = word + "Str".len();
        let analysis = analyze(source);
        let item = CompletionItem {
            label: "String".to_string(),
            insert_text: None,
            is_snippet: false,
            kind: CompletionKind::Type,
            detail: Some("UTF-8 text".to_string()),
        };

        let lsp_item = nautilus_completion_to_lsp(source, &analysis.tokens, offset, &item);
        let Some(CompletionTextEdit::Edit(edit)) = lsp_item.text_edit else {
            panic!("expected completion text edit");
        };

        assert_eq!(
            edit.range,
            Range {
                start: offset_to_position(source, word),
                end: offset_to_position(source, word + "Str".len()),
            }
        );
        assert_eq!(edit.new_text, "String");
    }
}

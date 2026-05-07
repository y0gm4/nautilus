//! Per-document state cached by the LSP server.
//!
//! Every time the client sends a `didOpen`, `didChange`, or `didSave`
//! notification the server updates the cached source, re-runs analysis only
//! when the content actually changed, and stores a fresh [`DocumentState`].
//! Subsequent `textDocument/completion`, `hover`, `definition`,
//! `semanticTokens/full`, and formatting requests read from this cache rather
//! than re-analysing.

use std::sync::OnceLock;

use crate::convert::{position_to_offset_with_index, semantic_tokens_to_lsp_with_index};
use nautilus_schema::{
    analysis::{
        analyze, completion_with_analysis, goto_definition_with_analysis, hover_with_analysis,
        semantic_tokens, AnalysisResult, CompletionItem, HoverInfo,
    },
    format_schema, LineIndex, Span,
};
use tower_lsp::lsp_types::{SemanticToken as LspSemanticToken, TextDocumentContentChangeEvent};

/// Snapshot of a single `.nautilus` document.
pub struct DocumentState {
    /// Full text of the document as last received from the client.
    pub source: String,
    /// Cached line offsets for fast span/position conversion.
    pub line_index: LineIndex,
    /// Analysis result produced from `source`.
    pub analysis: AnalysisResult,
    semantic_tokens: OnceLock<Option<Vec<LspSemanticToken>>>,
    formatted: OnceLock<Option<String>>,
}

impl DocumentState {
    /// Analyze `source` and build a new [`DocumentState`].
    pub fn new(source: String) -> Self {
        let line_index = LineIndex::new(&source);
        let analysis = analyze(&source);
        Self {
            source,
            line_index,
            analysis,
            semantic_tokens: OnceLock::new(),
            formatted: OnceLock::new(),
        }
    }

    /// Apply a batch of LSP content changes to the cached source text.
    pub fn apply_content_changes(&self, changes: &[TextDocumentContentChangeEvent]) -> String {
        if changes.is_empty() {
            return self.source.clone();
        }

        let mut source = self.source.clone();
        let mut line_index = self.line_index.clone();

        for change in changes {
            if let Some(range) = change.range {
                let start = position_to_offset_with_index(&source, &line_index, range.start);
                let end = position_to_offset_with_index(&source, &line_index, range.end);
                source.replace_range(start..end, &change.text);
            } else {
                source = change.text.clone();
            }
            line_index = LineIndex::new(&source);
        }

        source
    }

    /// Completion items derived from the cached analysis.
    pub fn completion(&self, offset: usize) -> Vec<CompletionItem> {
        completion_with_analysis(&self.source, &self.analysis, offset)
    }

    /// Hover info derived from the cached analysis.
    pub fn hover(&self, offset: usize) -> Option<HoverInfo> {
        hover_with_analysis(&self.source, &self.analysis, offset)
    }

    /// Definition span derived from the cached analysis.
    pub fn goto_definition(&self, offset: usize) -> Option<Span> {
        goto_definition_with_analysis(&self.analysis, offset)
    }

    /// Semantic tokens derived from the cached analysis and memoized per
    /// document version.
    pub fn semantic_tokens(&self) -> Option<&[LspSemanticToken]> {
        self.semantic_tokens
            .get_or_init(|| {
                let ast = self.analysis.ast.as_ref()?;
                let tokens = semantic_tokens(ast, &self.analysis.tokens);
                Some(semantic_tokens_to_lsp_with_index(
                    &self.source,
                    &self.line_index,
                    &tokens,
                ))
            })
            .as_deref()
    }

    /// Canonical formatted source derived from the cached AST.
    pub fn formatted(&self) -> Option<&str> {
        self.formatted
            .get_or_init(|| {
                self.analysis
                    .ast
                    .as_ref()
                    .map(|ast| format_schema(ast, &self.source))
            })
            .as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::DocumentState;
    use tower_lsp::lsp_types::{Position, Range, TextDocumentContentChangeEvent};

    #[test]
    fn cached_analysis_serves_completion_hover_and_definition() {
        let source = r#"
type Address {
  kind Role
}

enum Role {
  Home
  Work
}

model User {
  id      Int @id
  address 
}
"#;
        let state = DocumentState::new(source.to_string());

        let completion_offset = source.find("address ").unwrap() + "address ".len();
        let completion_labels: Vec<_> = state
            .completion(completion_offset)
            .into_iter()
            .map(|item| item.label)
            .collect();
        assert!(completion_labels.iter().any(|label| label == "Address"));

        let hover_offset = source.find("kind").unwrap() + 1;
        let hover = state.hover(hover_offset).expect("hover");
        assert!(hover.content.contains("Role"));

        let definition_offset = source.find("kind Role").unwrap() + "kind ".len() + 1;
        let definition = state
            .goto_definition(definition_offset)
            .expect("definition");
        assert!(source[definition.start..definition.end].contains("Role"));
    }

    #[test]
    fn formatted_uses_cached_ast() {
        let source = "model User {\nname String\nid Int @id\n}\n";
        let state = DocumentState::new(source.to_string());
        let formatted = state.formatted().expect("formatted source");
        assert!(formatted.contains("name String"));
        assert_ne!(formatted, source);
        let formatted_again = state.formatted().expect("cached formatted source");
        assert!(std::ptr::eq(formatted.as_ptr(), formatted_again.as_ptr()));
    }

    #[test]
    fn incremental_changes_are_applied_against_cached_source() {
        let source = "model User {\n  role \n}\n";
        let state = DocumentState::new(source.to_string());
        let updated = state.apply_content_changes(&[TextDocumentContentChangeEvent {
            range: Some(Range::new(Position::new(3, 0), Position::new(3, 0))),
            range_length: None,
            text: "enum Role {\n  Member\n}\n".to_string(),
        }]);

        assert_eq!(
            updated,
            "model User {\n  role \n}\nenum Role {\n  Member\n}\n"
        );
    }

    #[test]
    fn semantic_tokens_are_cached_per_document_version() {
        let source = r#"
enum Role {
  MEMBER
}

model User {
  id   Int  @id
  role Role
}
"#;
        let state = DocumentState::new(source.to_string());
        let first = state.semantic_tokens().expect("semantic tokens");
        assert_eq!(first.len(), 1);
        let second = state.semantic_tokens().expect("cached semantic tokens");
        assert!(std::ptr::eq(first.as_ptr(), second.as_ptr()));
    }
}

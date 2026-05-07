//! Per-document state cached by the LSP server.
//!
//! Every time the client sends a `didOpen`, `didChange`, or `didSave`
//! notification the server re-runs analysis and stores a fresh
//! [`DocumentState`].  Subsequent `textDocument/completion`, `hover`, and
//! `definition` requests read from this cache rather than re-analysing.

use nautilus_schema::{
    analysis::{
        analyze, completion_with_analysis, goto_definition_with_analysis, hover_with_analysis,
        AnalysisResult, CompletionItem, HoverInfo,
    },
    format_schema, LineIndex, Span,
};

/// Snapshot of a single `.nautilus` document.
#[derive(Clone)]
pub struct DocumentState {
    /// Full text of the document as last received from the client.
    pub source: String,
    /// Cached line offsets for fast span/position conversion.
    pub line_index: LineIndex,
    /// Analysis result produced from `source`.
    pub analysis: AnalysisResult,
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
        }
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

    /// Canonical formatted source derived from the cached AST.
    pub fn formatted(&self) -> Option<String> {
        self.analysis
            .ast
            .as_ref()
            .map(|ast| format_schema(ast, &self.source))
    }
}

#[cfg(test)]
mod tests {
    use super::DocumentState;

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
    }
}

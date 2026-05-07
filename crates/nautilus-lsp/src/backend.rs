//! LSP [`LanguageServer`] implementation for nautilus schemas.
//!
//! All schema intelligence (parse, validate, complete, hover, goto-definition)
//! lives in `nautilus-schema`; this module is pure glue.

use dashmap::DashMap;
use tower_lsp::jsonrpc::Result as LspResult;
use tower_lsp::lsp_types::{
    CompletionItem, CompletionOptions, CompletionParams, CompletionResponse, Diagnostic,
    DidChangeTextDocumentParams, DidCloseTextDocumentParams, DidOpenTextDocumentParams,
    DidSaveTextDocumentParams, DocumentFormattingParams, GotoDefinitionParams,
    GotoDefinitionResponse, Hover, HoverParams, HoverProviderCapability, InitializeParams,
    InitializeResult, InitializedParams, Location, MessageType, OneOf, SaveOptions,
    SemanticTokenType, SemanticTokens, SemanticTokensFullOptions, SemanticTokensLegend,
    SemanticTokensOptions, SemanticTokensParams, SemanticTokensResult,
    SemanticTokensServerCapabilities, ServerCapabilities, ServerInfo, TextDocumentSyncCapability,
    TextDocumentSyncKind, TextDocumentSyncOptions, TextDocumentSyncSaveOptions, TextEdit, Url,
};
use tower_lsp::{Client, LanguageServer};

use nautilus_schema::analysis::semantic_tokens;

use crate::convert::{
    hover_info_to_lsp_with_index, nautilus_completion_to_lsp_with_index,
    nautilus_diagnostic_to_lsp_with_index, offset_to_position_with_index,
    position_to_offset_with_index, semantic_tokens_to_lsp_with_index, span_to_range_with_index,
};
use crate::document::DocumentState;

/// The LSP backend.  Holds the client handle and the per-document cache.
pub struct Backend {
    pub client: Client,
    pub docs: DashMap<Url, DocumentState>,
}

impl Backend {
    /// Re-run analysis on `source`, store the result, and publish diagnostics.
    async fn reanalyze(&self, uri: Url, source: String) {
        let state = DocumentState::new(source.clone());
        let lsp_diags: Vec<Diagnostic> = state
            .analysis
            .diagnostics
            .iter()
            .map(|d| nautilus_diagnostic_to_lsp_with_index(&source, &state.line_index, d))
            .collect();
        self.docs.insert(uri.clone(), state);
        self.client.publish_diagnostics(uri, lsp_diags, None).await;
    }

    fn server_capabilities() -> ServerCapabilities {
        ServerCapabilities {
            text_document_sync: Some(TextDocumentSyncCapability::Options(
                TextDocumentSyncOptions {
                    open_close: Some(true),
                    change: Some(TextDocumentSyncKind::FULL),
                    save: Some(TextDocumentSyncSaveOptions::SaveOptions(SaveOptions {
                        include_text: Some(true),
                    })),
                    ..Default::default()
                },
            )),
            completion_provider: Some(CompletionOptions {
                trigger_characters: Some(vec!["@".to_string(), "=".to_string(), "\"".to_string()]),
                ..Default::default()
            }),
            hover_provider: Some(HoverProviderCapability::Simple(true)),
            definition_provider: Some(OneOf::Left(true)),
            document_formatting_provider: Some(OneOf::Left(true)),
            semantic_tokens_provider: Some(
                SemanticTokensServerCapabilities::SemanticTokensOptions(SemanticTokensOptions {
                    legend: SemanticTokensLegend {
                        token_types: vec![
                            SemanticTokenType::from("nautilusModel"),
                            SemanticTokenType::from("nautilusEnum"),
                            SemanticTokenType::from("nautilusCompositeType"),
                        ],
                        token_modifiers: vec![],
                    },
                    full: Some(SemanticTokensFullOptions::Bool(true)),
                    ..Default::default()
                }),
            ),
            ..Default::default()
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, _params: InitializeParams) -> LspResult<InitializeResult> {
        Ok(InitializeResult {
            capabilities: Self::server_capabilities(),
            server_info: Some(ServerInfo {
                name: "nautilus-lsp".to_string(),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            }),
        })
    }

    async fn initialized(&self, _params: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "nautilus-lsp initialized")
            .await;
    }

    async fn shutdown(&self) -> LspResult<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        self.reanalyze(uri, params.text_document.text).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        // FULL sync -> always exactly one content change with the full text.
        if let Some(change) = params.content_changes.into_iter().next() {
            self.reanalyze(uri, change.text).await;
        }
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        let uri = params.text_document.uri;
        self.docs.remove(&uri);
        self.client.publish_diagnostics(uri, Vec::new(), None).await;
    }

    async fn did_save(&self, params: DidSaveTextDocumentParams) {
        let uri = params.text_document.uri;
        // `include_text` is set to true in ServerCapabilities, so `text` is
        // always present.  Fall back to the cache only as a safety net.
        if let Some(text) = params.text {
            self.reanalyze(uri, text).await;
        } else if let Some(state) = self.docs.get(&uri) {
            let source = state.source.clone();
            drop(state);
            self.reanalyze(uri, source).await;
        }
    }

    async fn completion(&self, params: CompletionParams) -> LspResult<Option<CompletionResponse>> {
        let uri = &params.text_document_position.text_document.uri;
        let pos = params.text_document_position.position;

        let Some(state) = self.docs.get(uri) else {
            return Ok(None);
        };
        let offset = position_to_offset_with_index(&state.source, &state.line_index, pos);
        let items = state.completion(offset);
        let lsp_items: Vec<CompletionItem> = items
            .iter()
            .map(|item| {
                nautilus_completion_to_lsp_with_index(
                    &state.source,
                    &state.line_index,
                    &state.analysis.tokens,
                    offset,
                    item,
                )
            })
            .collect();

        Ok(Some(CompletionResponse::Array(lsp_items)))
    }

    async fn hover(&self, params: HoverParams) -> LspResult<Option<Hover>> {
        let uri = &params.text_document_position_params.text_document.uri;
        let pos = params.text_document_position_params.position;

        let Some(state) = self.docs.get(uri) else {
            return Ok(None);
        };
        let offset = position_to_offset_with_index(&state.source, &state.line_index, pos);

        Ok(state
            .hover(offset)
            .as_ref()
            .map(|h| hover_info_to_lsp_with_index(&state.source, &state.line_index, h)))
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> LspResult<Option<GotoDefinitionResponse>> {
        let uri = &params.text_document_position_params.text_document.uri;
        let pos = params.text_document_position_params.position;

        let Some(state) = self.docs.get(uri) else {
            return Ok(None);
        };
        let offset = position_to_offset_with_index(&state.source, &state.line_index, pos);

        let Some(span) = state.goto_definition(offset) else {
            return Ok(None);
        };

        let range = span_to_range_with_index(&state.source, &state.line_index, &span);
        let location = Location {
            uri: uri.clone(),
            range,
        };

        Ok(Some(GotoDefinitionResponse::Scalar(location)))
    }

    async fn semantic_tokens_full(
        &self,
        params: SemanticTokensParams,
    ) -> LspResult<Option<SemanticTokensResult>> {
        let uri = &params.text_document.uri;
        let Some(state) = self.docs.get(uri) else {
            return Ok(None);
        };
        let Some(ast) = &state.analysis.ast else {
            return Ok(None);
        };

        let tokens = semantic_tokens(ast, &state.analysis.tokens);
        let data = semantic_tokens_to_lsp_with_index(&state.source, &state.line_index, &tokens);

        Ok(Some(SemanticTokensResult::Tokens(SemanticTokens {
            result_id: None,
            data,
        })))
    }

    async fn formatting(
        &self,
        params: DocumentFormattingParams,
    ) -> LspResult<Option<Vec<TextEdit>>> {
        let uri = &params.text_document.uri;
        let Some(state) = self.docs.get(uri) else {
            return Ok(None);
        };
        let Some(formatted) = state.formatted() else {
            return Ok(None);
        };
        if formatted == state.source {
            return Ok(Some(Vec::new()));
        }

        let edit = TextEdit {
            range: tower_lsp::lsp_types::Range {
                start: tower_lsp::lsp_types::Position::new(0, 0),
                end: offset_to_position_with_index(
                    &state.source,
                    &state.line_index,
                    state.source.len(),
                ),
            },
            new_text: formatted,
        };

        Ok(Some(vec![edit]))
    }
}

#[cfg(test)]
mod tests {
    use super::Backend;
    use dashmap::DashMap;
    use tower_lsp::lsp_types::{
        CompletionParams, DidChangeTextDocumentParams, DidOpenTextDocumentParams,
        DidSaveTextDocumentParams, Position, TextDocumentContentChangeEvent,
        TextDocumentIdentifier, TextDocumentItem, TextDocumentPositionParams,
        VersionedTextDocumentIdentifier,
    };
    use tower_lsp::{LanguageServer, LspService};

    #[test]
    fn server_capabilities_match_documented_triggers_and_formatting() {
        let caps = Backend::server_capabilities();
        let completion = caps.completion_provider.expect("completion provider");
        let triggers = completion.trigger_characters.expect("trigger characters");
        assert_eq!(triggers, vec!["@", "=", "\""]);
        assert_eq!(
            caps.document_formatting_provider,
            Some(tower_lsp::lsp_types::OneOf::Left(true))
        );
    }

    #[tokio::test]
    async fn untitled_documents_are_cached_and_serve_requests() {
        let (service, _socket) = LspService::new(|client| Backend {
            client,
            docs: DashMap::new(),
        });
        let backend = service.inner();
        let uri = tower_lsp::lsp_types::Url::parse("untitled:Untitled-1").expect("valid uri");

        backend
            .did_open(DidOpenTextDocumentParams {
                text_document: TextDocumentItem {
                    uri: uri.clone(),
                    language_id: "nautilus".to_string(),
                    version: 1,
                    text: "model User {\n  name \n}\n".to_string(),
                },
            })
            .await;

        let state = backend.docs.get(&uri).expect("cached untitled document");
        assert_eq!(state.source, "model User {\n  name \n}\n");
        drop(state);

        let completion = backend
            .completion(CompletionParams {
                text_document_position: TextDocumentPositionParams {
                    text_document: TextDocumentIdentifier { uri: uri.clone() },
                    position: Position::new(1, 7),
                },
                work_done_progress_params: Default::default(),
                partial_result_params: Default::default(),
                context: None,
            })
            .await
            .expect("completion result")
            .expect("completion payload");
        let tower_lsp::lsp_types::CompletionResponse::Array(items) = completion else {
            panic!("expected completion array");
        };
        assert!(
            items.iter().any(|item| item.label == "String"),
            "expected scalar completions for untitled document"
        );

        backend
            .did_change(DidChangeTextDocumentParams {
                text_document: VersionedTextDocumentIdentifier {
                    uri: uri.clone(),
                    version: 2,
                },
                content_changes: vec![TextDocumentContentChangeEvent {
                    range: None,
                    range_length: None,
                    text: "model User {\n  role \n}\n\nenum Role {\n  Member\n}\n".to_string(),
                }],
            })
            .await;

        let state = backend
            .docs
            .get(&uri)
            .expect("updated untitled document remains cached");
        assert!(state.source.contains("role"));
        drop(state);

        backend
            .did_save(DidSaveTextDocumentParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
                text: None,
            })
            .await;

        let completion = backend
            .completion(CompletionParams {
                text_document_position: TextDocumentPositionParams {
                    text_document: TextDocumentIdentifier { uri: uri.clone() },
                    position: Position::new(1, 7),
                },
                work_done_progress_params: Default::default(),
                partial_result_params: Default::default(),
                context: None,
            })
            .await
            .expect("completion result after save")
            .expect("completion payload after save");
        let tower_lsp::lsp_types::CompletionResponse::Array(items) = completion else {
            panic!("expected completion array");
        };
        assert!(
            items.iter().any(|item| item.label == "Role"),
            "expected updated completions after save fallback for untitled document"
        );
    }
}

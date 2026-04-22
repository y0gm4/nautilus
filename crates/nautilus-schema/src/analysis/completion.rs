//! Completion suggestions for `.nautilus` schema files.

use super::{analyze, span_contains, AnalysisResult};
use crate::ast::Declaration;
use crate::token::{Token, TokenKind};
use crate::validator::KNOWN_POSTGRES_EXTENSIONS;

/// The kind of a completion item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionKind {
    /// A language keyword (`model`, `enum`, …).
    Keyword,
    /// A scalar or user-defined field type.
    Type,
    /// A field-level attribute (`@id`, `@unique`, …).
    FieldAttribute,
    /// A model-level attribute (`@@id`, `@@map`, …).
    ModelAttribute,
    /// A reference to a model name.
    ModelName,
    /// A reference to an enum name.
    EnumName,
    /// A field name inside a model or datasource.
    FieldName,
}

/// A single completion suggestion.
#[derive(Debug, Clone, PartialEq)]
pub struct CompletionItem {
    /// The text displayed in the completion popup.
    pub label: String,
    /// The text to actually insert (defaults to `label` when `None`).
    pub insert_text: Option<String>,
    /// Whether `insert_text` uses LSP snippet syntax (`$1`, `${1:placeholder}`, etc.).
    pub is_snippet: bool,
    /// What kind of thing this item represents.
    pub kind: CompletionKind,
    /// Optional extra description shown in the completion popup.
    pub detail: Option<String>,
}

impl CompletionItem {
    pub(super) fn new(
        label: impl Into<String>,
        kind: CompletionKind,
        detail: impl Into<Option<String>>,
    ) -> Self {
        Self {
            label: label.into(),
            insert_text: None,
            is_snippet: false,
            kind,
            detail: detail.into(),
        }
    }

    pub(super) fn with_insert(
        label: impl Into<String>,
        insert_text: impl Into<String>,
        kind: CompletionKind,
        detail: impl Into<Option<String>>,
    ) -> Self {
        Self {
            label: label.into(),
            insert_text: Some(insert_text.into()),
            is_snippet: false,
            kind,
            detail: detail.into(),
        }
    }

    pub(super) fn with_snippet(
        label: impl Into<String>,
        snippet: impl Into<String>,
        kind: CompletionKind,
        detail: impl Into<Option<String>>,
    ) -> Self {
        Self {
            label: label.into(),
            insert_text: Some(snippet.into()),
            is_snippet: true,
            kind,
            detail: detail.into(),
        }
    }
}

/// Returns completions appropriate at `offset` (byte offset) in `source`.
///
/// Uses the parsed AST to determine context:
/// - Outside all declarations -> top-level keywords.
/// - Inside a `datasource` or `generator` block -> config key suggestions.
/// - Inside a `model` block:
///   - After `@` -> field attribute names.
///   - After `@@` -> model attribute names.
///   - Otherwise -> scalar types, user-defined model/enum names, and common
///     field attributes as a convenience.
pub fn completion(source: &str, offset: usize) -> Vec<CompletionItem> {
    let result = analyze(source);
    completion_with_analysis(source, &result, offset)
}

/// Returns completions appropriate at `offset` using a previously computed
/// [`AnalysisResult`].
pub fn completion_with_analysis(
    _source: &str,
    result: &AnalysisResult,
    offset: usize,
) -> Vec<CompletionItem> {
    let tokens = &result.tokens;
    let provider: Option<String> = extract_provider_from_tokens(tokens);

    if let Some(attr_name) = inside_attr_args_at(tokens, offset) {
        let arg_index = attr_arg_index_at(tokens, offset).unwrap_or(0);
        return attr_argument_completions(&attr_name, provider.as_deref(), arg_index);
    }

    let attr_ctx = attribute_context_at(tokens, offset);
    if attr_ctx == AttributeContext::FieldAttr {
        return field_attribute_completions();
    }
    if attr_ctx == AttributeContext::ModelAttr {
        return model_attribute_completions();
    }

    if let Some(completions) =
        datasource_extension_array_item_completions(tokens, offset, provider.as_deref())
    {
        return completions;
    }

    if let Some(key) = config_value_context_at(tokens, offset) {
        let block_kind = config_block_kind_at(tokens, offset);
        if key == "extensions" {
            let completions =
                datasource_extension_value_completions(tokens, offset, provider.as_deref());
            if !completions.is_empty() {
                return completions;
            }
        }
        let completions = config_value_completions(&key, block_kind);
        if !completions.is_empty() {
            return completions;
        }
    }

    let ast = match &result.ast {
        Some(a) => a,
        None => {
            // AST unavailable (e.g. fatal parse error).  Use the raw token
            // stream to make a best-effort guess about the enclosing block.
            return match declaration_context_at_tokens(tokens, offset) {
                DeclarationContext::Model => scalar_type_completions(provider.as_deref()),
                DeclarationContext::Type => {
                    let mut items = scalar_type_completions(provider.as_deref());
                    for name in user_enums_from_tokens(tokens) {
                        items.push(CompletionItem::new(
                            name,
                            CompletionKind::EnumName,
                            Some("Enum reference".to_string()),
                        ));
                    }
                    items
                }
                DeclarationContext::Other => top_level_completions(),
            };
        }
    };

    let user_models: Vec<String> = ast
        .declarations
        .iter()
        .filter_map(|d| {
            if let Declaration::Model(m) = d {
                Some(m.name.value.clone())
            } else {
                None
            }
        })
        .collect();

    let user_enums: Vec<String> = ast
        .declarations
        .iter()
        .filter_map(|d| {
            if let Declaration::Enum(e) = d {
                Some(e.name.value.clone())
            } else {
                None
            }
        })
        .collect();

    let user_composite_types: Vec<String> = ast
        .declarations
        .iter()
        .filter_map(|d| {
            if let Declaration::Type(t) = d {
                Some(t.name.value.clone())
            } else {
                None
            }
        })
        .collect();

    let containing_decl = ast
        .declarations
        .iter()
        .find(|d| span_contains(d.span(), offset));

    match containing_decl {
        None => {
            // The offset isn't inside any parsed declaration. This can happen
            // when error recovery dropped the enclosing block.  Fall back to
            // the token stream to make a best-effort guess.
            match declaration_context_at_tokens(tokens, offset) {
                DeclarationContext::Model => {
                    let mut items = scalar_type_completions(provider.as_deref());
                    for name in &user_models {
                        items.push(CompletionItem::new(
                            name.clone(),
                            CompletionKind::ModelName,
                            Some("Model reference".to_string()),
                        ));
                    }
                    for name in &user_enums {
                        items.push(CompletionItem::new(
                            name.clone(),
                            CompletionKind::EnumName,
                            Some("Enum reference".to_string()),
                        ));
                    }
                    for name in &user_composite_types {
                        items.push(CompletionItem::new(
                            name.clone(),
                            CompletionKind::Type,
                            Some("Composite type reference".to_string()),
                        ));
                    }
                    items
                }
                DeclarationContext::Type => {
                    let mut items = scalar_type_completions(provider.as_deref());
                    for name in &user_enums {
                        items.push(CompletionItem::new(
                            name.clone(),
                            CompletionKind::EnumName,
                            Some("Enum reference".to_string()),
                        ));
                    }
                    items
                }
                DeclarationContext::Other => top_level_completions(),
            }
        }

        Some(Declaration::Datasource(_)) => datasource_field_completions(),

        Some(Declaration::Generator(_)) => generator_field_completions(),

        Some(Declaration::Enum(_)) => {
            // Inside an enum body: only enum variants are meaningful here,
            // nothing to complete (they are user-defined identifiers).
            Vec::new()
        }

        Some(Declaration::Model(_)) => {
            let mut items = scalar_type_completions(provider.as_deref());
            for name in &user_models {
                items.push(CompletionItem::new(
                    name.clone(),
                    CompletionKind::ModelName,
                    Some("Model reference".to_string()),
                ));
            }
            for name in &user_enums {
                items.push(CompletionItem::new(
                    name.clone(),
                    CompletionKind::EnumName,
                    Some("Enum reference".to_string()),
                ));
            }
            for name in &user_composite_types {
                items.push(CompletionItem::new(
                    name.clone(),
                    CompletionKind::Type,
                    Some("Composite type reference".to_string()),
                ));
            }
            items
        }

        Some(Declaration::Type(_)) => {
            let mut items = scalar_type_completions(provider.as_deref());
            for name in &user_enums {
                items.push(CompletionItem::new(
                    name.clone(),
                    CompletionKind::EnumName,
                    Some("Enum reference".to_string()),
                ));
            }
            items
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigBlockKind {
    Datasource,
    Generator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttributeContext {
    FieldAttr,
    ModelAttr,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeclarationContext {
    Model,
    Type,
    Other,
}

/// Returns the name of the attribute whose argument list contains `offset`,
/// e.g. `"store"` when the cursor is inside `@store(|)`, `"relation"` for
/// `@relation(|)`, etc.  Returns `None` when `offset` is not inside any
/// attribute argument list.
fn inside_attr_args_at(tokens: &[Token], offset: usize) -> Option<String> {
    let relevant: Vec<&Token> = tokens
        .iter()
        .filter(|t| t.span.end <= offset && !matches!(t.kind, TokenKind::Newline))
        .collect();

    let mut depth: i32 = 0;
    for tok in relevant.iter().rev() {
        match tok.kind {
            TokenKind::RParen => depth += 1,
            TokenKind::LParen => {
                if depth == 0 {
                    let lparen_start = tok.span.start;
                    let before: Vec<&Token> = tokens
                        .iter()
                        .filter(|t| {
                            t.span.end <= lparen_start && !matches!(t.kind, TokenKind::Newline)
                        })
                        .collect();
                    if let Some(name_tok) = before.last() {
                        if let TokenKind::Ident(attr_name) = &name_tok.kind {
                            let attr_name = attr_name.clone();
                            let before_name: Vec<&Token> = tokens
                                .iter()
                                .filter(|t| {
                                    t.span.end <= name_tok.span.start
                                        && !matches!(t.kind, TokenKind::Newline)
                                })
                                .collect();
                            if let Some(at_tok) = before_name.last() {
                                if matches!(at_tok.kind, TokenKind::At | TokenKind::AtAt) {
                                    return Some(attr_name);
                                }
                            }
                        }
                    }
                    return None;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

/// Detects whether `offset` sits in a config value position (`key = <cursor>`)
/// within a datasource or generator block. Returns the key name if so.
fn config_value_context_at(tokens: &[Token], offset: usize) -> Option<String> {
    let mut eq_pos: Option<usize> = None;
    let mut key_pos: Option<usize> = None;

    for (i, tok) in tokens.iter().enumerate() {
        if tok.span.end > offset {
            break;
        }
        if tok.kind == TokenKind::Newline {
            eq_pos = None;
            key_pos = None;
        } else if tok.kind == TokenKind::Equal {
            eq_pos = Some(i);
        } else if let TokenKind::Ident(_) = tok.kind {
            if eq_pos.is_none() {
                key_pos = Some(i);
            }
        }
    }

    let eq_idx = eq_pos?;
    let key_idx = key_pos?;

    if eq_idx != key_idx + 1 {
        return None;
    }

    if let TokenKind::Ident(key) = &tokens[key_idx].kind {
        return Some(key.clone());
    }
    None
}

/// Detect whether `offset` immediately follows a `@` or `@@` token.
fn attribute_context_at(tokens: &[Token], offset: usize) -> AttributeContext {
    let last = tokens
        .iter()
        .rfind(|t| t.span.end <= offset && !matches!(t.kind, TokenKind::Newline));

    match last {
        Some(t) if t.kind == TokenKind::AtAt => AttributeContext::ModelAttr,
        Some(t) if t.kind == TokenKind::At => AttributeContext::FieldAttr,
        // The cursor might be in the middle of an identifier that started
        // after `@` — look one token further back.
        Some(t) if matches!(t.kind, TokenKind::Ident(_)) => {
            let before = tokens.iter().rfind(|tok| tok.span.end <= t.span.start);
            match before {
                Some(b) if b.kind == TokenKind::AtAt => AttributeContext::ModelAttr,
                Some(b) if b.kind == TokenKind::At => AttributeContext::FieldAttr,
                _ => AttributeContext::None,
            }
        }
        _ => AttributeContext::None,
    }
}

/// Returns the declaration block that appears to enclose `offset`, based
/// purely on the token stream (no AST required).
fn declaration_context_at_tokens(tokens: &[Token], offset: usize) -> DeclarationContext {
    let relevant: Vec<&Token> = tokens.iter().filter(|t| t.span.end <= offset).collect();

    let mut depth: i32 = 0;
    for tok in relevant.iter().rev() {
        match tok.kind {
            TokenKind::RBrace => depth += 1,
            TokenKind::LBrace => {
                if depth == 0 {
                    let idx = tokens
                        .iter()
                        .position(|t| std::ptr::eq(t, *tok))
                        .unwrap_or(0);
                    let before: Vec<&Token> = tokens[..idx]
                        .iter()
                        .filter(|t| !matches!(t.kind, TokenKind::Newline))
                        .collect();
                    if let Some(name_tok) = before.last() {
                        if matches!(name_tok.kind, TokenKind::Ident(_)) {
                            let before_name: Vec<&Token> = tokens[..idx]
                                .iter()
                                .filter(|t| !matches!(t.kind, TokenKind::Newline))
                                .rev()
                                .skip(1)
                                .take(1)
                                .collect();
                            if let Some(kw) = before_name.first() {
                                return match kw.kind {
                                    TokenKind::Model => DeclarationContext::Model,
                                    TokenKind::Type => DeclarationContext::Type,
                                    _ => DeclarationContext::Other,
                                };
                            }
                        }
                    }
                    return DeclarationContext::Other;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    DeclarationContext::Other
}

fn user_enums_from_tokens(tokens: &[Token]) -> Vec<String> {
    let mut enums = Vec::new();

    for window in tokens.windows(2) {
        if window[0].kind == TokenKind::Enum {
            if let TokenKind::Ident(name) = &window[1].kind {
                enums.push(name.clone());
            }
        }
    }

    enums
}

/// Extract the datasource `provider` value from a token stream.
///
/// Looks for the pattern:  `datasource <ident> { … provider = "<value>" … }`
/// Returns `Some("postgresql" | "mysql" | "sqlite")` when found, `None` otherwise.
fn extract_provider_from_tokens(tokens: &[Token]) -> Option<String> {
    let n = tokens.len();
    for i in 0..n {
        if let TokenKind::Ident(ref kw) = tokens[i].kind {
            if kw != "provider" {
                continue;
            }
        } else {
            continue;
        }
        let mut j = i + 1;
        while j < n && matches!(tokens[j].kind, TokenKind::Newline) {
            j += 1;
        }
        if j >= n || tokens[j].kind != TokenKind::Equal {
            continue;
        }
        j += 1;
        while j < n && matches!(tokens[j].kind, TokenKind::Newline) {
            j += 1;
        }
        if j < n {
            if let TokenKind::String(ref val) = tokens[j].kind {
                let v = val.as_str();
                if matches!(v, "postgresql" | "mysql" | "sqlite") {
                    return Some(v.to_string());
                }
            }
        }
    }
    None
}

/// Returns context-sensitive completions for the arguments of a specific attribute.
///
/// Called when the cursor is detected to be inside `@attr(|)` argument parens.
/// Returns the 0-based argument index of `offset` inside the innermost
/// unmatched `(...)`, scanning backwards through `tokens`.
/// Returns `None` if not inside any parentheses.
fn attr_arg_index_at(tokens: &[Token], offset: usize) -> Option<usize> {
    let relevant: Vec<&Token> = tokens
        .iter()
        .filter(|t| t.span.end <= offset && !matches!(t.kind, TokenKind::Newline))
        .collect();
    let mut depth: i32 = 0;
    let mut commas: usize = 0;
    for tok in relevant.iter().rev() {
        match tok.kind {
            TokenKind::RParen => depth += 1,
            TokenKind::LParen => {
                if depth == 0 {
                    return Some(commas);
                }
                depth -= 1;
            }
            TokenKind::Comma if depth == 0 => commas += 1,
            _ => {}
        }
    }
    None
}

fn attr_argument_completions(
    attr_name: &str,
    provider: Option<&str>,
    arg_index: usize,
) -> Vec<CompletionItem> {
    match attr_name {
        "store" => vec![CompletionItem::new(
            "json",
            CompletionKind::FieldAttribute,
            Some("Serialize array as JSON in the database".to_string()),
        )],
        "relation" => vec![
            CompletionItem::new(
                "fields: []",
                CompletionKind::FieldName,
                Some("Local FK field(s) on this model".to_string()),
            ),
            CompletionItem::new(
                "references: []",
                CompletionKind::FieldName,
                Some("Referenced field(s) on the target model".to_string()),
            ),
            CompletionItem::new(
                "name: \"\"",
                CompletionKind::FieldName,
                Some(
                    "Relation name (required when multiple relations to the same model)"
                        .to_string(),
                ),
            ),
            CompletionItem::new(
                "onDelete: Cascade",
                CompletionKind::FieldName,
                Some("Referential action on parent record delete".to_string()),
            ),
            CompletionItem::new(
                "onUpdate: Cascade",
                CompletionKind::FieldName,
                Some("Referential action on parent record update".to_string()),
            ),
        ],
        "default" => vec![
            CompletionItem::new(
                "autoincrement()",
                CompletionKind::Keyword,
                Some("Auto-incrementing integer sequence".to_string()),
            ),
            CompletionItem::new(
                "now()",
                CompletionKind::Keyword,
                Some("Current timestamp at insert time".to_string()),
            ),
            CompletionItem::new(
                "uuid()",
                CompletionKind::Keyword,
                Some("Randomly generated UUID".to_string()),
            ),
        ],
        "computed" => match arg_index {
            0 => vec![CompletionItem::new(
                "SQL expression",
                CompletionKind::Keyword,
                Some("e.g. price * quantity  or  first_name || ' ' || last_name".to_string()),
            )],
            _ => vec![
                CompletionItem::new(
                    "Stored",
                    CompletionKind::Keyword,
                    Some("Computed on write, persisted on disk (all databases)".to_string()),
                ),
                CompletionItem::new(
                    "Virtual",
                    CompletionKind::Keyword,
                    Some("Computed on read, never stored (MySQL / SQLite only)".to_string()),
                ),
            ],
        },
        "index" => index_argument_completions(provider),
        _ => vec![],
    }
}

fn top_level_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem::new(
            "model",
            CompletionKind::Keyword,
            Some("Define a data model".to_string()),
        ),
        CompletionItem::new(
            "enum",
            CompletionKind::Keyword,
            Some("Define an enumeration".to_string()),
        ),
        CompletionItem::new(
            "type",
            CompletionKind::Keyword,
            Some("Define a composite type".to_string()),
        ),
        CompletionItem::new(
            "datasource",
            CompletionKind::Keyword,
            Some("Configure a data source".to_string()),
        ),
        CompletionItem::new(
            "generator",
            CompletionKind::Keyword,
            Some("Configure code generation".to_string()),
        ),
    ]
}

/// Return argument completions for `@@index(…)`, filtered by DB provider when known.
///
/// All DB types:   BTree (default, always shown)
/// PG + MySQL:     Hash
/// PG only:        Gin, Gist, Brin
/// MySQL only:     FullText
fn index_argument_completions(provider: Option<&str>) -> Vec<CompletionItem> {
    struct TypeEntry {
        label: &'static str,
        desc: &'static str,
        providers: &'static [&'static str],
    }
    let type_entries = [
        TypeEntry {
            label: "type: BTree",
            desc: "B-Tree index — default on all databases",
            providers: &["postgresql", "mysql", "sqlite"],
        },
        TypeEntry {
            label: "type: Hash",
            desc: "Hash index — PostgreSQL and MySQL 8+",
            providers: &["postgresql", "mysql"],
        },
        TypeEntry {
            label: "type: Gin",
            desc: "GIN index — PostgreSQL only (arrays, JSONB, full-text)",
            providers: &["postgresql"],
        },
        TypeEntry {
            label: "type: Gist",
            desc: "GiST index — PostgreSQL only (geometry, range types)",
            providers: &["postgresql"],
        },
        TypeEntry {
            label: "type: Brin",
            desc: "BRIN index — PostgreSQL only (ordered large tables)",
            providers: &["postgresql"],
        },
        TypeEntry {
            label: "type: FullText",
            desc: "FULLTEXT index — MySQL only",
            providers: &["mysql"],
        },
    ];

    let mut items: Vec<CompletionItem> = type_entries
        .iter()
        .filter(|e| match provider {
            Some(p) => e.providers.contains(&p),
            None => true,
        })
        .map(|e| CompletionItem::new(e.label, CompletionKind::Keyword, Some(e.desc.to_string())))
        .collect();

    items.push(CompletionItem::new(
        "name: \"\"",
        CompletionKind::FieldName,
        Some("Logical developer name for this index".to_string()),
    ));
    items.push(CompletionItem::new(
        "map: \"\"",
        CompletionKind::FieldName,
        Some("Physical DDL index name (overrides auto-generated idx_… name)".to_string()),
    ));

    items
}

fn scalar_type_completions(provider: Option<&str>) -> Vec<CompletionItem> {
    let pg = matches!(provider, Some("postgresql") | None);
    let pg_or_mysql = matches!(provider, Some("postgresql") | Some("mysql") | None);

    let mut items = vec![
        CompletionItem::new(
            "String",
            CompletionKind::Type,
            Some("UTF-8 text -> VARCHAR / TEXT".to_string()),
        ),
        CompletionItem::new(
            "Boolean",
            CompletionKind::Type,
            Some("true / false -> BOOLEAN".to_string()),
        ),
        CompletionItem::new(
            "Int",
            CompletionKind::Type,
            Some("32-bit integer -> INTEGER".to_string()),
        ),
        CompletionItem::new(
            "BigInt",
            CompletionKind::Type,
            Some("64-bit integer -> BIGINT".to_string()),
        ),
        CompletionItem::new(
            "Float",
            CompletionKind::Type,
            Some("64-bit float -> DOUBLE PRECISION".to_string()),
        ),
        CompletionItem::new(
            "Decimal",
            CompletionKind::Type,
            Some("Exact decimal -> NUMERIC".to_string()),
        ),
        CompletionItem::new(
            "DateTime",
            CompletionKind::Type,
            Some("Timestamp with time zone -> TIMESTAMPTZ".to_string()),
        ),
        CompletionItem::new(
            "Bytes",
            CompletionKind::Type,
            Some("Binary data -> BYTEA".to_string()),
        ),
        CompletionItem::new(
            "Json",
            CompletionKind::Type,
            Some("JSON document -> JSONB".to_string()),
        ),
        CompletionItem::new(
            "Uuid",
            CompletionKind::Type,
            Some("UUID -> UUID".to_string()),
        ),
    ];

    if pg {
        items.push(CompletionItem::new(
            "Citext",
            CompletionKind::Type,
            Some("Case-insensitive text -> CITEXT (PostgreSQL + citext extension)".to_string()),
        ));
        items.push(CompletionItem::new(
            "Hstore",
            CompletionKind::Type,
            Some("Key/value text map -> HSTORE (PostgreSQL + hstore extension)".to_string()),
        ));
        items.push(CompletionItem::new(
            "Ltree",
            CompletionKind::Type,
            Some("Label tree path -> LTREE (PostgreSQL + ltree extension)".to_string()),
        ));
        items.push(CompletionItem::new(
            "Jsonb",
            CompletionKind::Type,
            Some("JSONB document -> JSONB (PostgreSQL only)".to_string()),
        ));
        items.push(CompletionItem::new(
            "Xml",
            CompletionKind::Type,
            Some("XML document -> XML (PostgreSQL only)".to_string()),
        ));
    }

    if pg_or_mysql {
        items.push(CompletionItem::with_snippet(
            "Char(n)",
            "Char(${1:n})",
            CompletionKind::Type,
            Some("Fixed-length string -> CHAR(n) (PostgreSQL and MySQL)".to_string()),
        ));
        items.push(CompletionItem::with_snippet(
            "VarChar(n)",
            "VarChar(${1:n})",
            CompletionKind::Type,
            Some("Variable-length string -> VARCHAR(n) (PostgreSQL and MySQL)".to_string()),
        ));
    }

    items
}

fn field_attribute_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem::new(
            "id",
            CompletionKind::FieldAttribute,
            Some("Mark as primary key".to_string()),
        ),
        CompletionItem::new(
            "unique",
            CompletionKind::FieldAttribute,
            Some("Add a unique constraint".to_string()),
        ),
        CompletionItem::new(
            "default()",
            CompletionKind::FieldAttribute,
            Some("Set a default value".to_string()),
        ),
        CompletionItem::new(
            "relation()",
            CompletionKind::FieldAttribute,
            Some("Define a relation".to_string()),
        ),
        CompletionItem::new(
            "map(\"\")",
            CompletionKind::FieldAttribute,
            Some("Override the column name".to_string()),
        ),
        CompletionItem::new(
            "store(json)",
            CompletionKind::FieldAttribute,
            Some("Store as JSON column".to_string()),
        ),
        CompletionItem::new(
            "updatedAt",
            CompletionKind::FieldAttribute,
            Some("Auto-set to current timestamp on every write".to_string()),
        ),
        CompletionItem::with_snippet(
            "computed(…, Stored)",
            "computed(${1:expr}, ${2|Stored,Virtual|})",
            CompletionKind::FieldAttribute,
            Some("Database-generated column (Stored or Virtual)".to_string()),
        ),
        CompletionItem::with_snippet(
            "check(…)",
            "check(${1:expr})",
            CompletionKind::FieldAttribute,
            Some("Add a CHECK constraint on this field".to_string()),
        ),
    ]
}

fn model_attribute_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem::new(
            "id([])",
            CompletionKind::ModelAttribute,
            Some("Composite primary key".to_string()),
        ),
        CompletionItem::new(
            "unique([])",
            CompletionKind::ModelAttribute,
            Some("Composite unique constraint".to_string()),
        ),
        CompletionItem::new(
            "index([])",
            CompletionKind::ModelAttribute,
            Some(
                "Add a database index — optionally with type: BTree|Hash|Gin|Gist|Brin|FullText"
                    .to_string(),
            ),
        ),
        CompletionItem::new(
            "map(\"\")",
            CompletionKind::ModelAttribute,
            Some("Override the table name".to_string()),
        ),
        CompletionItem::with_snippet(
            "check(…)",
            "check(${1:expr})",
            CompletionKind::ModelAttribute,
            Some("Add a table-level CHECK constraint".to_string()),
        ),
    ]
}

fn datasource_field_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem::new(
            "provider",
            CompletionKind::FieldName,
            Some("Database provider".to_string()),
        ),
        CompletionItem::new(
            "url",
            CompletionKind::FieldName,
            Some("Connection URL".to_string()),
        ),
        CompletionItem::new(
            "direct_url",
            CompletionKind::FieldName,
            Some("Direct admin/introspection URL".to_string()),
        ),
        CompletionItem::new(
            "extensions",
            CompletionKind::FieldName,
            Some("PostgreSQL extensions to install before DDL".to_string()),
        ),
    ]
}

fn datasource_extension_value_completions(
    tokens: &[Token],
    offset: usize,
    provider: Option<&str>,
) -> Vec<CompletionItem> {
    if matches!(provider, Some("mysql") | Some("sqlite")) {
        return Vec::new();
    }

    match extensions_value_mode_at(tokens, offset) {
        ExtensionsValueMode::StartOfValue => vec![CompletionItem::with_snippet(
            "extensions = [..]",
            "[${1:pg_trgm}]",
            CompletionKind::Keyword,
            Some("PostgreSQL-only array of extension names".to_string()),
        )],
        ExtensionsValueMode::InsideArray => KNOWN_POSTGRES_EXTENSIONS
            .iter()
            .map(|extension| {
                CompletionItem::with_insert(
                    *extension,
                    render_extension_completion_insert(extension),
                    CompletionKind::Keyword,
                    Some("Known PostgreSQL extension".to_string()),
                )
            })
            .collect(),
        ExtensionsValueMode::None => Vec::new(),
    }
}

fn datasource_extension_array_item_completions(
    tokens: &[Token],
    offset: usize,
    provider: Option<&str>,
) -> Option<Vec<CompletionItem>> {
    if matches!(provider, Some("mysql") | Some("sqlite")) {
        return None;
    }

    if extensions_value_mode_at(tokens, offset) != ExtensionsValueMode::InsideArray {
        return None;
    }

    Some(
        KNOWN_POSTGRES_EXTENSIONS
            .iter()
            .map(|extension| {
                CompletionItem::with_insert(
                    *extension,
                    render_extension_completion_insert(extension),
                    CompletionKind::Keyword,
                    Some("Known PostgreSQL extension".to_string()),
                )
            })
            .collect(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtensionsValueMode {
    None,
    StartOfValue,
    InsideArray,
}

fn extensions_value_mode_at(tokens: &[Token], offset: usize) -> ExtensionsValueMode {
    let mut pending_block_kind: Option<Option<ConfigBlockKind>> = None;
    let mut block_stack: Vec<Option<ConfigBlockKind>> = Vec::new();
    let mut current_field_key: Option<String> = None;
    let mut seen_equal = false;
    let mut saw_value_token_after_equal = false;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;

    for token in tokens.iter().take_while(|token| token.span.end <= offset) {
        match token.kind {
            TokenKind::Datasource => pending_block_kind = Some(Some(ConfigBlockKind::Datasource)),
            TokenKind::Generator => pending_block_kind = Some(Some(ConfigBlockKind::Generator)),
            TokenKind::Model | TokenKind::Enum | TokenKind::Type => pending_block_kind = Some(None),
            TokenKind::LBrace => {
                block_stack.push(pending_block_kind.take().unwrap_or(None));
                current_field_key = None;
                seen_equal = false;
                saw_value_token_after_equal = false;
                paren_depth = 0;
                bracket_depth = 0;
            }
            TokenKind::RBrace => {
                block_stack.pop();
                current_field_key = None;
                seen_equal = false;
                saw_value_token_after_equal = false;
                paren_depth = 0;
                bracket_depth = 0;
            }
            _ if block_stack.last() != Some(&Some(ConfigBlockKind::Datasource)) => {}
            TokenKind::Newline => {
                if bracket_depth == 0 && paren_depth == 0 {
                    current_field_key = None;
                    seen_equal = false;
                    saw_value_token_after_equal = false;
                }
            }
            TokenKind::Ident(ref name) if current_field_key.is_none() && !seen_equal => {
                current_field_key = Some(name.clone());
            }
            TokenKind::Equal if current_field_key.is_some() => {
                seen_equal = true;
            }
            TokenKind::LParen if seen_equal => {
                saw_value_token_after_equal = true;
                paren_depth += 1;
            }
            TokenKind::RParen if seen_equal && paren_depth > 0 => {
                saw_value_token_after_equal = true;
                paren_depth -= 1;
            }
            TokenKind::LBracket if seen_equal => {
                saw_value_token_after_equal = true;
                bracket_depth += 1;
            }
            TokenKind::RBracket if seen_equal && bracket_depth > 0 => {
                saw_value_token_after_equal = true;
                bracket_depth -= 1;
            }
            _ if seen_equal => {
                saw_value_token_after_equal = true;
            }
            _ => {}
        }
    }

    if current_field_key.as_deref() != Some("extensions") || !seen_equal {
        return ExtensionsValueMode::None;
    }

    if bracket_depth > 0 {
        ExtensionsValueMode::InsideArray
    } else if !saw_value_token_after_equal {
        ExtensionsValueMode::StartOfValue
    } else {
        ExtensionsValueMode::None
    }
}

fn render_extension_completion_insert(extension: &str) -> String {
    if is_bare_schema_identifier(extension) {
        extension.to_string()
    } else {
        format!("\"{}\"", extension)
    }
}

fn is_bare_schema_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return false;
    }
    matches!(TokenKind::from_ident(name), TokenKind::Ident(_))
}

fn generator_field_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem::new(
            "provider",
            CompletionKind::FieldName,
            Some("Client generator provider".to_string()),
        ),
        CompletionItem::new(
            "output",
            CompletionKind::FieldName,
            Some("Output path for generated files".to_string()),
        ),
        CompletionItem::new(
            "interface",
            CompletionKind::FieldName,
            Some("Client interface style: \"sync\" (default) or \"async\"".to_string()),
        ),
        CompletionItem::new(
            "recursive_type_depth",
            CompletionKind::FieldName,
            Some(
                "Depth of recursive include TypedDicts — Python client only (default: 5)"
                    .to_string(),
            ),
        ),
        CompletionItem::new(
            "package",
            CompletionKind::FieldName,
            Some("Root Java package for generated sources".to_string()),
        ),
        CompletionItem::new(
            "group_id",
            CompletionKind::FieldName,
            Some("Maven groupId for the Java module".to_string()),
        ),
        CompletionItem::new(
            "artifact_id",
            CompletionKind::FieldName,
            Some("Maven artifactId for the Java module".to_string()),
        ),
        CompletionItem::new(
            "mode",
            CompletionKind::FieldName,
            Some("Java output mode: \"maven\" (default) or \"jar\"".to_string()),
        ),
    ]
}

/// Detects whether `offset` is inside a `datasource` or `generator` block,
/// by scanning the token stream backwards to find the enclosing block keyword.
fn config_block_kind_at(tokens: &[Token], offset: usize) -> Option<ConfigBlockKind> {
    let relevant: Vec<&Token> = tokens.iter().filter(|t| t.span.end <= offset).collect();

    let mut depth: i32 = 0;
    for tok in relevant.iter().rev() {
        match tok.kind {
            TokenKind::RBrace => depth += 1,
            TokenKind::LBrace => {
                if depth == 0 {
                    let idx = tokens
                        .iter()
                        .position(|t| std::ptr::eq(t, *tok))
                        .unwrap_or(0);
                    let before: Vec<&Token> = tokens[..idx]
                        .iter()
                        .filter(|t| !matches!(t.kind, TokenKind::Newline))
                        .collect();
                    if before.len() >= 2 {
                        let kw_tok = &before[before.len() - 2];
                        return match kw_tok.kind {
                            TokenKind::Datasource => Some(ConfigBlockKind::Datasource),
                            TokenKind::Generator => Some(ConfigBlockKind::Generator),
                            _ => None,
                        };
                    }
                    return None;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    None
}

fn config_value_completions(key: &str, block_kind: Option<ConfigBlockKind>) -> Vec<CompletionItem> {
    match key {
        "provider" => match block_kind {
            Some(ConfigBlockKind::Datasource) => vec![
                CompletionItem::with_insert(
                    "postgresql",
                    "\"postgresql\"",
                    CompletionKind::Keyword,
                    Some("PostgreSQL database".to_string()),
                ),
                CompletionItem::with_insert(
                    "mysql",
                    "\"mysql\"",
                    CompletionKind::Keyword,
                    Some("MySQL database".to_string()),
                ),
                CompletionItem::with_insert(
                    "sqlite",
                    "\"sqlite\"",
                    CompletionKind::Keyword,
                    Some("SQLite database".to_string()),
                ),
            ],
            Some(ConfigBlockKind::Generator) => vec![
                CompletionItem::with_insert(
                    "nautilus-client-rs",
                    "\"nautilus-client-rs\"",
                    CompletionKind::Keyword,
                    Some("Rust client generator".to_string()),
                ),
                CompletionItem::with_insert(
                    "nautilus-client-py",
                    "\"nautilus-client-py\"",
                    CompletionKind::Keyword,
                    Some("Python client generator".to_string()),
                ),
                CompletionItem::with_insert(
                    "nautilus-client-js",
                    "\"nautilus-client-js\"",
                    CompletionKind::Keyword,
                    Some("JavaScript/TypeScript client generator".to_string()),
                ),
                CompletionItem::with_insert(
                    "nautilus-client-java",
                    "\"nautilus-client-java\"",
                    CompletionKind::Keyword,
                    Some("Java client generator".to_string()),
                ),
            ],
            None => vec![
                CompletionItem::with_insert(
                    "postgresql",
                    "\"postgresql\"",
                    CompletionKind::Keyword,
                    Some("PostgreSQL database".to_string()),
                ),
                CompletionItem::with_insert(
                    "mysql",
                    "\"mysql\"",
                    CompletionKind::Keyword,
                    Some("MySQL database".to_string()),
                ),
                CompletionItem::with_insert(
                    "sqlite",
                    "\"sqlite\"",
                    CompletionKind::Keyword,
                    Some("SQLite database".to_string()),
                ),
                CompletionItem::with_insert(
                    "nautilus-client-rs",
                    "\"nautilus-client-rs\"",
                    CompletionKind::Keyword,
                    Some("Rust client generator".to_string()),
                ),
                CompletionItem::with_insert(
                    "nautilus-client-py",
                    "\"nautilus-client-py\"",
                    CompletionKind::Keyword,
                    Some("Python client generator".to_string()),
                ),
                CompletionItem::with_insert(
                    "nautilus-client-js",
                    "\"nautilus-client-js\"",
                    CompletionKind::Keyword,
                    Some("JavaScript/TypeScript client generator".to_string()),
                ),
                CompletionItem::with_insert(
                    "nautilus-client-java",
                    "\"nautilus-client-java\"",
                    CompletionKind::Keyword,
                    Some("Java client generator".to_string()),
                ),
            ],
        },
        "interface" => vec![
            CompletionItem::with_insert(
                "sync",
                "\"sync\"",
                CompletionKind::Keyword,
                Some("Synchronous client interface (default)".to_string()),
            ),
            CompletionItem::with_insert(
                "async",
                "\"async\"",
                CompletionKind::Keyword,
                Some("Asynchronous client interface".to_string()),
            ),
        ],
        "mode" => vec![
            CompletionItem::with_insert(
                "maven",
                "\"maven\"",
                CompletionKind::Keyword,
                Some("Generate a Maven module (default)".to_string()),
            ),
            CompletionItem::with_insert(
                "jar",
                "\"jar\"",
                CompletionKind::Keyword,
                Some("Also build a plain Java jar bundle under output/dist".to_string()),
            ),
        ],
        _ => Vec::new(),
    }
}

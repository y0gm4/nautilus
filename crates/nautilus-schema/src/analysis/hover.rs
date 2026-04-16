//! Hover documentation for `.nautilus` schema files.

use super::{analyze, span_contains, AnalysisResult};
use crate::ast::{
    ComputedKind, Declaration, FieldAttribute, FieldModifier, FieldType, ModelAttribute, Schema,
};
use crate::span::Span;
use crate::token::{Token, TokenKind};

/// Information to display when hovering over a token.
#[derive(Debug, Clone, PartialEq)]
pub struct HoverInfo {
    /// Markdown-formatted documentation string.
    pub content: String,
    /// Span of the token the hover applies to (for range highlighting).
    pub span: Option<Span>,
}

/// Returns hover documentation for the symbol at byte `offset` in `source`.
///
/// Looks up the innermost AST node whose span contains `offset` and returns
/// relevant documentation:
/// - Scalar types -> SQL mapping and description.
/// - Identifiers matching a model name -> model summary.
/// - Identifiers matching an enum name -> enum variant list.
/// - Field declarations -> field type and modifiers.
pub fn hover(source: &str, offset: usize) -> Option<HoverInfo> {
    let result = analyze(source);
    hover_with_analysis(source, &result, offset)
}

/// Returns hover documentation for the symbol at `offset` using a previously
/// computed [`AnalysisResult`].
pub fn hover_with_analysis(
    _source: &str,
    result: &AnalysisResult,
    offset: usize,
) -> Option<HoverInfo> {
    let ast = result.ast.as_ref()?;

    if let Some(h) = attribute_hover_at(&result.tokens, offset, Some(ast)) {
        return Some(h);
    }

    for decl in &ast.declarations {
        if !span_contains(decl.span(), offset) {
            continue;
        }

        match decl {
            Declaration::Model(model) => {
                for field in &model.fields {
                    if span_contains(field.span, offset) {
                        let modifier = match field.modifier {
                            FieldModifier::Array => "[]",
                            FieldModifier::Optional => "?",
                            FieldModifier::NotNull => "!",
                            FieldModifier::None => "",
                        };
                        let type_str =
                            format!("{}{}", field_type_name(&field.field_type), modifier);

                        if field.has_relation_attribute() {
                            let base = format!("**{}**: `{}`", field.name.value, type_str);
                            let extra = relation_hover_details(ast, offset).unwrap_or_default();
                            let content = if extra.is_empty() {
                                base
                            } else {
                                format!("{base}  \n\n{extra}")
                            };
                            return Some(HoverInfo {
                                content,
                                span: Some(field.span),
                            });
                        }

                        let attrs_str = format_field_attrs_short(&field.attributes);
                        let detail = field_type_description(&field.field_type);
                        let nullability = match field.modifier {
                            FieldModifier::Optional => Some("nullable"),
                            FieldModifier::NotNull => Some("not null"),
                            _ => None,
                        };
                        let mut content = format!("**{}**: `{}`", field.name.value, type_str);
                        if !attrs_str.is_empty() {
                            content.push_str(&format!("  \n{}", attrs_str));
                        }
                        if let Some(hint) = nullability {
                            content.push_str(&format!("  \n_{}_", hint));
                        }
                        if !detail.is_empty() {
                            content.push_str(&format!("  \n{}", detail));
                        }
                        return Some(HoverInfo {
                            content,
                            span: Some(field.span),
                        });
                    }
                }
                let composite_names: std::collections::HashSet<String> =
                    ast.types().map(|t| t.name.value.clone()).collect();
                return Some(HoverInfo {
                    content: model_hover_content(model, &composite_names),
                    span: Some(model.span),
                });
            }

            Declaration::Enum(enum_decl) => {
                let variants: Vec<&str> = enum_decl
                    .variants
                    .iter()
                    .map(|v| v.name.value.as_str())
                    .collect();
                return Some(HoverInfo {
                    content: format!(
                        "**enum** `{}`  \n**Variants ({}):** {}  \n",
                        enum_decl.name.value,
                        variants.len(),
                        variants
                            .iter()
                            .map(|v| format!("`{v}`"))
                            .collect::<Vec<_>>()
                            .join(" · ")
                    ),
                    span: Some(enum_decl.span),
                });
            }

            Declaration::Datasource(ds) => {
                for field in &ds.fields {
                    if span_contains(field.span, offset) {
                        return Some(HoverInfo {
                            content: config_field_hover(&field.name.value),
                            span: Some(field.span),
                        });
                    }
                }
                return Some(HoverInfo {
                    content: format!("**datasource** `{}`", ds.name.value),
                    span: Some(ds.span),
                });
            }

            Declaration::Generator(gen) => {
                for field in &gen.fields {
                    if span_contains(field.span, offset) {
                        return Some(HoverInfo {
                            content: config_field_hover(&field.name.value),
                            span: Some(field.span),
                        });
                    }
                }
                return Some(HoverInfo {
                    content: format!("**generator** `{}`", gen.name.value),
                    span: Some(gen.span),
                });
            }

            Declaration::Type(type_decl) => {
                for field in &type_decl.fields {
                    if span_contains(field.span, offset) {
                        let modifier = match field.modifier {
                            FieldModifier::Array => "[]",
                            FieldModifier::Optional => "?",
                            FieldModifier::NotNull => "!",
                            FieldModifier::None => "",
                        };
                        let type_str =
                            format!("{}{}", field_type_name(&field.field_type), modifier);
                        let attrs_str = format_field_attrs_short(&field.attributes);
                        let mut content = format!("**{}**: `{}`", field.name.value, type_str);
                        if !attrs_str.is_empty() {
                            content.push_str(&format!("  \n{}", attrs_str));
                        }
                        return Some(HoverInfo {
                            content,
                            span: Some(field.span),
                        });
                    }
                }
                return Some(HoverInfo {
                    content: composite_type_hover_content(type_decl),
                    span: Some(type_decl.span),
                });
            }
        }
    }

    None
}

/// Hover documentation for datasource/generator config fields.
pub fn config_field_hover(key: &str) -> String {
    match key {
        "provider" => concat!(
            "**provider**  \n",
            "Specifies the database provider or code-generator target.  \n\n",
            "Datasource values: `\"postgresql\"`, `\"mysql\"`, `\"sqlite\"`  \n",
            "Generator values: `\"nautilus-client-rs\"`, `\"nautilus-client-py\"`, `\"nautilus-client-js\"`, `\"nautilus-client-java\"`",
        ).to_string(),
        "url" => concat!(
            "**url**  \n",
            "Database connection URL.  \n\n",
            "Supports the `env(\"VAR\")` helper to read from environment variables.",
        ).to_string(),
        "direct_url" => concat!(
            "**direct_url**  \n",
            "Optional direct database connection URL for admin tooling.  \n\n",
            "Use this for migrations, introspection, and schema management when `url` points at a pooled or proxied connection.  \n\n",
            "Supports the `env(\"VAR\")` helper to read from environment variables.",
        ).to_string(),
        "output" => concat!(
            "**output**  \n",
            "Output directory path for generated client files.  \n\n",
            "Relative paths are resolved from the schema file location.",
        ).to_string(),
        "interface" => concat!(
            "**interface**  \n",
            "Controls whether the generated client uses a synchronous or asynchronous API.  \n\n",
            "- `\"sync\"` *(default)* — blocking API; safe to call from any context.  \n",
            "- `\"async\"` — `async/await` API; requires an async runtime.",
        ).to_string(),
        "recursive_type_depth" => concat!(
            "**recursive_type_depth**  \n",
            "*(Python client only)* Depth of recursive include TypedDicts generated for the Python client.  \n\n",
            "Default: `5`.  \n\n",
            "Each depth level adds a `{Model}IncludeRecursive{N}` type and the corresponding  \n",
            "`FindMany{Target}ArgsFrom{Source}Recursive{N}` typed-dict classes.  \n",
            "At the maximum depth the `include` field is omitted to prevent infinite type recursion.  \n\n",
            "Example: `recursive_type_depth = 3`",
        ).to_string(),
        "package" => concat!(
            "**package**  \n",
            "*(Java client only)* Root Java package for the generated client sources.  \n\n",
            "Example: `package = \"com.acme.db\"`",
        )
        .to_string(),
        "group_id" => concat!(
            "**group_id**  \n",
            "*(Java client only)* Maven `groupId` used in the generated `pom.xml`.  \n\n",
            "Example: `group_id = \"com.acme\"`",
        )
        .to_string(),
        "artifact_id" => concat!(
            "**artifact_id**  \n",
            "*(Java client only)* Maven `artifactId` used in the generated `pom.xml`.  \n\n",
            "Example: `artifact_id = \"db-client\"`",
        )
        .to_string(),
        "mode" => concat!(
            "**mode**  \n",
            "*(Java client only)* Controls the Java packaging output.  \n\n",
            "- `\"maven\"` *(default)* — generate the Maven module layout under `output/`.  \n",
            "- `\"jar\"` — generate the Maven module layout and also build a plain Java jar bundle under `output/dist/`.",
        )
        .to_string(),
        other => format!("**{other}**"),
    }
}

/// Returns hover info if `offset` falls on a `@attr` or `@@attr` token
/// (including its parenthesised argument list, if any).
fn attribute_hover_at(tokens: &[Token], offset: usize, ast: Option<&Schema>) -> Option<HoverInfo> {
    let n = tokens.len();
    let mut i = 0;
    while i < n {
        let tok = &tokens[i];
        let is_double = tok.kind == TokenKind::AtAt;
        let is_single = tok.kind == TokenKind::At;
        if !is_double && !is_single {
            i += 1;
            continue;
        }

        let ident_i = match (i + 1..n).find(|&j| !matches!(tokens[j].kind, TokenKind::Newline)) {
            Some(j) => j,
            None => {
                i += 1;
                continue;
            }
        };
        let ident_tok = &tokens[ident_i];
        let attr_name = match &ident_tok.kind {
            TokenKind::Ident(name) => name.clone(),
            _ => {
                i += 1;
                continue;
            }
        };

        let attr_start = tok.span.start;
        let attr_name_end = ident_tok.span.end;

        let lparen_i = (ident_i + 1..n).find(|&j| !matches!(tokens[j].kind, TokenKind::Newline));
        let full_end = if lparen_i.map(|j| &tokens[j].kind) == Some(&TokenKind::LParen) {
            find_paren_end(tokens, lparen_i.unwrap()).unwrap_or(attr_name_end)
        } else {
            attr_name_end
        };

        if offset >= attr_start && offset <= full_end {
            let content = if is_double {
                model_attr_hover_text(&attr_name)
            } else {
                field_attr_hover_text(&attr_name, ast, offset)
            };
            return Some(HoverInfo {
                content,
                span: Some(Span {
                    start: attr_start,
                    end: attr_name_end,
                }),
            });
        }
        i += 1;
    }
    None
}

/// Walk tokens from the `(` at `lparen_idx` and return the byte-end of the
/// matching `)`.
fn find_paren_end(tokens: &[Token], lparen_idx: usize) -> Option<usize> {
    let mut depth: i32 = 0;
    for tok in &tokens[lparen_idx..] {
        match tok.kind {
            TokenKind::LParen => depth += 1,
            TokenKind::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(tok.span.end);
                }
            }
            _ => {}
        }
    }
    None
}

fn field_attr_hover_text(name: &str, ast: Option<&Schema>, offset: usize) -> String {
    match name {
        "id" => "**@id**  \nMarks this field as the primary key of the model.".to_string(),
        "unique" => "**@unique**  \nAdds a `UNIQUE` constraint on this column.".to_string(),
        "default" => [
            "**@default(expr)**  ",
            "Sets the default value for this field when not explicitly provided.  \n",
            "Common expressions: `autoincrement()`, `now()`, `uuid()`,",
            " enum variants, or literal values.",
        ].concat(),
        "map" => "**@map(\"name\")** \nMaps this field to a different physical column name in the database.".to_string(),
        "store" => [
            "**@store(json)**  \n",
            "Stores this array field as a JSON value in the database.  \n",
            "Useful for databases without native array support (MySQL, SQLite).",
        ].concat(),
        "updatedAt" => [
            "**@updatedAt**  \n",
            "Marks this `DateTime` field to be automatically set to the current timestamp ",
            "on every CREATE and UPDATE operation.  \n",
            "The framework manages this value — it is excluded from all user-input types.",
        ].concat(),
        "computed" => [
            "**@computed(expr, Stored | Virtual)**  \n",
            "Declares a database-generated (computed) column.  \n\n",
            "- `expr` — raw SQL expression evaluated by the database (e.g. `price * quantity`, ",
            "`first_name || ' ' || last_name`)  \n",
            "- `Stored` — value is computed on write and persisted physically  \n",
            "- `Virtual` — value is computed on read (not supported on PostgreSQL)  \n\n",
            "Maps to SQL `GENERATED ALWAYS AS (expr) STORED` (PostgreSQL / MySQL) or ",
            "`AS (expr) STORED` (SQLite).  \n",
            "Computed fields are **read-only** — they are excluded from all create/update input types.",
        ].concat(),
        "check" => [
            "**@check(expr)**  \n",
            "Adds a SQL `CHECK` constraint on this column.  \n\n",
            "The boolean expression can use SQL-style operators: ",
            "`=`, `!=`, `<`, `>`, `<=`, `>=`, `AND`, `OR`, `NOT`, `IN`.  \n\n",
            "Field-level `@check` can only reference the decorated field itself.  \n",
            "Use `@@check` at the model level to reference multiple fields.  \n\n",
            "**Examples:**  \n",
            "```  \n",
            "age    Int  @check(age >= 0 AND age <= 150)  \n",
            "status Status @check(status IN [ACTIVE, PENDING])  \n",
            "```",
        ].concat(),
        "relation" => {
            let base = concat!(
                "**@relation**  \n",
                "Defines an explicit foreign-key relation between two models."
            );
            if let Some(schema) = ast {
                if let Some(extra) = relation_hover_details(schema, offset) {
                    return format!("{base}  \n\n{extra}");
                }
            }
            base.to_string()
        }
        other => format!("**@{other}**"),
    }
}

fn model_attr_hover_text(name: &str) -> String {
    match name {
        "map"    => "**@@map(\"name\")** \nMaps this model to a different physical table name in the database.".to_string(),
        "id"     => "**@@id([fields])**  \nDefines a composite primary key spanning multiple fields.".to_string(),
        "unique" => "**@@unique([fields])**  \nDefines a composite unique constraint spanning multiple fields.".to_string(),
        "index"  => "**@@index([fields], type?, name?, map?)**  \nCreates a database index on the listed fields.  \n\nOptional arguments:  \n- `type:` — index access method: `BTree` (default, all DBs), `Hash` (PG/MySQL), `Gin` / `Gist` / `Brin` (PostgreSQL only), `FullText` (MySQL only)  \n- `name:` — logical developer name (ignored in DDL)  \n- `map:` — physical DDL index name override  \n\n**Examples:**  \n```  \n@@index([email])  \n@@index([email], type: Hash)  \n@@index([content], type: Gin)  \n@@index([createdAt], type: Brin, map: \"idx_created\")  \n```".to_string(),
        "check"  => [
            "**@@check(expr)**  \n",
            "Adds a table-level SQL `CHECK` constraint.  \n\n",
            "Unlike field-level `@check`, the expression can reference any scalar field in the model.  \n\n",
            "**Example:**  \n",
            "```  \n",
            "@@check(start_date < end_date)  \n",
            "@@check(age > 18 OR status IN [MINOR])  \n",
            "```",
        ].concat(),
        other    => format!("**@@{other}**"),
    }
}

/// Extracts a rich Markdown summary of the `@relation(...)` attribute on the
/// field whose span contains `offset`.
///
/// Shows:
/// - Inferred relation type (one-to-many / one-to-one)
/// - `ParentModel -> TargetType` arrow
/// - All explicit arguments: name, fields, references, onDelete, onUpdate
fn relation_hover_details(ast: &Schema, offset: usize) -> Option<String> {
    for decl in &ast.declarations {
        if let Declaration::Model(model) = decl {
            for field in &model.fields {
                if !span_contains(field.span, offset) {
                    continue;
                }
                for attr in &field.attributes {
                    if let FieldAttribute::Relation {
                        name,
                        fields,
                        references,
                        on_delete,
                        on_update,
                        ..
                    } = attr
                    {
                        let target = field_type_name(&field.field_type);
                        let modifier_str = match field.modifier {
                            FieldModifier::Array => "[]",
                            FieldModifier::Optional => "?",
                            FieldModifier::NotNull => "!",
                            FieldModifier::None => "",
                        };
                        let relation_kind = match field.modifier {
                            FieldModifier::Array => "one-to-many",
                            _ if fields.is_some() => "one-to-many",
                            _ => "one-to-one",
                        };

                        let mut lines: Vec<String> = vec![format!(
                            "**Type:** `{relation_kind}`  ·  `{}` -> `{target}{modifier_str}`",
                            model.name.value
                        )];

                        let has_args = name.is_some()
                            || fields.is_some()
                            || references.is_some()
                            || on_delete.is_some()
                            || on_update.is_some();

                        if has_args {
                            lines.push(String::new());
                            if let Some(n) = name {
                                lines.push(format!("- **name**: `\"{n}\"` "));
                            }
                            if let Some(fs) = fields {
                                let names: Vec<&str> =
                                    fs.iter().map(|f| f.value.as_str()).collect();
                                lines.push(format!("- **fields**: `[{}]`", names.join(", ")));
                            }
                            if let Some(rs) = references {
                                let names: Vec<&str> =
                                    rs.iter().map(|r| r.value.as_str()).collect();
                                lines.push(format!("- **references**: `[{}]`", names.join(", ")));
                            }
                            if let Some(od) = on_delete {
                                lines.push(format!("- **onDelete**: `{od}`"));
                            }
                            if let Some(ou) = on_update {
                                lines.push(format!("- **onUpdate**: `{ou}`"));
                            }
                        }

                        return Some(lines.join("  \n"));
                    }
                }
            }
        }
    }
    None
}

/// Formats field-level attributes as an inline string, e.g.
/// `@id · @default(uuid()) · @map("user_id")`.
/// `@relation` is omitted — it has its own dedicated hover.
fn format_field_attrs_short(attrs: &[FieldAttribute]) -> String {
    attrs
        .iter()
        .filter_map(|attr| match attr {
            FieldAttribute::Id => Some("@id".to_string()),
            FieldAttribute::Unique => Some("@unique".to_string()),
            FieldAttribute::Default(expr, _) => {
                Some(format!("@default({})", crate::formatter::format_expr(expr)))
            }
            FieldAttribute::Map(name) => Some(format!("@map(\"{}\")", name)),
            FieldAttribute::Store { .. } => Some("@store(json)".to_string()),
            FieldAttribute::UpdatedAt { .. } => Some("@updatedAt".to_string()),
            FieldAttribute::Computed { expr, kind, .. } => {
                let kind_str = match kind {
                    ComputedKind::Stored => "Stored",
                    ComputedKind::Virtual => "Virtual",
                };
                Some(format!("@computed({}, {})", expr, kind_str))
            }
            FieldAttribute::Check { expr, .. } => Some(format!("@check({})", expr)),
            FieldAttribute::Relation { .. } => None,
        })
        .collect::<Vec<_>>()
        .join(" · ")
}

/// Builds the full Markdown hover content for a `model` declaration.
fn model_hover_content(
    model: &crate::ast::ModelDecl,
    composite_names: &std::collections::HashSet<String>,
) -> String {
    let table_name = model.table_name();
    let mut lines: Vec<String> = vec![format!("**model** `{}`", model.name.value)];

    if table_name != model.name.value {
        lines.push(format!("**Table:** `{}`", table_name));
    }

    let composite_count = model
        .fields
        .iter()
        .filter(|f| matches!(&f.field_type, FieldType::UserType(n) if composite_names.contains(n)))
        .count();
    let relation_count = model
        .fields
        .iter()
        .filter(|f| matches!(&f.field_type, FieldType::UserType(n) if !composite_names.contains(n)))
        .count();
    let scalar_count = model.fields.len() - composite_count - relation_count;
    let mut count_parts = vec![format!("{} scalar", scalar_count)];
    if relation_count > 0 {
        count_parts.push(format!("{} relation", relation_count));
    }
    if composite_count > 0 {
        count_parts.push(format!("{} composite", composite_count));
    }
    lines.push(format!("**Fields:** {}", count_parts.join(" · ")));
    lines.push(String::new());

    for field in &model.fields {
        let modifier = match field.modifier {
            FieldModifier::Array => "[]",
            FieldModifier::Optional => "?",
            FieldModifier::NotNull => "!",
            FieldModifier::None => "",
        };
        let type_str = format!("{}{}", field_type_name(&field.field_type), modifier);
        let attrs_str = format_field_attrs_short(&field.attributes);
        if attrs_str.is_empty() {
            lines.push(format!("- `{}`: `{}`", field.name.value, type_str));
        } else {
            lines.push(format!(
                "- `{}`: `{}`  — {}",
                field.name.value, type_str, attrs_str
            ));
        }
    }

    let extra_attrs: Vec<String> = model
        .attributes
        .iter()
        .filter_map(|attr| match attr {
            ModelAttribute::Map(_) => None,
            ModelAttribute::Id(fields) => {
                let fs: Vec<&str> = fields.iter().map(|f| f.value.as_str()).collect();
                Some(format!("_@@id([{}])_", fs.join(", ")))
            }
            ModelAttribute::Unique(fields) => {
                let fs: Vec<&str> = fields.iter().map(|f| f.value.as_str()).collect();
                Some(format!("_@@unique([{}])_", fs.join(", ")))
            }
            ModelAttribute::Index {
                fields,
                index_type,
                name,
                map,
            } => {
                let fs: Vec<&str> = fields.iter().map(|f| f.value.as_str()).collect();
                let mut parts = vec![format!("[{}]", fs.join(", "))];
                if let Some(t) = index_type {
                    parts.push(format!("type: {}", t.value));
                }
                if let Some(n) = name {
                    parts.push(format!("name: \"{}\"", n));
                }
                if let Some(m) = map {
                    parts.push(format!("map: \"{}\"", m));
                }
                Some(format!("_@@index({})_", parts.join(", ")))
            }
            ModelAttribute::Check { expr, .. } => Some(format!("_@@check({})_", expr)),
        })
        .collect();

    if !extra_attrs.is_empty() {
        lines.push(String::new());
        lines.extend(extra_attrs);
    }

    lines.join("  \n")
}

/// Builds the full Markdown hover content for a `type` declaration.
fn composite_type_hover_content(type_decl: &crate::ast::TypeDecl) -> String {
    let mut lines: Vec<String> = vec![format!("**type** `{}`", type_decl.name.value)];
    lines.push(format!("**Fields:** {}", type_decl.fields.len()));
    lines.push(String::new());

    for field in &type_decl.fields {
        let modifier = match field.modifier {
            FieldModifier::Array => "[]",
            FieldModifier::Optional => "?",
            FieldModifier::NotNull => "!",
            FieldModifier::None => "",
        };
        let type_str = format!("{}{}", field_type_name(&field.field_type), modifier);
        let attrs_str = format_field_attrs_short(&field.attributes);
        if attrs_str.is_empty() {
            lines.push(format!("- `{}`: `{}`", field.name.value, type_str));
        } else {
            lines.push(format!(
                "- `{}`: `{}`  — {}",
                field.name.value, type_str, attrs_str
            ));
        }
    }

    lines.join("  \n")
}

fn field_type_name(ft: &FieldType) -> String {
    match ft {
        FieldType::String => "String".to_string(),
        FieldType::Boolean => "Boolean".to_string(),
        FieldType::Int => "Int".to_string(),
        FieldType::BigInt => "BigInt".to_string(),
        FieldType::Float => "Float".to_string(),
        FieldType::Decimal { precision, scale } => format!("Decimal({}, {})", precision, scale),
        FieldType::DateTime => "DateTime".to_string(),
        FieldType::Bytes => "Bytes".to_string(),
        FieldType::Json => "Json".to_string(),
        FieldType::Uuid => "Uuid".to_string(),
        FieldType::Jsonb => "Jsonb".to_string(),
        FieldType::Xml => "Xml".to_string(),
        FieldType::Char { length } => format!("Char({})", length),
        FieldType::VarChar { length } => format!("VarChar({})", length),
        FieldType::UserType(name) => name.clone(),
    }
}

fn field_type_description(ft: &FieldType) -> &'static str {
    match ft {
        FieldType::String => "UTF-8 text string.  Maps to `VARCHAR` / `TEXT` in SQL.",
        FieldType::Boolean => "Boolean value (`true` / `false`).  Maps to `BOOLEAN`.",
        FieldType::Int => "32-bit signed integer.  Maps to `INTEGER`.",
        FieldType::BigInt => "64-bit signed integer.  Maps to `BIGINT`.",
        FieldType::Float => "64-bit IEEE 754 float.  Maps to `DOUBLE PRECISION`.",
        FieldType::Decimal { .. } => "Exact-precision decimal number.  Maps to `NUMERIC(p, s)`.",
        FieldType::DateTime => "Date and time with timezone.  Maps to `TIMESTAMPTZ`.",
        FieldType::Bytes => "Raw binary data.  Maps to `BYTEA` / `BLOB`.",
        FieldType::Json => "JSON document.  Maps to `JSONB` (Postgres) or `JSON` (MySQL/SQLite).",
        FieldType::Uuid => "Universally unique identifier.  Maps to `UUID`.",
        FieldType::Jsonb => "JSONB document (PostgreSQL only).  Maps to `JSONB`.",
        FieldType::Xml => "XML document (PostgreSQL only).  Maps to `XML`.",
        FieldType::Char { .. } => {
            "Fixed-length character column.  Maps to `CHAR(n)` (PostgreSQL and MySQL)."
        }
        FieldType::VarChar { .. } => {
            "Variable-length character column.  Maps to `VARCHAR(n)` (PostgreSQL and MySQL)."
        }
        FieldType::UserType(_) => "Reference to another model or enum.",
    }
}

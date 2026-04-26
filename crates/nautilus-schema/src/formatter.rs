//! Formatter: converts a `Schema` AST back to canonical `.nautilus` source text.
//!
//! - 2-space indentation
//! - Blank lines between top-level blocks
//! - Column spacing of 1 (padding = max_width - len + 1)
//! - Field name column always padded (even for fields with no attributes)
//! - Field type column padded only for fields that have attributes
//! - Single space between attributes on the same field
//! - Key-value pairs in datasource/generator blocks are aligned
//! - Blank lines between field groups are preserved (detected via spans)
//! - Blank line inserted before model-level `@@` attributes
//! - Comments are preserved (both inline trailing comments and standalone comment lines)

use crate::ast::{
    ComputedKind, Declaration, Expr, FieldAttribute, FieldModifier, FieldType, Literal,
    ModelAttribute, ReferentialAction, Schema, StorageStrategy, TypeDecl,
};

/// Format a [`Schema`] AST back to canonical `.nautilus` source text.
///
/// `source` is the original schema text that was parsed into `schema`; it is
/// used to detect blank lines between consecutive fields, to preserve inline
/// trailing comments (`// …`) and standalone comment lines between items.
///
/// Running `format_schema` twice on the same input produces identical output
/// (the operation is idempotent).
pub fn format_schema(schema: &Schema, source: &str) -> String {
    let mut parts: Vec<(String, usize)> = Vec::new();

    for decl in &schema.declarations {
        let block = match decl {
            Declaration::Datasource(ds) => {
                let max_key = ds
                    .fields
                    .iter()
                    .map(|f| f.name.value.len())
                    .max()
                    .unwrap_or(0);
                let mut lines = vec![format!("datasource {} {{", ds.name.value)];
                for (idx, field) in ds.fields.iter().enumerate() {
                    // Preserve inter-field comments/blank-lines inside datasource blocks.
                    if idx > 0 {
                        let prev = &ds.fields[idx - 1];
                        push_gap_content(source, prev.span.end, field.span.start, &mut lines, "  ");
                    }
                    let padding = max_key - field.name.value.len() + 1;
                    let mut line = format!(
                        "  {}{}= {}",
                        field.name.value,
                        " ".repeat(padding),
                        format_expr(&field.value)
                    );
                    if let Some(c) = trailing_inline_comment(source, field.span.end) {
                        line.push_str("  ");
                        line.push_str(&c);
                    }
                    lines.push(line);
                }
                lines.push("}".to_string());
                lines.join("\n")
            }

            Declaration::Generator(gen) => {
                let max_key = gen
                    .fields
                    .iter()
                    .map(|f| f.name.value.len())
                    .max()
                    .unwrap_or(0);
                let mut lines = vec![format!("generator {} {{", gen.name.value)];
                for (idx, field) in gen.fields.iter().enumerate() {
                    if idx > 0 {
                        let prev = &gen.fields[idx - 1];
                        push_gap_content(source, prev.span.end, field.span.start, &mut lines, "  ");
                    }
                    let padding = max_key - field.name.value.len() + 1;
                    let mut line = format!(
                        "  {}{}= {}",
                        field.name.value,
                        " ".repeat(padding),
                        format_expr(&field.value)
                    );
                    if let Some(c) = trailing_inline_comment(source, field.span.end) {
                        line.push_str("  ");
                        line.push_str(&c);
                    }
                    lines.push(line);
                }
                lines.push("}".to_string());
                lines.join("\n")
            }

            Declaration::Enum(e) => {
                let mut lines = vec![format!("enum {} {{", e.name.value)];
                for (idx, variant) in e.variants.iter().enumerate() {
                    if idx > 0 {
                        let prev = &e.variants[idx - 1];
                        push_gap_content(
                            source,
                            prev.span.end,
                            variant.span.start,
                            &mut lines,
                            "  ",
                        );
                    }
                    let mut line = format!("  {}", variant.name.value);
                    if let Some(c) = trailing_inline_comment(source, variant.span.end) {
                        line.push_str("  ");
                        line.push_str(&c);
                    }
                    lines.push(line);
                }
                lines.push("}".to_string());
                lines.join("\n")
            }

            Declaration::Model(model) => {
                let max_name = model
                    .fields
                    .iter()
                    .map(|f| f.name.value.len())
                    .max()
                    .unwrap_or(0);
                let max_type = model
                    .fields
                    .iter()
                    .map(|f| format_field_type_with_modifier(&f.field_type, f.modifier).len())
                    .max()
                    .unwrap_or(0);

                let mut lines = vec![format!("model {} {{", model.name.value)];

                for (idx, field) in model.fields.iter().enumerate() {
                    // Preserve blank lines and comment lines between consecutive fields.
                    if idx > 0 {
                        let prev = &model.fields[idx - 1];
                        push_gap_content(source, prev.span.end, field.span.start, &mut lines, "  ");
                    }

                    let type_str =
                        format_field_type_with_modifier(&field.field_type, field.modifier);
                    let attrs: Vec<String> =
                        field.attributes.iter().map(format_field_attr).collect();

                    let name_padding = max_name - field.name.value.len() + 1;

                    let mut line = if attrs.is_empty() {
                        format!(
                            "  {}{}{}",
                            field.name.value,
                            " ".repeat(name_padding),
                            type_str,
                        )
                    } else {
                        let type_padding = max_type - type_str.len() + 1;
                        format!(
                            "  {}{}{}{}{}",
                            field.name.value,
                            " ".repeat(name_padding),
                            type_str,
                            " ".repeat(type_padding),
                            attrs.join(" "),
                        )
                    };

                    line = line.trim_end().to_string();

                    if let Some(c) = trailing_inline_comment(source, field.span.end) {
                        line.push_str("  ");
                        line.push_str(&c);
                    }

                    lines.push(line);
                }

                if !model.attributes.is_empty() && !model.fields.is_empty() {
                    lines.push(String::new());
                }
                for attr in &model.attributes {
                    lines.push(format!("  {}", format_model_attr(attr)));
                }

                lines.push("}".to_string());
                lines.join("\n")
            }

            Declaration::Type(type_decl) => format_type_decl(type_decl, source),
        };

        parts.push((block, decl.span().end));
    }

    let mut out = String::new();

    if let Some((_, _)) = parts.first() {
        if let Some(first_decl) = schema.declarations.first() {
            let leading = top_level_comments(source, 0, first_decl.span().start);
            for line in &leading {
                out.push_str(line);
                out.push('\n');
            }
            if !leading.is_empty() {
                out.push('\n');
            }
        }
    }

    for (i, (block, span_end)) in parts.iter().enumerate() {
        if i > 0 {
            let prev_end = parts[i - 1].1;
            let curr_start = schema.declarations[i].span().start;
            let gap_comments = top_level_comments(source, prev_end, curr_start);
            if gap_comments.is_empty() {
                out.push_str("\n\n");
            } else {
                out.push_str("\n\n");
                for comment in &gap_comments {
                    out.push_str(comment);
                    out.push('\n');
                }
                out.push('\n');
            }
        }
        out.push_str(block);
        let _ = span_end;
    }

    if let Some(last_decl) = schema.declarations.last() {
        let trailing = top_level_comments(source, last_decl.span().end, source.len());
        for comment in &trailing {
            out.push('\n');
            out.push_str(comment);
        }
    }

    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// Extract the trailing inline `//` comment from `source` starting at byte `pos`,
/// looking only at the rest of the current line.  Returns `None` if no comment
/// is present on that line.
fn trailing_inline_comment(source: &str, pos: usize) -> Option<String> {
    let rest = source.get(pos..)?;
    let line_end = rest.find('\n').unwrap_or(rest.len());
    let rest_of_line = rest[..line_end].trim();
    if rest_of_line.starts_with("//") {
        Some(rest_of_line.to_string())
    } else {
        None
    }
}

/// Push gap content (blank lines and indented comment lines) that appears
/// between two adjacent items in a block.  `indent` is prepended to each
/// comment line (e.g. `"  "` for 2-space indent).
///
/// The function looks at `source[prev_end..curr_start]`, skips the first
/// partial line (which may already be covered by a trailing inline comment),
/// then emits blank lines and `//` comment lines as needed.
fn push_gap_content(
    source: &str,
    prev_end: usize,
    curr_start: usize,
    lines: &mut Vec<String>,
    indent: &str,
) {
    if prev_end >= curr_start {
        return;
    }
    let gap = match source.get(prev_end..curr_start) {
        Some(s) => s,
        None => return,
    };

    let after_first_nl = match gap.find('\n') {
        Some(pos) => &gap[pos + 1..],
        None => return,
    };

    let mut comment_lines: Vec<String> = Vec::new();
    let mut blank_before_first = false;

    for raw_line in after_first_nl.lines() {
        let trimmed = raw_line.trim();
        if trimmed.starts_with("//") {
            comment_lines.push(trimmed.to_string());
        } else if trimmed.is_empty() && comment_lines.is_empty() {
            blank_before_first = true;
        }
    }

    if !comment_lines.is_empty() {
        if blank_before_first {
            lines.push(String::new());
        }
        for c in &comment_lines {
            lines.push(format!("{}{}", indent, c));
        }
    } else {
        let nl_count = after_first_nl.chars().filter(|&c| c == '\n').count();
        if nl_count >= 1 {
            lines.push(String::new());
        }
    }
}

/// Extract top-level comment lines (and blank-line structure around them) from
/// a source range, for use between top-level declarations.
///
/// Returns a `Vec` of strings where each entry is either a `//` comment line
/// (without leading indentation) or an empty string representing a blank line.
/// Leading and trailing blank-only entries are stripped.
fn top_level_comments(source: &str, from: usize, to: usize) -> Vec<String> {
    let slice = match source.get(from..to) {
        Some(s) => s,
        None => return Vec::new(),
    };

    let mut result: Vec<String> = Vec::new();
    for raw_line in slice.lines() {
        let trimmed = raw_line.trim();
        if trimmed.starts_with("//") {
            result.push(trimmed.to_string());
        } else if trimmed.is_empty() && !result.is_empty() {
            result.push(String::new());
        }
    }

    while result
        .last()
        .map(|s: &String| s.is_empty())
        .unwrap_or(false)
    {
        result.pop();
    }

    result
}

/// Format a composite type declaration block.
fn format_type_decl(type_decl: &TypeDecl, source: &str) -> String {
    let max_name = type_decl
        .fields
        .iter()
        .map(|f| f.name.value.len())
        .max()
        .unwrap_or(0);
    let max_type = type_decl
        .fields
        .iter()
        .map(|f| format_field_type_with_modifier(&f.field_type, f.modifier).len())
        .max()
        .unwrap_or(0);

    let mut lines = vec![format!("type {} {{", type_decl.name.value)];

    for (idx, field) in type_decl.fields.iter().enumerate() {
        if idx > 0 {
            let prev = &type_decl.fields[idx - 1];
            push_gap_content(source, prev.span.end, field.span.start, &mut lines, "  ");
        }

        let type_str = format_field_type_with_modifier(&field.field_type, field.modifier);
        let attrs: Vec<String> = field.attributes.iter().map(format_field_attr).collect();

        let name_padding = max_name - field.name.value.len() + 1;

        let mut line = if attrs.is_empty() {
            format!(
                "  {}{}{}",
                field.name.value,
                " ".repeat(name_padding),
                type_str,
            )
        } else {
            let type_padding = max_type - type_str.len() + 1;
            format!(
                "  {}{}{}{}{}",
                field.name.value,
                " ".repeat(name_padding),
                type_str,
                " ".repeat(type_padding),
                attrs.join(" "),
            )
        };

        line = line.trim_end().to_string();

        if let Some(c) = trailing_inline_comment(source, field.span.end) {
            line.push_str("  ");
            line.push_str(&c);
        }

        lines.push(line);
    }

    lines.push("}".to_string());
    lines.join("\n")
}

fn format_field_type_with_modifier(ft: &FieldType, modifier: FieldModifier) -> String {
    let base = ft.to_string();
    match modifier {
        FieldModifier::None => base,
        FieldModifier::Optional => format!("{}?", base),
        FieldModifier::NotNull => format!("{}!", base),
        FieldModifier::Array => format!("{}[]", base),
    }
}

/// Format a field-level attribute (`@id`, `@default(…)`, …).
fn format_field_attr(attr: &FieldAttribute) -> String {
    match attr {
        FieldAttribute::Id => "@id".to_string(),
        FieldAttribute::Unique => "@unique".to_string(),

        FieldAttribute::Default(expr, _) => format!("@default({})", format_expr(expr)),

        FieldAttribute::Map(name) => format!("@map(\"{}\")", name),

        FieldAttribute::Store { strategy, .. } => match strategy {
            StorageStrategy::Json => "@store(json)".to_string(),
            StorageStrategy::Native => "@store(native)".to_string(),
        },

        FieldAttribute::Relation {
            name,
            fields,
            references,
            on_delete,
            on_update,
            ..
        } => {
            let mut args: Vec<String> = Vec::new();

            if let Some(n) = name {
                args.push(format!("name: \"{}\"", n));
            }
            if let Some(flds) = fields {
                let names: Vec<_> = flds.iter().map(|i| i.value.clone()).collect();
                args.push(format!("fields: [{}]", names.join(", ")));
            }
            if let Some(refs) = references {
                let names: Vec<_> = refs.iter().map(|i| i.value.clone()).collect();
                args.push(format!("references: [{}]", names.join(", ")));
            }
            if let Some(action) = on_delete {
                args.push(format!("onDelete: {}", format_referential_action(action)));
            }
            if let Some(action) = on_update {
                args.push(format!("onUpdate: {}", format_referential_action(action)));
            }

            format!("@relation({})", args.join(", "))
        }

        FieldAttribute::UpdatedAt { .. } => "@updatedAt".to_string(),

        FieldAttribute::Computed { expr, kind, .. } => {
            let kind_str = match kind {
                ComputedKind::Stored => "Stored",
                ComputedKind::Virtual => "Virtual",
            };
            format!("@computed({}, {})", expr, kind_str)
        }

        FieldAttribute::Check { expr, .. } => format!("@check({})", expr),
    }
}

/// Format a model-level attribute (`@@map`, `@@id`, `@@unique`, `@@index`).
fn format_model_attr(attr: &ModelAttribute) -> String {
    match attr {
        ModelAttribute::Map(name) => format!("@@map(\"{}\")", name),
        ModelAttribute::Id(fields) => {
            let names: Vec<_> = fields.iter().map(|i| i.value.clone()).collect();
            format!("@@id([{}])", names.join(", "))
        }
        ModelAttribute::Unique(fields) => {
            let names: Vec<_> = fields.iter().map(|i| i.value.clone()).collect();
            format!("@@unique([{}])", names.join(", "))
        }
        ModelAttribute::Index {
            fields,
            index_type,
            opclass,
            m,
            ef_construction,
            lists,
            name,
            map,
        } => {
            let names: Vec<_> = fields.iter().map(|i| i.value.clone()).collect();
            let mut s = format!("@@index([{}])", names.join(", "));
            if index_type.is_some()
                || opclass.is_some()
                || m.is_some()
                || ef_construction.is_some()
                || lists.is_some()
                || name.is_some()
                || map.is_some()
            {
                s.pop();
                if let Some(t) = index_type {
                    s.push_str(&format!(", type: {}", t.value));
                }
                if let Some(opclass) = opclass {
                    s.push_str(&format!(", opclass: {}", opclass.value));
                }
                if let Some(m) = m {
                    s.push_str(&format!(", m: {}", m));
                }
                if let Some(ef_construction) = ef_construction {
                    s.push_str(&format!(", ef_construction: {}", ef_construction));
                }
                if let Some(lists) = lists {
                    s.push_str(&format!(", lists: {}", lists));
                }
                if let Some(n) = name {
                    s.push_str(&format!(", name: \"{}\"", n));
                }
                if let Some(m) = map {
                    s.push_str(&format!(", map: \"{}\"", m));
                }
                s.push(')');
            }
            s
        }

        ModelAttribute::Check { expr, .. } => format!("@@check({})", expr),
    }
}

/// Format a [`ReferentialAction`] to the identifier form used in the schema language.
fn format_referential_action(action: &ReferentialAction) -> &'static str {
    match action {
        ReferentialAction::Cascade => "Cascade",
        ReferentialAction::Restrict => "Restrict",
        ReferentialAction::NoAction => "NoAction",
        ReferentialAction::SetNull => "SetNull",
        ReferentialAction::SetDefault => "SetDefault",
    }
}

/// Format an [`Expr`] node to its source representation.
pub(crate) fn format_expr(expr: &Expr) -> String {
    match expr {
        Expr::Literal(lit) => format_literal(lit),

        Expr::FunctionCall { name, args, .. } => {
            if args.is_empty() {
                format!("{}()", name.value)
            } else {
                let formatted: Vec<_> = args.iter().map(format_expr).collect();
                format!("{}({})", name.value, formatted.join(", "))
            }
        }

        Expr::Array { elements, .. } => {
            let formatted: Vec<_> = elements.iter().map(format_expr).collect();
            format!("[{}]", formatted.join(", "))
        }

        Expr::NamedArg { name, value, .. } => {
            format!("{}: {}", name.value, format_expr(value))
        }

        Expr::Ident(ident) => ident.value.clone(),
    }
}

/// Format a [`Literal`] to its source representation.
fn format_literal(lit: &Literal) -> String {
    match lit {
        Literal::String(s, _) => format!("\"{}\"", s),
        Literal::Number(n, _) => n.clone(),
        Literal::Boolean(b, _) => b.to_string(),
    }
}

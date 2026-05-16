use nautilus_schema::ir::{ComputedKind, DefaultValue, FieldIr, ModelIr};

fn format_default_value(default: &DefaultValue) -> String {
    match default {
        DefaultValue::String(value) => format!("\"{}\"", value),
        DefaultValue::Number(value) => value.clone(),
        DefaultValue::Boolean(value) => value.to_string(),
        DefaultValue::EnumVariant(value) => value.clone(),
        DefaultValue::Function(call) if call.args.is_empty() => format!("{}()", call.name),
        DefaultValue::Function(call) => format!("{}({})", call.name, call.args.join(", ")),
    }
}

fn format_computed_kind(kind: ComputedKind) -> &'static str {
    match kind {
        ComputedKind::Virtual => "virtual",
        ComputedKind::Stored => "stored",
    }
}

/// Return a compact, provider-agnostic documentation string for field
/// modifiers preserved in the validated IR.
pub(crate) fn field_modifier_doc(model: &ModelIr, field: &FieldIr) -> String {
    let mut parts = Vec::new();
    let pk_fields = model.primary_key.fields();

    if pk_fields.contains(&field.logical_name.as_str()) {
        parts.push("@id".to_string());
    }
    if field.is_unique {
        parts.push("@unique".to_string());
    }
    if let Some(default) = &field.default_value {
        parts.push(format!("@default({})", format_default_value(default)));
    }
    if field.logical_name != field.db_name {
        parts.push(format!("@map(\"{}\")", field.db_name));
    }
    if field.is_updated_at {
        parts.push("@updatedAt".to_string());
    }
    if let Some((expr, kind)) = &field.computed {
        parts.push(format!(
            "@computed({}, kind: {})",
            expr,
            format_computed_kind(*kind)
        ));
    }
    if let Some(expr) = &field.check {
        parts.push(format!("@check({})", expr));
    }

    parts.join(" ")
}

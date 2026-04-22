//! JavaScript/TypeScript code generator for Nautilus models, delegates, and input types.

use heck::{ToLowerCamelCase, ToSnakeCase};
use nautilus_schema::ir::{
    CompositeTypeIr, EnumIr, ModelIr, ResolvedFieldType, ScalarType, SchemaIr,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tera::{Context, Tera};

use crate::js::type_mapper::{
    field_to_ts_type, get_base_ts_type, get_filter_operators_for_field, get_ts_default_value,
    is_auto_generated, scalar_to_ts_type,
};

/// JS/TS template registry — loaded once at first use.
pub static JS_TEMPLATES: std::sync::LazyLock<Tera> = std::sync::LazyLock::new(|| {
    let mut tera = Tera::default();
    tera.add_raw_templates(vec![
        (
            "model.js.tera",
            include_str!("../../templates/js/model.js.tera"),
        ),
        (
            "model.d.ts.tera",
            include_str!("../../templates/js/model.d.ts.tera"),
        ),
        (
            "enums.js.tera",
            include_str!("../../templates/js/enums.js.tera"),
        ),
        (
            "enums.d.ts.tera",
            include_str!("../../templates/js/enums.d.ts.tera"),
        ),
        (
            "client.js.tera",
            include_str!("../../templates/js/client.js.tera"),
        ),
        (
            "client.d.ts.tera",
            include_str!("../../templates/js/client.d.ts.tera"),
        ),
        (
            "models_index.js.tera",
            include_str!("../../templates/js/models_index.js.tera"),
        ),
        (
            "models_index.d.ts.tera",
            include_str!("../../templates/js/models_index.d.ts.tera"),
        ),
        (
            "composite_types.d.ts.tera",
            include_str!("../../templates/js/composite_types.d.ts.tera"),
        ),
    ])
    .expect("embedded JS templates must parse");
    tera
});

fn render(template: &str, ctx: &Context) -> String {
    crate::template::render(&JS_TEMPLATES, template, ctx)
}

#[derive(Debug, Clone, Serialize)]
struct JsFieldContext {
    /// Logical JS field name (camelCase, same as schema logical name).
    name: String,
    /// Logical name from the schema IR (may differ from `name` after `@map`).
    logical_name: String,
    /// Database column name.
    db_name: String,
    /// Full TypeScript type, e.g. `string | null`, `number[]`.
    ts_type: String,
    /// Inner base type without wrappers, e.g. `string`, `number`, `Date`.
    base_type: String,
    is_optional: bool,
    is_array: bool,
    is_enum: bool,
    has_default: bool,
    default: String,
    is_pk: bool,
    index: usize,
}

#[derive(Debug, Clone, Serialize)]
struct JsFilterOperatorContext {
    suffix: String,
    ts_type: String,
}

#[derive(Debug, Clone, Serialize)]
struct JsWhereInputFieldContext {
    name: String,
    /// Base TS type used by the template to pick the right filter interface.
    base_type: String,
    ts_type: String,
    operators: Vec<JsFilterOperatorContext>,
}

#[derive(Debug, Clone, Serialize)]
struct JsCreateInputFieldContext {
    name: String,
    ts_type: String,
    is_required: bool,
}

#[derive(Debug, Clone, Serialize)]
struct JsUpdateInputFieldContext {
    name: String,
    ts_type: String,
}

#[derive(Debug, Clone, Serialize)]
struct JsOrderByFieldContext {
    name: String,
}

#[derive(Debug, Clone, Serialize)]
struct JsIncludeFieldContext {
    name: String,
    target_model: String,
    target_snake: String,
    /// camelCase — property name on the generated Nautilus class.
    target_camel: String,
    is_array: bool,
}

#[derive(Debug, Clone, Serialize)]
struct JsAggregateFieldContext {
    name: String,
    ts_type: String,
}

/// Generate JavaScript + declaration code for a single model.
///
/// Returns `((js_filename, js_code), (dts_filename, dts_code))`.
pub fn generate_js_model(model: &ModelIr, ir: &SchemaIr) -> ((String, String), (String, String)) {
    let mut context = Context::new();

    context.insert("model_name", &model.logical_name);
    context.insert("snake_name", &model.logical_name.to_snake_case());
    context.insert("table_name", &model.db_name);
    context.insert("delegate_name", &format!("{}Delegate", model.logical_name));

    let pk_field_names = model.primary_key.fields();
    context.insert("primary_key_fields", &pk_field_names);

    let mut enum_imports: HashSet<String> = HashSet::new();
    let mut composite_type_imports: HashSet<String> = HashSet::new();

    let mut scalar_fields: Vec<JsFieldContext> = Vec::new();
    let mut where_input_fields: Vec<JsWhereInputFieldContext> = Vec::new();
    let mut create_input_fields: Vec<JsCreateInputFieldContext> = Vec::new();
    let mut update_input_fields: Vec<JsUpdateInputFieldContext> = Vec::new();
    let mut order_by_fields: Vec<JsOrderByFieldContext> = Vec::new();
    let mut numeric_fields: Vec<JsAggregateFieldContext> = Vec::new();
    let mut orderable_fields: Vec<JsAggregateFieldContext> = Vec::new();
    let mut object_value_db_fields: Vec<String> = Vec::new();

    for (idx, field) in model.scalar_fields().enumerate() {
        match &field.field_type {
            ResolvedFieldType::Enum { enum_name } => {
                if ir.enums.contains_key(enum_name) {
                    enum_imports.insert(enum_name.clone());
                }
            }
            ResolvedFieldType::CompositeType { type_name } => {
                if ir.composite_types.contains_key(type_name) {
                    composite_type_imports.insert(type_name.clone());
                }
            }
            _ => {}
        }

        let ts_type = field_to_ts_type(field, &ir.enums);
        let base_type = get_base_ts_type(field, &ir.enums);
        let is_enum = matches!(field.field_type, ResolvedFieldType::Enum { .. });
        let auto_generated = is_auto_generated(field);
        let default_val = get_ts_default_value(field);
        let is_pk = pk_field_names.contains(&field.logical_name.as_str());

        scalar_fields.push(JsFieldContext {
            name: field.logical_name.clone(),
            logical_name: field.logical_name.clone(),
            db_name: field.db_name.clone(),
            ts_type: ts_type.clone(),
            base_type: base_type.clone(),
            is_optional: !field.is_required,
            is_array: field.is_array,
            is_enum,
            has_default: default_val.is_some(),
            default: default_val.unwrap_or_default(),
            is_pk,
            index: idx,
        });

        if !matches!(field.field_type, ResolvedFieldType::Relation(_)) {
            let operators = get_filter_operators_for_field(field, &ir.enums);
            where_input_fields.push(JsWhereInputFieldContext {
                name: field.logical_name.clone(),
                base_type: base_type.clone(),
                ts_type: ts_type.clone(),
                operators: operators
                    .into_iter()
                    .map(|op| JsFilterOperatorContext {
                        suffix: op.suffix,
                        ts_type: op.type_name,
                    })
                    .collect(),
            });
        }

        if matches!(
            field.field_type,
            ResolvedFieldType::Scalar(ScalarType::Json)
                | ResolvedFieldType::Scalar(ScalarType::Jsonb)
                | ResolvedFieldType::Scalar(ScalarType::Hstore)
        ) && !field.is_array
        {
            object_value_db_fields.push(field.db_name.clone());
        }

        if !auto_generated {
            let input_base = base_type.clone();
            let typed = if field.is_array {
                format!("{}[]", input_base)
            } else {
                input_base
            };
            create_input_fields.push(JsCreateInputFieldContext {
                name: field.logical_name.clone(),
                ts_type: typed,
                is_required: field.is_required
                    && field.default_value.is_none()
                    && !field.is_updated_at,
            });
        }

        let is_auto_pk = auto_generated
            && pk_field_names.contains(&field.logical_name.as_str())
            && matches!(
                field.field_type,
                ResolvedFieldType::Scalar(ScalarType::Int)
                    | ResolvedFieldType::Scalar(ScalarType::BigInt)
            );
        if !is_auto_pk {
            let input_base = base_type.clone();
            let typed = if field.is_array {
                format!("{}[]", input_base)
            } else {
                format!("{} | null", input_base)
            };
            update_input_fields.push(JsUpdateInputFieldContext {
                name: field.logical_name.clone(),
                ts_type: typed,
            });
        }

        let is_numeric = matches!(
            &field.field_type,
            ResolvedFieldType::Scalar(ScalarType::Int)
                | ResolvedFieldType::Scalar(ScalarType::BigInt)
                | ResolvedFieldType::Scalar(ScalarType::Float)
                | ResolvedFieldType::Scalar(ScalarType::Decimal { .. })
        );
        if is_numeric {
            let agg_type = if let ResolvedFieldType::Scalar(s) = &field.field_type {
                scalar_to_ts_type(s).to_string()
            } else {
                unreachable!()
            };
            numeric_fields.push(JsAggregateFieldContext {
                name: field.logical_name.clone(),
                ts_type: agg_type,
            });
        }

        let is_non_orderable = matches!(
            &field.field_type,
            ResolvedFieldType::Scalar(ScalarType::Boolean)
                | ResolvedFieldType::Scalar(ScalarType::Json)
                | ResolvedFieldType::Scalar(ScalarType::Jsonb)
                | ResolvedFieldType::Scalar(ScalarType::Hstore)
                | ResolvedFieldType::Scalar(ScalarType::Bytes)
        );
        if !is_non_orderable {
            order_by_fields.push(JsOrderByFieldContext {
                name: field.logical_name.clone(),
            });
            orderable_fields.push(JsAggregateFieldContext {
                name: field.logical_name.clone(),
                ts_type: base_type,
            });
        }
    }

    let relation_fields: Vec<JsFieldContext> = model
        .relation_fields()
        .enumerate()
        .map(|(idx, field)| {
            let ts_type = if let ResolvedFieldType::Relation(rel) = &field.field_type {
                if field.is_array {
                    format!("{}Model[]", rel.target_model)
                } else {
                    format!("{}Model | null", rel.target_model)
                }
            } else {
                "unknown".to_string()
            };
            let base_type = if let ResolvedFieldType::Relation(rel) = &field.field_type {
                format!("{}Model", rel.target_model)
            } else {
                "unknown".to_string()
            };

            JsFieldContext {
                name: field.logical_name.clone(),
                logical_name: field.logical_name.clone(),
                db_name: field.db_name.clone(),
                ts_type,
                base_type,
                is_optional: true,
                is_array: field.is_array,
                is_enum: false,
                has_default: true,
                default: if field.is_array {
                    "[]".to_string()
                } else {
                    "null".to_string()
                },
                is_pk: false,
                index: idx,
            }
        })
        .collect();

    let include_fields: Vec<JsIncludeFieldContext> = model
        .relation_fields()
        .filter_map(|field| {
            if let ResolvedFieldType::Relation(rel) = &field.field_type {
                Some(JsIncludeFieldContext {
                    name: field.logical_name.clone(),
                    target_model: rel.target_model.clone(),
                    target_snake: rel.target_model.to_snake_case(),
                    target_camel: rel.target_model.to_lower_camel_case(),
                    is_array: field.is_array,
                })
            } else {
                None
            }
        })
        .collect();

    let has_numeric_fields = !numeric_fields.is_empty();
    let has_includes = !include_fields.is_empty();
    let has_enums = !enum_imports.is_empty();

    context.insert("scalar_fields", &scalar_fields);
    context.insert("relation_fields", &relation_fields);
    context.insert("where_input_fields", &where_input_fields);
    context.insert("create_input_fields", &create_input_fields);
    context.insert("update_input_fields", &update_input_fields);
    context.insert("order_by_fields", &order_by_fields);
    context.insert("include_fields", &include_fields);
    context.insert("has_includes", &has_includes);
    context.insert("numeric_fields", &numeric_fields);
    context.insert("orderable_fields", &orderable_fields);
    context.insert("object_value_db_fields", &object_value_db_fields);
    context.insert("has_numeric_fields", &has_numeric_fields);
    context.insert("has_enums", &has_enums);
    context.insert(
        "enum_imports",
        &enum_imports.into_iter().collect::<Vec<_>>(),
    );
    context.insert("has_composite_types", &!composite_type_imports.is_empty());
    context.insert(
        "composite_type_imports",
        &composite_type_imports.into_iter().collect::<Vec<_>>(),
    );

    let snake = model.logical_name.to_snake_case();
    let js_code = render("model.js.tera", &context);
    let dts_code = render("model.d.ts.tera", &context);

    (
        (format!("{}.js", snake), js_code),
        (format!("{}.d.ts", snake), dts_code),
    )
}

/// Generate JavaScript + declaration code for all models in the schema.
///
/// Returns `(js_models, dts_models)`, each sorted by filename.
#[allow(clippy::type_complexity)]
pub fn generate_all_js_models(ir: &SchemaIr) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let pairs: Vec<((String, String), (String, String))> = ir
        .models
        .values()
        .map(|model| generate_js_model(model, ir))
        .collect();

    let mut js_models: Vec<(String, String)> = pairs.iter().map(|(js, _)| js.clone()).collect();
    let mut dts_models: Vec<(String, String)> = pairs.iter().map(|(_, dts)| dts.clone()).collect();

    js_models.sort_by(|a, b| a.0.cmp(&b.0));
    dts_models.sort_by(|a, b| a.0.cmp(&b.0));

    (js_models, dts_models)
}

/// Generate `types.d.ts` — TypeScript interfaces for all composite types.
///
/// Returns `None` when there are no composite types.
pub fn generate_js_composite_types(
    composite_types: &HashMap<String, CompositeTypeIr>,
) -> Option<String> {
    if composite_types.is_empty() {
        return None;
    }

    #[derive(Serialize)]
    struct CompositeFieldCtx {
        name: String,
        ts_type: String,
    }

    #[derive(Serialize)]
    struct CompositeTypeCtx {
        name: String,
        fields: Vec<CompositeFieldCtx>,
    }

    let mut type_list: Vec<CompositeTypeCtx> = composite_types
        .values()
        .map(|ct| {
            let fields = ct
                .fields
                .iter()
                .map(|f| {
                    let base = match &f.field_type {
                        ResolvedFieldType::Scalar(s) => scalar_to_ts_type(s).to_string(),
                        ResolvedFieldType::Enum { enum_name } => enum_name.clone(),
                        ResolvedFieldType::CompositeType { type_name } => type_name.clone(),
                        ResolvedFieldType::Relation(_) => "unknown".to_string(),
                    };
                    let ts_type = if f.is_array {
                        format!("{}[]", base)
                    } else if !f.is_required {
                        format!("{} | null", base)
                    } else {
                        base
                    };
                    CompositeFieldCtx {
                        name: f.logical_name.clone(),
                        ts_type,
                    }
                })
                .collect();
            CompositeTypeCtx {
                name: ct.logical_name.clone(),
                fields,
            }
        })
        .collect();
    type_list.sort_by(|a, b| a.name.cmp(&b.name));

    let mut context = Context::new();
    context.insert("composite_types", &type_list);

    Some(render("composite_types.d.ts.tera", &context))
}

/// Generate `enums.js` + `enums.d.ts` for all enum definitions.
///
/// Returns `(js_code, dts_code)`.
pub fn generate_js_enums(enums: &HashMap<String, EnumIr>) -> (String, String) {
    #[derive(Serialize)]
    struct EnumCtx {
        name: String,
        variants: Vec<String>,
    }

    let mut enum_list: Vec<EnumCtx> = enums
        .values()
        .map(|e| EnumCtx {
            name: e.logical_name.clone(),
            variants: e.variants.clone(),
        })
        .collect();
    enum_list.sort_by(|a, b| a.name.cmp(&b.name));

    let mut context = Context::new();
    context.insert("enums", &enum_list);
    let js_code = render("enums.js.tera", &context);
    let dts_code = render("enums.d.ts.tera", &context);
    (js_code, dts_code)
}

/// Generate `index.js` + `index.d.ts` — the typed `Nautilus` class with model delegates.
///
/// Returns `(js_code, dts_code)`.
pub fn generate_js_client(
    models: &HashMap<String, ModelIr>,
    schema_path: &str,
) -> (String, String) {
    #[derive(Serialize)]
    struct ModelCtx {
        /// camelCase — property name on `Nautilus`, e.g. `user`.
        camel_name: String,
        /// snake_case — import file name, e.g. `user`.
        snake_name: String,
        /// PascalCase + "Delegate", e.g. `UserDelegate`.
        delegate_name: String,
    }

    let mut model_list: Vec<ModelCtx> = models
        .values()
        .map(|m| ModelCtx {
            camel_name: m.logical_name.to_lower_camel_case(),
            snake_name: m.logical_name.to_snake_case(),
            delegate_name: format!("{}Delegate", m.logical_name),
        })
        .collect();
    model_list.sort_by(|a, b| a.camel_name.cmp(&b.camel_name));

    let mut context = Context::new();
    context.insert("models", &model_list);
    context.insert("schema_path", schema_path);
    let js_code = render("client.js.tera", &context);
    let dts_code = render("client.d.ts.tera", &context);
    (js_code, dts_code)
}

/// Generate `models/index.js` + `models/index.d.ts` — barrel re-exports for all model files.
///
/// `js_models` contains the `.js` model filenames. Returns `(js_code, dts_code)`.
pub fn generate_js_models_index(js_models: &[(String, String)]) -> (String, String) {
    let mut modules: Vec<String> = js_models
        .iter()
        .map(|(file_name, _)| file_name.trim_end_matches(".js").to_string())
        .collect();
    modules.sort();

    let mut context = Context::new();
    context.insert("model_modules", &modules);
    let js_code = render("models_index.js.tera", &context);
    let dts_code = render("models_index.d.ts.tera", &context);
    (js_code, dts_code)
}

/// Static JavaScript + declaration runtime files embedded at compile time.
/// Returns `Vec<(filename, content)>` containing both `.js` and `.d.ts` pairs.
pub fn js_runtime_files() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "_errors.js",
            include_str!("../../templates/js/runtime/_errors.js"),
        ),
        (
            "_errors.d.ts",
            include_str!("../../templates/js/runtime/_errors.d.ts"),
        ),
        (
            "_protocol.js",
            include_str!("../../templates/js/runtime/_protocol.js"),
        ),
        (
            "_protocol.d.ts",
            include_str!("../../templates/js/runtime/_protocol.d.ts"),
        ),
        (
            "_engine.js",
            include_str!("../../templates/js/runtime/_engine.js"),
        ),
        (
            "_engine.d.ts",
            include_str!("../../templates/js/runtime/_engine.d.ts"),
        ),
        (
            "_client.js",
            include_str!("../../templates/js/runtime/_client.js"),
        ),
        (
            "_client.d.ts",
            include_str!("../../templates/js/runtime/_client.d.ts"),
        ),
        (
            "_transaction.js",
            include_str!("../../templates/js/runtime/_transaction.js"),
        ),
        (
            "_transaction.d.ts",
            include_str!("../../templates/js/runtime/_transaction.d.ts"),
        ),
    ]
}

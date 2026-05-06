//! Code generator for Nautilus models, delegates, and builders.

use heck::{ToPascalCase, ToSnakeCase};
use nautilus_schema::ast::StorageStrategy;
use nautilus_schema::ir::{FieldIr, ModelIr, ResolvedFieldType, ScalarType, SchemaIr};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tera::{Context, Tera};

use crate::extension_types::ExtensionRegistry;
use crate::type_helpers::{
    field_to_rust_avg_type, field_to_rust_base_type, field_to_rust_sum_type, field_to_rust_type,
    scalar_to_rust_type,
};

pub static TEMPLATES: std::sync::LazyLock<Tera> = std::sync::LazyLock::new(|| {
    let mut tera = Tera::default();
    tera.add_raw_templates(vec![
        (
            "columns_struct.tera",
            include_str!("../templates/rust/columns_struct.tera"),
        ),
        (
            "column_impl.tera",
            include_str!("../templates/rust/column_impl.tera"),
        ),
        ("create.tera", include_str!("../templates/rust/create.tera")),
        (
            "create_many.tera",
            include_str!("../templates/rust/create_many.tera"),
        ),
        (
            "delegate.tera",
            include_str!("../templates/rust/delegate.tera"),
        ),
        ("delete.tera", include_str!("../templates/rust/delete.tera")),
        ("enum.tera", include_str!("../templates/rust/enum.tera")),
        (
            "find_many.tera",
            include_str!("../templates/rust/find_many.tera"),
        ),
        (
            "from_row_impl.tera",
            include_str!("../templates/rust/from_row_impl.tera"),
        ),
        (
            "model_file.tera",
            include_str!("../templates/rust/model_file.tera"),
        ),
        ("lib_rs.tera", include_str!("../templates/rust/lib_rs.tera")),
        (
            "model_struct.tera",
            include_str!("../templates/rust/model_struct.tera"),
        ),
        ("update.tera", include_str!("../templates/rust/update.tera")),
        (
            "composite_type.tera",
            include_str!("../templates/rust/composite_type.tera"),
        ),
    ])
    .expect("embedded Rust templates must parse");
    tera
});

fn render(template: &str, ctx: &Context) -> String {
    crate::template::render(&TEMPLATES, template, ctx)
}

/// Template context for a single model field in the Rust codegen backend.
///
/// This struct is intentionally separate from [`PythonFieldContext`] in
/// `python/generator.rs`: the two backends expose different template
/// variables (Rust needs `rust_type` / `column_type`; Python needs
/// `python_type` / `base_type` / `is_enum` / `has_default` / `default`) and
/// are expected to evolve independently.
#[derive(Debug, Clone, Serialize)]
struct FieldContext {
    name: String,
    logical_name: String,
    db_name: String,
    rust_type: String,
    base_rust_type: String,
    column_type: String,
    read_hint_expr: String,
    variant_name: String,
    is_array: bool,
    index: usize,
    is_pk: bool,
    /// `true` when the field maps to an `Option<T>` Rust type
    /// (i.e. the schema field is not required and is not a relation).
    is_optional: bool,
    /// `true` when the field has `@updatedAt` — auto-defaults to `now()` if not provided.
    is_updated_at: bool,
    /// `true` when the field is a `@computed` generated column (read-only from client side).
    is_computed: bool,
}

#[derive(Debug, Clone, Serialize)]
struct AggregateFieldContext {
    name: String,
    logical_name: String,
    rust_type: String,
    avg_rust_type: String,
    sum_rust_type: String,
    variant_name: String,
}

/// Serialisable (logical_name, db_name) pair for primary-key fields.
/// Used in templates to generate cursor predicate slices.
#[derive(Debug, Clone, Serialize)]
struct PkFieldContext {
    /// Snake-case logical name — used as the cursor map key in generated code.
    name: String,
    /// Original logical field name from the schema.
    logical_name: String,
    /// Database column name — used to build the `table__db_col` column reference.
    db_name: String,
}

#[derive(Debug, Clone, Serialize)]
struct RelationContext {
    field_name: String,
    target_model: String,
    target_table: String,
    is_array: bool,
    fields: Vec<String>,
    references: Vec<String>,
    fields_db: Vec<String>,
    references_db: Vec<String>,
    target_scalar_fields: Vec<FieldContext>,
}

fn resolve_inverse_relation_fields(
    source_model_name: &str,
    relation_name: Option<&str>,
    target_model: &ModelIr,
) -> (Vec<String>, Vec<String>) {
    let inverse = target_model.relation_fields().find(|field| {
        if let ResolvedFieldType::Relation(inv_rel) = &field.field_type {
            if inv_rel.target_model != source_model_name {
                return false;
            }

            match (relation_name, inv_rel.name.as_deref()) {
                (Some(expected), Some(actual)) => actual == expected,
                (Some(_), None) => false,
                (None, Some(_)) => false,
                (None, None) => true,
            }
        } else {
            false
        }
    });

    let Some(inverse_field) = inverse else {
        return (vec![], vec![]);
    };
    let ResolvedFieldType::Relation(inv_rel) = &inverse_field.field_type else {
        return (vec![], vec![]);
    };

    (inv_rel.references.clone(), inv_rel.fields.clone())
}

fn field_read_hint_expr(field: &FieldIr) -> String {
    if field.is_array && field.storage_strategy == Some(StorageStrategy::Json) {
        return "Some(crate::ValueHint::Json)".to_string();
    }

    match &field.field_type {
        ResolvedFieldType::Scalar(ScalarType::Decimal { .. }) => {
            "Some(crate::ValueHint::Decimal)".to_string()
        }
        ResolvedFieldType::Scalar(ScalarType::DateTime) => {
            "Some(crate::ValueHint::DateTime)".to_string()
        }
        ResolvedFieldType::Scalar(ScalarType::Json | ScalarType::Jsonb) => {
            "Some(crate::ValueHint::Json)".to_string()
        }
        ResolvedFieldType::Scalar(ScalarType::Uuid) => "Some(crate::ValueHint::Uuid)".to_string(),
        ResolvedFieldType::Scalar(ScalarType::Geometry) => {
            "Some(crate::ValueHint::Geometry)".to_string()
        }
        ResolvedFieldType::Scalar(ScalarType::Geography) => {
            "Some(crate::ValueHint::Geography)".to_string()
        }
        ResolvedFieldType::CompositeType { .. }
            if field.storage_strategy == Some(StorageStrategy::Json) =>
        {
            "Some(crate::ValueHint::Json)".to_string()
        }
        _ => "None".to_string(),
    }
}

/// Generate complete code for a model (struct, impls, delegate, builders).
///
/// `is_async` determines whether the generated delegate methods and internal
/// builders use `async fn`/`.await` (`true`) or blocking sync wrappers (`false`).
pub fn generate_model(model: &ModelIr, ir: &SchemaIr, is_async: bool) -> String {
    let extensions = ExtensionRegistry::from_schema(ir);
    generate_model_with_registry(model, ir, is_async, &extensions)
}

fn generate_model_with_registry(
    model: &ModelIr,
    ir: &SchemaIr,
    is_async: bool,
    extensions: &ExtensionRegistry,
) -> String {
    let mut context = Context::new();

    context.insert("model_name", &model.logical_name);
    context.insert("table_name", &model.db_name);
    context.insert("delegate_name", &format!("{}Delegate", model.logical_name));
    context.insert("columns_name", &format!("{}Columns", model.logical_name));
    context.insert("find_many_name", &format!("{}FindMany", model.logical_name));
    context.insert("create_name", &format!("{}Create", model.logical_name));
    context.insert(
        "create_many_name",
        &format!("{}CreateMany", model.logical_name),
    );
    context.insert("entry_name", &format!("{}CreateEntry", model.logical_name));
    context.insert("update_name", &format!("{}Update", model.logical_name));
    context.insert("delete_name", &format!("{}Delete", model.logical_name));

    let pk_field_names = model.primary_key.fields();
    context.insert("primary_key_fields", &pk_field_names);

    let pk_fields_with_db: Vec<PkFieldContext> = pk_field_names
        .iter()
        .filter_map(|logical| {
            model
                .scalar_fields()
                .find(|f| f.logical_name.as_str() == *logical)
                .map(|f| PkFieldContext {
                    name: f.logical_name.to_snake_case(),
                    logical_name: f.logical_name.clone(),
                    db_name: f.db_name.clone(),
                })
        })
        .collect();
    context.insert("pk_fields_with_db", &pk_fields_with_db);

    let mut single_record_constraints = Vec::new();
    let mut seen_constraint_keys = HashSet::new();

    if !pk_fields_with_db.is_empty() {
        let pk_key: Vec<String> = pk_fields_with_db
            .iter()
            .map(|field| field.db_name.clone())
            .collect();
        if seen_constraint_keys.insert(pk_key) {
            single_record_constraints.push(pk_fields_with_db.clone());
        }
    }

    for constraint in &model.unique_constraints {
        let fields: Vec<PkFieldContext> = constraint
            .fields
            .iter()
            .filter_map(|logical| {
                model
                    .scalar_fields()
                    .find(|f| f.logical_name == *logical)
                    .map(|f| PkFieldContext {
                        name: f.logical_name.to_snake_case(),
                        logical_name: f.logical_name.clone(),
                        db_name: f.db_name.clone(),
                    })
            })
            .collect();

        if fields.len() != constraint.fields.len() || fields.is_empty() {
            continue;
        }

        let constraint_key: Vec<String> =
            fields.iter().map(|field| field.db_name.clone()).collect();
        if seen_constraint_keys.insert(constraint_key) {
            single_record_constraints.push(fields);
        }
    }
    context.insert("single_record_constraints", &single_record_constraints);

    let mut enum_imports = HashSet::new();
    let mut composite_type_imports = HashSet::new();

    let mut scalar_fields: Vec<FieldContext> = Vec::new();
    let mut create_fields: Vec<FieldContext> = Vec::new();
    let mut updated_at_fields: Vec<FieldContext> = Vec::new();
    let mut numeric_fields: Vec<AggregateFieldContext> = Vec::new();
    let mut orderable_fields: Vec<AggregateFieldContext> = Vec::new();

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

        let column_type = match &field.field_type {
            ResolvedFieldType::Scalar(scalar) => scalar_to_rust_type(scalar, extensions),
            ResolvedFieldType::Enum { enum_name } => enum_name.clone(),
            _ => String::new(),
        };
        let is_pk = pk_field_names.contains(&field.logical_name.as_str());
        let base_rust_type = field_to_rust_base_type(field, extensions);

        let field_ctx = FieldContext {
            name: field.logical_name.to_snake_case(),
            logical_name: field.logical_name.clone(),
            db_name: field.db_name.clone(),
            rust_type: field_to_rust_type(field, extensions),
            base_rust_type: base_rust_type.clone(),
            column_type,
            read_hint_expr: field_read_hint_expr(field),
            variant_name: field.logical_name.to_pascal_case(),
            is_array: field.is_array,
            index: idx,
            is_pk,
            is_optional: !field.is_required && !field.is_array,
            is_updated_at: field.is_updated_at,
            is_computed: field.computed.is_some(),
        };

        create_fields.push(field_ctx.clone());

        if field.is_updated_at {
            updated_at_fields.push(field_ctx.clone());
        }

        scalar_fields.push(field_ctx);

        let is_numeric = matches!(
            &field.field_type,
            ResolvedFieldType::Scalar(ScalarType::Int)
                | ResolvedFieldType::Scalar(ScalarType::BigInt)
                | ResolvedFieldType::Scalar(ScalarType::Float)
                | ResolvedFieldType::Scalar(ScalarType::Decimal { .. })
        );
        if is_numeric {
            numeric_fields.push(AggregateFieldContext {
                name: field.logical_name.to_snake_case(),
                logical_name: field.logical_name.clone(),
                rust_type: base_rust_type.clone(),
                avg_rust_type: field_to_rust_avg_type(field),
                sum_rust_type: field_to_rust_sum_type(field, extensions),
                variant_name: field.logical_name.to_pascal_case(),
            });
        }

        let is_non_orderable = matches!(
            &field.field_type,
            ResolvedFieldType::Scalar(ScalarType::Boolean)
                | ResolvedFieldType::Scalar(ScalarType::Json)
                | ResolvedFieldType::Scalar(ScalarType::Jsonb)
                | ResolvedFieldType::Scalar(ScalarType::Hstore)
                | ResolvedFieldType::Scalar(ScalarType::Geometry)
                | ResolvedFieldType::Scalar(ScalarType::Geography)
                | ResolvedFieldType::Scalar(ScalarType::Vector { .. })
                | ResolvedFieldType::Scalar(ScalarType::Bytes)
        );
        if !is_non_orderable {
            orderable_fields.push(AggregateFieldContext {
                name: field.logical_name.to_snake_case(),
                logical_name: field.logical_name.clone(),
                rust_type: base_rust_type,
                avg_rust_type: String::new(),
                sum_rust_type: String::new(),
                variant_name: field.logical_name.to_pascal_case(),
            });
        }
    }

    let mut relation_imports = HashSet::new();
    for field in model.relation_fields() {
        if let ResolvedFieldType::Relation(rel) = &field.field_type {
            relation_imports.insert(rel.target_model.clone());
        }
    }

    context.insert("has_enums", &!enum_imports.is_empty());
    context.insert(
        "enum_imports",
        &enum_imports.into_iter().collect::<Vec<_>>(),
    );
    context.insert("has_relations", &!relation_imports.is_empty());
    context.insert(
        "relation_imports",
        &relation_imports.into_iter().collect::<Vec<_>>(),
    );
    context.insert("has_composite_types", &!composite_type_imports.is_empty());
    context.insert(
        "composite_type_imports",
        &composite_type_imports.into_iter().collect::<Vec<_>>(),
    );

    let relation_fields: Vec<FieldContext> = model
        .relation_fields()
        .map(|field| FieldContext {
            name: field.logical_name.to_snake_case(),
            logical_name: field.logical_name.clone(),
            db_name: field.db_name.clone(),
            rust_type: field_to_rust_type(field, extensions),
            base_rust_type: field_to_rust_base_type(field, extensions),
            column_type: String::new(),
            read_hint_expr: "None".to_string(),
            variant_name: field.logical_name.to_pascal_case(),
            is_array: field.is_array,
            index: 0,
            is_pk: false,
            is_optional: true,
            is_updated_at: false,
            is_computed: false,
        })
        .collect();

    let relations: Vec<RelationContext> = model
        .relation_fields()
        .filter_map(|field| {
            let ResolvedFieldType::Relation(rel) = &field.field_type else {
                return None;
            };
            let target_model = ir.models.get(&rel.target_model)?;

            let target_pk_names = target_model.primary_key.fields();
            let target_scalar_fields: Vec<FieldContext> = target_model
                .scalar_fields()
                .enumerate()
                .map(|(idx, f)| {
                    let column_type = match &f.field_type {
                        ResolvedFieldType::Scalar(scalar) => {
                            scalar_to_rust_type(scalar, extensions)
                        }
                        ResolvedFieldType::Enum { enum_name } => enum_name.clone(),
                        _ => String::new(),
                    };
                    let f_is_pk = target_pk_names.contains(&f.logical_name.as_str());
                    FieldContext {
                        name: f.logical_name.to_snake_case(),
                        logical_name: f.logical_name.clone(),
                        db_name: f.db_name.clone(),
                        rust_type: field_to_rust_type(f, extensions),
                        base_rust_type: field_to_rust_base_type(f, extensions),
                        column_type,
                        read_hint_expr: field_read_hint_expr(f),
                        variant_name: f.logical_name.to_pascal_case(),
                        is_array: f.is_array,
                        index: idx,
                        is_pk: f_is_pk,
                        is_optional: !f.is_required && !f.is_array,
                        is_updated_at: f.is_updated_at,
                        is_computed: f.computed.is_some(),
                    }
                })
                .collect();

            let (fields, references) = if rel.fields.is_empty() {
                resolve_inverse_relation_fields(
                    &model.logical_name,
                    rel.name.as_deref(),
                    target_model,
                )
            } else {
                (rel.fields.clone(), rel.references.clone())
            };

            let fields_db: Vec<String> = fields
                .iter()
                .filter_map(|logical_name| {
                    model
                        .fields
                        .iter()
                        .find(|f| &f.logical_name == logical_name)
                        .map(|f| f.db_name.clone())
                })
                .collect();

            let references_db: Vec<String> = references
                .iter()
                .filter_map(|logical_name| {
                    target_model
                        .fields
                        .iter()
                        .find(|f| &f.logical_name == logical_name)
                        .map(|f| f.db_name.clone())
                })
                .collect();

            Some(RelationContext {
                field_name: field.logical_name.to_snake_case(),
                target_model: rel.target_model.clone(),
                target_table: target_model.db_name.clone(),
                is_array: field.is_array,
                fields,
                references,
                fields_db,
                references_db,
                target_scalar_fields,
            })
        })
        .collect();

    context.insert("scalar_fields", &scalar_fields);
    context.insert("relation_fields", &relation_fields);
    context.insert("relations", &relations);
    context.insert("create_fields", &create_fields);
    context.insert("updated_at_fields", &updated_at_fields);
    context.insert("all_scalar_fields", &scalar_fields);
    context.insert("numeric_fields", &numeric_fields);
    context.insert("orderable_fields", &orderable_fields);
    context.insert("has_numeric_fields", &!numeric_fields.is_empty());
    context.insert("has_orderable_fields", &!orderable_fields.is_empty());
    context.insert("is_async", &is_async);

    render("model_file.tera", &context)
}

/// Generate all models from a schema IR.
///
/// `is_async` is forwarded to every [`generate_model`] call.
pub fn generate_all_models(ir: &SchemaIr, is_async: bool) -> HashMap<String, String> {
    let extensions = ExtensionRegistry::from_schema(ir);
    generate_all_models_with_registry(ir, is_async, &extensions)
}

pub(crate) fn generate_all_models_with_registry(
    ir: &SchemaIr,
    is_async: bool,
    extensions: &ExtensionRegistry,
) -> HashMap<String, String> {
    let mut generated = HashMap::new();

    for (model_name, model_ir) in &ir.models {
        let code = generate_model_with_registry(model_ir, ir, is_async, extensions);
        generated.insert(model_name.clone(), code);
    }

    generated
}

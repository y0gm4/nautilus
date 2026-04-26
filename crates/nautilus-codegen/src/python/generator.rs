//! Python code generator for Nautilus models, delegates, and builders.

use heck::{ToPascalCase, ToSnakeCase};
use nautilus_schema::ir::{CompositeTypeIr, EnumIr, ModelIr, ResolvedFieldType, SchemaIr};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tera::{Context, Tera};

use crate::python::type_mapper::{
    field_to_python_type, get_base_python_type, get_default_value, get_filter_operators_for_field,
    is_auto_generated,
};

/// Python template registry — loaded once at first use.
pub static PYTHON_TEMPLATES: std::sync::LazyLock<Tera> = std::sync::LazyLock::new(|| {
    let mut tera = Tera::default();
    tera.add_raw_templates(vec![
        (
            "composite_types.py.tera",
            include_str!("../../templates/python/composite_types.py.tera"),
        ),
        (
            "model_file.py.tera",
            include_str!("../../templates/python/model_file.py.tera"),
        ),
        (
            "input_types.py.tera",
            include_str!("../../templates/python/input_types.py.tera"),
        ),
        (
            "enums.py.tera",
            include_str!("../../templates/python/enums.py.tera"),
        ),
        (
            "client.py.tera",
            include_str!("../../templates/python/client.py.tera"),
        ),
        (
            "package_init.py.tera",
            include_str!("../../templates/python/package_init.py.tera"),
        ),
        (
            "models_init.py.tera",
            include_str!("../../templates/python/models_init.py.tera"),
        ),
        (
            "enums_init.py.tera",
            include_str!("../../templates/python/enums_init.py.tera"),
        ),
        (
            "errors_init.py.tera",
            include_str!("../../templates/python/errors_init.py.tera"),
        ),
        (
            "internal_init.py.tera",
            include_str!("../../templates/python/internal_init.py.tera"),
        ),
        (
            "transaction_init.py.tera",
            include_str!("../../templates/python/transaction_init.py.tera"),
        ),
    ])
    .expect("embedded Python templates must parse");
    tera
});

fn render(template: &str, ctx: &Context) -> String {
    crate::template::render(&PYTHON_TEMPLATES, template, ctx)
}

/// Template context for a single model field in the Python codegen backend.
///
/// This struct is intentionally separate from `FieldContext` in
/// `generator.rs`: Python needs additional template variables
/// (`logical_name`, `python_type`, `base_type`, `is_enum`, `has_default`,
/// `default`) that have no counterpart in the Rust backend, and the two are
/// expected to evolve independently.
#[derive(Debug, Clone, Serialize)]
struct PythonFieldContext {
    name: String,
    logical_name: String,
    db_name: String,
    python_type: String,
    model_python_type: String,
    base_type: String,
    is_optional: bool,
    is_array: bool,
    is_enum: bool,
    has_default: bool,
    default: String,
    model_has_default: bool,
    model_default: String,
    is_pk: bool,
    index: usize,
}

#[derive(Debug, Clone, Serialize)]
struct PythonRelationContext {
    field_name: String,
    target_model: String,
    target_table: String,
    is_array: bool,
    fields: Vec<String>,
    references: Vec<String>,
    fields_db: Vec<String>,
    references_db: Vec<String>,
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

    if let Some(inverse_field) = inverse {
        if let ResolvedFieldType::Relation(inv_rel) = &inverse_field.field_type {
            return (inv_rel.references.clone(), inv_rel.fields.clone());
        }
    }

    (vec![], vec![])
}

#[derive(Debug, Clone, Serialize)]
struct FilterOperatorContext {
    suffix: String,
    python_type: String,
}

#[derive(Debug, Clone, Serialize)]
struct WhereInputFieldContext {
    name: String,
    python_type: String,
    is_vector: bool,
    operators: Vec<FilterOperatorContext>,
}

#[derive(Debug, Clone, Serialize)]
struct CreateInputFieldContext {
    name: String,
    python_type: String,
    is_required: bool,
}

#[derive(Debug, Clone, Serialize)]
struct UpdateInputFieldContext {
    name: String,
    python_type: String,
}

#[derive(Debug, Clone, Serialize)]
struct OrderByFieldContext {
    name: String,
}

#[derive(Debug, Clone, Serialize)]
struct IncludeFieldContext {
    name: String,
    logical_name: String,
    target_model: String,
    /// snake_case module name of the target model (e.g. "post" for Post)
    target_snake: String,
    /// true if this is a one-to-many relation (List/array)
    is_array: bool,
}

/// Context for a scalar field used in aggregate input types (avg/sum/min/max).
#[derive(Debug, Clone, Serialize)]
struct AggregateFieldContext {
    name: String,
    python_type: String,
}

fn optional_output_python_type(python_type: &str) -> String {
    if python_type.starts_with("Optional[") {
        python_type.to_string()
    } else {
        format!("Optional[{}]", python_type)
    }
}

/// Generate complete Python code for a model.
///
/// `is_async` determines whether delegate methods use `async def`/`await` (`true`)
/// or synchronous `def` + `asyncio.run()` wrappers (`false`).
/// `recursive_type_depth` controls the depth of generated recursive include TypedDicts.
pub fn generate_python_model(
    model: &ModelIr,
    ir: &SchemaIr,
    is_async: bool,
    recursive_type_depth: usize,
) -> (String, String) {
    let mut context = Context::new();

    context.insert("model_name", &model.logical_name);
    context.insert("snake_name", &model.logical_name.to_snake_case());
    context.insert("table_name", &model.db_name);
    context.insert("delegate_name", &format!("{}Delegate", model.logical_name));
    context.insert("find_many_name", &format!("{}FindMany", model.logical_name));
    context.insert("create_name", &format!("{}Create", model.logical_name));
    context.insert(
        "create_many_name",
        &format!("{}CreateMany", model.logical_name),
    );
    context.insert("update_name", &format!("{}Update", model.logical_name));
    context.insert("delete_name", &format!("{}Delete", model.logical_name));

    let pk_field_names = model.primary_key.fields();
    context.insert("primary_key_fields", &pk_field_names);

    let mut enum_imports = HashSet::new();
    let mut composite_type_imports = HashSet::new();
    let mut has_datetime = false;
    let mut has_uuid = false;
    let mut has_decimal = false;
    let mut has_dict = false;

    let mut scalar_fields: Vec<PythonFieldContext> = Vec::new();
    let mut create_fields: Vec<PythonFieldContext> = Vec::new();
    let mut where_input_fields: Vec<WhereInputFieldContext> = Vec::new();
    let mut create_input_fields: Vec<CreateInputFieldContext> = Vec::new();
    let mut update_input_fields: Vec<UpdateInputFieldContext> = Vec::new();
    let mut order_by_fields: Vec<OrderByFieldContext> = Vec::new();
    let mut numeric_fields: Vec<AggregateFieldContext> = Vec::new();
    let mut orderable_fields: Vec<AggregateFieldContext> = Vec::new();
    let mut object_value_db_fields: Vec<String> = Vec::new();
    let mut vector_field_names: Vec<String> = Vec::new();

    for (idx, field) in model.scalar_fields().enumerate() {
        use nautilus_schema::ir::ScalarType;

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
            ResolvedFieldType::Scalar(scalar) => match scalar {
                ScalarType::DateTime => has_datetime = true,
                ScalarType::Uuid => has_uuid = true,
                ScalarType::Decimal { .. } => has_decimal = true,
                ScalarType::Json | ScalarType::Jsonb | ScalarType::Hstore => has_dict = true,
                _ => {}
            },
            _ => {}
        }

        let python_type = field_to_python_type(field, &ir.enums);
        let base_type = match &field.field_type {
            ResolvedFieldType::Scalar(s) => {
                crate::python::type_mapper::scalar_to_python_type(s).to_string()
            }
            ResolvedFieldType::Enum { enum_name } => enum_name.clone(),
            _ => "Any".to_string(),
        };
        let base_python_type = get_base_python_type(field, &ir.enums);
        let is_enum = matches!(field.field_type, ResolvedFieldType::Enum { .. });
        let auto_generated = is_auto_generated(field);
        let is_pk = pk_field_names.contains(&field.logical_name.as_str());

        // Render enum defaults as `EnumName.VARIANT`.
        let mut default_val = get_default_value(field);
        if let Some(ref def) = default_val {
            if let ResolvedFieldType::Enum { enum_name } = &field.field_type {
                if !def.contains('.') && !def.contains('(') && def != "None" {
                    default_val = Some(format!("{}.{}", enum_name, def));
                }
            }
        }

        let model_python_type = if is_pk {
            python_type.clone()
        } else {
            optional_output_python_type(&python_type)
        };
        let model_has_default = if is_pk { default_val.is_some() } else { true };
        let model_default = if is_pk {
            default_val.clone().unwrap_or_default()
        } else {
            "None".to_string()
        };

        let field_ctx = PythonFieldContext {
            name: field.logical_name.to_snake_case(),
            logical_name: field.logical_name.clone(),
            db_name: field.db_name.clone(),
            python_type: python_type.clone(),
            model_python_type,
            base_type,
            is_optional: !field.is_required,
            is_array: field.is_array,
            is_enum,
            has_default: default_val.is_some(),
            default: default_val.unwrap_or_default(),
            model_has_default,
            model_default,
            is_pk,
            index: idx,
        };

        create_fields.push(field_ctx.clone());

        scalar_fields.push(field_ctx);

        if !matches!(field.field_type, ResolvedFieldType::Relation(_)) {
            let operators = get_filter_operators_for_field(field, &ir.enums);
            let is_vector = field.is_vector();
            if is_vector {
                vector_field_names.push(field.logical_name.clone());
            }
            where_input_fields.push(WhereInputFieldContext {
                name: field.logical_name.clone(),
                python_type: base_python_type.clone(),
                is_vector,
                operators: operators
                    .into_iter()
                    .map(|op| FilterOperatorContext {
                        suffix: op.suffix,
                        python_type: op.type_name,
                    })
                    .collect(),
            });
        }

        if matches!(
            field.field_type,
            ResolvedFieldType::Scalar(nautilus_schema::ir::ScalarType::Json)
                | ResolvedFieldType::Scalar(nautilus_schema::ir::ScalarType::Jsonb)
                | ResolvedFieldType::Scalar(nautilus_schema::ir::ScalarType::Hstore)
        ) && !field.is_array
        {
            object_value_db_fields.push(field.db_name.clone());
        }

        {
            let input_base = base_python_type.clone();
            let typed = if field.is_array {
                format!("List[{}]", input_base)
            } else {
                input_base
            };
            create_input_fields.push(CreateInputFieldContext {
                name: field.logical_name.clone(),
                python_type: typed,
                is_required: field.is_required
                    && field.default_value.is_none()
                    && !field.is_updated_at
                    && field.computed.is_none(),
            });
        }

        let is_auto_pk = auto_generated && pk_field_names.contains(&field.logical_name.as_str());
        if !is_auto_pk {
            let input_base = base_python_type.clone();
            let typed = if field.is_array {
                format!("List[{}]", input_base)
            } else {
                input_base
            };
            update_input_fields.push(UpdateInputFieldContext {
                name: field.logical_name.clone(),
                python_type: typed,
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
            numeric_fields.push(AggregateFieldContext {
                name: field.logical_name.clone(),
                python_type: base_python_type.clone(),
            });
        }

        let is_non_orderable = matches!(
            &field.field_type,
            ResolvedFieldType::Scalar(ScalarType::Boolean)
                | ResolvedFieldType::Scalar(ScalarType::Json)
                | ResolvedFieldType::Scalar(ScalarType::Jsonb)
                | ResolvedFieldType::Scalar(ScalarType::Hstore)
                | ResolvedFieldType::Scalar(ScalarType::Vector { .. })
                | ResolvedFieldType::Scalar(ScalarType::Bytes)
        );
        if !is_non_orderable {
            order_by_fields.push(OrderByFieldContext {
                name: field.logical_name.clone(),
            });
            orderable_fields.push(AggregateFieldContext {
                name: field.logical_name.clone(),
                python_type: base_python_type,
            });
        }
    }

    let mut relation_imports = HashSet::new();
    for field in model.relation_fields() {
        if let ResolvedFieldType::Relation(rel) = &field.field_type {
            relation_imports.insert(rel.target_model.clone());
        }
    }

    context.insert("has_datetime", &has_datetime);
    context.insert("has_uuid", &has_uuid);
    context.insert("has_decimal", &has_decimal);
    context.insert("has_dict", &has_dict);
    context.insert("has_enums", &!enum_imports.is_empty());
    context.insert(
        "enum_imports",
        &enum_imports.into_iter().collect::<Vec<_>>(),
    );
    context.insert("has_composite_types", &!composite_type_imports.is_empty());
    context.insert(
        "composite_type_imports",
        &composite_type_imports.into_iter().collect::<Vec<_>>(),
    );
    context.insert("has_relations", &!relation_imports.is_empty());
    context.insert(
        "relation_imports",
        &relation_imports.into_iter().collect::<Vec<_>>(),
    );

    let relation_fields: Vec<PythonFieldContext> = model
        .relation_fields()
        .enumerate()
        .map(|(idx, field)| {
            let python_type = field_to_python_type(field, &ir.enums);
            let default_val = if field.is_array {
                "Field(default_factory=list)".to_string()
            } else {
                "None".to_string()
            };

            PythonFieldContext {
                name: field.logical_name.to_snake_case(),
                logical_name: field.logical_name.clone(),
                db_name: field.db_name.clone(),
                python_type: python_type.clone(),
                model_python_type: python_type.clone(),
                base_type: String::new(),
                is_optional: true,
                is_array: field.is_array,
                is_enum: false,
                has_default: true,
                default: default_val,
                model_has_default: true,
                model_default: "None".to_string(),
                is_pk: false,
                index: idx,
            }
        })
        .collect();

    let relations: Vec<PythonRelationContext> = model
        .relation_fields()
        .filter_map(|field| {
            if let ResolvedFieldType::Relation(rel) = &field.field_type {
                if let Some(target_model) = ir.models.get(&rel.target_model) {
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

                    Some(PythonRelationContext {
                        field_name: field.logical_name.to_snake_case(),
                        target_model: rel.target_model.clone(),
                        target_table: target_model.db_name.clone(),
                        is_array: field.is_array,
                        fields,
                        references,
                        fields_db,
                        references_db,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let include_fields: Vec<IncludeFieldContext> = model
        .relation_fields()
        .filter_map(|field| {
            if let ResolvedFieldType::Relation(rel) = &field.field_type {
                Some(IncludeFieldContext {
                    name: field.logical_name.to_snake_case(),
                    logical_name: field.logical_name.clone(),
                    target_model: rel.target_model.clone(),
                    target_snake: rel.target_model.to_snake_case(),
                    is_array: field.is_array,
                })
            } else {
                None
            }
        })
        .collect();

    let has_numeric_fields = !numeric_fields.is_empty();
    let has_orderable_fields = !orderable_fields.is_empty();

    let needs_typeddict = !where_input_fields.is_empty()
        || !create_input_fields.is_empty()
        || !update_input_fields.is_empty();

    context.insert("needs_typeddict", &needs_typeddict);
    context.insert("where_input_fields", &where_input_fields);
    context.insert("create_input_fields", &create_input_fields);
    context.insert("update_input_fields", &update_input_fields);
    context.insert("order_by_fields", &order_by_fields);
    context.insert("include_fields", &include_fields);
    context.insert("has_includes", &!include_fields.is_empty());
    context.insert("numeric_fields", &numeric_fields);
    context.insert("orderable_fields", &orderable_fields);
    context.insert("object_value_db_fields", &object_value_db_fields);
    context.insert("has_numeric_fields", &has_numeric_fields);
    context.insert("has_orderable_fields", &has_orderable_fields);
    context.insert("has_vector_fields", &!vector_field_names.is_empty());
    context.insert("vector_field_names", &vector_field_names);

    context.insert("scalar_fields", &scalar_fields);
    context.insert("relation_fields", &relation_fields);
    context.insert("create_fields", &create_fields);
    context.insert("relations", &relations);
    context.insert("is_async", &is_async);
    context.insert("recursive_type_depth", &recursive_type_depth);

    let model_code = render("model_file.py.tera", &context);

    (
        format!("{}.py", model.logical_name.to_snake_case()),
        model_code,
    )
}

/// Generate all Python models.
///
/// `is_async` is forwarded to every [`generate_python_model`] call.
/// `recursive_type_depth` controls the depth of generated recursive include TypedDicts.
pub fn generate_all_python_models(
    ir: &SchemaIr,
    is_async: bool,
    recursive_type_depth: usize,
) -> Vec<(String, String)> {
    ir.models
        .values()
        .map(|model| generate_python_model(model, ir, is_async, recursive_type_depth))
        .collect()
}

/// Generate `types/types.py` — dataclasses for all composite types.
///
/// Returns `None` when there are no composite types.
pub fn generate_python_composite_types(
    composite_types: &HashMap<String, CompositeTypeIr>,
) -> Option<String> {
    if composite_types.is_empty() {
        return None;
    }

    #[derive(Serialize)]
    struct CompositeFieldCtx {
        name: String,
        python_type: String,
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
                        ResolvedFieldType::Scalar(s) => {
                            crate::python::type_mapper::scalar_to_python_type(s).to_string()
                        }
                        ResolvedFieldType::Enum { enum_name } => enum_name.clone(),
                        ResolvedFieldType::CompositeType { type_name } => type_name.clone(),
                        ResolvedFieldType::Relation(_) => "Any".to_string(),
                    };
                    let python_type = if f.is_array {
                        format!("List[{}]", base)
                    } else if !f.is_required {
                        format!("Optional[{}]", base)
                    } else {
                        base
                    };
                    CompositeFieldCtx {
                        name: f.logical_name.to_snake_case(),
                        python_type,
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

    Some(render("composite_types.py.tera", &context))
}

/// Generate Python enums file.
pub fn generate_python_enums(enums: &HashMap<String, EnumIr>) -> String {
    let mut context = Context::new();

    #[derive(Serialize)]
    struct EnumContext {
        name: String,
        variants: Vec<String>,
    }

    let enum_contexts: Vec<EnumContext> = enums
        .values()
        .map(|e| EnumContext {
            name: e.logical_name.clone(),
            variants: e.variants.clone(),
        })
        .collect();

    context.insert("enums", &enum_contexts);

    render("enums.py.tera", &context)
}

/// Generate Python client file with model delegates.
///
/// `is_async` determines whether the generated `Nautilus` class exposes an async
/// context manager (`async with Nautilus(...) as db`) or a sync one (`with Nautilus(...) as db`).
pub fn generate_python_client(
    models: &HashMap<String, ModelIr>,
    schema_path: &str,
    is_async: bool,
) -> String {
    let mut context = Context::new();

    #[derive(Serialize)]
    struct ModelContext {
        snake_name: String,
        delegate_name: String,
    }

    let mut model_contexts: Vec<ModelContext> = models
        .values()
        .map(|m| ModelContext {
            snake_name: m.logical_name.to_snake_case(),
            delegate_name: format!("{}Delegate", m.logical_name),
        })
        .collect();
    model_contexts.sort_by(|a, b| a.snake_name.cmp(&b.snake_name));

    context.insert("models", &model_contexts);
    context.insert("schema_path", schema_path);
    context.insert("is_async", &is_async);

    render("client.py.tera", &context)
}

/// Generate package __init__.py
pub fn generate_package_init(has_enums: bool) -> String {
    let mut context = Context::new();
    context.insert("has_enums", &has_enums);

    render("package_init.py.tera", &context)
}

/// Generate models/__init__.py
pub fn generate_models_init(models: &[(String, String)]) -> String {
    let mut context = Context::new();

    let mut model_modules: Vec<String> = models
        .iter()
        .map(|(file_name, _)| file_name.trim_end_matches(".py").to_string())
        .collect();
    model_modules.sort();

    let mut model_classes: Vec<String> = model_modules.iter().map(|m| m.to_pascal_case()).collect();
    model_classes.sort();

    context.insert("model_modules", &model_modules);
    context.insert("model_classes", &model_classes);

    render("models_init.py.tera", &context)
}

/// Generate enums/__init__.py
pub fn generate_enums_init(has_enums: bool) -> String {
    let mut context = Context::new();
    context.insert("has_enums", &has_enums);

    render("enums_init.py.tera", &context)
}

/// Generate errors/__init__.py.
///
/// Content is static (no template variables needed).
pub fn generate_errors_init() -> &'static str {
    include_str!("../../templates/python/errors_init.py.tera")
}

/// Generate _internal/__init__.py.
///
/// Content is static (no template variables needed).
pub fn generate_internal_init() -> &'static str {
    include_str!("../../templates/python/internal_init.py.tera")
}

/// Generate transaction.py at the package root.
///
/// Content is static: re-exports `IsolationLevel` and `TransactionClient`
/// from the internal `_internal.transaction` module so users can write
/// `from nautilus.transaction import IsolationLevel`.
pub fn generate_transaction_init() -> &'static str {
    include_str!("../../templates/python/transaction_init.py.tera")
}

/// Returns static runtime Python files to be written alongside generated code.
/// These files implement the base client, engine process manager, protocol, and errors.
pub fn python_runtime_files() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "_errors.py",
            include_str!("../../templates/python/runtime/_errors.py"),
        ),
        (
            "_protocol.py",
            include_str!("../../templates/python/runtime/_protocol.py"),
        ),
        (
            "_engine.py",
            include_str!("../../templates/python/runtime/_engine.py"),
        ),
        (
            "_client.py",
            include_str!("../../templates/python/runtime/_client.py"),
        ),
        (
            "_descriptors.py",
            include_str!("../../templates/python/runtime/_descriptors.py"),
        ),
        (
            "_transaction.py",
            include_str!("../../templates/python/runtime/_transaction.py"),
        ),
    ]
}

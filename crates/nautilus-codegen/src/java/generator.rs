//! Java client generator.

use anyhow::{anyhow, Result};
use heck::ToLowerCamelCase;
use heck::ToUpperCamelCase;
use nautilus_schema::ir::{
    CompositeFieldIr, CompositeTypeIr, EnumIr, FieldIr, ModelIr, ResolvedFieldType, ScalarType,
    SchemaIr,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use tera::{Context, Tera};

use crate::java::type_mapper::{
    composite_field_to_java_type, field_base_type, field_to_java_type, filter_operators_for_field,
    is_numeric_field, is_orderable_field, is_writable_on_create, is_writable_on_update,
};

pub(crate) const JACKSON_VERSION: &str = "2.17.2";
const DEFAULT_MAVEN_VERSION: &str = "0.1.0-SNAPSHOT";

static JAVA_TEMPLATES: std::sync::LazyLock<Tera> = std::sync::LazyLock::new(|| {
    let mut tera = Tera::default();
    tera.add_raw_templates(vec![
        (
            "java_pom.tera",
            include_str!("../../templates/java/pom.xml.tera"),
        ),
        // schema-driven templates
        (
            "java_enum.tera",
            include_str!("../../templates/java/enum.java.tera"),
        ),
        (
            "java_composite.tera",
            include_str!("../../templates/java/composite.java.tera"),
        ),
        (
            "java_model.tera",
            include_str!("../../templates/java/model.java.tera"),
        ),
        (
            "java_delegate.tera",
            include_str!("../../templates/java/delegate.java.tera"),
        ),
        (
            "_dsl_macros.tera",
            include_str!("../../templates/java/_dsl_macros.tera"),
        ),
        (
            "java_dsl.tera",
            include_str!("../../templates/java/dsl.java.tera"),
        ),
        (
            "java_transaction_client.tera",
            include_str!("../../templates/java/transaction_client.java.tera"),
        ),
        (
            "java_nautilus.tera",
            include_str!("../../templates/java/nautilus.java.tera"),
        ),
        // static utility templates (only `package_name` variable)
        (
            "java_nautilus_model.tera",
            include_str!("../../templates/java/NautilusModel.java.tera"),
        ),
        (
            "java_sort_order.tera",
            include_str!("../../templates/java/SortOrder.java.tera"),
        ),
        (
            "java_filters.tera",
            include_str!("../../templates/java/Filters.java.tera"),
        ),
        (
            "java_nautilus_options.tera",
            include_str!("../../templates/java/NautilusOptions.java.tera"),
        ),
        (
            "java_isolation_level.tera",
            include_str!("../../templates/java/IsolationLevel.java.tera"),
        ),
        (
            "java_transaction_options.tera",
            include_str!("../../templates/java/TransactionOptions.java.tera"),
        ),
        (
            "java_transaction_batch_op.tera",
            include_str!("../../templates/java/TransactionBatchOperation.java.tera"),
        ),
        // runtime templates (need `package_name` and sometimes `version`)
        (
            "java_rt_wire_serializable.tera",
            include_str!("../../templates/java/runtime/WireSerializable.java.tera"),
        ),
        (
            "java_rt_rpc_caller.tera",
            include_str!("../../templates/java/runtime/RpcCaller.java.tera"),
        ),
        (
            "java_rt_global_registry.tera",
            include_str!("../../templates/java/runtime/GlobalNautilusRegistry.java.tera"),
        ),
        (
            "java_rt_nautilus_exception.tera",
            include_str!("../../templates/java/runtime/NautilusException.java.tera"),
        ),
        (
            "java_rt_protocol_exception.tera",
            include_str!("../../templates/java/runtime/ProtocolException.java.tera"),
        ),
        (
            "java_rt_handshake_exception.tera",
            include_str!("../../templates/java/runtime/HandshakeException.java.tera"),
        ),
        (
            "java_rt_transaction_exception.tera",
            include_str!("../../templates/java/runtime/TransactionException.java.tera"),
        ),
        (
            "java_rt_not_found_exception.tera",
            include_str!("../../templates/java/runtime/NotFoundException.java.tera"),
        ),
        (
            "java_rt_json_support.tera",
            include_str!("../../templates/java/runtime/JsonSupport.java.tera"),
        ),
        (
            "java_rt_engine_process.tera",
            include_str!("../../templates/java/runtime/EngineProcess.java.tera"),
        ),
        (
            "java_rt_base_client.tera",
            include_str!("../../templates/java/runtime/BaseNautilusClient.java.tera"),
        ),
        (
            "java_rt_base_tx_client.tera",
            include_str!("../../templates/java/runtime/BaseTransactionClient.java.tera"),
        ),
        (
            "java_rt_abstract_delegate.tera",
            include_str!("../../templates/java/runtime/AbstractDelegate.java.tera"),
        ),
    ])
    .expect("embedded Java templates must parse");
    tera
});

#[derive(Debug, Clone)]
struct JavaConfig {
    root_package: String,
    group_id: String,
    artifact_id: String,
    version: String,
    schema_path: String,
    is_async: bool,
}

#[derive(Debug, Clone)]
struct ModelMeta {
    name: String,
    camel: String,
    delegate_name: String,
}

#[derive(Debug, Serialize)]
struct PomContext {
    group_id: String,
    artifact_id: String,
    version: String,
    jackson_version: String,
}

#[derive(Debug, Serialize)]
struct EnumTemplateContext {
    package_name: String,
    name: String,
    variants: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RecordComponentContext {
    ty: String,
    name: String,
}

#[derive(Debug, Serialize)]
struct RecordTemplateContext {
    package_name: String,
    imports: Vec<String>,
    name: String,
    components: Vec<RecordComponentContext>,
    reads: Vec<String>,
    writes: Vec<String>,
    ctor_args: Vec<String>,
    static_delegate: Option<String>,
    static_delegate_accessor: Option<String>,
    implements_type: String,
}

#[derive(Debug, Serialize)]
struct DslFilterOpCtx {
    /// Raw wire operator suffix, e.g. `"gt"`.
    suffix: String,
    /// PascalCase suffix for the Java method name, e.g. `"Gt"`.
    suffix_pascal: String,
    /// Java type for the method parameter, e.g. `"Integer"`.
    java_type: String,
}

#[derive(Debug, Serialize)]
struct DslScalarFieldCtx {
    /// Logical field name – used as method name, ScalarField enum wire value, Select/OrderBy key.
    name: String,
    /// PascalCase variant name for the ScalarField enum.
    variant_name: String,
    /// DB column name – used as Where/CreateInput/UpdateInput wire key.
    db_name: String,
    /// Base Java type for the Where `equals` method parameter.
    java_type: String,
    filter_ops: Vec<DslFilterOpCtx>,
}

#[derive(Debug, Serialize)]
struct DslWritableFieldCtx {
    /// Logical field name – Java setter method name.
    name: String,
    /// DB column name – wire key sent to the engine.
    db_name: String,
    /// Full Java type (e.g. `"List<String>"` for arrays).
    java_type: String,
}

#[derive(Debug, Serialize)]
struct DslRelationFieldCtx {
    name: String,
    target_model: String,
}

#[derive(Debug, Serialize)]
struct DslTemplateContext {
    package_name: String,
    imports: Vec<String>,
    name: String,
    scalar_fields: Vec<DslScalarFieldCtx>,
    create_fields: Vec<DslWritableFieldCtx>,
    update_fields: Vec<DslWritableFieldCtx>,
    relation_fields: Vec<DslRelationFieldCtx>,
    numeric_field_names: Vec<String>,
    orderable_field_names: Vec<String>,
    vector_field_names: Vec<String>,
    all_scalar_field_names: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DelegateTemplateContext {
    package_name: String,
    imports: Vec<String>,
    name: String,
    model_name: String,
    dsl_name: String,
    is_async: bool,
}

#[derive(Debug, Serialize)]
struct ClientModelContext {
    camel: String,
    delegate_name: String,
}

#[derive(Debug, Serialize)]
struct TransactionClientTemplateContext {
    package_name: String,
    imports: Vec<String>,
    models: Vec<ClientModelContext>,
}

#[derive(Debug, Serialize)]
struct NautilusTemplateContext {
    package_name: String,
    imports: Vec<String>,
    schema_path_literal: String,
    models: Vec<ClientModelContext>,
    is_async: bool,
}

fn render(template: &str, context: &Context) -> String {
    crate::template::render(&JAVA_TEMPLATES, template, context)
}

/// Render a template that only requires `package_name`.
fn render_pkg(template: &str, package_name: &str) -> String {
    let mut ctx = Context::new();
    ctx.insert("package_name", package_name);
    render(template, &ctx)
}

/// Public entry point: generate all Java source files for the given schema.
pub fn generate_java_client(
    ir: &SchemaIr,
    schema_path: &str,
    is_async: bool,
) -> Result<Vec<(String, String)>> {
    let generator = ir
        .generator
        .as_ref()
        .ok_or_else(|| anyhow!("Java generation requires a generator block"))?;

    let config = JavaConfig {
        root_package: generator
            .java_package
            .clone()
            .ok_or_else(|| anyhow!("Java generation requires generator.package"))?,
        group_id: generator
            .java_group_id
            .clone()
            .ok_or_else(|| anyhow!("Java generation requires generator.group_id"))?,
        artifact_id: generator
            .java_artifact_id
            .clone()
            .ok_or_else(|| anyhow!("Java generation requires generator.artifact_id"))?,
        version: DEFAULT_MAVEN_VERSION.to_string(),
        schema_path: schema_path.to_string(),
        is_async,
    };

    let models = sorted_model_meta(ir.models.values());
    let enums_map: BTreeMap<String, EnumIr> = ir
        .enums
        .iter()
        .map(|(name, item)| (name.clone(), item.clone()))
        .collect();

    let mut files = Vec::new();
    files.push(("pom.xml".to_string(), generate_pom(&config)));
    files.extend(java_runtime_files(&config.root_package));
    files.push((
        java_source_path(&config.root_package, "model", "NautilusModel.java"),
        render_pkg("java_nautilus_model.tera", &config.root_package),
    ));
    files.push((
        java_source_path(&config.root_package, "dsl", "SortOrder.java"),
        render_pkg("java_sort_order.tera", &config.root_package),
    ));
    files.push((
        java_source_path(&config.root_package, "dsl", "Filters.java"),
        render_pkg("java_filters.tera", &config.root_package),
    ));
    files.push((
        java_source_path(&config.root_package, "client", "NautilusOptions.java"),
        render_pkg("java_nautilus_options.tera", &config.root_package),
    ));
    files.push((
        java_source_path(&config.root_package, "client", "IsolationLevel.java"),
        render_pkg("java_isolation_level.tera", &config.root_package),
    ));
    files.push((
        java_source_path(&config.root_package, "client", "TransactionOptions.java"),
        render_pkg("java_transaction_options.tera", &config.root_package),
    ));
    files.push((
        java_source_path(
            &config.root_package,
            "client",
            "TransactionBatchOperation.java",
        ),
        render_pkg("java_transaction_batch_op.tera", &config.root_package),
    ));

    for enum_ir in sorted_named(ir.enums.values(), |item| item.logical_name.clone()) {
        files.push((
            java_source_path(
                &config.root_package,
                "enums",
                &format!("{}.java", enum_ir.logical_name),
            ),
            generate_enum_file(&config, enum_ir),
        ));
    }

    for composite in sorted_named(ir.composite_types.values(), |item| {
        item.logical_name.clone()
    }) {
        files.push((
            java_source_path(
                &config.root_package,
                "types",
                &format!("{}.java", composite.logical_name),
            ),
            generate_composite_file(&config, composite),
        ));
    }

    for model in sorted_named(ir.models.values(), |item| item.logical_name.clone()) {
        files.push((
            java_source_path(
                &config.root_package,
                "dsl",
                &format!("{}Dsl.java", model.logical_name),
            ),
            generate_dsl_file(&config, model, ir, &enums_map),
        ));
        files.push((
            java_source_path(
                &config.root_package,
                "client",
                &format!("{}Delegate.java", model.logical_name),
            ),
            generate_delegate_file(&config, model),
        ));
        files.push((
            java_source_path(
                &config.root_package,
                "model",
                &format!("{}.java", model.logical_name),
            ),
            generate_model_file(&config, model),
        ));
    }

    files.push((
        java_source_path(&config.root_package, "client", "TransactionClient.java"),
        generate_transaction_client(&config, &models),
    ));
    files.push((
        java_source_path(&config.root_package, "client", "Nautilus.java"),
        generate_nautilus_client(&config, &models),
    ));

    Ok(files)
}

/// Returns the rendered Java runtime files (the `internal` package) for the
/// given root package. Each tuple is `(maven-relative path, file content)`.
///
/// This is the Java equivalent of `python_runtime_files()` / `js_runtime_files()`:
/// the source content lives in `templates/java/runtime/*.java.tera` and is
/// embedded at compile time; only `package_name` (and `version`) are substituted
/// at generation time.
pub fn java_runtime_files(package_name: &str) -> Vec<(String, String)> {
    let mut ctx_pkg = Context::new();
    ctx_pkg.insert("package_name", package_name);

    let mut ctx_ver = Context::new();
    ctx_ver.insert("package_name", package_name);
    ctx_ver.insert("version", env!("CARGO_PKG_VERSION"));

    let pkg = package_name;
    vec![
        (
            java_source_path(pkg, "internal", "WireSerializable.java"),
            render("java_rt_wire_serializable.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "RpcCaller.java"),
            render("java_rt_rpc_caller.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "GlobalNautilusRegistry.java"),
            render("java_rt_global_registry.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "NautilusException.java"),
            render("java_rt_nautilus_exception.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "ProtocolException.java"),
            render("java_rt_protocol_exception.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "HandshakeException.java"),
            render("java_rt_handshake_exception.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "TransactionException.java"),
            render("java_rt_transaction_exception.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "NotFoundException.java"),
            render("java_rt_not_found_exception.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "JsonSupport.java"),
            render("java_rt_json_support.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "EngineProcess.java"),
            render("java_rt_engine_process.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "BaseNautilusClient.java"),
            render("java_rt_base_client.tera", &ctx_ver),
        ),
        (
            java_source_path(pkg, "internal", "BaseTransactionClient.java"),
            render("java_rt_base_tx_client.tera", &ctx_pkg),
        ),
        (
            java_source_path(pkg, "internal", "AbstractDelegate.java"),
            render("java_rt_abstract_delegate.tera", &ctx_pkg),
        ),
    ]
}

fn generate_pom(config: &JavaConfig) -> String {
    let context = Context::from_serialize(&PomContext {
        group_id: config.group_id.clone(),
        artifact_id: config.artifact_id.clone(),
        version: config.version.clone(),
        jackson_version: JACKSON_VERSION.to_string(),
    })
    .expect("Java pom context should serialize");
    render("java_pom.tera", &context)
}

fn generate_enum_file(config: &JavaConfig, enum_ir: &EnumIr) -> String {
    let context = Context::from_serialize(&EnumTemplateContext {
        package_name: config.root_package.clone(),
        name: enum_ir.logical_name.clone(),
        variants: enum_ir.variants.clone(),
    })
    .expect("Java enum context should serialize");
    render("java_enum.tera", &context)
}

/// Shared imports every Java record template (composite, model) needs for
/// Jackson serialization helpers plus the crate-local `JsonSupport`.
fn base_record_imports(root_package: &str) -> BTreeSet<String> {
    let mut imports = BTreeSet::new();
    imports.insert(format!("{root_package}.internal.JsonSupport"));
    imports.insert("com.fasterxml.jackson.databind.JsonNode".to_string());
    imports.insert("com.fasterxml.jackson.databind.node.ObjectNode".to_string());
    imports
}

/// Render the per-field `toJsonNode` write guard used by composite and model
/// records. Field name doubles as the wire key because composite and model
/// records both serialize using the logical name unchanged.
fn format_record_field_write(field_name: &str) -> String {
    format!(
        "        if (this.{field_name} != null) {{\n            node.set(\"{field_name}\", JsonSupport.toJsonNode(this.{field_name}));\n        }}\n",
    )
}

/// Per-field render artifacts collected by [`build_record_context`].
struct RecordField {
    component: RecordComponentContext,
    imports: BTreeSet<String>,
    read: String,
    ctor_arg: String,
}

/// Assemble the Tera [`Context`] shared by `composite.java.tera` and
/// `model.java.tera`. Callers supply the record name, the extra imports
/// (beyond the Jackson/JsonSupport base set), the per-field renderer, and the
/// optional static-delegate accessor that only models emit.
fn build_record_context<I, F>(
    config: &JavaConfig,
    name: String,
    extra_imports: impl IntoIterator<Item = String>,
    fields: I,
    mut per_field: F,
    implements_type: String,
    static_delegate: Option<(String, String)>,
) -> Context
where
    I: IntoIterator,
    F: FnMut(I::Item) -> RecordField,
{
    let mut imports = base_record_imports(&config.root_package);
    imports.extend(extra_imports);

    let mut components = Vec::new();
    let mut reads = Vec::new();
    let mut writes = Vec::new();
    let mut ctor_args = Vec::new();
    for field in fields {
        let rendered = per_field(field);
        imports.extend(rendered.imports);
        writes.push(format_record_field_write(&rendered.component.name));
        components.push(rendered.component);
        reads.push(rendered.read);
        ctor_args.push(rendered.ctor_arg);
    }

    let (static_delegate, static_delegate_accessor) = match static_delegate {
        Some((delegate, accessor)) => (Some(delegate), Some(accessor)),
        None => (None, None),
    };

    Context::from_serialize(&RecordTemplateContext {
        package_name: config.root_package.clone(),
        imports: imports.into_iter().collect(),
        name,
        components,
        reads,
        writes,
        ctor_args,
        static_delegate,
        static_delegate_accessor,
        implements_type,
    })
    .expect("Java record context should serialize")
}

fn generate_composite_file(config: &JavaConfig, composite: &CompositeTypeIr) -> String {
    let extra_imports = [format!("{}.internal.WireSerializable", config.root_package)];
    let context = build_record_context(
        config,
        composite.logical_name.clone(),
        extra_imports,
        &composite.fields,
        |field| {
            let (ty, field_imports) =
                composite_field_to_java_type(field, &config.root_package, &composite.logical_name);
            RecordField {
                component: RecordComponentContext {
                    ty,
                    name: field.logical_name.clone(),
                },
                imports: field_imports,
                read: generate_composite_field_read(field),
                ctor_arg: field.logical_name.clone(),
            }
        },
        "WireSerializable".to_string(),
        None,
    );
    render("java_composite.tera", &context)
}

fn generate_model_file(config: &JavaConfig, model: &ModelIr) -> String {
    let extra_imports = [
        format!("{}.client.Nautilus", config.root_package),
        format!(
            "{}.client.{}Delegate",
            config.root_package, model.logical_name
        ),
        format!("{}.internal.GlobalNautilusRegistry", config.root_package),
    ];
    let context = build_record_context(
        config,
        model.logical_name.clone(),
        extra_imports,
        &model.fields,
        |field| {
            let (ty, field_imports) =
                field_to_java_type(field, &config.root_package, &model.logical_name);
            RecordField {
                component: RecordComponentContext {
                    ty,
                    name: field.logical_name.clone(),
                },
                imports: field_imports,
                read: generate_model_field_read(model, field),
                ctor_arg: field.logical_name.clone(),
            }
        },
        "NautilusModel".to_string(),
        Some((
            format!("{}Delegate", model.logical_name),
            model.logical_name.to_lower_camel_case(),
        )),
    );
    render("java_model.tera", &context)
}

fn generate_delegate_file(config: &JavaConfig, model: &ModelIr) -> String {
    let delegate_name = format!("{}Delegate", model.logical_name);
    let dsl_name = format!("{}Dsl", model.logical_name);

    let mut imports = BTreeSet::new();
    imports.insert(format!("{}.dsl.{}", config.root_package, dsl_name));
    imports.insert(format!("{}.internal.AbstractDelegate", config.root_package));
    imports.insert(format!("{}.internal.JsonSupport", config.root_package));
    imports.insert(format!(
        "{}.internal.NotFoundException",
        config.root_package
    ));
    imports.insert(format!(
        "{}.internal.NautilusException",
        config.root_package
    ));
    imports.insert(format!("{}.internal.RpcCaller", config.root_package));
    imports.insert(format!(
        "{}.model.{}",
        config.root_package, model.logical_name
    ));
    imports.insert("com.fasterxml.jackson.databind.JsonNode".to_string());
    imports.insert("com.fasterxml.jackson.databind.node.ArrayNode".to_string());
    imports.insert("com.fasterxml.jackson.databind.node.ObjectNode".to_string());
    imports.insert("java.util.List".to_string());
    if config.is_async {
        imports.insert("java.util.concurrent.CompletableFuture".to_string());
    }
    imports.insert("java.util.function.Consumer".to_string());

    let context = Context::from_serialize(&DelegateTemplateContext {
        package_name: config.root_package.clone(),
        imports: imports.into_iter().collect(),
        name: delegate_name,
        model_name: model.logical_name.clone(),
        dsl_name,
        is_async: config.is_async,
    })
    .expect("Java delegate context should serialize");
    render("java_delegate.tera", &context)
}

fn generate_dsl_file(
    config: &JavaConfig,
    model: &ModelIr,
    ir: &SchemaIr,
    enums: &BTreeMap<String, EnumIr>,
) -> String {
    let dsl_name = format!("{}Dsl", model.logical_name);
    let pk_fields = model.primary_key.fields();

    let mut imports = BTreeSet::new();
    imports.insert(format!("{}.internal.JsonSupport", config.root_package));
    imports.insert(format!("{}.internal.WireSerializable", config.root_package));
    imports.insert("com.fasterxml.jackson.databind.JsonNode".to_string());
    imports.insert("com.fasterxml.jackson.databind.node.ArrayNode".to_string());
    imports.insert("com.fasterxml.jackson.databind.node.ObjectNode".to_string());
    imports.insert("java.util.List".to_string());
    imports.insert("java.util.function.Consumer".to_string());

    for field in &model.fields {
        let (_, field_imports) =
            field_to_java_type(field, &config.root_package, &model.logical_name);
        imports.extend(field_imports);
    }

    let mut scalar_fields: Vec<DslScalarFieldCtx> = Vec::new();
    let mut create_fields: Vec<DslWritableFieldCtx> = Vec::new();
    let mut update_fields: Vec<DslWritableFieldCtx> = Vec::new();
    let mut numeric_field_names: Vec<String> = Vec::new();
    let mut orderable_field_names: Vec<String> = Vec::new();
    let mut vector_field_names: Vec<String> = Vec::new();
    let mut all_scalar_field_names: Vec<String> = Vec::new();

    for field in model.scalar_fields() {
        let (base_type, _) = field_base_type(field, &config.root_package, &model.logical_name);
        let filter_ops: Vec<DslFilterOpCtx> = filter_operators_for_field(field, enums)
            .into_iter()
            .map(|(suffix, java_type)| DslFilterOpCtx {
                suffix_pascal: suffix.to_upper_camel_case(),
                suffix,
                java_type,
            })
            .collect();

        scalar_fields.push(DslScalarFieldCtx {
            variant_name: field.logical_name.to_upper_camel_case(),
            name: field.logical_name.clone(),
            db_name: field.db_name.clone(),
            java_type: base_type,
            filter_ops,
        });

        all_scalar_field_names.push(field.logical_name.clone());

        if is_numeric_field(field) {
            numeric_field_names.push(field.logical_name.clone());
        }
        if field.is_vector() {
            vector_field_names.push(field.logical_name.clone());
        }
        if is_orderable_field(field) {
            orderable_field_names.push(field.logical_name.clone());
        }

        if is_writable_on_create(field) {
            let (ty, _) = field_to_java_type(field, "", &model.logical_name);
            create_fields.push(DslWritableFieldCtx {
                name: field.logical_name.clone(),
                db_name: field.db_name.clone(),
                java_type: ty,
            });
        }

        if is_writable_on_update(field, &pk_fields) {
            let (ty, _) = field_to_java_type(field, "", &model.logical_name);
            update_fields.push(DslWritableFieldCtx {
                name: field.logical_name.clone(),
                db_name: field.db_name.clone(),
                java_type: ty,
            });
        }
    }

    // Build relation field contexts (only models that exist in the IR).
    let relation_fields: Vec<DslRelationFieldCtx> = model
        .relation_fields()
        .filter_map(|field| {
            if let ResolvedFieldType::Relation(rel) = &field.field_type {
                if ir.models.contains_key(&rel.target_model) {
                    return Some(DslRelationFieldCtx {
                        name: field.logical_name.clone(),
                        target_model: rel.target_model.clone(),
                    });
                }
            }
            None
        })
        .collect();

    let context = Context::from_serialize(&DslTemplateContext {
        package_name: config.root_package.clone(),
        imports: imports.into_iter().collect(),
        name: dsl_name,
        scalar_fields,
        create_fields,
        update_fields,
        relation_fields,
        numeric_field_names,
        orderable_field_names,
        vector_field_names,
        all_scalar_field_names,
    })
    .expect("Java DSL context should serialize");
    render("java_dsl.tera", &context)
}

fn generate_transaction_client(config: &JavaConfig, models: &[ModelMeta]) -> String {
    let mut imports = BTreeSet::new();
    imports.insert(format!(
        "{}.internal.BaseNautilusClient",
        config.root_package
    ));
    imports.insert(format!(
        "{}.internal.BaseTransactionClient",
        config.root_package
    ));
    for model in models {
        imports.insert(format!(
            "{}.client.{}",
            config.root_package, model.delegate_name
        ));
    }

    let context = Context::from_serialize(&TransactionClientTemplateContext {
        package_name: config.root_package.clone(),
        imports: imports.into_iter().collect(),
        models: models
            .iter()
            .map(|model| ClientModelContext {
                camel: model.camel.clone(),
                delegate_name: model.delegate_name.clone(),
            })
            .collect(),
    })
    .expect("Java transaction client context should serialize");
    render("java_transaction_client.tera", &context)
}

fn generate_nautilus_client(config: &JavaConfig, models: &[ModelMeta]) -> String {
    let mut imports = BTreeSet::new();
    imports.insert(format!(
        "{}.internal.BaseNautilusClient",
        config.root_package
    ));
    imports.insert(format!(
        "{}.internal.GlobalNautilusRegistry",
        config.root_package
    ));
    imports.insert("com.fasterxml.jackson.databind.JsonNode".to_string());
    imports.insert("java.util.List".to_string());
    if config.is_async {
        imports.insert("java.util.concurrent.CompletableFuture".to_string());
        imports.insert("java.util.concurrent.CompletionException".to_string());
    }
    imports.insert("java.util.function.Function".to_string());
    for model in models {
        imports.insert(format!(
            "{}.client.{}",
            config.root_package, model.delegate_name
        ));
    }

    let context = Context::from_serialize(&NautilusTemplateContext {
        package_name: config.root_package.clone(),
        imports: imports.into_iter().collect(),
        schema_path_literal: format!("{:?}", config.schema_path),
        models: models
            .iter()
            .map(|model| ClientModelContext {
                camel: model.camel.clone(),
                delegate_name: model.delegate_name.clone(),
            })
            .collect(),
        is_async: config.is_async,
    })
    .expect("Java Nautilus client context should serialize");
    render("java_nautilus.tera", &context)
}

fn generate_model_field_read(model: &ModelIr, field: &FieldIr) -> String {
    match &field.field_type {
        ResolvedFieldType::Relation(rel) => {
            if field.is_array {
                format!(
                    "        List<{target}> {name} = JsonSupport.asList(JsonSupport.firstPresent(row, \"{logical}_json\"), {target}::fromJsonNode);\n",
                    target = rel.target_model,
                    name = field.logical_name,
                    logical = field.logical_name,
                )
            } else {
                format!(
                    "        {target} {name} = JsonSupport.asObject(JsonSupport.firstPresent(row, \"{logical}_json\"), {target}::fromJsonNode);\n",
                    target = rel.target_model,
                    name = field.logical_name,
                    logical = field.logical_name,
                )
            }
        }
        _ => generate_regular_field_read(
            &field.logical_name,
            &field.db_name,
            Some(&model.db_name),
            &field.field_type,
            field.is_array,
        ),
    }
}

fn generate_composite_field_read(field: &CompositeFieldIr) -> String {
    generate_regular_field_read(
        &field.logical_name,
        &field.db_name,
        None,
        &field.field_type,
        field.is_array,
    )
}

fn generate_regular_field_read(
    logical_name: &str,
    db_name: &str,
    table_name: Option<&str>,
    field_type: &ResolvedFieldType,
    is_array: bool,
) -> String {
    let source = match table_name {
        Some(table) => format!(
            "JsonSupport.firstPresent(row, \"{}__{}\", \"{}\")",
            table, db_name, logical_name
        ),
        None => format!(
            "JsonSupport.firstPresent(node, \"{}\", \"{}\")",
            db_name, logical_name
        ),
    };

    if is_array {
        match field_type {
            ResolvedFieldType::Scalar(scalar) => {
                let reader = array_reader_for_scalar(scalar);
                format!(
                    "        List<{ty}> {name} = JsonSupport.asList({source}, {reader});\n",
                    ty = base_java_type_name(field_type),
                    name = logical_name,
                )
            }
            ResolvedFieldType::Enum { enum_name } => format!(
                "        List<{enum_name}> {name} = JsonSupport.asList({source}, value -> JsonSupport.asEnum(value, {enum_name}.class));\n",
                name = logical_name,
            ),
            ResolvedFieldType::CompositeType { type_name } => format!(
                "        List<{type_name}> {name} = JsonSupport.asList({source}, {type_name}::fromJsonNode);\n",
                name = logical_name,
            ),
            ResolvedFieldType::Relation(_) => unreachable!(),
        }
    } else {
        match field_type {
            ResolvedFieldType::Scalar(scalar) => {
                let reader = scalar_reader_for_type(scalar);
                format!(
                    "        {ty} {name} = JsonSupport.{reader}({source});\n",
                    ty = base_java_type_name(field_type),
                    name = logical_name,
                )
            }
            ResolvedFieldType::Enum { enum_name } => format!(
                "        {enum_name} {name} = JsonSupport.asEnum({source}, {enum_name}.class);\n",
                name = logical_name,
            ),
            ResolvedFieldType::CompositeType { type_name } => format!(
                "        {type_name} {name} = JsonSupport.asObject({source}, {type_name}::fromJsonNode);\n",
                name = logical_name,
            ),
            ResolvedFieldType::Relation(_) => unreachable!(),
        }
    }
}

fn base_java_type_name(field_type: &ResolvedFieldType) -> &'static str {
    match field_type {
        ResolvedFieldType::Scalar(ScalarType::String)
        | ResolvedFieldType::Scalar(ScalarType::Citext)
        | ResolvedFieldType::Scalar(ScalarType::Ltree)
        | ResolvedFieldType::Scalar(ScalarType::Xml)
        | ResolvedFieldType::Scalar(ScalarType::Char { .. })
        | ResolvedFieldType::Scalar(ScalarType::VarChar { .. }) => "String",
        ResolvedFieldType::Scalar(ScalarType::Hstore) => "JsonSupport.Hstore",
        ResolvedFieldType::Scalar(ScalarType::Vector { .. }) => "List<Float>",
        ResolvedFieldType::Scalar(ScalarType::Boolean) => "Boolean",
        ResolvedFieldType::Scalar(ScalarType::Int) => "Integer",
        ResolvedFieldType::Scalar(ScalarType::BigInt) => "Long",
        ResolvedFieldType::Scalar(ScalarType::Float) => "Double",
        ResolvedFieldType::Scalar(ScalarType::Decimal { .. }) => "BigDecimal",
        ResolvedFieldType::Scalar(ScalarType::DateTime) => "OffsetDateTime",
        ResolvedFieldType::Scalar(ScalarType::Bytes) => "byte[]",
        ResolvedFieldType::Scalar(ScalarType::Json)
        | ResolvedFieldType::Scalar(ScalarType::Jsonb) => "JsonNode",
        ResolvedFieldType::Scalar(ScalarType::Uuid) => "UUID",
        _ => "Object",
    }
}

fn scalar_reader_for_type(scalar: &ScalarType) -> &'static str {
    match scalar {
        ScalarType::String
        | ScalarType::Citext
        | ScalarType::Ltree
        | ScalarType::Xml
        | ScalarType::Char { .. }
        | ScalarType::VarChar { .. } => "asString",
        ScalarType::Hstore => "asHstore",
        ScalarType::Vector { .. } => "asFloatList",
        ScalarType::Boolean => "asBoolean",
        ScalarType::Int => "asInteger",
        ScalarType::BigInt => "asLong",
        ScalarType::Float => "asDouble",
        ScalarType::Decimal { .. } => "asBigDecimal",
        ScalarType::DateTime => "asOffsetDateTime",
        ScalarType::Bytes => "asBytes",
        ScalarType::Json | ScalarType::Jsonb => "asJsonNode",
        ScalarType::Uuid => "asUuid",
    }
}

fn array_reader_for_scalar(scalar: &ScalarType) -> &'static str {
    match scalar {
        ScalarType::String
        | ScalarType::Citext
        | ScalarType::Ltree
        | ScalarType::Xml
        | ScalarType::Char { .. }
        | ScalarType::VarChar { .. } => "JsonSupport::asString",
        ScalarType::Hstore => "JsonSupport::asHstore",
        ScalarType::Vector { .. } => "JsonSupport::asFloatList",
        ScalarType::Boolean => "JsonSupport::asBoolean",
        ScalarType::Int => "JsonSupport::asInteger",
        ScalarType::BigInt => "JsonSupport::asLong",
        ScalarType::Float => "JsonSupport::asDouble",
        ScalarType::Decimal { .. } => "JsonSupport::asBigDecimal",
        ScalarType::DateTime => "JsonSupport::asOffsetDateTime",
        ScalarType::Bytes => "JsonSupport::asBytes",
        ScalarType::Json | ScalarType::Jsonb => "JsonSupport::asJsonNode",
        ScalarType::Uuid => "JsonSupport::asUuid",
    }
}

fn sorted_named<T, F>(items: impl Iterator<Item = T>, key: F) -> Vec<T>
where
    T: Clone,
    F: Fn(&T) -> String,
{
    let mut values: Vec<T> = items.collect();
    values.sort_by_key(|item| key(item));
    values
}

fn sorted_model_meta<'a>(models: impl Iterator<Item = &'a ModelIr>) -> Vec<ModelMeta> {
    let mut values: Vec<ModelMeta> = models
        .map(|model| ModelMeta {
            name: model.logical_name.clone(),
            camel: model.logical_name.to_lower_camel_case(),
            delegate_name: format!("{}Delegate", model.logical_name),
        })
        .collect();
    values.sort_by(|left, right| left.name.cmp(&right.name));
    values
}

fn java_source_path(root_package: &str, subpackage: &str, file_name: &str) -> String {
    let package_path = root_package.replace('.', "/");
    format!(
        "src/main/java/{}/{}/{}",
        package_path, subpackage, file_name
    )
}

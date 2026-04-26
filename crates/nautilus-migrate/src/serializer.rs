//! Serializer: converts a [`LiveSchema`] snapshot into canonical `.nautilus` source text.
//!
//! Used by `nautilus db pull` to introspect an existing database and emit a
//! schema file that can be fed back into `db push`.

use std::collections::{HashMap, HashSet};

use crate::live::LiveIndexKind;
use crate::{
    ddl::DatabaseProvider,
    live::{ComputedKind, LiveCompositeType, LiveForeignKey, LiveSchema, LiveTable},
};
use nautilus_schema::ir::{BasicIndexType, PgvectorIndexOptions};
use nautilus_schema::{
    bool_expr::{parse_bool_expr, BoolExpr, Operand},
    sql_expr::{parse_sql_expr, SqlExpr},
    Lexer, Span, Token, TokenKind,
};

/// Naming mode for identifiers emitted by `db pull`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PullNameCase {
    /// Preserve the current serializer behaviour.
    #[default]
    Auto,
    /// Render identifiers in `snake_case`.
    Snake,
    /// Render identifiers in `PascalCase`.
    Pascal,
}

/// Naming options used when serialising an introspected schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PullNamingOptions {
    /// Logical model name rendering mode.
    pub model_case: PullNameCase,
    /// Logical field name rendering mode.
    pub field_case: PullNameCase,
}

#[derive(Debug, Clone)]
struct ForwardRelation {
    fk_index: usize,
    field_name: String,
    relation_name: Option<String>,
}

#[derive(Debug, Clone)]
struct BackRelation {
    owning_table: String,
    field_name: String,
    relation_name: Option<String>,
    is_one_to_one: bool,
}

#[derive(Debug, Clone)]
struct TableNamingContext {
    model_name: String,
    db_to_logical_field: HashMap<String, String>,
    logical_field_order: Vec<String>,
}

/// Convert a [`LiveSchema`] to a `.nautilus` schema source string.
///
/// * `live` - the introspected live database schema
/// * `provider` - which SQL dialect was used during introspection
/// * `url` - the raw database URL written into the datasource block verbatim
pub fn serialize_live_schema(live: &LiveSchema, provider: DatabaseProvider, url: &str) -> String {
    serialize_live_schema_with_options(live, provider, url, PullNamingOptions::default())
}

/// Convert a [`LiveSchema`] to a `.nautilus` schema source string using custom
/// logical naming options for models and fields.
pub fn serialize_live_schema_with_options(
    live: &LiveSchema,
    provider: DatabaseProvider,
    url: &str,
    options: PullNamingOptions,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    parts.push(render_datasource_block(live, provider, url));

    let mut ct_names: Vec<&String> = live.composite_types.keys().collect();
    ct_names.sort();
    for ct_db_name in ct_names {
        let ct = &live.composite_types[ct_db_name];
        let type_name = to_pascal_case(ct_db_name);
        let mut used_field_names = HashSet::new();
        let composite_fields: Vec<(String, &crate::live::LiveCompositeField)> = ct
            .fields
            .iter()
            .map(|field| {
                let logical_name = choose_unique_field_name(
                    vec![apply_scalar_field_case(&field.name, options.field_case)],
                    &mut used_field_names,
                );
                (logical_name, field)
            })
            .collect();
        let max_name = composite_fields
            .iter()
            .map(|(logical_name, _)| logical_name.len())
            .max()
            .unwrap_or(0);
        let mut lines = vec![format!("type {} {{", type_name)];
        for (logical_name, field) in &composite_fields {
            let nautilus_type =
                infer_nautilus_type(&field.col_type, &live.enums, &live.composite_types);
            if logical_name == &field.name {
                lines.push(format!(
                    "  {:<name_w$}  {}",
                    logical_name,
                    nautilus_type,
                    name_w = max_name,
                ));
            } else {
                lines.push(format!(
                    "  {:<name_w$}  {}  @map(\"{}\")",
                    logical_name,
                    nautilus_type,
                    escape_schema_string(&field.name),
                    name_w = max_name,
                ));
            }
        }
        lines.push("}".to_string());
        parts.push(lines.join("\n"));
    }

    let mut enum_names: Vec<&String> = live.enums.keys().collect();
    enum_names.sort();
    for enum_db_name in enum_names {
        let variants = &live.enums[enum_db_name];
        let type_name = to_pascal_case(enum_db_name);
        let mut lines = vec![format!("enum {} {{", type_name)];
        for variant in variants {
            lines.push(format!("  {}", variant));
        }
        lines.push("}".to_string());
        parts.push(lines.join("\n"));
    }

    let mut table_names: Vec<&String> = live.tables.keys().collect();
    table_names.sort();
    let table_naming = build_table_naming_contexts(live, &table_names, options);
    let relation_pair_counts = build_relation_pair_counts(live, &table_names);
    let directional_relation_counts = build_directional_relation_counts(live, &table_names);
    let forward_relations = build_forward_relations(
        live,
        &table_names,
        &table_naming,
        &relation_pair_counts,
        options,
    );
    let back_relations = build_back_relations(
        live,
        &table_names,
        &table_naming,
        &forward_relations,
        &directional_relation_counts,
        options,
    );

    for table_name in &table_names {
        let table = &live.tables[*table_name];
        let naming = &table_naming[*table_name];
        let model_name = &naming.model_name;
        let is_composite_pk = table.primary_key.len() > 1;

        let max_name = naming
            .logical_field_order
            .iter()
            .map(|name| name.len())
            .max()
            .unwrap_or(0);
        let max_type = table
            .columns
            .iter()
            .map(|c| {
                let t = infer_nautilus_type(&c.col_type, &live.enums, &live.composite_types);
                let nullable_suffix = if c.nullable && type_supports_optional_modifier(&t) {
                    1
                } else {
                    0
                };
                t.len() + nullable_suffix
            })
            .max()
            .unwrap_or(0);

        let mut lines = vec![format!("model {} {{", model_name)];

        for (index, col) in table.columns.iter().enumerate() {
            let logical_field_name = &naming.logical_field_order[index];
            let type_str = infer_nautilus_type(&col.col_type, &live.enums, &live.composite_types);
            let type_with_mod = if col.nullable && type_supports_optional_modifier(&type_str) {
                format!("{}?", type_str)
            } else {
                type_str
            };

            let is_pk_col = table.primary_key.contains(&col.name);
            let mut attrs: Vec<String> = Vec::new();

            if is_pk_col && !is_composite_pk {
                attrs.push("@id".to_string());
            }
            if let (Some(expr), Some(kind)) = (&col.generated_expr, &col.computed_kind) {
                let kind_str = match kind {
                    ComputedKind::Stored => "Stored",
                    ComputedKind::Virtual => "Virtual",
                };
                attrs.push(format!(
                    "@computed({}, {})",
                    remap_sql_expr_identifiers(expr, &naming.db_to_logical_field),
                    kind_str
                ));
            } else if let Some(def) = &col.default_value {
                if let Some(attr) = infer_default_attr(def, &col.col_type, &live.enums) {
                    attrs.push(attr);
                }
            }
            if let Some(check) = &col.check_expr {
                attrs.push(format!(
                    "@check({})",
                    remap_bool_expr_identifiers(check, &naming.db_to_logical_field)
                ));
            }
            if logical_field_name != &col.name {
                attrs.push(format!("@map(\"{}\")", escape_schema_string(&col.name)));
            }

            let line = if attrs.is_empty() {
                format!("  {}  {}", logical_field_name, type_with_mod)
            } else {
                format!(
                    "  {:<name_w$}  {:<type_w$}  {}",
                    logical_field_name,
                    type_with_mod,
                    attrs.join("  "),
                    name_w = max_name,
                    type_w = max_type,
                )
            };
            lines.push(line.trim_end().to_string());
        }

        for relation in forward_relations
            .get(*table_name)
            .into_iter()
            .flat_map(|relations| relations.iter())
        {
            let fk = &table.foreign_keys[relation.fk_index];
            let ref_model = &table_naming[&fk.referenced_table].model_name;

            let is_nullable = fk.columns.iter().any(|col_name| {
                table
                    .columns
                    .iter()
                    .find(|c| &c.name == col_name)
                    .map(|c| c.nullable)
                    .unwrap_or(true)
            });
            let type_str = if is_nullable {
                format!("{}?", ref_model)
            } else {
                ref_model.clone()
            };

            let fields_list = fk
                .columns
                .iter()
                .map(|column| logical_field_name(naming, column))
                .collect::<Vec<_>>()
                .join(", ");
            let target_naming = &table_naming[&fk.referenced_table];
            let references_list = fk
                .referenced_columns
                .iter()
                .map(|column| logical_field_name(target_naming, column))
                .collect::<Vec<_>>()
                .join(", ");
            let mut rel_args: Vec<String> = Vec::new();
            if let Some(relation_name) = &relation.relation_name {
                rel_args.push(format!("name: \"{}\"", escape_schema_string(relation_name)));
            }
            rel_args.push(format!(
                "fields: [{}], references: [{}]",
                fields_list, references_list
            ));
            if let Some(action) = &fk.on_delete {
                rel_args.push(format!("onDelete: {}", render_referential_action(action)));
            }
            if let Some(action) = &fk.on_update {
                rel_args.push(format!("onUpdate: {}", render_referential_action(action)));
            }
            lines.push(format!(
                "  {}  {}  @relation({})",
                relation.field_name,
                type_str,
                rel_args.join(", ")
            ));
        }

        if let Some(refs) = back_relations.get(*table_name) {
            for relation in refs {
                let owning_model = &table_naming[&relation.owning_table].model_name;
                let relation_type = if relation.is_one_to_one {
                    format!("{}?", owning_model)
                } else {
                    format!("{}[]", owning_model)
                };
                if let Some(relation_name) = &relation.relation_name {
                    lines.push(format!(
                        "  {}  {}  @relation(name: \"{}\")",
                        relation.field_name,
                        relation_type,
                        escape_schema_string(relation_name)
                    ));
                } else {
                    lines.push(format!("  {}  {}", relation.field_name, relation_type));
                }
            }
        }

        if is_composite_pk {
            lines.push(format!(
                "  @@id([{}])",
                table
                    .primary_key
                    .iter()
                    .map(|column| logical_field_name(naming, column))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        // Keep @@map explicit so the model/table mapping survives round-trips.
        lines.push(format!("  @@map(\"{}\")", table_name));

        for idx in &table.indexes {
            if idx.unique {
                lines.push(format!(
                    "  @@unique([{}])",
                    idx.columns
                        .iter()
                        .map(|column| logical_field_name(naming, column))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            } else {
                let mut args = Vec::new();
                match &idx.kind {
                    LiveIndexKind::Unknown(_) => {}
                    LiveIndexKind::Basic(b) => {
                        if !matches!(b, BasicIndexType::BTree) {
                            args.push(format!("type: {}", b.as_str()));
                        }
                    }
                    LiveIndexKind::Pgvector(p) => {
                        args.push(format!("type: {}", p.method.as_str()));
                        if let Some(opclass) = p.opclass {
                            args.push(format!("opclass: {}", opclass.as_str()));
                        }
                        push_pgvector_option_args(&mut args, &p.options);
                    }
                }
                let default_name = default_index_name(table_name, &idx.columns);
                if idx.name != default_name {
                    args.push(format!("map: \"{}\"", idx.name));
                }

                if args.is_empty() {
                    lines.push(format!(
                        "  @@index([{}])",
                        idx.columns
                            .iter()
                            .map(|column| logical_field_name(naming, column))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                } else {
                    lines.push(format!(
                        "  @@index([{}], {})",
                        idx.columns
                            .iter()
                            .map(|column| logical_field_name(naming, column))
                            .collect::<Vec<_>>()
                            .join(", "),
                        args.join(", ")
                    ));
                }
            }
        }

        for check in &table.check_constraints {
            lines.push(format!(
                "  @@check({})",
                remap_bool_expr_identifiers(check, &naming.db_to_logical_field)
            ));
        }

        lines.push("}".to_string());
        parts.push(lines.join("\n"));
    }

    let mut out = parts.join("\n\n");
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

fn render_datasource_block(live: &LiveSchema, provider: DatabaseProvider, url: &str) -> String {
    let mut fields = vec![
        (
            "provider".to_string(),
            format!("\"{}\"", provider.schema_provider_name()),
        ),
        (
            "url".to_string(),
            format!("\"{}\"", escape_schema_string(url)),
        ),
    ];

    if provider == DatabaseProvider::Postgres && !live.extensions.is_empty() {
        let mut extensions: Vec<(&str, &str)> = live
            .extensions
            .iter()
            .map(|(name, state)| (name.as_str(), state.schema.as_str()))
            .collect();
        extensions.sort_unstable_by(|a, b| a.0.cmp(b.0));
        fields.push((
            "extensions".to_string(),
            format!(
                "[{}]",
                extensions
                    .iter()
                    .map(|(name, schema)| render_extension_entry(name, schema))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }

    let max_key = fields.iter().map(|(key, _)| key.len()).max().unwrap_or(0);
    let mut lines = vec!["datasource db {".to_string()];
    for (key, value) in fields {
        let padding = max_key - key.len() + 1;
        lines.push(format!("  {}{}= {}", key, " ".repeat(padding), value));
    }
    lines.push("}".to_string());
    lines.join("\n")
}

/// Infer the `.nautilus` scalar type name from a normalised SQL type string.
///
/// `enums` is the map of live enum type names (lower-cased) to their variants.
/// `composite_types` is the map of live composite type names (lower-cased) to their definitions.
/// When `sql_type` matches a known enum or composite type the corresponding PascalCase name
/// is returned.  Array types (ending with `[]`) are handled recursively.
/// Unrecognised types fall back to `String`.
fn infer_nautilus_type(
    sql_type: &str,
    enums: &HashMap<String, Vec<String>>,
    composite_types: &HashMap<String, LiveCompositeType>,
) -> String {
    let t = sql_type.trim().to_lowercase();

    if let Some(inner) = t.strip_suffix("[]") {
        let inner_type = infer_nautilus_type(inner, enums, composite_types);
        return format!("{}[]", inner_type);
    }

    if let Some(enum_name) = matching_named_type(t.as_str(), enums) {
        return to_pascal_case(enum_name);
    }

    if let Some(composite_name) = matching_named_type(t.as_str(), composite_types) {
        return to_pascal_case(composite_name);
    }

    if let Some(inner) = t
        .strip_prefix("decimal(")
        .or_else(|| t.strip_prefix("numeric("))
    {
        if let Some(inner) = inner.strip_suffix(')') {
            let parts: Vec<&str> = inner.splitn(2, ',').collect();
            if parts.len() == 2 {
                let p = parts[0].trim();
                let s = parts[1].trim();
                return format!("Decimal({}, {})", p, s);
            }
        }
    }

    if let Some(length) = parse_sized_type_length(&t, "varchar(")
        .or_else(|| parse_sized_type_length(&t, "character varying("))
    {
        return format!("VarChar({})", length);
    }

    if let Some(dimension) = parse_sized_type_length(&t, "vector(") {
        return format!("Vector({})", dimension);
    }

    if let Some(length) =
        parse_sized_type_length(&t, "char(").or_else(|| parse_sized_type_length(&t, "character("))
    {
        if length == 36 {
            return "Uuid".to_string();
        }
        return format!("Char({})", length);
    }

    match t.as_str() {
        "text" | "clob" => "String".to_string(),
        "citext" => "Citext".to_string(),
        "hstore" => "Hstore".to_string(),
        "ltree" => "Ltree".to_string(),
        t if t.starts_with("varchar") || t.starts_with("character varying") => "String".to_string(),
        "uuid" | "char(36)" => "Uuid".to_string(),
        t if t.starts_with("char(") && !t.starts_with("char(36") => "String".to_string(),
        "integer" | "int" | "int4" | "int2" | "smallint" | "tinyint" | "mediumint" => {
            "Int".to_string()
        }
        "bigint" | "int8" | "bigserial" | "unsigned bigint" => "BigInt".to_string(),
        "boolean" | "bool" => "Boolean".to_string(),
        "real" | "float4" | "double precision" | "float8" | "double" | "float" => {
            "Float".to_string()
        }
        "decimal" | "numeric" => "Float".to_string(),
        "timestamp"
        | "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamptz"
        | "datetime" => "DateTime".to_string(),
        "bytea" | "blob" | "binary" | "varbinary" => "Bytes".to_string(),
        "json" => "Json".to_string(),
        "jsonb" => "Jsonb".to_string(),
        _ => "String".to_string(),
    }
}

/// Try to produce a `@default(...)` attribute from a raw DEFAULT expression
/// string as returned by the database. Returns `None` when the default is too
/// complex to round-trip safely.
fn infer_default_attr(
    raw: &str,
    col_type: &str,
    enums: &HashMap<String, Vec<String>>,
) -> Option<String> {
    let t = raw.trim().to_lowercase();

    if t.contains("nextval") || t.contains("autoincrement") {
        if can_infer_autoincrement(col_type) {
            return Some("@default(autoincrement())".to_string());
        }
        return None;
    }

    if t == "true" || t == "false" {
        return Some(format!("@default({})", t));
    }

    if t.parse::<f64>().is_ok() {
        return Some(format!("@default({})", t));
    }

    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        let inner = &raw.trim()[1..raw.trim().len() - 1];
        let base_type = col_type.trim().to_lowercase();
        let base_type = base_type.strip_suffix("[]").unwrap_or(&base_type);
        if matching_named_type(base_type, enums).is_some() {
            return Some(format!("@default({})", inner));
        }
        return Some(format!("@default(\"{}\")", inner));
    }

    if t == "now()" || t == "current_timestamp" || t.starts_with("current_timestamp") {
        return Some("@default(now())".to_string());
    }

    if t.contains("uuid") || t.contains("newid") {
        return Some("@default(uuid())".to_string());
    }

    None
}

fn matching_named_type<'a, T>(
    candidate: &str,
    named_types: &'a HashMap<String, T>,
) -> Option<&'a str> {
    named_types
        .keys()
        .find(|type_name| type_name.eq_ignore_ascii_case(candidate))
        .map(String::as_str)
}

/// Infer a logical relation field name from FK columns and the referenced table.
///
/// Examples:
/// - columns = `["user_id"]`  -> `"user"`   (strip `_id` suffix)
/// - columns = `["author_id"]` -> `"author"`
/// - columns = `["a_id", "b_id"]` -> singular form of `referenced_table`
fn infer_relation_field_name(fk_cols: &[String], ref_table: &str) -> String {
    if fk_cols.len() == 1 {
        let col = &fk_cols[0];
        if let Some(name) = col.strip_suffix("_id") {
            if !name.is_empty() {
                return name.to_string();
            }
        }
    }
    singular_name(ref_table)
}

fn build_table_naming_contexts(
    live: &LiveSchema,
    table_names: &[&String],
    options: PullNamingOptions,
) -> HashMap<String, TableNamingContext> {
    let mut contexts = HashMap::new();
    let mut used_model_names = HashSet::new();

    for &table_name in table_names {
        let table = &live.tables[table_name];
        let model_name = choose_unique_field_name(
            vec![apply_model_case(table_name, options.model_case)],
            &mut used_model_names,
        );

        let mut used_field_names = HashSet::new();
        let mut db_to_logical_field = HashMap::new();
        let mut logical_field_order = Vec::new();

        for column in &table.columns {
            let logical_name = choose_unique_field_name(
                vec![apply_scalar_field_case(&column.name, options.field_case)],
                &mut used_field_names,
            );
            db_to_logical_field.insert(column.name.clone(), logical_name.clone());
            logical_field_order.push(logical_name);
        }

        contexts.insert(
            table_name.clone(),
            TableNamingContext {
                model_name,
                db_to_logical_field,
                logical_field_order,
            },
        );
    }

    contexts
}

fn build_relation_pair_counts(
    live: &LiveSchema,
    table_names: &[&String],
) -> HashMap<(String, String), usize> {
    let mut counts = HashMap::new();
    for &table_name in table_names {
        for fk in &live.tables[table_name].foreign_keys {
            *counts
                .entry(relation_pair_key(table_name, &fk.referenced_table))
                .or_insert(0) += 1;
        }
    }
    counts
}

fn build_directional_relation_counts(
    live: &LiveSchema,
    table_names: &[&String],
) -> HashMap<(String, String), usize> {
    let mut counts = HashMap::new();
    for &table_name in table_names {
        for fk in &live.tables[table_name].foreign_keys {
            *counts
                .entry((table_name.clone(), fk.referenced_table.clone()))
                .or_insert(0) += 1;
        }
    }
    counts
}

fn build_forward_relations(
    live: &LiveSchema,
    table_names: &[&String],
    table_naming: &HashMap<String, TableNamingContext>,
    relation_pair_counts: &HashMap<(String, String), usize>,
    options: PullNamingOptions,
) -> HashMap<String, Vec<ForwardRelation>> {
    let mut result = HashMap::new();

    for &table_name in table_names {
        let table = &live.tables[table_name];
        let mut used_fields: HashSet<String> = table_naming[table_name]
            .logical_field_order
            .iter()
            .cloned()
            .collect();
        let mut relations = Vec::new();

        for (fk_index, fk) in table.foreign_keys.iter().enumerate() {
            let base_name = relation_field_name_base(&fk.columns, &fk.referenced_table);
            let fallback_name = apply_derived_field_case(
                &to_snake_case_identifier(&singular_name(&fk.referenced_table)),
                options.field_case,
            );
            let mut candidates = vec![apply_derived_field_case(&base_name, options.field_case)];
            if fallback_name != candidates[0] {
                candidates.push(fallback_name);
            }
            if let Some(first_col) = fk.columns.first() {
                let qualified = apply_derived_field_case(
                    &format!("{}_{}", base_name, to_snake_case_identifier(first_col)),
                    options.field_case,
                );
                if qualified != candidates[0] {
                    candidates.push(qualified);
                }
            }

            let field_name = choose_unique_field_name(candidates, &mut used_fields);
            let relation_name = needs_explicit_relation_name(
                table_name,
                &fk.referenced_table,
                relation_pair_counts,
            )
            .then(|| format!("{}_{}", table_naming[table_name].model_name, field_name));

            relations.push(ForwardRelation {
                fk_index,
                field_name,
                relation_name,
            });
        }

        result.insert(table_name.clone(), relations);
    }

    result
}

fn build_back_relations(
    live: &LiveSchema,
    table_names: &[&String],
    table_naming: &HashMap<String, TableNamingContext>,
    forward_relations: &HashMap<String, Vec<ForwardRelation>>,
    directional_relation_counts: &HashMap<(String, String), usize>,
    options: PullNamingOptions,
) -> HashMap<String, Vec<BackRelation>> {
    type IncomingEntry = (String, String, Option<String>, bool);
    let mut incoming: HashMap<String, Vec<IncomingEntry>> = HashMap::new();

    for &table_name in table_names {
        let table = &live.tables[table_name];
        for relation in forward_relations
            .get(table_name)
            .into_iter()
            .flat_map(|relations| relations.iter())
        {
            let fk = &table.foreign_keys[relation.fk_index];
            incoming
                .entry(fk.referenced_table.clone())
                .or_default()
                .push((
                    table_name.clone(),
                    relation.field_name.clone(),
                    relation.relation_name.clone(),
                    is_one_to_one_back_relation(live, table_name, fk),
                ));
        }
    }

    let mut result = HashMap::new();

    for &table_name in table_names {
        let mut used_fields: HashSet<String> = table_naming[table_name]
            .logical_field_order
            .iter()
            .cloned()
            .collect();
        if let Some(relations) = forward_relations.get(table_name) {
            used_fields.extend(relations.iter().map(|relation| relation.field_name.clone()));
        }

        let mut back_refs = Vec::new();
        if let Some(entries) = incoming.remove(table_name) {
            for (owning_table, forward_field_name, relation_name, is_one_to_one) in entries {
                let is_self_relation = owning_table == *table_name;
                let default_name =
                    default_back_relation_field_name(&owning_table, is_one_to_one, options);
                let qualified_name =
                    qualify_back_relation_field_name(&default_name, &forward_field_name, options);
                let direction_count = directional_relation_counts
                    .get(&(owning_table.clone(), table_name.clone()))
                    .copied()
                    .unwrap_or(0);

                let mut candidates = Vec::new();
                if direction_count <= 1 {
                    candidates.push(default_name.clone());
                }
                if qualified_name != default_name {
                    candidates.push(qualified_name);
                }
                candidates.push(default_name);

                let field_name = choose_unique_field_name(candidates, &mut used_fields);
                back_refs.push(BackRelation {
                    owning_table,
                    field_name,
                    relation_name: if is_self_relation {
                        None
                    } else {
                        relation_name
                    },
                    is_one_to_one,
                });
            }
        }

        result.insert(table_name.clone(), back_refs);
    }

    result
}

fn relation_pair_key(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.to_string(), right.to_string())
    } else {
        (right.to_string(), left.to_string())
    }
}

fn needs_explicit_relation_name(
    owning_table: &str,
    referenced_table: &str,
    relation_pair_counts: &HashMap<(String, String), usize>,
) -> bool {
    owning_table == referenced_table
        || relation_pair_counts
            .get(&relation_pair_key(owning_table, referenced_table))
            .copied()
            .unwrap_or(0)
            > 1
}

fn relation_field_name_base(fk_cols: &[String], ref_table: &str) -> String {
    let raw = infer_relation_field_name(fk_cols, ref_table);
    let normalized = to_snake_case_identifier(&raw);
    if normalized.is_empty() {
        "relation".to_string()
    } else {
        normalized
    }
}

fn default_back_relation_field_name(
    owning_table: &str,
    is_one_to_one: bool,
    options: PullNamingOptions,
) -> String {
    let singular = to_snake_case_identifier(&singular_name(owning_table));
    if is_one_to_one {
        apply_derived_field_case(&singular, options.field_case)
    } else {
        apply_derived_field_case(&pluralize_name(&singular), options.field_case)
    }
}

fn qualify_back_relation_field_name(
    default_name: &str,
    forward_field_name: &str,
    options: PullNamingOptions,
) -> String {
    apply_derived_field_case(
        &format!(
            "{}_{}",
            to_snake_case_identifier(default_name),
            to_snake_case_identifier(forward_field_name)
        ),
        options.field_case,
    )
}

fn choose_unique_field_name(candidates: Vec<String>, used_fields: &mut HashSet<String>) -> String {
    let mut first_candidate = None;
    for candidate in candidates {
        let candidate = sanitize_logical_identifier(&candidate);
        if candidate.is_empty() {
            continue;
        }
        if first_candidate.is_none() {
            first_candidate = Some(candidate.clone());
        }
        if used_fields.insert(candidate.clone()) {
            return candidate;
        }
    }

    let base = first_candidate.unwrap_or_else(|| "relation".to_string());
    let mut suffix = 2usize;
    loop {
        let candidate = sanitize_logical_identifier(&format!("{}_{}", base, suffix));
        if used_fields.insert(candidate.clone()) {
            return candidate;
        }
        suffix += 1;
    }
}

fn escape_schema_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn render_extension_schema_name(name: &str) -> String {
    if is_bare_schema_identifier(name) {
        name.to_string()
    } else {
        format!("\"{}\"", escape_schema_string(name))
    }
}

/// Render a single array entry for the `extensions = [...]` field in a
/// serialized datasource block.
///
/// Round-trips live state back into source: when the extension lives in the
/// default `public` namespace we emit the compact form (`pg_trgm` or
/// `"uuid-ossp"`); any other schema is captured explicitly via the structured
/// `extension(name = ..., schema = "...")` syntax so `db pull` does not
/// silently drop the namespace information.
fn render_extension_entry(name: &str, schema: &str) -> String {
    if schema == "public" {
        render_extension_schema_name(name)
    } else {
        format!(
            "extension(name = {}, schema = \"{}\")",
            render_extension_schema_name(name),
            escape_schema_string(schema)
        )
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

fn logical_field_name(naming: &TableNamingContext, db_column_name: &str) -> String {
    naming
        .db_to_logical_field
        .get(db_column_name)
        .cloned()
        .unwrap_or_else(|| db_column_name.to_string())
}

fn sanitize_logical_identifier(name: &str) -> String {
    let mut candidate = name
        .chars()
        .map(|ch| {
            if ch.is_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();

    if candidate.is_empty() {
        candidate.push('_');
    }

    if candidate
        .chars()
        .next()
        .is_some_and(|ch| !ch.is_alphabetic() && ch != '_')
    {
        candidate.insert(0, '_');
    }

    if TokenKind::from_ident(&candidate).is_keyword() {
        candidate.push('_');
    }

    candidate
}

fn apply_model_case(name: &str, case: PullNameCase) -> String {
    match case {
        PullNameCase::Auto => to_pascal_case(name),
        PullNameCase::Snake => to_snake_case_identifier(name),
        PullNameCase::Pascal => normalized_pascal_case(name),
    }
}

fn apply_scalar_field_case(name: &str, case: PullNameCase) -> String {
    match case {
        PullNameCase::Auto => name.to_string(),
        PullNameCase::Snake => to_snake_case_identifier(name),
        PullNameCase::Pascal => normalized_pascal_case(name),
    }
}

fn apply_derived_field_case(name: &str, case: PullNameCase) -> String {
    match case {
        PullNameCase::Auto | PullNameCase::Snake => to_snake_case_identifier(name),
        PullNameCase::Pascal => normalized_pascal_case(name),
    }
}

fn normalized_pascal_case(name: &str) -> String {
    let snake = to_snake_case_identifier(name);
    if snake.is_empty() {
        String::new()
    } else {
        to_pascal_case(&snake)
    }
}

fn remap_sql_expr_identifiers(expr: &str, field_map: &HashMap<String, String>) -> String {
    parse_schema_sql_expr(expr)
        .map(|parsed| render_sql_expr_with_field_map(&parsed, field_map))
        .unwrap_or_else(|| expr.to_string())
}

fn remap_bool_expr_identifiers(expr: &str, field_map: &HashMap<String, String>) -> String {
    parse_schema_bool_expr(expr)
        .map(|parsed| render_bool_expr_with_field_map(&parsed, field_map))
        .unwrap_or_else(|| expr.to_string())
}

fn parse_schema_sql_expr(expr: &str) -> Option<SqlExpr> {
    let tokens = lex_expression_tokens(expr).ok()?;
    parse_sql_expr(&tokens, Span::new(0, expr.len())).ok()
}

fn parse_schema_bool_expr(expr: &str) -> Option<BoolExpr> {
    let tokens = lex_expression_tokens(expr).ok()?;
    parse_bool_expr(&tokens, Span::new(0, expr.len())).ok()
}

fn lex_expression_tokens(expr: &str) -> nautilus_schema::Result<Vec<Token>> {
    let mut lexer = Lexer::new(expr);
    let mut tokens = Vec::new();
    loop {
        let token = lexer.next_token()?;
        if matches!(token.kind, TokenKind::Eof) {
            break;
        }
        tokens.push(token);
    }
    Ok(tokens)
}

fn render_sql_expr_with_field_map(expr: &SqlExpr, field_map: &HashMap<String, String>) -> String {
    match expr {
        SqlExpr::Ident(name) => field_map.get(name).cloned().unwrap_or_else(|| name.clone()),
        SqlExpr::Number(n) => n.clone(),
        SqlExpr::StringLit(s) => format!("\"{}\"", s),
        SqlExpr::Bool(b) => b.to_string(),
        SqlExpr::BinaryOp { left, op, right } => format!(
            "{} {} {}",
            render_sql_expr_with_field_map(left, field_map),
            op,
            render_sql_expr_with_field_map(right, field_map)
        ),
        SqlExpr::UnaryOp { op, operand } => {
            format!(
                "{}{}",
                op,
                render_sql_expr_with_field_map(operand, field_map)
            )
        }
        SqlExpr::FnCall { name, args } => format!(
            "{}({})",
            name,
            args.iter()
                .map(|arg| render_sql_expr_with_field_map(arg, field_map))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlExpr::Paren(inner) => format!("({})", render_sql_expr_with_field_map(inner, field_map)),
    }
}

fn render_bool_expr_with_field_map(expr: &BoolExpr, field_map: &HashMap<String, String>) -> String {
    match expr {
        BoolExpr::Comparison { left, op, right } => format!(
            "{} {} {}",
            render_bool_operand_with_field_map(left, field_map, false),
            op,
            render_bool_operand_with_field_map(right, field_map, false)
        ),
        BoolExpr::And(left, right) => format!(
            "{} AND {}",
            render_bool_expr_with_field_map(left, field_map),
            render_bool_expr_with_field_map(right, field_map)
        ),
        BoolExpr::Or(left, right) => format!(
            "{} OR {}",
            render_bool_expr_with_field_map(left, field_map),
            render_bool_expr_with_field_map(right, field_map)
        ),
        BoolExpr::Not(inner) => {
            format!("NOT {}", render_bool_expr_with_field_map(inner, field_map))
        }
        BoolExpr::In { field, values } => format!(
            "{} IN [{}]",
            field_map
                .get(field)
                .cloned()
                .unwrap_or_else(|| field.clone()),
            values
                .iter()
                .map(|value| render_bool_operand_with_field_map(value, field_map, true))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        BoolExpr::Paren(inner) => {
            format!("({})", render_bool_expr_with_field_map(inner, field_map))
        }
    }
}

fn render_bool_operand_with_field_map(
    operand: &Operand,
    field_map: &HashMap<String, String>,
    enum_variant_in_list: bool,
) -> String {
    match operand {
        Operand::Field(name) => field_map.get(name).cloned().unwrap_or_else(|| name.clone()),
        Operand::Number(n) => n.clone(),
        Operand::StringLit(s) => format!("'{}'", s),
        Operand::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Operand::EnumVariant(variant) if enum_variant_in_list => variant.clone(),
        Operand::EnumVariant(variant) => format!("'{}'", variant),
    }
}

fn type_supports_optional_modifier(nautilus_type: &str) -> bool {
    !nautilus_type.ends_with("[]")
}

fn render_referential_action(action: &str) -> String {
    let normalized: String = action
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .flat_map(|ch| ch.to_lowercase())
        .collect();
    match normalized.as_str() {
        "cascade" => "Cascade".to_string(),
        "restrict" => "Restrict".to_string(),
        "noaction" => "NoAction".to_string(),
        "setnull" => "SetNull".to_string(),
        "setdefault" => "SetDefault".to_string(),
        _ => action.to_string(),
    }
}

fn push_pgvector_option_args(args: &mut Vec<String>, options: &PgvectorIndexOptions) {
    if let Some(value) = options.m {
        args.push(format!("m: {}", value));
    }
    if let Some(value) = options.ef_construction {
        args.push(format!("ef_construction: {}", value));
    }
    if let Some(value) = options.lists {
        args.push(format!("lists: {}", value));
    }
}

fn default_index_name(table_name: &str, columns: &[String]) -> String {
    let mut sorted_columns = columns.to_vec();
    sorted_columns.sort();
    format!("idx_{}_{}", table_name, sorted_columns.join("_"))
}

fn is_one_to_one_back_relation(live: &LiveSchema, owning_table: &str, fk: &LiveForeignKey) -> bool {
    live.tables
        .get(owning_table)
        .is_some_and(|table| columns_form_unique_key(table, &fk.columns))
}

fn columns_form_unique_key(table: &LiveTable, columns: &[String]) -> bool {
    let mut normalized_columns = columns.to_vec();
    normalized_columns.sort();

    let mut primary_key = table.primary_key.clone();
    primary_key.sort();
    if normalized_columns == primary_key {
        return true;
    }

    table.indexes.iter().any(|idx| {
        if !idx.unique {
            return false;
        }
        let mut index_columns = idx.columns.clone();
        index_columns.sort();
        index_columns == normalized_columns
    })
}

fn parse_sized_type_length(sql_type: &str, prefix: &str) -> Option<usize> {
    let inner = sql_type.strip_prefix(prefix)?.strip_suffix(')')?;
    inner.trim().parse().ok()
}

fn can_infer_autoincrement(col_type: &str) -> bool {
    let normalized = col_type.trim().to_lowercase();
    let base = normalized.strip_suffix("[]").unwrap_or(&normalized);
    matches!(
        base,
        "integer"
            | "int"
            | "int2"
            | "int4"
            | "smallint"
            | "tinyint"
            | "mediumint"
            | "bigint"
            | "int8"
            | "unsigned bigint"
    )
}

fn pluralize_name(name: &str) -> String {
    if name.ends_with('y')
        && !matches!(name.chars().rev().nth(1), Some('a' | 'e' | 'i' | 'o' | 'u'))
    {
        format!("{}ies", &name[..name.len() - 1])
    } else if matches!(name.chars().last(), Some('s' | 'x' | 'z'))
        || name.ends_with("ch")
        || name.ends_with("sh")
    {
        format!("{name}es")
    } else {
        format!("{name}s")
    }
}

/// Very simple singularisation: strip a trailing `s` (handles the common
/// plural pattern; no full inflection library is needed here).
fn singular_name(name: &str) -> String {
    if name.ends_with("ies") && name.len() > 3 {
        format!("{}y", &name[..name.len() - 3])
    } else if name.ends_with('s') && name.len() > 1 {
        name[..name.len() - 1].to_string()
    } else {
        name.to_string()
    }
}

fn to_snake_case_identifier(s: &str) -> String {
    let chars: Vec<char> = s
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect();
    let mut out = String::new();

    for (idx, ch) in chars.iter().copied().enumerate() {
        if ch == '_' {
            if !out.is_empty() && !out.ends_with('_') {
                out.push('_');
            }
            continue;
        }

        let prev = idx.checked_sub(1).and_then(|i| chars.get(i)).copied();
        let next = chars.get(idx + 1).copied();
        let is_upper = ch.is_ascii_uppercase();
        let prev_is_lower_or_digit =
            prev.is_some_and(|prev| prev.is_ascii_lowercase() || prev.is_ascii_digit());
        let prev_is_upper = prev.is_some_and(|prev| prev.is_ascii_uppercase());
        let next_is_lower = next.is_some_and(|next| next.is_ascii_lowercase());

        if is_upper
            && !out.is_empty()
            && (prev_is_lower_or_digit || (prev_is_upper && next_is_lower))
            && !out.ends_with('_')
        {
            out.push('_');
        }

        out.push(ch.to_ascii_lowercase());
    }

    out.trim_matches('_').to_string()
}

/// Convert a snake_case table name to PascalCase (for example `blog_posts` -> `BlogPosts`).
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut chars = p.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pascal_case_snake() {
        assert_eq!(to_pascal_case("blog_posts"), "BlogPosts");
    }

    #[test]
    fn pascal_case_single() {
        assert_eq!(to_pascal_case("users"), "Users");
    }

    #[test]
    fn pascal_case_already() {
        assert_eq!(to_pascal_case("User"), "User");
    }

    #[test]
    fn infers_types_correctly() {
        let no_enums = HashMap::new();
        let no_composites = HashMap::new();
        assert_eq!(
            infer_nautilus_type("text", &no_enums, &no_composites),
            "String"
        );
        assert_eq!(
            infer_nautilus_type("integer", &no_enums, &no_composites),
            "Int"
        );
        assert_eq!(
            infer_nautilus_type("bigint", &no_enums, &no_composites),
            "BigInt"
        );
        assert_eq!(
            infer_nautilus_type("boolean", &no_enums, &no_composites),
            "Boolean"
        );
        assert_eq!(
            infer_nautilus_type("double precision", &no_enums, &no_composites),
            "Float"
        );
        assert_eq!(
            infer_nautilus_type("timestamp", &no_enums, &no_composites),
            "DateTime"
        );
        assert_eq!(
            infer_nautilus_type("uuid", &no_enums, &no_composites),
            "Uuid"
        );
        assert_eq!(
            infer_nautilus_type("citext", &no_enums, &no_composites),
            "Citext"
        );
        assert_eq!(
            infer_nautilus_type("hstore", &no_enums, &no_composites),
            "Hstore"
        );
        assert_eq!(
            infer_nautilus_type("ltree", &no_enums, &no_composites),
            "Ltree"
        );
        assert_eq!(
            infer_nautilus_type("vector(1536)", &no_enums, &no_composites),
            "Vector(1536)"
        );
        assert_eq!(
            infer_nautilus_type("jsonb", &no_enums, &no_composites),
            "Jsonb"
        );
        assert_eq!(
            infer_nautilus_type("bytea", &no_enums, &no_composites),
            "Bytes"
        );
        assert_eq!(
            infer_nautilus_type("decimal(10, 2)", &no_enums, &no_composites),
            "Decimal(10, 2)"
        );
        assert_eq!(
            infer_nautilus_type("varchar(255)", &no_enums, &no_composites),
            "VarChar(255)"
        );
        assert_eq!(
            infer_nautilus_type("char(36)", &no_enums, &no_composites),
            "Uuid"
        );
        assert_eq!(
            infer_nautilus_type("char(10)", &no_enums, &no_composites),
            "Char(10)"
        );

        let mut with_enums = HashMap::new();
        with_enums.insert(
            "role".to_string(),
            vec!["ADMIN".to_string(), "USER".to_string()],
        );
        assert_eq!(
            infer_nautilus_type("role", &with_enums, &no_composites),
            "Role"
        );
    }

    #[test]
    fn infers_scalar_arrays() {
        let no_enums = HashMap::new();
        let no_composites = HashMap::new();
        assert_eq!(
            infer_nautilus_type("integer[]", &no_enums, &no_composites),
            "Int[]"
        );
        assert_eq!(
            infer_nautilus_type("text[]", &no_enums, &no_composites),
            "String[]"
        );
        assert_eq!(
            infer_nautilus_type("boolean[]", &no_enums, &no_composites),
            "Boolean[]"
        );
        assert_eq!(
            infer_nautilus_type("uuid[]", &no_enums, &no_composites),
            "Uuid[]"
        );
        assert_eq!(
            infer_nautilus_type("citext[]", &no_enums, &no_composites),
            "Citext[]"
        );
        assert_eq!(
            infer_nautilus_type("jsonb[]", &no_enums, &no_composites),
            "Jsonb[]"
        );
    }

    #[test]
    fn infers_enum_array() {
        let no_composites = HashMap::new();
        let mut enums = HashMap::new();
        enums.insert(
            "status".to_string(),
            vec!["ACTIVE".to_string(), "INACTIVE".to_string()],
        );
        assert_eq!(
            infer_nautilus_type("status[]", &enums, &no_composites),
            "Status[]"
        );
    }

    #[test]
    fn infers_composite_type() {
        use crate::live::LiveCompositeType;
        let no_enums = HashMap::new();
        let mut composites = HashMap::new();
        composites.insert(
            "address".to_string(),
            LiveCompositeType {
                name: "address".to_string(),
                fields: vec![],
            },
        );
        assert_eq!(
            infer_nautilus_type("address", &no_enums, &composites),
            "Address"
        );
        assert_eq!(
            infer_nautilus_type("address[]", &no_enums, &composites),
            "Address[]"
        );
    }

    #[test]
    fn default_boolean() {
        let no_enums: HashMap<String, Vec<String>> = HashMap::new();
        assert_eq!(
            infer_default_attr("true", "boolean", &no_enums),
            Some("@default(true)".into())
        );
    }

    #[test]
    fn default_number() {
        let no_enums: HashMap<String, Vec<String>> = HashMap::new();
        assert_eq!(
            infer_default_attr("42", "integer", &no_enums),
            Some("@default(42)".into())
        );
    }

    #[test]
    fn default_string() {
        let no_enums: HashMap<String, Vec<String>> = HashMap::new();
        assert_eq!(
            infer_default_attr("'hello'", "text", &no_enums),
            Some("@default(\"hello\")".into())
        );
    }

    #[test]
    fn default_now() {
        let no_enums: HashMap<String, Vec<String>> = HashMap::new();
        assert_eq!(
            infer_default_attr("current_timestamp", "timestamp", &no_enums),
            Some("@default(now())".into())
        );
    }

    #[test]
    fn default_uuid() {
        let no_enums: HashMap<String, Vec<String>> = HashMap::new();
        assert_eq!(
            infer_default_attr("gen_random_uuid()", "uuid", &no_enums),
            Some("@default(uuid())".into())
        );
    }

    #[test]
    fn default_nextval_skipped() {
        let no_enums: HashMap<String, Vec<String>> = HashMap::new();
        assert_eq!(
            infer_default_attr("nextval('seq')", "integer", &no_enums),
            Some("@default(autoincrement())".into())
        );
    }

    #[test]
    fn default_enum_literal() {
        let mut enums: HashMap<String, Vec<String>> = HashMap::new();
        enums.insert(
            "status".to_string(),
            vec!["DRAFT".to_string(), "PUBLISHED".to_string()],
        );
        assert_eq!(
            infer_default_attr("'DRAFT'", "status", &enums),
            Some("@default(DRAFT)".into())
        );
    }

    #[test]
    fn default_string_not_confused_with_enum() {
        let no_enums: HashMap<String, Vec<String>> = HashMap::new();
        assert_eq!(
            infer_default_attr("'hello'", "text", &no_enums),
            Some("@default(\"hello\")".into())
        );
    }
}

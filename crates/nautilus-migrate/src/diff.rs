//! Schema diff engine — compares a [`LiveSchema`] snapshot against a target
//! [`SchemaIr`] and returns a list of [`Change`]s that need to be applied.

use nautilus_schema::ir::{
    BasicIndexType, FieldIr, IndexKind, ModelIr, PostgresExtensionIr, ResolvedFieldType, SchemaIr,
};
use nautilus_schema::{Lexer, Span, TokenKind};

use crate::ddl::{DatabaseProvider, DdlGenerator};
use crate::live::{LiveIndex, LiveIndexKind, LiveSchema};

/// A single schema change between the live database and the target schema.
#[derive(Debug, Clone)]
pub enum Change {
    /// A model exists in the target schema but has no corresponding table in
    /// the live database.
    NewTable(ModelIr),

    /// A table exists in the live database but has no corresponding model in
    /// the target schema.
    DroppedTable {
        /// DB table name.
        name: String,
    },

    /// A scalar field exists in the target model but the corresponding column
    /// is missing from the live table.
    AddedColumn {
        /// DB table name.
        table: String,
        /// Target field IR (contains `db_name`, type info, etc.).
        field: FieldIr,
    },

    /// A column exists in the live table but has no corresponding scalar field
    /// in the target model.
    DroppedColumn {
        /// DB table name.
        table: String,
        /// DB column name.
        column: String,
    },

    /// The SQL type of an existing column does not match the target field type.
    TypeChanged {
        /// DB table name.
        table: String,
        /// DB column name.
        column: String,
        /// Current live SQL type (normalised, lower-cased).
        from: String,
        /// Target SQL type (from schema, normalised, lower-cased).
        to: String,
    },

    /// The nullability of an existing column differs from the target field.
    NullabilityChanged {
        /// DB table name.
        table: String,
        /// DB column name.
        column: String,
        /// `true` when the target requires `NOT NULL` (column is becoming
        /// required); `false` when the target allows `NULL`.
        now_required: bool,
    },

    /// The DEFAULT expression of an existing column differs from the target.
    DefaultChanged {
        /// DB table name.
        table: String,
        /// DB column name.
        column: String,
        /// Current live default (lower-cased), or `None`.
        from: Option<String>,
        /// Target default (lower-cased), or `None`.
        to: Option<String>,
    },

    /// The set of primary-key columns has changed.
    PrimaryKeyChanged {
        /// DB table name.
        table: String,
    },

    /// A new index (defined in the target schema) is not present in the live
    /// database.
    IndexAdded {
        /// DB table name.
        table: String,
        /// Sorted DB column names that form the index key.
        columns: Vec<String>,
        /// Whether the index enforces uniqueness.
        unique: bool,
        /// Access method + extension payload (BTree, pgvector HNSW, ...).
        kind: IndexKind,
        /// Optional DDL name override (from `@@index(map: "...")` or `name:`).
        index_name: Option<String>,
    },

    /// An index that exists in the live database is not present in the target
    /// schema.
    IndexDropped {
        /// DB table name.
        table: String,
        /// Sorted DB column names that form the index key.
        columns: Vec<String>,
        /// Whether the index enforces uniqueness.
        unique: bool,
        /// Physical name of the index as it exists in the database.
        index_name: String,
    },

    /// The generation expression of a computed column has changed.
    ComputedExprChanged {
        /// DB table name.
        table: String,
        /// DB column name.
        column: String,
        /// Target field IR (needed to regenerate the column definition).
        field: FieldIr,
    },

    /// A CHECK constraint was added, removed, or changed.
    CheckChanged {
        /// DB table name.
        table: String,
        /// DB column name (`None` for table-level checks).
        column: Option<String>,
        /// Old expression, or `None` if being added.
        from: Option<String>,
        /// New expression, or `None` if being dropped.
        to: Option<String>,
    },

    /// A PostgreSQL composite type exists in the target schema but not in the live
    /// database.
    CreateCompositeType {
        /// DB type name (lower-cased).
        name: String,
    },

    /// A PostgreSQL composite type exists in the live database but has been removed
    /// from the target schema.
    DropCompositeType {
        /// DB type name (lower-cased).
        name: String,
    },

    /// A PostgreSQL composite type exists in both live and target but its field
    /// list has changed.
    AlterCompositeType {
        /// DB type name (lower-cased).
        name: String,
        /// Fields present in target but not in live: `(db_name, sql_type)`.
        added_fields: Vec<(String, String)>,
        /// Field DB names present in live but not in target.
        dropped_fields: Vec<String>,
        /// Fields whose SQL type changed: `(db_name, from_type, to_type)`.
        type_changed_fields: Vec<(String, String, String)>,
    },

    /// A PostgreSQL enum type exists in the target schema but not in the live
    /// database.
    CreateEnum {
        /// DB type name (lower-cased).
        name: String,
        /// Ordered variant labels.
        variants: Vec<String>,
    },

    /// A PostgreSQL enum type exists in the live database but has been removed
    /// from the target schema.
    DropEnum {
        /// DB type name (lower-cased).
        name: String,
    },

    /// A PostgreSQL extension is declared in the target datasource but is not
    /// currently installed in the live database.
    CreateExtension {
        /// Extension name (lower-cased, as it appears in `pg_extension.extname`).
        name: String,
        /// Optional schema qualifier. When `Some`, the emitted DDL will include
        /// `WITH SCHEMA "<schema>"`.
        schema: Option<String>,
    },

    /// A PostgreSQL extension is installed in the live database but is no
    /// longer declared in the target datasource.
    DropExtension {
        /// Extension name (lower-cased).
        name: String,
    },

    /// A PostgreSQL enum type exists in both live and target but its variant
    /// list has changed.
    AlterEnum {
        /// DB type name (lower-cased).
        name: String,
        /// Variants present in target but not in live (to be added).
        added_variants: Vec<String>,
        /// Variants present in live but not in target (to be removed).
        removed_variants: Vec<String>,
    },

    /// A foreign-key constraint is present in the target schema but absent from
    /// the live database (or its referential actions have changed and the old
    /// constraint was already emitted as `ForeignKeyDropped`).
    ForeignKeyAdded {
        /// DB table name.
        table: String,
        /// Constraint name to create (auto-derived from table + columns).
        constraint_name: String,
        /// Local FK column names, in declaration order.
        columns: Vec<String>,
        /// Referenced table name.
        referenced_table: String,
        /// Referenced column names, in declaration order.
        referenced_columns: Vec<String>,
        /// ON DELETE action, or `None` for the database default (NO ACTION).
        on_delete: Option<String>,
        /// ON UPDATE action, or `None` for the database default (NO ACTION).
        on_update: Option<String>,
    },

    /// A foreign-key constraint exists in the live database but has no
    /// corresponding relation field in the target schema (or its referential
    /// actions changed and a replacement `ForeignKeyAdded` follows).
    ForeignKeyDropped {
        /// DB table name.
        table: String,
        /// Live constraint name to drop.
        constraint_name: String,
    },
}

/// Risk classification for a schema [`Change`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeRisk {
    /// Safe to apply — no data loss is possible.
    Safe,
    /// Requires confirmation — potential data loss or migration complexity.
    Destructive,
}

/// Classify a [`Change`] by its risk level.
pub fn change_risk(change: &Change) -> ChangeRisk {
    match change {
        Change::NewTable(_)
        | Change::DefaultChanged { .. }
        | Change::IndexAdded { .. }
        | Change::IndexDropped { .. }
        | Change::ComputedExprChanged { .. }
        | Change::CheckChanged { .. } => ChangeRisk::Safe,

        Change::AddedColumn { field, .. } => {
            if field.is_required && field.default_value.is_none() && field.computed.is_none() {
                ChangeRisk::Destructive
            } else {
                ChangeRisk::Safe
            }
        }

        Change::NullabilityChanged {
            now_required: false,
            ..
        } => ChangeRisk::Safe,

        Change::DroppedTable { .. }
        | Change::DroppedColumn { .. }
        | Change::TypeChanged { .. }
        | Change::PrimaryKeyChanged { .. }
        | Change::NullabilityChanged {
            now_required: true, ..
        }
        | Change::DropEnum { .. }
        | Change::DropCompositeType { .. }
        | Change::DropExtension { .. } => ChangeRisk::Destructive,

        Change::CreateEnum { .. }
        | Change::CreateCompositeType { .. }
        | Change::CreateExtension { .. } => ChangeRisk::Safe,

        Change::AlterEnum {
            removed_variants, ..
        } => {
            if removed_variants.is_empty() {
                ChangeRisk::Safe
            } else {
                ChangeRisk::Destructive
            }
        }

        Change::AlterCompositeType {
            dropped_fields,
            type_changed_fields,
            ..
        } => {
            if dropped_fields.is_empty() && type_changed_fields.is_empty() {
                ChangeRisk::Safe
            } else {
                ChangeRisk::Destructive
            }
        }

        Change::ForeignKeyAdded { .. } => ChangeRisk::Destructive,
        Change::ForeignKeyDropped { .. } => ChangeRisk::Safe,
    }
}

/// Reorder computed changes into a safer execution plan.
///
/// The plan prefers dropping foreign keys before destructive column/table
/// changes, drops tables in reverse live dependency order, and defers foreign
/// key creation until after structural changes complete.
pub fn order_changes_for_apply(changes: &[Change], live: &LiveSchema) -> Vec<Change> {
    use std::collections::HashMap;

    let mut pre_type_changes = Vec::new();
    let mut new_tables = Vec::new();
    let mut added_columns = Vec::new();
    let mut foreign_key_drops = Vec::new();
    let mut main_changes = Vec::new();
    let mut dropped_table_names = Vec::new();
    let mut dropped_tables: HashMap<String, Change> = HashMap::new();
    let mut index_adds = Vec::new();
    let mut foreign_key_adds = Vec::new();
    let mut post_type_changes = Vec::new();

    for change in changes {
        match change {
            Change::CreateCompositeType { .. }
            | Change::CreateEnum { .. }
            | Change::CreateExtension { .. } => {
                pre_type_changes.push(change.clone());
            }
            Change::AlterCompositeType {
                dropped_fields,
                type_changed_fields,
                ..
            } if dropped_fields.is_empty() && type_changed_fields.is_empty() => {
                pre_type_changes.push(change.clone());
            }
            Change::AlterEnum {
                removed_variants, ..
            } if removed_variants.is_empty() => {
                pre_type_changes.push(change.clone());
            }
            Change::NewTable(_) => new_tables.push(change.clone()),
            Change::AddedColumn { .. } => added_columns.push(change.clone()),
            Change::ForeignKeyDropped { .. } => foreign_key_drops.push(change.clone()),
            Change::DroppedTable { name } => {
                dropped_table_names.push(name.clone());
                dropped_tables.insert(name.clone(), change.clone());
            }
            Change::IndexAdded { .. } => index_adds.push(change.clone()),
            Change::ForeignKeyAdded { .. } => foreign_key_adds.push(change.clone()),
            Change::DropCompositeType { .. }
            | Change::DropEnum { .. }
            | Change::DropExtension { .. } => {
                post_type_changes.push(change.clone());
            }
            Change::AlterCompositeType { .. } | Change::AlterEnum { .. } => {
                post_type_changes.push(change.clone());
            }
            _ => main_changes.push(change.clone()),
        }
    }

    let mut ordered = Vec::with_capacity(changes.len());
    ordered.extend(pre_type_changes);
    ordered.extend(new_tables);
    ordered.extend(added_columns);
    ordered.extend(foreign_key_drops);
    ordered.extend(main_changes);

    for name in order_dropped_live_tables(live, &dropped_table_names) {
        if let Some(change) = dropped_tables.remove(name.as_str()) {
            ordered.push(change);
        }
    }
    for name in &dropped_table_names {
        if let Some(change) = dropped_tables.remove(name.as_str()) {
            ordered.push(change);
        }
    }

    ordered.extend(index_adds);
    ordered.extend(foreign_key_adds);
    ordered.extend(post_type_changes);
    ordered
}

/// Computes the difference between a live database and a target schema.
pub struct SchemaDiff;

impl SchemaDiff {
    /// Compare `live` (current DB state) against `target` (desired schema) and
    /// return an ordered list of changes that must be applied to make the live
    /// DB match the target.
    pub fn compute(
        live: &LiveSchema,
        target: &SchemaIr,
        provider: DatabaseProvider,
    ) -> Vec<Change> {
        let gen = DdlGenerator::new(provider);
        let mut changes: Vec<Change> = Vec::new();

        let mut pre_type_changes: Vec<Change> = Vec::new();
        let mut post_type_changes: Vec<Change> = Vec::new();

        if provider == DatabaseProvider::Postgres {
            let target_extensions: &[PostgresExtensionIr] = target
                .datasource
                .as_ref()
                .map(|d| d.extensions.as_slice())
                .unwrap_or(&[]);
            let preserve_extensions = target
                .datasource
                .as_ref()
                .is_some_and(|d| d.preserve_extensions);
            let target_extensions_set: std::collections::HashSet<&str> =
                target_extensions.iter().map(|e| e.name.as_str()).collect();

            for ext in target_extensions {
                if !live.extensions.contains_key(&ext.name) {
                    pre_type_changes.push(Change::CreateExtension {
                        name: ext.name.clone(),
                        schema: ext.schema.clone(),
                    });
                }
            }

            if !preserve_extensions {
                let mut live_extension_names: Vec<&str> =
                    live.extensions.keys().map(String::as_str).collect();
                live_extension_names.sort_unstable();
                for live_ext in live_extension_names {
                    if !target_extensions_set.contains(live_ext) {
                        post_type_changes.push(Change::DropExtension {
                            name: live_ext.to_string(),
                        });
                    }
                }
            }

            for ct in target.composite_types.values() {
                let db_name = ct.logical_name.to_lowercase();
                if !live.composite_types.contains_key(&db_name) {
                    pre_type_changes.push(Change::CreateCompositeType { name: db_name });
                }
            }

            for live_ct_name in live.composite_types.keys() {
                let still_in_target = target
                    .composite_types
                    .values()
                    .any(|ct| ct.logical_name.to_lowercase() == *live_ct_name);
                if !still_in_target {
                    post_type_changes.push(Change::DropCompositeType {
                        name: live_ct_name.clone(),
                    });
                }
            }

            for ct in target.composite_types.values() {
                let db_name = ct.logical_name.to_lowercase();
                if let Some(live_ct) = live.composite_types.get(&db_name) {
                    let live_field_map: std::collections::HashMap<&str, &str> = live_ct
                        .fields
                        .iter()
                        .map(|f| (f.name.as_str(), f.col_type.as_str()))
                        .collect();
                    let target_field_map: std::collections::HashMap<&str, String> = ct
                        .fields
                        .iter()
                        .filter_map(|f| {
                            gen.column_type_sql_for_composite(f)
                                .ok()
                                .map(|t| (f.db_name.as_str(), t))
                        })
                        .collect();

                    let mut added_fields: Vec<(String, String)> = Vec::new();
                    let mut type_changed_fields: Vec<(String, String, String)> = Vec::new();
                    let mut dropped_fields: Vec<String> = Vec::new();

                    for (db_name_f, sql_type) in &target_field_map {
                        match live_field_map.get(db_name_f) {
                            None => added_fields.push((db_name_f.to_string(), sql_type.clone())),
                            Some(&live_type) if live_type != sql_type.as_str() => {
                                type_changed_fields.push((
                                    db_name_f.to_string(),
                                    live_type.to_string(),
                                    sql_type.clone(),
                                ));
                            }
                            _ => {}
                        }
                    }
                    for live_field in &live_ct.fields {
                        if !target_field_map.contains_key(live_field.name.as_str()) {
                            dropped_fields.push(live_field.name.clone());
                        }
                    }

                    if !added_fields.is_empty()
                        || !dropped_fields.is_empty()
                        || !type_changed_fields.is_empty()
                    {
                        let change = Change::AlterCompositeType {
                            name: db_name,
                            added_fields,
                            dropped_fields: dropped_fields.clone(),
                            type_changed_fields: type_changed_fields.clone(),
                        };
                        if dropped_fields.is_empty() && type_changed_fields.is_empty() {
                            pre_type_changes.push(change);
                        } else {
                            post_type_changes.push(change);
                        }
                    }
                }
            }

            for enum_def in target.enums.values() {
                let db_name = enum_def.logical_name.to_lowercase();
                if !live.enums.contains_key(&db_name) {
                    pre_type_changes.push(Change::CreateEnum {
                        name: db_name,
                        variants: enum_def.variants.clone(),
                    });
                }
            }

            for live_enum_name in live.enums.keys() {
                let still_in_target = target
                    .enums
                    .values()
                    .any(|e| e.logical_name.to_lowercase() == *live_enum_name);
                if !still_in_target {
                    post_type_changes.push(Change::DropEnum {
                        name: live_enum_name.clone(),
                    });
                }
            }

            for enum_def in target.enums.values() {
                let db_name = enum_def.logical_name.to_lowercase();
                if let Some(live_variants) = live.enums.get(&db_name) {
                    let added: Vec<String> = enum_def
                        .variants
                        .iter()
                        .filter(|v| !live_variants.contains(*v))
                        .cloned()
                        .collect();
                    let removed: Vec<String> = live_variants
                        .iter()
                        .filter(|v| !enum_def.variants.contains(*v))
                        .cloned()
                        .collect();
                    if !added.is_empty() || !removed.is_empty() {
                        let change = Change::AlterEnum {
                            name: db_name,
                            added_variants: added,
                            removed_variants: removed.clone(),
                        };
                        if removed.is_empty() {
                            pre_type_changes.push(change);
                        } else {
                            post_type_changes.push(change);
                        }
                    }
                }
            }
        }

        let target_by_db: std::collections::HashMap<&str, &ModelIr> = target
            .models
            .values()
            .map(|m| (m.db_name.as_str(), m))
            .collect();

        {
            let new_models: Vec<&ModelIr> = target
                .models
                .values()
                .filter(|m| !live.tables.contains_key(&m.db_name))
                .collect();

            for model in topo_sort_models(&new_models) {
                changes.push(Change::NewTable(model.clone()));
            }
        }

        for live_table_name in live.tables.keys() {
            if !target_by_db.contains_key(live_table_name.as_str()) {
                changes.push(Change::DroppedTable {
                    name: live_table_name.clone(),
                });
            }
        }

        for (table_name, live_table) in &live.tables {
            let model = match target_by_db.get(table_name.as_str()) {
                Some(m) => m,
                None => continue, // already emitted DroppedTable
            };

            let live_cols: std::collections::HashMap<&str, _> = live_table
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c))
                .collect();

            let target_scalar_fields: Vec<&FieldIr> = model
                .fields
                .iter()
                .filter(|f| !matches!(f.field_type, ResolvedFieldType::Relation(_)))
                .collect();

            let target_cols_by_db: std::collections::HashMap<&str, &FieldIr> = target_scalar_fields
                .iter()
                .map(|f| (f.db_name.as_str(), *f))
                .collect();

            for field in &target_scalar_fields {
                if !live_cols.contains_key(field.db_name.as_str()) {
                    changes.push(Change::AddedColumn {
                        table: table_name.clone(),
                        field: (*field).clone(),
                    });
                }
            }

            for live_col_name in live_cols.keys() {
                if !target_cols_by_db.contains_key(*live_col_name) {
                    changes.push(Change::DroppedColumn {
                        table: table_name.clone(),
                        column: (*live_col_name).to_string(),
                    });
                }
            }

            for field in &target_scalar_fields {
                let live_col = match live_cols.get(field.db_name.as_str()) {
                    Some(c) => c,
                    None => continue, // AddedColumn already emitted
                };

                let target_type = gen.column_type_sql(field).unwrap_or_default();
                if !target_type.is_empty()
                    && target_type != live_col.col_type
                    && !types_storage_equivalent(provider, &live_col.col_type, &target_type)
                {
                    changes.push(Change::TypeChanged {
                        table: table_name.clone(),
                        column: field.db_name.clone(),
                        from: live_col.col_type.clone(),
                        to: target_type,
                    });
                }

                // `field.is_required` means NOT NULL; `live_col.nullable` means NULL allowed.
                let target_nullable = !field.is_required;
                if target_nullable != live_col.nullable {
                    changes.push(Change::NullabilityChanged {
                        table: table_name.clone(),
                        column: field.db_name.clone(),
                        now_required: !target_nullable,
                    });
                }

                // Normalise both sides so that superficial formatting differences
                // (e.g. outer parentheses that some databases strip) don't produce
                // false positives.
                //
                // Skip entirely for `autoincrement()` fields: PostgreSQL SERIAL
                // implicitly creates a `nextval(...)` column default that the
                // inspector reports; `column_default_sql()` returns `None` for
                // autoincrement (it's managed by the SERIAL type, not a plain
                // DEFAULT clause).  Without this guard the diff would see
                // `None` vs `Some("nextval(...)")` and emit `DROP DEFAULT`,
                // destroying the sequence link and breaking all future INSERTs.
                let is_autoincrement = matches!(
                    &field.default_value,
                    Some(nautilus_schema::ir::DefaultValue::Function(f)) if f.name == "autoincrement"
                );
                if !is_autoincrement {
                    let target_default = gen
                        .column_default_sql(field)
                        .unwrap_or(None)
                        .map(|s| normalize_default(&s));
                    let live_default = live_col.default_value.as_deref().map(normalize_default);
                    if target_default != live_default {
                        changes.push(Change::DefaultChanged {
                            table: table_name.clone(),
                            column: field.db_name.clone(),
                            from: live_col.default_value.clone(),
                            to: gen.column_default_sql(field).unwrap_or(None),
                        });
                    }
                }

                // Database engines reformat generated expressions heavily
                // (adding casts, parens, spacing), so canonicalise both sides
                // before comparing.
                let target_expr = field
                    .computed
                    .as_ref()
                    .map(|(expr, _)| normalize_generated_expr(expr));
                let live_expr = live_col
                    .generated_expr
                    .as_deref()
                    .map(normalize_generated_expr);
                if target_expr != live_expr {
                    changes.push(Change::ComputedExprChanged {
                        table: table_name.clone(),
                        column: field.db_name.clone(),
                        field: (*field).clone(),
                    });
                }

                // Suppress the change when the expression already exists
                // somewhere in the table's check-constraint pool.  This
                // handles older databases where column checks were created
                // inline (auto-named by PG, e.g. `order_items_quantity_check`)
                // and therefore ended up in live_table.check_constraints rather
                // than live_col.check_expr.
                let target_check = field.check.as_deref().map(normalize_check_expr);
                let live_check = live_col.check_expr.as_deref().map(normalize_check_expr);
                let check_already_in_table_pool = target_check.as_ref().is_some_and(|tc| {
                    live_table
                        .check_constraints
                        .iter()
                        .map(|lc| normalize_check_expr(lc))
                        .any(|lc| lc == *tc)
                });
                if target_check != live_check && !check_already_in_table_pool {
                    changes.push(Change::CheckChanged {
                        table: table_name.clone(),
                        column: Some(field.db_name.clone()),
                        from: live_col.check_expr.clone(),
                        to: field.check.clone(),
                    });
                }
            }

            {
                // Expressions covered by column-level @check fields — we must
                // not emit a "drop" for auto-named column constraints that were
                // bucketed into the table pool by the inspector.
                let column_check_exprs: std::collections::HashSet<String> = target_scalar_fields
                    .iter()
                    .filter_map(|f| f.check.as_deref())
                    .map(normalize_check_expr)
                    .collect();

                let mut target_checks: Vec<String> = model
                    .check_constraints
                    .iter()
                    .map(|s| normalize_check_expr(s))
                    .collect();
                target_checks.sort();
                let mut live_checks: Vec<String> = live_table
                    .check_constraints
                    .iter()
                    .map(|s| normalize_check_expr(s))
                    .collect();
                live_checks.sort();

                for tc in &target_checks {
                    if !live_checks.contains(tc) {
                        changes.push(Change::CheckChanged {
                            table: table_name.clone(),
                            column: None,
                            from: None,
                            to: Some(tc.clone()),
                        });
                    }
                }
                // Do not drop auto-named column constraints that belong to a
                // column-level @check target.
                for lc in &live_checks {
                    if !target_checks.contains(lc) && !column_check_exprs.contains(lc.as_str()) {
                        changes.push(Change::CheckChanged {
                            table: table_name.clone(),
                            column: None,
                            from: Some(lc.clone()),
                            to: None,
                        });
                    }
                }
            }

            // `model.primary_key.fields()` returns *logical* field names; the
            // live PKs come from the DB and use *db* column names.  Resolve
            // logical -> db before comparing so @map doesn't cause false positives.
            let mut target_pk: Vec<String> = model
                .primary_key
                .fields()
                .iter()
                .map(|logical| {
                    model
                        .find_field(logical)
                        .map(|f| f.db_name.clone())
                        .unwrap_or_else(|| (*logical).to_string())
                })
                .collect();
            target_pk.sort();
            let mut live_pk: Vec<String> = live_table.primary_key.clone();
            live_pk.sort();

            if target_pk != live_pk {
                changes.push(Change::PrimaryKeyChanged {
                    table: table_name.clone(),
                });
            }

            struct TargetIdx {
                sorted_cols: Vec<String>,
                unique: bool,
                kind: IndexKind,
                effective_name: String,
                name_must_match: bool,
            }

            let target_indexes: Vec<TargetIdx> = {
                let mut idxs: Vec<TargetIdx> = Vec::new();

                for idx in &model.indexes {
                    let mut cols: Vec<String> = idx
                        .fields
                        .iter()
                        .map(|name| {
                            model
                                .find_field(name)
                                .map(|f| f.db_name.clone())
                                .unwrap_or_else(|| name.clone())
                        })
                        .collect();
                    cols.sort();
                    let ddl_name = idx
                        .map
                        .clone()
                        .unwrap_or_else(|| format!("idx_{}_{}", table_name, cols.join("_")));
                    idxs.push(TargetIdx {
                        sorted_cols: cols,
                        unique: false,
                        kind: idx.kind.clone(),
                        effective_name: ddl_name,
                        name_must_match: idx.map.is_some(),
                    });
                }

                for uc in &model.unique_constraints {
                    let mut cols: Vec<String> = uc
                        .fields
                        .iter()
                        .map(|name| {
                            model
                                .find_field(name)
                                .map(|f| f.db_name.clone())
                                .unwrap_or_else(|| name.clone())
                        })
                        .collect();
                    cols.sort();
                    let ddl_name = format!("idx_{}_{}", table_name, cols.join("_"));
                    idxs.push(TargetIdx {
                        sorted_cols: cols,
                        unique: true,
                        kind: IndexKind::Default,
                        effective_name: ddl_name,
                        name_must_match: false,
                    });
                }

                idxs
            };

            struct LiveIdxNorm<'a> {
                sorted_cols: Vec<String>,
                unique: bool,
                live: &'a LiveIndex,
            }

            let live_indexes: Vec<LiveIdxNorm<'_>> = live_table
                .indexes
                .iter()
                .map(|i| {
                    let mut cols = i.columns.clone();
                    cols.sort();
                    LiveIdxNorm {
                        sorted_cols: cols,
                        unique: i.unique,
                        live: i,
                    }
                })
                .collect();

            for ti in &target_indexes {
                let found = live_indexes.iter().any(|li| {
                    li.sorted_cols == ti.sorted_cols
                        && li.unique == ti.unique
                        && (!ti.name_must_match || li.live.name == ti.effective_name)
                        && index_kinds_match(&ti.kind, &li.live.kind)
                });
                if !found {
                    changes.push(Change::IndexAdded {
                        table: table_name.clone(),
                        columns: ti.sorted_cols.clone(),
                        unique: ti.unique,
                        kind: ti.kind.clone(),
                        index_name: Some(ti.effective_name.clone()),
                    });
                }
            }

            for li in &live_indexes {
                let found = target_indexes.iter().any(|ti| {
                    li.sorted_cols == ti.sorted_cols
                        && li.unique == ti.unique
                        && (!ti.name_must_match || li.live.name == ti.effective_name)
                        && index_kinds_match(&ti.kind, &li.live.kind)
                });
                if !found {
                    changes.push(Change::IndexDropped {
                        table: table_name.clone(),
                        columns: li.sorted_cols.clone(),
                        unique: li.unique,
                        index_name: li.live.name.clone(),
                    });
                }
            }

            let target_fks: Vec<TargetFkDescriptor> = model
                .fields
                .iter()
                .filter_map(|field| {
                    if let ResolvedFieldType::Relation(rel) = &field.field_type {
                        if rel.fields.is_empty() {
                            return None;
                        }
                        let target_model = target.models.get(&rel.target_model)?;
                        let fk_cols: Vec<String> = rel
                            .fields
                            .iter()
                            .filter_map(|fname| model.find_field(fname))
                            .map(|f| f.db_name.clone())
                            .collect();
                        if fk_cols.is_empty() {
                            return None;
                        }
                        let ref_cols: Vec<String> = rel
                            .references
                            .iter()
                            .filter_map(|rname| target_model.find_field(rname))
                            .map(|f| f.db_name.clone())
                            .collect();
                        let on_delete = rel.on_delete.as_ref().map(fk_action_to_str);
                        let on_update = rel.on_update.as_ref().map(fk_action_to_str);
                        Some(TargetFkDescriptor {
                            columns: fk_cols,
                            referenced_table: target_model.db_name.clone(),
                            referenced_columns: ref_cols,
                            on_delete,
                            on_update,
                        })
                    } else {
                        None
                    }
                })
                .collect();

            for tfk in &target_fks {
                let live_match = live_table.foreign_keys.iter().find(|lk| {
                    lk.columns == tfk.columns
                        && lk.referenced_table == tfk.referenced_table
                        && lk.referenced_columns == tfk.referenced_columns
                });
                match live_match {
                    None => {
                        changes.push(Change::ForeignKeyAdded {
                            table: table_name.clone(),
                            constraint_name: fk_auto_name(table_name, &tfk.columns),
                            columns: tfk.columns.clone(),
                            referenced_table: tfk.referenced_table.clone(),
                            referenced_columns: tfk.referenced_columns.clone(),
                            on_delete: tfk.on_delete.clone(),
                            on_update: tfk.on_update.clone(),
                        });
                    }
                    Some(live_fk) => {
                        if !fk_actions_equal(
                            provider,
                            live_fk.on_delete.as_deref(),
                            tfk.on_delete.as_deref(),
                        ) || !fk_actions_equal(
                            provider,
                            live_fk.on_update.as_deref(),
                            tfk.on_update.as_deref(),
                        ) {
                            changes.push(Change::ForeignKeyDropped {
                                table: table_name.clone(),
                                constraint_name: live_fk.constraint_name.clone(),
                            });
                            changes.push(Change::ForeignKeyAdded {
                                table: table_name.clone(),
                                constraint_name: fk_auto_name(table_name, &tfk.columns),
                                columns: tfk.columns.clone(),
                                referenced_table: tfk.referenced_table.clone(),
                                referenced_columns: tfk.referenced_columns.clone(),
                                on_delete: tfk.on_delete.clone(),
                                on_update: tfk.on_update.clone(),
                            });
                        }
                    }
                }
            }

            for live_fk in &live_table.foreign_keys {
                let still_in_target = target_fks.iter().any(|tf| {
                    tf.columns == live_fk.columns
                        && tf.referenced_table == live_fk.referenced_table
                        && tf.referenced_columns == live_fk.referenced_columns
                });
                if !still_in_target {
                    changes.push(Change::ForeignKeyDropped {
                        table: table_name.clone(),
                        constraint_name: live_fk.constraint_name.clone(),
                    });
                }
            }
        }

        let mut all_changes = pre_type_changes;
        all_changes.append(&mut changes);
        all_changes.append(&mut post_type_changes);
        all_changes
    }
}

/// Returns `true` when `a` and `b` are storage-equivalent for the given provider,
/// meaning no data migration is needed despite differing declared type names.
///
/// Handles the SQLite backwards-compatibility case: older Nautilus versions
/// used bare `TEXT` for `DateTime`, `Uuid`, and `Json` columns, while newer
/// versions use `DATETIME`, `CHAR(36)`, and `JSON` respectively.  All of these
/// are stored identically on disk by SQLite (TEXT/NUMERIC affinity - text
/// storage for non-numeric values), so they must not trigger a rebuild.
fn types_storage_equivalent(provider: DatabaseProvider, a: &str, b: &str) -> bool {
    if provider != DatabaseProvider::Sqlite {
        return false;
    }
    // Canonical set of SQLite text-storage types Nautilus may produce.
    // `text` is the old spelling; the others are the new descriptive names.
    fn sqlite_text_group(t: &str) -> bool {
        matches!(t, "text" | "datetime" | "json")
            || t == "char(36)"
            || t.starts_with("varchar")
            || t.starts_with("char(")
    }
    // Two decimal(p,s) spellings whose precision/scale may differ are NOT
    // equivalent — keep them as real type changes.
    fn sqlite_decimal_group(t: &str) -> bool {
        t.starts_with("decimal(")
    }
    // `TEXT` (old)  any descriptive text-affinity type (new) - equivalent.
    if sqlite_text_group(a) && sqlite_text_group(b) {
        return true;
    }
    // `TEXT` (old)  `DECIMAL(p,s)` (new) - equivalent (both stored as text).
    if (a == "text" && sqlite_decimal_group(b)) || (sqlite_decimal_group(a) && b == "text") {
        return true;
    }
    false
}

/// Normalise a default-value expression for comparison so that cosmetic
/// differences don't cause false-positive [`Change::DefaultChanged`].
///
/// Lowercases, trims whitespace, and strips a single balanced layer of outer
/// parentheses so that enum literal casing (`'DRAFT'` vs `'draft'`) and
/// SQLite paren differences don't produce false positives.
pub(crate) fn normalize_default(s: &str) -> String {
    let lowered = s.trim().to_lowercase();
    crate::utils::strip_outer_parens(&lowered)
}

fn normalize_check_expr(s: &str) -> String {
    let s = strip_identifier_quotes(s.trim());
    let s = strip_check_casts(&s);
    let s = strip_numeric_check_parens(&s);
    let mut s = s.trim().to_string();
    loop {
        let stripped = crate::utils::strip_outer_parens(&s);
        if stripped == s {
            break;
        }
        s = stripped;
    }
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    let s = convert_check_any_array_to_in(&s);
    let s = convert_check_in_parens_to_brackets(&s);
    canonicalize_check_bool_expr(&s).unwrap_or(s)
}

fn strip_identifier_quotes(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_single = false;
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\'' {
            out.push(ch);
            if in_single && chars.peek() == Some(&'\'') {
                out.push('\'');
                chars.next();
                continue;
            }
            in_single = !in_single;
            continue;
        }

        if !in_single && (ch == '`' || ch == '"') {
            continue;
        }

        out.push(ch);
    }

    out
}

fn strip_check_casts(s: &str) -> String {
    let mut result = s.to_string();
    while let Some(idx) = result.rfind("::") {
        let after = &result[idx + 2..];
        let type_end = if let Some(rest) = after.strip_prefix('"') {
            rest.find('"').map(|i| i + 2).unwrap_or(after.len())
        } else {
            after
                .find(|c: char| !c.is_alphanumeric() && c != '_' && c != ' ')
                .unwrap_or(after.len())
        };
        result = format!("{}{}", &result[..idx], &result[idx + 2 + type_end..]);
    }
    result
}

fn strip_numeric_check_parens(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '(' {
            let mut inner = String::new();
            let mut depth = 1i32;
            for c in chars.by_ref() {
                match c {
                    '(' => {
                        depth += 1;
                        inner.push(c);
                    }
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                        inner.push(c);
                    }
                    _ => inner.push(c),
                }
            }
            let trimmed = inner.trim();
            let is_numeric = !trimmed.is_empty()
                && trimmed
                    .chars()
                    .all(|c| c.is_ascii_digit() || c == '.' || c == '-');
            if is_numeric {
                result.push_str(trimmed);
            } else {
                result.push('(');
                result.push_str(&strip_numeric_check_parens(&inner));
                result.push(')');
            }
        } else {
            result.push(ch);
        }
    }

    result
}

fn convert_check_any_array_to_in(s: &str) -> String {
    let lower = s.to_lowercase();
    let marker = "= any (array[";

    let Some(eq_pos) = lower.find(marker) else {
        return s.to_string();
    };

    let field = s[..eq_pos].trim();
    let bracket_start = eq_pos + marker.len();
    let rest = &s[bracket_start..];

    let mut depth = 1i32;
    let mut bracket_end = None;
    for (i, c) in rest.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    bracket_end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }

    let Some(bclose) = bracket_end else {
        return s.to_string();
    };

    let items = &rest[..bclose];
    let after_array = rest[bclose + 1..].trim_start();
    let after_paren = after_array.strip_prefix(')').unwrap_or(after_array);

    if after_paren.is_empty() {
        format!("{} IN [{}]", field, items)
    } else {
        format!("{} IN [{}] {}", field, items, after_paren.trim_start())
    }
}

fn convert_check_in_parens_to_brackets(s: &str) -> String {
    let lower = s.to_lowercase();
    let marker = " in (";

    if !lower.contains(marker) {
        return s.to_string();
    }

    let mut result = String::with_capacity(s.len());
    let mut pos = 0usize;

    while let Some(rel) = lower[pos..].find(marker) {
        let abs = pos + rel;
        result.push_str(&s[pos..abs]);
        result.push_str(" IN [");

        let after_open = abs + marker.len();
        let rest = &s[after_open..];

        let mut depth = 1i32;
        let mut close = rest.len();
        for (i, c) in rest.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        close = i;
                        break;
                    }
                }
                _ => {}
            }
        }

        result.push_str(&rest[..close]);
        result.push(']');
        pos = after_open + close + 1;
    }

    result.push_str(&s[pos..]);
    result
}

fn canonicalize_check_bool_expr(s: &str) -> Option<String> {
    let mut lexer = Lexer::new(s);
    let mut tokens = Vec::new();

    loop {
        let token = lexer.next_token().ok()?;
        match token.kind {
            TokenKind::Eof => break,
            TokenKind::Newline => {}
            _ => tokens.push(token),
        }
    }

    if tokens.is_empty() {
        return Some(String::new());
    }

    nautilus_schema::bool_expr::parse_bool_expr(&tokens, Span::new(0, 0))
        .ok()
        .map(|expr| expr.to_string())
}

/// Normalise a generation expression for comparison.
///
/// Databases reformat `GENERATED ALWAYS AS (...)` expressions aggressively:
///   - PostgreSQL adds type casts (`::text`, `::integer`) and extra parens
///   - MySQL lower-cases and may rewrite operator spacing
///   - SQLite preserves the original expression mostly as-is
///
/// We canonicalise by: lower-casing, stripping all `::type` casts (PG),
/// collapsing whitespace, and stripping balanced outer parentheses.
fn normalize_generated_expr(s: &str) -> String {
    let mut s = s.to_lowercase();
    // Strip Postgres-style type casts: `::text`, `::integer`, `::character varying`, `"enum"`
    while let Some(idx) = s.find("::") {
        let after = &s[idx + 2..];
        let type_end = if let Some(rest) = after.strip_prefix('"') {
            rest.find('"').map(|i| i + 2).unwrap_or(after.len())
        } else {
            after
                .find(|c: char| !c.is_alphanumeric() && c != '_' && c != ' ')
                .unwrap_or(after.len())
        };
        s = format!("{}{}", &s[..idx], &s[idx + 2 + type_end..]);
    }
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut s = s.trim().to_string();
    loop {
        let stripped = crate::utils::strip_outer_parens(&s);
        if stripped == s {
            break;
        }
        s = stripped;
    }
    s
}

/// Sort models so that a table is always created *before* any table that holds
/// a foreign-key pointing to it.
/// Internal descriptor for a foreign-key constraint derived from the target schema IR.
struct TargetFkDescriptor {
    columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
    on_delete: Option<String>,
    on_update: Option<String>,
}

/// Convert a [`ReferentialAction`] to its SQL keyword string (upper-cased).
fn fk_action_to_str(action: &nautilus_schema::ast::ReferentialAction) -> String {
    use nautilus_schema::ast::ReferentialAction;
    match action {
        ReferentialAction::Cascade => "CASCADE".to_string(),
        ReferentialAction::Restrict => "RESTRICT".to_string(),
        ReferentialAction::NoAction => "NO ACTION".to_string(),
        ReferentialAction::SetNull => "SET NULL".to_string(),
        ReferentialAction::SetDefault => "SET DEFAULT".to_string(),
    }
}

/// Compare two FK action values, treating `None` and `"NO ACTION"` as equivalent
/// (both represent the database default).
fn fk_actions_equal(provider: DatabaseProvider, live: Option<&str>, target: Option<&str>) -> bool {
    fn normalise(provider: DatabaseProvider, action: Option<&str>) -> &str {
        match action {
            None => match provider {
                DatabaseProvider::Mysql => "restrict",
                DatabaseProvider::Postgres | DatabaseProvider::Sqlite => "no action",
            },
            Some(action) if action.eq_ignore_ascii_case("NO ACTION") => match provider {
                DatabaseProvider::Mysql => "restrict",
                DatabaseProvider::Postgres | DatabaseProvider::Sqlite => "no action",
            },
            Some(action)
                if provider == DatabaseProvider::Mysql
                    && action.eq_ignore_ascii_case("RESTRICT") =>
            {
                "restrict"
            }
            Some(action) => action,
        }
    }

    normalise(provider, live).eq_ignore_ascii_case(normalise(provider, target))
}

/// Derive a deterministic FK constraint name from table and FK column list.
fn fk_auto_name(table: &str, columns: &[String]) -> String {
    format!("fk_{}_{}", table, columns.join("_"))
}

fn order_dropped_live_tables(live: &LiveSchema, dropped_tables: &[String]) -> Vec<String> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let dropped_set: HashSet<&str> = dropped_tables.iter().map(String::as_str).collect();
    let mut names: Vec<&str> = dropped_set.iter().copied().collect();
    names.sort_unstable();

    let name_to_idx: HashMap<&str, usize> = names
        .iter()
        .enumerate()
        .map(|(i, name)| (*name, i))
        .collect();
    let mut in_degree = vec![0usize; names.len()];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); names.len()];

    for &table_name in &names {
        let Some(table) = live.tables.get(table_name) else {
            continue;
        };
        let table_idx = name_to_idx[table_name];
        let mut seen_refs: HashSet<&str> = HashSet::new();

        for fk in &table.foreign_keys {
            let referenced = fk.referenced_table.as_str();
            if referenced == table_name
                || !dropped_set.contains(referenced)
                || !seen_refs.insert(referenced)
            {
                continue;
            }
            let referenced_idx = name_to_idx[referenced];
            dependents[referenced_idx].push(table_idx);
            in_degree[table_idx] += 1;
        }
    }

    let mut queue: VecDeque<usize> = (0..names.len()).filter(|&i| in_degree[i] == 0).collect();
    let mut create_order = Vec::with_capacity(names.len());

    while let Some(idx) = queue.pop_front() {
        create_order.push(names[idx]);
        let mut ready = Vec::new();
        for &dependent_idx in &dependents[idx] {
            in_degree[dependent_idx] -= 1;
            if in_degree[dependent_idx] == 0 {
                ready.push(dependent_idx);
            }
        }
        ready.sort_unstable_by_key(|idx| names[*idx]);
        queue.extend(ready);
    }

    let emitted: HashSet<&str> = create_order.iter().copied().collect();
    let mut remaining: Vec<&str> = names
        .into_iter()
        .filter(|name| !emitted.contains(name))
        .collect();
    remaining.sort_unstable();
    create_order.extend(remaining);

    create_order.reverse();
    create_order.into_iter().map(str::to_string).collect()
}

pub(crate) fn topo_sort_models<'a>(models: &[&'a ModelIr]) -> Vec<&'a ModelIr> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let name_to_idx: HashMap<&str, usize> = models
        .iter()
        .enumerate()
        .map(|(i, m)| (m.logical_name.as_str(), i))
        .collect();

    let n = models.len();
    let mut in_degree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n]; // dependents[dep] = [models that need dep]

    for (i, model) in models.iter().enumerate() {
        for field in &model.fields {
            if let ResolvedFieldType::Relation(rel) = &field.field_type {
                // Only the FK-owning side carries actual column dependencies
                if rel.fields.is_empty() {
                    continue;
                }
                // Skip self-references (can't reorder away from a cycle)
                if rel.target_model == model.logical_name {
                    continue;
                }
                if let Some(&dep_idx) = name_to_idx.get(rel.target_model.as_str()) {
                    if dep_idx != i {
                        dependents[dep_idx].push(i);
                        in_degree[i] += 1;
                    }
                }
            }
        }
    }

    // Kahn's algorithm
    let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut result: Vec<&ModelIr> = Vec::with_capacity(n);

    while let Some(idx) = queue.pop_front() {
        result.push(models[idx]);
        for &dep in &dependents[idx] {
            in_degree[dep] -= 1;
            if in_degree[dep] == 0 {
                queue.push_back(dep);
            }
        }
    }

    // Append any remaining (cyclic) models — FK cycles shouldn't exist in a
    // valid schema but we handle them gracefully rather than panicking.
    let emitted: HashSet<*const ModelIr> = result.iter().map(|m| *m as *const _).collect();
    for model in models {
        if !emitted.contains(&(*model as *const _)) {
            result.push(model);
        }
    }

    result
}

/// Returns `true` when a target [`IndexKind`] and an inspected [`LiveIndexKind`]
/// describe the same access method.
///
/// The interesting subtlety is that a target schema with no `type:` argument
/// (`IndexKind::Default`) must compare equal to a live BTree index — which
/// the database reports either as `LiveIndexKind::Basic(BTree)` (Postgres,
/// MySQL) or `LiveIndexKind::Unknown(None)` (SQLite, which does not expose
/// access methods at all).
fn index_kinds_match(target: &IndexKind, live: &LiveIndexKind) -> bool {
    match (target, live) {
        (IndexKind::Default, LiveIndexKind::Unknown(None)) => true,
        (IndexKind::Default, LiveIndexKind::Basic(BasicIndexType::BTree)) => true,
        (IndexKind::Basic(BasicIndexType::BTree), LiveIndexKind::Unknown(None)) => true,
        (IndexKind::Basic(t), LiveIndexKind::Basic(l)) => t == l,
        (IndexKind::Pgvector(t), LiveIndexKind::Pgvector(l)) => t == l,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_generated_expr_strips_pg_casts() {
        assert_eq!(
            normalize_generated_expr("(first_name || (' '::text) || last_name)"),
            "first_name || (' ') || last_name"
        );
    }

    #[test]
    fn normalize_generated_expr_strips_outer_parens() {
        assert_eq!(
            normalize_generated_expr("((price * quantity))"),
            "price * quantity"
        );
    }

    #[test]
    fn normalize_check_expr_sql_and_bracket_forms_match() {
        assert_eq!(
            normalize_check_expr("status IN ('Draft', 'PUBLISHED')"),
            normalize_check_expr("status IN ['Draft', 'PUBLISHED']")
        );
    }

    #[test]
    fn normalize_check_expr_preserves_string_literal_casing() {
        assert_ne!(
            normalize_check_expr("status IN ('Draft', 'PUBLISHED')"),
            normalize_check_expr("status IN ('draft', 'PUBLISHED')")
        );
    }

    #[test]
    fn normalize_check_expr_strips_mysql_backticks() {
        assert_eq!(
            normalize_check_expr("(`status` in ('Draft', 'PUBLISHED'))"),
            "status IN ['Draft', 'PUBLISHED']"
        );
    }
}

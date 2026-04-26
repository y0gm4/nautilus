//! Schema validation and IR generation.
//!
//! This module implements multi-pass semantic validation and converts
//! the syntax AST into a validated intermediate representation (IR).

mod composites;
mod declarations;
mod defaults;
mod index;
mod ir_builder;
mod models;
mod names;
mod relations;

use crate::ast::*;
use crate::error::{Result, SchemaError};
use crate::ir::*;
use crate::span::Span;
use std::collections::{HashMap, HashSet, VecDeque};

const KNOWN_DATASOURCE_FIELDS: &[&str] = &[
    "provider",
    "url",
    "direct_url",
    "extensions",
    "preserve_extensions",
];

/// Curated whitelist of PostgreSQL contrib extensions that Nautilus knows about.
///
/// Names outside this list are still accepted (the DDL pipeline will emit
/// `CREATE EXTENSION IF NOT EXISTS <name>`), but a warning is surfaced so the
/// user can catch typos and be aware they are leaving the supported set.
pub(crate) const KNOWN_POSTGRES_EXTENSIONS: &[&str] = &[
    "btree_gin",
    "btree_gist",
    "citext",
    "hstore",
    "intarray",
    "ltree",
    "pgcrypto",
    "pg_trgm",
    "unaccent",
    "uuid-ossp",
    "vector",
];
const KNOWN_GENERATOR_FIELDS: &[&str] = &["provider", "output", "interface"];
const PYTHON_ONLY_GENERATOR_FIELDS: &[&str] = &["recursive_type_depth"];
const JAVA_ONLY_GENERATOR_FIELDS: &[&str] = &["package", "group_id", "artifact_id", "mode"];

/// Validates a schema AST and produces a validated IR.
///
/// This performs multi-pass validation:
/// 1. Collect all model and enum names, check for duplicates
/// 2. Validate model fields, types, and constraints
/// 3. Validate relations and foreign key integrity
/// 4. Validate default values and type compatibility
/// 5. Build the final IR
///
/// All validation errors are collected and returned together when possible.
pub fn validate_schema(schema: Schema) -> Result<SchemaIr> {
    let validator = SchemaValidator::new(schema);
    validator.validate()
}

/// Validates a schema AST and returns **all** diagnostics, not just the first one.
///
/// Unlike [`validate_schema`], which stops at the first error, this function
/// runs every validation pass and collects all errors so that the analysis API
/// and LSP server can surface the complete set of problems in one shot.
///
/// Returns `(Option<SchemaIr>, Vec<SchemaError>)`:
/// - The validated IR when there are no errors; `None` otherwise.
/// - Every error encountered across all validation passes (may be empty on success).
pub(crate) fn validate_all(schema: Schema) -> (Option<SchemaIr>, Vec<SchemaError>) {
    let validator = SchemaValidator::new(schema);
    validator.validate_collect_all()
}

struct SchemaValidator {
    schema: Schema,
    errors: VecDeque<SchemaError>,
    warnings: VecDeque<SchemaError>,
    models: HashMap<String, Span>,
    enums: HashMap<String, Span>,
    composite_types: HashMap<String, Span>,
}

impl SchemaValidator {
    fn new(schema: Schema) -> Self {
        Self {
            schema,
            errors: VecDeque::new(),
            warnings: VecDeque::new(),
            models: HashMap::new(),
            enums: HashMap::new(),
            composite_types: HashMap::new(),
        }
    }

    fn validate(mut self) -> Result<SchemaIr> {
        self.collect_names();

        if !self.errors.is_empty() {
            return Err(self.errors.pop_front().unwrap());
        }

        self.validate_datasources();
        self.validate_generators();
        self.validate_composite_types();
        self.validate_models();
        self.validate_relations();
        self.validate_back_relations();
        self.validate_defaults();
        self.validate_updated_at_fields();
        self.validate_computed_fields();
        self.validate_check_constraints();
        self.check_physical_name_collisions();

        if !self.errors.is_empty() {
            return Err(self.errors.pop_front().unwrap());
        }

        self.build_ir()
    }

    fn validate_collect_all(mut self) -> (Option<SchemaIr>, Vec<SchemaError>) {
        self.collect_names();

        if !self.errors.is_empty() {
            return (None, self.errors.into_iter().collect());
        }

        self.validate_datasources();
        self.validate_generators();
        self.validate_composite_types();
        self.validate_models();
        self.validate_relations();
        self.validate_back_relations();
        self.validate_defaults();
        self.validate_updated_at_fields();
        self.validate_computed_fields();
        self.validate_check_constraints();
        self.check_physical_name_collisions();

        if !self.errors.is_empty() {
            let mut all: Vec<SchemaError> = self.errors.into_iter().collect();
            all.extend(self.warnings);
            return (None, all);
        }

        let warnings: Vec<SchemaError> = self.warnings.drain(..).collect();
        match self.build_ir() {
            Ok(ir) => (Some(ir), warnings),
            Err(e) => (None, vec![e]),
        }
    }
}

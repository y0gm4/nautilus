use std::collections::HashMap;
use std::sync::OnceLock;

use nautilus_core::ColumnMarker;
use nautilus_protocol::ProtocolError;
use nautilus_schema::ast::StorageStrategy;
use nautilus_schema::ir::{FieldIr, ModelIr, ResolvedFieldType, ScalarType};

use crate::conversion::ValueHint;
use crate::filter::{FieldTypeMap, RelationMap};

#[derive(Debug, Clone)]
pub(crate) struct ScalarFieldMetadata {
    logical_name: String,
    db_name: String,
    marker: ColumnMarker,
    hint: Option<ValueHint>,
}

impl ScalarFieldMetadata {
    pub(crate) fn logical_name(&self) -> &str {
        &self.logical_name
    }

    pub(crate) fn db_name(&self) -> &str {
        &self.db_name
    }

    pub(crate) fn marker(&self) -> &ColumnMarker {
        &self.marker
    }

    pub(crate) fn hint(&self) -> Option<ValueHint> {
        self.hint
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PrimaryKeyFieldMetadata {
    logical_name: String,
    db_name: String,
    qualified_column: String,
}

impl PrimaryKeyFieldMetadata {
    pub(crate) fn logical_name(&self) -> &str {
        &self.logical_name
    }

    pub(crate) fn db_name(&self) -> &str {
        &self.db_name
    }

    pub(crate) fn qualified_column(&self) -> &str {
        &self.qualified_column
    }
}

#[derive(Debug)]
pub(crate) struct ModelMetadata {
    field_types: FieldTypeMap,
    logical_to_db: HashMap<String, String>,
    db_to_logical: HashMap<String, String>,
    scalar_fields: Vec<ScalarFieldMetadata>,
    scalar_markers: Vec<ColumnMarker>,
    scalar_hints: Vec<Option<ValueHint>>,
    primary_key_fields: Vec<PrimaryKeyFieldMetadata>,
    relation_map: OnceLock<Result<RelationMap, String>>,
}

impl ModelMetadata {
    pub(crate) fn new(model: &ModelIr) -> Self {
        let scalar_fields: Vec<_> = model
            .scalar_fields()
            .map(|field| ScalarFieldMetadata {
                logical_name: field.logical_name.clone(),
                db_name: field.db_name.clone(),
                marker: ColumnMarker::new(&model.db_name, &field.db_name),
                hint: field_value_hint(field),
            })
            .collect();

        let scalar_markers = scalar_fields
            .iter()
            .map(|field| field.marker.clone())
            .collect();
        let scalar_hints = scalar_fields
            .iter()
            .map(ScalarFieldMetadata::hint)
            .collect();

        let primary_key_fields = model
            .primary_key
            .fields()
            .into_iter()
            .filter_map(|logical_name| {
                scalar_fields
                    .iter()
                    .find(|field| field.logical_name() == logical_name)
                    .map(|field| PrimaryKeyFieldMetadata {
                        logical_name: field.logical_name.clone(),
                        db_name: field.db_name.clone(),
                        qualified_column: format!("{}__{}", model.db_name, field.db_name()),
                    })
            })
            .collect();

        Self {
            field_types: build_field_type_map(model),
            logical_to_db: build_logical_to_db_map(model),
            db_to_logical: build_db_to_logical_map(model),
            scalar_fields,
            scalar_markers,
            scalar_hints,
            primary_key_fields,
            relation_map: OnceLock::new(),
        }
    }

    pub(crate) fn field_types(&self) -> &FieldTypeMap {
        &self.field_types
    }

    pub(crate) fn logical_to_db(&self) -> &HashMap<String, String> {
        &self.logical_to_db
    }

    pub(crate) fn db_to_logical(&self) -> &HashMap<String, String> {
        &self.db_to_logical
    }

    pub(crate) fn scalar_fields(&self) -> &[ScalarFieldMetadata] {
        &self.scalar_fields
    }

    pub(crate) fn scalar_markers(&self) -> &[ColumnMarker] {
        &self.scalar_markers
    }

    pub(crate) fn scalar_hints(&self) -> &[Option<ValueHint>] {
        &self.scalar_hints
    }

    pub(crate) fn primary_key_fields(&self) -> &[PrimaryKeyFieldMetadata] {
        &self.primary_key_fields
    }

    pub(crate) fn relation_map<'a>(
        &'a self,
        model: &ModelIr,
        models: &HashMap<String, ModelIr>,
    ) -> Result<&'a RelationMap, ProtocolError> {
        match self.relation_map.get_or_init(|| {
            crate::handlers::build_relation_map(model, models).map_err(|error| match error {
                ProtocolError::QueryPlanning(message) => message,
                other => other.to_string(),
            })
        }) {
            Ok(map) => Ok(map),
            Err(message) => Err(ProtocolError::QueryPlanning(message.clone())),
        }
    }
}

pub(crate) fn build_field_type_map(model: &ModelIr) -> FieldTypeMap {
    model
        .fields
        .iter()
        .filter(|field| !matches!(field.field_type, ResolvedFieldType::Relation(_)))
        .flat_map(|field| {
            let mut entries = vec![(field.logical_name.clone(), field.field_type.clone())];
            if field.db_name != field.logical_name {
                entries.push((field.db_name.clone(), field.field_type.clone()));
            }
            entries
        })
        .collect()
}

pub(crate) fn build_logical_to_db_map(model: &ModelIr) -> HashMap<String, String> {
    model
        .scalar_fields()
        .flat_map(|field| {
            let mut entries = vec![(field.logical_name.clone(), field.db_name.clone())];
            if field.db_name != field.logical_name {
                entries.push((field.db_name.clone(), field.db_name.clone()));
            }
            entries
        })
        .collect()
}

pub(crate) fn build_db_to_logical_map(model: &ModelIr) -> HashMap<String, String> {
    model
        .scalar_fields()
        .map(|field| (field.db_name.clone(), field.logical_name.clone()))
        .collect()
}

pub(crate) fn field_value_hint(field: &FieldIr) -> Option<ValueHint> {
    if field.is_array && field.storage_strategy == Some(StorageStrategy::Json) {
        return Some(ValueHint::Json);
    }

    match &field.field_type {
        ResolvedFieldType::Scalar(ScalarType::Decimal { .. }) => Some(ValueHint::Decimal),
        ResolvedFieldType::Scalar(ScalarType::DateTime) => Some(ValueHint::DateTime),
        ResolvedFieldType::Scalar(ScalarType::Json | ScalarType::Jsonb) => Some(ValueHint::Json),
        ResolvedFieldType::Scalar(ScalarType::Uuid) => Some(ValueHint::Uuid),
        ResolvedFieldType::Scalar(ScalarType::Geometry) => Some(ValueHint::Geometry),
        ResolvedFieldType::Scalar(ScalarType::Geography) => Some(ValueHint::Geography),
        ResolvedFieldType::CompositeType { .. }
            if field.storage_strategy == Some(StorageStrategy::Json) =>
        {
            Some(ValueHint::Json)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_schema::validate_schema_source;

    fn parse_ir(source: &str) -> nautilus_schema::ir::SchemaIr {
        validate_schema_source(source)
            .expect("validation failed")
            .ir
    }

    #[test]
    fn model_metadata_caches_mappings_hints_and_relation_map() {
        let ir = parse_ir(
            r#"
model User {
  id        Int      @id @default(autoincrement())
  createdAt DateTime @map("created_at")
  profile   Profile?
}

model Profile {
  id     Int  @id @default(autoincrement())
  userId Int  @unique @map("user_id")
  user   User @relation(fields: [userId], references: [id])
}
"#,
        );
        let user_model = ir.models.get("User").expect("User model missing");
        let metadata = ModelMetadata::new(user_model);

        assert_eq!(
            metadata
                .logical_to_db()
                .get("createdAt")
                .map(String::as_str),
            Some("created_at")
        );
        assert_eq!(
            metadata
                .db_to_logical()
                .get("created_at")
                .map(String::as_str),
            Some("createdAt")
        );
        assert_eq!(metadata.scalar_hints().len(), 2);
        assert_eq!(metadata.scalar_hints()[1], Some(ValueHint::DateTime));
        assert_eq!(metadata.primary_key_fields().len(), 1);
        assert_eq!(
            metadata.primary_key_fields()[0].qualified_column(),
            "User__id"
        );

        let first = metadata
            .relation_map(user_model, &ir.models)
            .expect("relation map should build");
        let second = metadata
            .relation_map(user_model, &ir.models)
            .expect("relation map should be cached");

        assert!(std::ptr::eq(first, second));
        assert!(first.contains_key("profile"));
    }
}

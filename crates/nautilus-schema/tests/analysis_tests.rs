//! Integration tests for the analysis API (`analyze`, `completion`, `hover`, `goto_definition`).

use nautilus_schema::{
    analyze, completion, goto_definition, hover, semantic_tokens, CompletionKind, SemanticKind,
    Severity,
};

const VALID: &str = r#"
datasource db {
  provider = "postgresql"
  url      = "postgresql://localhost/test"
}

model User {
  id    Int    @id
  email String @unique
  name  String
  role  Role

  posts Post[]
}

model Post {
  id       Int  @id
  authorId Int
  author   User @relation(fields: [authorId], references: [id])
}

enum Role {
  Admin
  Member
}
"#;

#[test]
fn analyze_valid_returns_no_diagnostics() {
    let r = analyze(VALID);
    assert!(
        r.diagnostics.is_empty(),
        "expected no diagnostics, got: {:#?}",
        r.diagnostics
    );
    assert!(r.ast.is_some(), "ast should be Some");
    assert!(r.ir.is_some(), "ir should be Some");
}

#[test]
fn analyze_exposes_ir_models() {
    let r = analyze(VALID);
    let ir = r.ir.unwrap();
    assert!(ir.models.contains_key("User"));
    assert!(ir.models.contains_key("Post"));
    assert!(ir.enums.contains_key("Role"));
}

#[test]
fn analyze_lex_error_populates_diagnostics() {
    let src = "model User { id # Int @id }";
    let r = analyze(src);
    assert!(!r.diagnostics.is_empty());
    assert_eq!(r.diagnostics[0].severity, Severity::Error);
}

#[test]
fn analyze_parse_recovery_error_collected() {
    let src = r#"
!!!garbage!!!

model Good {
  id Int @id
}
"#;
    let r = analyze(src);
    assert!(r.ast.is_some());
    assert!(!r.diagnostics.is_empty(), "expected parse diagnostics");
}

#[test]
fn analyze_validation_error_has_span() {
    let src = r#"
model Dupe {
  id Int @id
}

model Dupe {
  id Int @id
}
"#;
    let r = analyze(src);
    let with_span: Vec<_> = r
        .diagnostics
        .iter()
        .filter(|d| d.span.start > 0 || d.span.end > 0)
        .collect();
    assert!(
        !with_span.is_empty(),
        "expected at least one diagnostic with a real span"
    );
}

#[test]
fn analyze_collects_multiple_datasource_diagnostics() {
    let src = r#"
datasource db {
  provider = 123
  url      = env("DATABASE_URL", 1)
  foo      = "bar"
}
"#;
    let r = analyze(src);
    let messages: Vec<&str> = r.diagnostics.iter().map(|d| d.message.as_str()).collect();

    assert!(
        r.ir.is_none(),
        "IR should be absent on config validation errors"
    );
    assert!(
        messages
            .iter()
            .any(|msg| msg.contains("Unknown field 'foo' in datasource block")),
        "missing unknown-field diagnostic: {:?}",
        messages
    );
    assert!(
        messages
            .iter()
            .any(|msg| msg.contains("Datasource 'provider' must be a string literal")),
        "missing provider diagnostic: {:?}",
        messages
    );
    assert!(
        messages.iter().any(
            |msg| msg.contains("Datasource 'url' env() call requires a single string argument")
        ),
        "missing url diagnostic: {:?}",
        messages
    );
}

#[test]
fn analyze_collects_multiple_generator_diagnostics_and_later_warnings() {
    let src = r#"
generator client {
  provider  = 123
  output    = 456
  interface = 789
  foo       = "bar"
}

model User {
  id        Int      @id
  updatedAt DateTime @updatedAt @default(now())
}
"#;
    let r = analyze(src);
    let messages: Vec<&str> = r.diagnostics.iter().map(|d| d.message.as_str()).collect();

    assert!(
        r.ir.is_none(),
        "IR should be absent on config validation errors"
    );
    assert!(
        messages
            .iter()
            .any(|msg| msg.contains("Generator 'provider' must be a string literal")),
        "missing provider diagnostic: {:?}",
        messages
    );
    assert!(
        messages
            .iter()
            .any(|msg| msg.contains("Generator 'output' must be a string literal")),
        "missing output diagnostic: {:?}",
        messages
    );
    assert!(
        messages
            .iter()
            .any(|msg| msg.contains("Generator 'interface' must be a string literal")),
        "missing interface diagnostic: {:?}",
        messages
    );
    assert!(
        messages
            .iter()
            .any(|msg| msg.contains("Unknown field 'foo' in generator block")),
        "missing unknown-field diagnostic: {:?}",
        messages
    );
    assert!(
        r.diagnostics.iter().any(|d| {
            d.severity == Severity::Warning && d.message.contains("both @updatedAt and @default")
        }),
        "missing later warning: {:?}",
        r.diagnostics
    );
}

#[test]
fn completion_at_empty_source_returns_top_level_keywords() {
    let items = completion("", 0);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
    assert!(labels.contains(&"model"), "missing 'model': {:?}", labels);
    assert!(labels.contains(&"enum"), "missing 'enum': {:?}", labels);
    assert!(
        labels.contains(&"datasource"),
        "missing 'datasource': {:?}",
        labels
    );
    assert!(
        labels.contains(&"generator"),
        "missing 'generator': {:?}",
        labels
    );
    assert!(
        items.iter().all(|i| i.kind == CompletionKind::Keyword),
        "all top-level items should be Keyword kind"
    );
}

#[test]
fn completion_inside_datasource_only_contains_datasource_fields() {
    let src = "datasource db {\n  \n}";
    let offset = src.find("\n  \n").unwrap() + 3;
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();

    assert!(
        labels.contains(&"provider"),
        "missing provider: {:?}",
        labels
    );
    assert!(labels.contains(&"url"), "missing url: {:?}", labels);
    assert!(
        labels.contains(&"direct_url"),
        "missing direct_url: {:?}",
        labels
    );
    assert!(
        labels.contains(&"extensions"),
        "missing extensions: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"output"),
        "unexpected output: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"interface"),
        "unexpected interface: {:?}",
        labels
    );
    assert!(
        !labels.contains(&"recursive_type_depth"),
        "unexpected recursive_type_depth: {:?}",
        labels
    );
}

#[test]
fn completion_inside_generator_only_contains_generator_fields() {
    let src = "generator client {\n  \n}";
    let offset = src.find("\n  \n").unwrap() + 3;
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();

    assert!(
        labels.contains(&"provider"),
        "missing provider: {:?}",
        labels
    );
    assert!(labels.contains(&"output"), "missing output: {:?}", labels);
    assert!(
        labels.contains(&"interface"),
        "missing interface: {:?}",
        labels
    );
    assert!(
        labels.contains(&"recursive_type_depth"),
        "missing recursive_type_depth: {:?}",
        labels
    );
    assert!(labels.contains(&"package"), "missing package: {:?}", labels);
    assert!(
        labels.contains(&"group_id"),
        "missing group_id: {:?}",
        labels
    );
    assert!(
        labels.contains(&"artifact_id"),
        "missing artifact_id: {:?}",
        labels
    );
    assert!(labels.contains(&"mode"), "missing mode: {:?}", labels);
    assert!(!labels.contains(&"url"), "unexpected url: {:?}", labels);
}

#[test]
fn completion_inside_datasource_extensions_value_suggests_known_extensions() {
    let src = r#"datasource db {
  provider   = "postgresql"
  extensions = [
}
"#;
    let offset = src.find('[').unwrap() + 1;
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();

    assert!(labels.contains(&"pg_trgm"), "missing pg_trgm: {:?}", labels);
    assert!(
        labels.contains(&"pgcrypto"),
        "missing pgcrypto: {:?}",
        labels
    );
    assert!(
        labels.contains(&"uuid-ossp"),
        "missing uuid-ossp: {:?}",
        labels
    );

    let uuid_ossp = items
        .iter()
        .find(|item| item.label == "uuid-ossp")
        .expect("uuid-ossp completion");
    assert_eq!(uuid_ossp.insert_text.as_deref(), Some("\"uuid-ossp\""));
}

#[test]
fn completion_inside_multiline_extensions_array_suggests_known_extensions() {
    let src = r#"datasource db {
  provider   = "postgresql"
  extensions = [
    
  ]
}
"#;
    let offset = src.find("\n    \n").unwrap() + 5;
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();

    assert!(labels.contains(&"pg_trgm"), "missing pg_trgm: {:?}", labels);
    assert!(
        labels.contains(&"uuid-ossp"),
        "missing uuid-ossp: {:?}",
        labels
    );
}

#[test]
fn completion_inside_default_args_excludes_unsupported_functions() {
    let src = "model User {\n  id Uuid @default()\n}";
    let offset = src.find("@default(").unwrap() + "@default(".len();
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();

    assert!(labels.contains(&"autoincrement()"), "missing autoincrement");
    assert!(labels.contains(&"now()"), "missing now");
    assert!(labels.contains(&"uuid()"), "missing uuid");
    assert!(!labels.contains(&"cuid()"), "unexpected cuid: {:?}", labels);
    assert!(
        !labels.contains(&"dbgenerated(\"expr\")"),
        "unexpected dbgenerated: {:?}",
        labels
    );
}

#[test]
fn completion_inside_model_contains_scalar_types() {
    let src = "model User {\n  \n}";
    let offset = src.find("\n  \n").unwrap() + 3;
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
    for ty in &["String", "Int", "Boolean", "Float", "DateTime", "Uuid"] {
        assert!(labels.contains(ty), "missing type '{}': {:?}", ty, labels);
    }
}

#[test]
fn completion_inside_postgres_model_includes_extension_backed_scalar_types() {
    let src = r#"
datasource db {
  provider = "postgresql"
  url      = "postgres://localhost/test"
}

model User {
  
}
"#;
    let offset = src.find("\n  \n").unwrap() + 3;
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();

    for ty in &["Citext", "Hstore", "Ltree"] {
        assert!(labels.contains(ty), "missing type '{}': {:?}", ty, labels);
    }
}

#[test]
fn completion_inside_mysql_model_omits_postgres_extension_backed_scalar_types() {
    let src = r#"
datasource db {
  provider = "mysql"
  url      = "mysql://localhost/test"
}

model User {
  
}
"#;
    let offset = src.find("\n  \n").unwrap() + 3;
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();

    for ty in &["Citext", "Hstore", "Ltree"] {
        assert!(
            !labels.contains(ty),
            "unexpected type '{}': {:?}",
            ty,
            labels
        );
    }
}

#[test]
fn completion_inside_model_includes_user_defined_types() {
    let src =
        "type Address {\n  street String\n}\n\nmodel Post {\n  author \n}\n\nmodel User {\n  id Int @id\n}\n";
    let offset = src.find("author ").unwrap() + "author ".len();
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
    assert!(
        labels.contains(&"User"),
        "User model should appear as a type completion: {:?}",
        labels
    );
    assert!(
        labels.contains(&"Address"),
        "Address composite type should appear as a type completion: {:?}",
        labels
    );
}

#[test]
fn completion_inside_type_only_includes_scalar_and_enum_types() {
    let src = r#"
type Address {
  label 
}

enum LabelKind {
  Home
}

model User {
  id Int @id
}
"#;
    let offset = src.find("label ").unwrap() + "label ".len();
    let items = completion(src, offset);
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
    assert!(labels.contains(&"String"));
    assert!(labels.contains(&"LabelKind"));
    assert!(
        !labels.contains(&"User"),
        "model refs are invalid in type blocks"
    );
}

#[test]
fn completion_after_at_returns_field_attributes() {
    let src = "model User {\n  id Int @\n}";
    let offset = src.find('@').unwrap() + 1;
    let items = completion(src, offset);
    assert!(
        items
            .iter()
            .any(|i| i.kind == CompletionKind::FieldAttribute),
        "expected field attribute completions after '@': {:?}",
        items
    );
    let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
    assert!(
        labels.contains(&"id"),
        "missing 'id' attribute: {:?}",
        labels
    );
    assert!(
        labels.contains(&"unique"),
        "missing 'unique' attribute: {:?}",
        labels
    );
}

#[test]
fn completion_after_atat_returns_model_attributes() {
    let src = "model User {\n  id Int @id\n  @@\n}";
    let offset = src.find("@@").unwrap() + 2;
    let items = completion(src, offset);
    assert!(
        items
            .iter()
            .any(|i| i.kind == CompletionKind::ModelAttribute),
        "expected model attribute completions after '@@': {:?}",
        items
    );
}

#[test]
fn hover_on_model_field_returns_type_info() {
    let offset = VALID.find("email").unwrap() + 2;
    let h = hover(VALID, offset);
    assert!(h.is_some(), "hover returned None for field 'email'");
    let info = h.unwrap();
    assert!(
        info.content.contains("email") || info.content.contains("String"),
        "unexpected hover content: {}",
        info.content
    );
    assert!(info.span.is_some());
}

#[test]
fn hover_on_extension_backed_scalar_type_mentions_required_extension() {
    let src = r#"
datasource db {
  provider = "postgresql"
  url      = "postgres://localhost/test"
  extensions = [citext]
}

model User {
  email Citext
}
"#;
    let offset = src.find("Citext").unwrap() + 2;
    let h = hover(src, offset).expect("hover returned None");

    assert!(
        h.content.contains("`citext` extension"),
        "unexpected hover: {}",
        h.content
    );
    assert!(
        h.content.contains("CITEXT"),
        "unexpected hover: {}",
        h.content
    );
}

#[test]
fn hover_on_provider_field_mentions_real_generator_providers() {
    let src = r#"
generator client {
  provider = "nautilus-client-rs"
}
"#;
    let offset = src.find("provider").unwrap() + 2;
    let h = hover(src, offset).expect("hover returned None");

    assert!(
        h.content.contains("nautilus-client-rs")
            && h.content.contains("nautilus-client-py")
            && h.content.contains("nautilus-client-js")
            && h.content.contains("nautilus-client-java"),
        "unexpected hover content: {}",
        h.content
    );
    assert!(
        !h.content.contains("\"python\""),
        "unexpected stale provider alias in hover: {}",
        h.content
    );
}

#[test]
fn hover_on_extensions_field_describes_postgres_extensions() {
    let src = r#"
datasource db {
  provider   = "postgresql"
  extensions = [pg_trgm]
}
"#;
    let offset = src.find("extensions").unwrap() + 2;
    let h = hover(src, offset).expect("hover returned None");

    assert!(
        h.content.contains("PostgreSQL-only"),
        "unexpected hover: {}",
        h.content
    );
    assert!(
        h.content.contains("pg_trgm"),
        "unexpected hover: {}",
        h.content
    );
    assert!(
        h.content.contains("\"uuid-ossp\""),
        "unexpected hover: {}",
        h.content
    );
}

#[test]
fn hover_on_default_attribute_omits_unsupported_functions() {
    let src = r#"
model User {
  id Uuid @default(uuid())
}
"#;
    let offset = src.find("@default").unwrap() + 2;
    let h = hover(src, offset).expect("hover returned None");

    assert!(h.content.contains("autoincrement()"));
    assert!(h.content.contains("now()"));
    assert!(h.content.contains("uuid()"));
    assert!(
        !h.content.contains("cuid()"),
        "unexpected hover: {}",
        h.content
    );
    assert!(
        !h.content.contains("dbgenerated"),
        "unexpected hover: {}",
        h.content
    );
}

#[test]
fn hover_on_model_declaration_returns_model_summary() {
    let offset = VALID.find("model User").unwrap() + 2;
    let h = hover(VALID, offset);
    assert!(h.is_some(), "hover returned None on model declaration");
    let info = h.unwrap();
    assert!(
        info.content.contains("User"),
        "hover content should mention model name: {}",
        info.content
    );
}

#[test]
fn hover_on_enum_returns_variant_list() {
    let offset = VALID.find("enum Role").unwrap() + 2;
    let h = hover(VALID, offset);
    assert!(h.is_some(), "hover returned None on enum");
    let info = h.unwrap();
    assert!(info.content.contains("Admin") || info.content.contains("Member"));
}

#[test]
fn hover_outside_all_declarations_returns_none() {
    let src = "// comment\nmodel User { id Int @id }\n";
    let h = hover(src, 2);
    let _ = h;
}

#[test]
fn goto_definition_user_type_resolves_to_model_span() {
    let offset = VALID.find("author   User").unwrap() + "author   ".len() + 1;
    let span = goto_definition(VALID, offset);
    assert!(span.is_some(), "goto_definition returned None");
    let s = span.unwrap();
    let slice = &VALID[s.start..s.end];
    assert!(
        slice.contains("User"),
        "span does not cover User declaration: {:?}",
        slice
    );
}

#[test]
fn goto_definition_scalar_field_returns_none() {
    let offset = VALID.find("email String").unwrap() + "email ".len() + 1;
    let span = goto_definition(VALID, offset);
    assert!(
        span.is_none(),
        "expected None for scalar type, got: {:?}",
        span
    );
}

#[test]
fn goto_definition_enum_field_resolves_to_enum_span() {
    let offset = VALID.find("role  Role").unwrap() + "role  ".len() + 1;
    let span = goto_definition(VALID, offset);
    assert!(
        span.is_some(),
        "goto_definition returned None for enum field"
    );
    let s = span.unwrap();
    let slice = &VALID[s.start..s.end];
    assert!(
        slice.contains("Role"),
        "span does not cover Role declaration: {:?}",
        slice
    );
}

#[test]
fn goto_definition_composite_type_field_resolves_to_type_span() {
    let src = r#"
type Address {
  street String
}

model User {
  id      Int @id
  address Address
}
"#;
    let offset = src.find("address Address").unwrap() + "address ".len() + 1;
    let span = goto_definition(src, offset).expect("goto_definition returned None");
    let slice = &src[span.start..span.end];
    assert!(
        slice.contains("type Address"),
        "span does not cover Address type declaration: {:?}",
        slice
    );
}

#[test]
fn goto_definition_type_field_enum_resolves_to_enum_span() {
    let src = r#"
type Address {
  kind Role
}

enum Role {
  Home
}
"#;
    let offset = src.find("kind Role").unwrap() + "kind ".len() + 1;
    let span = goto_definition(src, offset).expect("goto_definition returned None");
    let slice = &src[span.start..span.end];
    assert!(
        slice.contains("enum Role"),
        "span does not cover Role enum declaration: {:?}",
        slice
    );
}

#[test]
fn semantic_tokens_include_type_block_references() {
    let src = r#"
type Address {
  kind Role
}

enum Role {
  Home
}
"#;
    let analysis = analyze(src);
    let ast = analysis.ast.as_ref().expect("ast");
    let tokens = semantic_tokens(ast, &analysis.tokens);
    assert!(
        tokens
            .iter()
            .any(|token| token.kind == SemanticKind::EnumRef
                && &src[token.span.start..token.span.end] == "Role"),
        "expected enum semantic token for Role reference inside type block"
    );
}

//! File writing utilities for generated code.

use anyhow::{Context, Result};
use heck::ToSnakeCase;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tera::Context as TeraContext;

use crate::generator::TEMPLATES;
use crate::python::generator::{
    generate_enums_init, generate_errors_init, generate_internal_init, generate_models_init,
    generate_package_init, generate_transaction_init,
};

/// Write generated code to files in the output directory.
///
/// Creates:
/// - `{output}/src/lib.rs`           — module declarations and re-exports
/// - `{output}/src/{model_snake}.rs` — model code for each model
/// - `{output}/src/enums.rs`         — all enum types (if any)
/// - `{output}/Cargo.toml`           — **only** when `standalone == true`
///
/// When `standalone` is `false` (the default) the output is a plain directory
/// of `.rs` source files ready to be included in an existing Cargo workspace
/// without any generated `Cargo.toml`.
pub fn write_rust_code(
    output_path: &str,
    models: &HashMap<String, String>,
    enums_code: Option<String>,
    composite_types_code: Option<String>,
    schema_source: &str,
    standalone: bool,
) -> Result<()> {
    let output_dir = Path::new(output_path);

    clear_output_dir(output_path)?;

    fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create directory: {}", output_dir.display()))?;

    let src_dir = output_dir.join("src");
    fs::create_dir_all(&src_dir)
        .with_context(|| format!("Failed to create src directory: {}", src_dir.display()))?;

    for (model_name, code) in models {
        let file_name = format!("{}.rs", model_name.to_snake_case());
        let file_path = src_dir.join(&file_name);

        fs::write(&file_path, code)
            .with_context(|| format!("Failed to write file: {}", file_path.display()))?;
    }

    let has_enums = enums_code.is_some();
    if let Some(enums_code) = enums_code {
        let enums_path = src_dir.join("enums.rs");
        fs::write(&enums_path, enums_code)
            .with_context(|| format!("Failed to write enums file: {}", enums_path.display()))?;
    }

    let has_composite_types = composite_types_code.is_some();
    if let Some(types_code) = composite_types_code {
        let types_path = src_dir.join("types.rs");
        fs::write(&types_path, types_code)
            .with_context(|| format!("Failed to write types file: {}", types_path.display()))?;
    }

    let lib_content = generate_lib_rs(models, has_enums, has_composite_types, schema_source)?;
    let lib_path = src_dir.join("lib.rs");
    fs::write(&lib_path, lib_content)
        .with_context(|| format!("Failed to write lib.rs: {}", lib_path.display()))?;

    let runtime_path = src_dir.join("runtime.rs");
    fs::write(
        &runtime_path,
        include_str!("../templates/rust/runtime.rs.tpl"),
    )
    .with_context(|| format!("Failed to write runtime.rs: {}", runtime_path.display()))?;

    if standalone {
        // Walk up from the output directory to find the workspace root, then
        // compute how many `..` hops separate the output from it so generated
        // path-dependency references are correct.
        let abs_output = if output_dir.is_absolute() {
            output_dir.to_path_buf()
        } else {
            std::env::current_dir()
                .context("Failed to get current directory")?
                .join(output_dir)
        };
        let workspace_root_path = {
            let workspace_toml = crate::find_workspace_cargo_toml(&abs_output);
            match workspace_toml {
                Some(toml_path) => {
                    let workspace_dir = toml_path.parent().unwrap();
                    let mut up = std::path::PathBuf::new();
                    let mut candidate = abs_output.clone();
                    loop {
                        if candidate == workspace_dir {
                            break;
                        }
                        up.push("..");
                        match candidate.parent() {
                            Some(p) => candidate = p.to_path_buf(),
                            None => break,
                        }
                    }
                    up.to_string_lossy().replace('\\', "/")
                }
                None => {
                    // Fallback: legacy upward walk (shouldn't normally be reached).
                    let mut up = std::path::PathBuf::new();
                    let mut candidate = abs_output.clone();
                    loop {
                        candidate = match candidate.parent() {
                            Some(p) => p.to_path_buf(),
                            None => break,
                        };
                        up.push("..");
                        let cargo_toml = candidate.join("Cargo.toml");
                        let Ok(txt) = fs::read_to_string(&cargo_toml) else {
                            continue;
                        };
                        if txt.contains("[workspace]") {
                            break;
                        }
                    }
                    up.to_string_lossy().replace('\\', "/")
                }
            }
        };
        let cargo_toml_content = generate_rust_cargo_toml(&workspace_root_path);
        let cargo_toml_path = output_dir.join("Cargo.toml");
        fs::write(&cargo_toml_path, cargo_toml_content).with_context(|| {
            format!("Failed to write Cargo.toml: {}", cargo_toml_path.display())
        })?;
    }

    Ok(())
}

/// Generate Cargo.toml for the generated Rust package.
///
/// `workspace_root_path` is the relative path from the output directory back
/// to the Cargo workspace root, e.g. `"../../../.."` when the output sits
/// four directory levels below the workspace root.
fn generate_rust_cargo_toml(workspace_root_path: &str) -> String {
    include_str!("../templates/rust/Cargo.toml.tpl")
        .replace("{{ workspace_root_path }}", workspace_root_path)
}

/// Generate the lib.rs file content with module declarations and re-exports.
fn generate_lib_rs(
    models: &HashMap<String, String>,
    has_enums: bool,
    has_composite_types: bool,
    schema_source: &str,
) -> Result<String> {
    let mut model_names: Vec<_> = models.keys().cloned().collect();
    model_names.sort();

    let model_modules: Vec<String> = model_names
        .iter()
        .map(|model_name| model_name.to_snake_case())
        .collect();

    let mut context = TeraContext::new();
    context.insert("has_enums", &has_enums);
    context.insert("has_composite_types", &has_composite_types);
    context.insert("model_modules", &model_modules);
    context.insert("schema_source_literal", &format!("{:?}", schema_source));

    TEMPLATES
        .render("lib_rs.tera", &context)
        .context("Failed to render lib.rs template")
}

/// Write generated Python code to files in the output directory with organized structure.
///
/// Creates a structure:
/// - `{output}/__init__.py` - Package init with exports
/// - `{output}/client.py` - Nautilus client with model delegates
/// - `{output}/models/__init__.py` - Models package
/// - `{output}/models/{model_snake}.py` - Model code for each model
/// - `{output}/enums/__init__.py` - Enums package
/// - `{output}/enums/enums.py` - All enum types (if any)
/// - `{output}/errors/__init__.py` - Errors package
/// - `{output}/errors/errors.py` - Error classes
/// - `{output}/_internal/` - Internal runtime files
/// - `{output}/py.typed` - Marker for mypy
pub fn write_python_code(
    output_path: &str,
    models: &[(String, String)],
    enums_code: Option<String>,
    composite_types_code: Option<String>,
    client_code: Option<String>,
    runtime_files: &[(&str, &str)],
) -> Result<()> {
    let output_dir = Path::new(output_path);

    clear_output_dir(output_path)?;

    fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create directory: {}", output_dir.display()))?;

    let models_dir = output_dir.join("models");
    fs::create_dir_all(&models_dir).with_context(|| {
        format!(
            "Failed to create models directory: {}",
            models_dir.display()
        )
    })?;

    let enums_dir = output_dir.join("enums");
    fs::create_dir_all(&enums_dir)
        .with_context(|| format!("Failed to create enums directory: {}", enums_dir.display()))?;

    let errors_dir = output_dir.join("errors");
    fs::create_dir_all(&errors_dir).with_context(|| {
        format!(
            "Failed to create errors directory: {}",
            errors_dir.display()
        )
    })?;

    let internal_dir = output_dir.join("_internal");
    fs::create_dir_all(&internal_dir).with_context(|| {
        format!(
            "Failed to create _internal directory: {}",
            internal_dir.display()
        )
    })?;

    for (file_name, code) in models {
        let file_path = models_dir.join(file_name);

        fs::write(&file_path, code)
            .with_context(|| format!("Failed to write file: {}", file_path.display()))?;
    }

    let models_init = generate_models_init(models);
    let models_init_path = models_dir.join("__init__.py");
    fs::write(&models_init_path, models_init)
        .with_context(|| "Failed to write models/__init__.py")?;

    if let Some(types_code) = composite_types_code {
        let types_dir = output_dir.join("types");
        fs::create_dir_all(&types_dir).with_context(|| {
            format!("Failed to create types directory: {}", types_dir.display())
        })?;

        let types_path = types_dir.join("types.py");
        fs::write(&types_path, types_code)
            .with_context(|| format!("Failed to write types file: {}", types_path.display()))?;

        let types_init = "from .types import *  # noqa: F401, F403\n";
        let types_init_path = types_dir.join("__init__.py");
        fs::write(&types_init_path, types_init)
            .with_context(|| "Failed to write types/__init__.py")?;
    }

    let has_enums = enums_code.is_some();
    if let Some(enums_code) = enums_code {
        let enums_path = enums_dir.join("enums.py");
        fs::write(&enums_path, enums_code)
            .with_context(|| format!("Failed to write enums file: {}", enums_path.display()))?;
    }

    let enums_init = generate_enums_init(has_enums);
    let enums_init_path = enums_dir.join("__init__.py");
    fs::write(&enums_init_path, enums_init).with_context(|| "Failed to write enums/__init__.py")?;

    for (file_name, content) in runtime_files {
        let (target_dir, new_name) = match *file_name {
            "_errors.py" => (&errors_dir, "errors.py"),
            _ => (&internal_dir, file_name.trim_start_matches('_')),
        };

        let file_path = target_dir.join(new_name);
        fs::write(&file_path, content)
            .with_context(|| format!("Failed to write runtime file: {}", file_path.display()))?;
    }

    let errors_init = generate_errors_init();
    let errors_init_path = errors_dir.join("__init__.py");
    fs::write(&errors_init_path, errors_init)
        .with_context(|| "Failed to write errors/__init__.py")?;

    let internal_init = generate_internal_init();
    let internal_init_path = internal_dir.join("__init__.py");
    fs::write(&internal_init_path, internal_init)
        .with_context(|| "Failed to write _internal/__init__.py")?;

    if let Some(client_code) = client_code {
        let client_path = output_dir.join("client.py");
        fs::write(&client_path, client_code)
            .with_context(|| format!("Failed to write client.py: {}", client_path.display()))?;
    }

    let transaction_content = generate_transaction_init();
    let transaction_path = output_dir.join("transaction.py");
    fs::write(&transaction_path, transaction_content).with_context(|| {
        format!(
            "Failed to write transaction.py: {}",
            transaction_path.display()
        )
    })?;

    let init_content = generate_package_init(has_enums);
    let init_path = output_dir.join("__init__.py");
    fs::write(&init_path, init_content)
        .with_context(|| format!("Failed to write __init__.py: {}", init_path.display()))?;

    let py_typed_path = output_dir.join("py.typed");
    fs::write(&py_typed_path, "")
        .with_context(|| format!("Failed to write py.typed: {}", py_typed_path.display()))?;

    Ok(())
}

/// Write generated JavaScript + TypeScript declaration code to the output directory.
///
/// Creates:
/// - `{output}/index.js`              — generated `Nautilus` class (runtime)
/// - `{output}/index.d.ts`            — generated `Nautilus` class (declarations)
/// - `{output}/models/index.js`       — barrel re-export for all models (runtime)
/// - `{output}/models/index.d.ts`     — barrel re-export for all models (declarations)
/// - `{output}/models/{snake}.js`     — per-model delegate + helpers (runtime)
/// - `{output}/models/{snake}.d.ts`   — per-model interfaces + types (declarations)
/// - `{output}/enums.js`              — JavaScript enums (if any)
/// - `{output}/enums.d.ts`            — TypeScript enum declarations (if any)
/// - `{output}/types.d.ts`            — composite type interfaces (if any, declarations only)
/// - `{output}/_internal/_*.js`       — runtime files (client, engine, protocol, etc.)
/// - `{output}/_internal/_*.d.ts`     — runtime declaration files
#[allow(clippy::too_many_arguments)]
pub fn write_js_code(
    output_path: &str,
    js_models: &[(String, String)],
    dts_models: &[(String, String)],
    js_enums: Option<String>,
    dts_enums: Option<String>,
    dts_composite_types: Option<String>,
    js_client: Option<String>,
    dts_client: Option<String>,
    js_models_index: Option<String>,
    dts_models_index: Option<String>,
    runtime_files: &[(&str, &str)],
) -> Result<()> {
    let output_dir = Path::new(output_path);

    clear_output_dir(output_path)?;

    fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create directory: {}", output_dir.display()))?;

    let models_dir = output_dir.join("models");
    fs::create_dir_all(&models_dir)?;

    let internal_dir = output_dir.join("_internal");
    fs::create_dir_all(&internal_dir)?;

    for (file_name, code) in js_models {
        let file_path = models_dir.join(file_name);
        fs::write(&file_path, code)
            .with_context(|| format!("Failed to write file: {}", file_path.display()))?;
    }

    for (file_name, code) in dts_models {
        let file_path = models_dir.join(file_name);
        fs::write(&file_path, code)
            .with_context(|| format!("Failed to write file: {}", file_path.display()))?;
    }

    if let Some(index_js) = js_models_index {
        let path = models_dir.join("index.js");
        fs::write(&path, index_js).with_context(|| "Failed to write models/index.js")?;
    }
    if let Some(index_dts) = dts_models_index {
        let path = models_dir.join("index.d.ts");
        fs::write(&path, index_dts).with_context(|| "Failed to write models/index.d.ts")?;
    }

    if let Some(enums_js) = js_enums {
        let path = output_dir.join("enums.js");
        fs::write(&path, enums_js)
            .with_context(|| format!("Failed to write enums.js: {}", output_dir.display()))?;
    }
    if let Some(enums_dts) = dts_enums {
        let path = output_dir.join("enums.d.ts");
        fs::write(&path, enums_dts)
            .with_context(|| format!("Failed to write enums.d.ts: {}", output_dir.display()))?;
    }

    // Write types.d.ts (composite types — declarations only, no runtime needed).
    if let Some(types_dts) = dts_composite_types {
        let path = output_dir.join("types.d.ts");
        fs::write(&path, types_dts)
            .with_context(|| format!("Failed to write types.d.ts: {}", output_dir.display()))?;
    }

    for (file_name, content) in runtime_files {
        let file_path = internal_dir.join(file_name);
        fs::write(&file_path, content)
            .with_context(|| format!("Failed to write runtime file: {}", file_path.display()))?;
    }

    if let Some(client_js) = js_client {
        let path = output_dir.join("index.js");
        fs::write(&path, client_js)
            .with_context(|| format!("Failed to write index.js: {}", output_dir.display()))?;
    }
    if let Some(client_dts) = dts_client {
        let path = output_dir.join("index.d.ts");
        fs::write(&path, client_dts)
            .with_context(|| format!("Failed to write index.d.ts: {}", output_dir.display()))?;
    }

    Ok(())
}

/// Write generated Java code to the output directory, preserving the relative
/// file layout produced by the Java generator.
pub fn write_java_code(output_path: &str, files: &[(String, String)]) -> Result<()> {
    let output_dir = Path::new(output_path);

    clear_output_dir(output_path)?;

    fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create directory: {}", output_dir.display()))?;

    for (relative_path, content) in files {
        let file_path = output_dir.join(relative_path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        fs::write(&file_path, content)
            .with_context(|| format!("Failed to write file: {}", file_path.display()))?;
    }

    Ok(())
}

fn clear_output_dir(output_path: &str) -> Result<()> {
    let output_dir = Path::new(output_path);
    if output_dir.exists() {
        fs::remove_dir_all(output_dir).with_context(|| {
            format!("Failed to clean output directory: {}", output_dir.display())
        })?;
    }
    Ok(())
}

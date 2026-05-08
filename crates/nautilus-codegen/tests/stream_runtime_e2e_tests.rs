use nautilus_codegen::{
    java::generate_java_client,
    js::{generate_all_js_models, generate_js_client, generate_js_models_index, js_runtime_files},
    python::{generate_all_python_models, generate_python_client, python_runtime_files},
    writer::{write_java_code, write_js_code, write_python_code},
};
use nautilus_schema::{ir::SchemaIr, validate_schema_source};
use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::OnceLock,
};
use tempfile::TempDir;

const ROW_COUNT: usize = 10_050;
const BREAK_AFTER: usize = 10;
const TAIL_SKIP: usize = ROW_COUNT - 1;
const EXPECTED_FOLLOW: &str = "User-00001,User-00002,User-00003,User-00004,User-00005";
const EXPECTED_TAIL: &str = "User-10050";
const JAVA_JACKSON_VERSION: &str = "2.17.2";
const PYDANTIC_STUB: &str = include_str!("fixtures/stream_runtime_e2e/pydantic_stub.py");
const PYTHON_RUNNER_TEMPLATE: &str =
    include_str!("fixtures/stream_runtime_e2e/run_python_stream_many.py.tpl");
const JS_PACKAGE_JSON: &str = include_str!("fixtures/stream_runtime_e2e/package.json");
const JS_RUNNER_TEMPLATE: &str =
    include_str!("fixtures/stream_runtime_e2e/run_js_stream_many.mjs.tpl");
const JAVA_RUNNER_TEMPLATE: &str =
    include_str!("fixtures/stream_runtime_e2e/StreamManyE2e.java.tpl");

const BASE_SCHEMA: &str = r#"
datasource db {
  provider = "sqlite"
  url      = env("DATABASE_URL")
}

model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#;

const JAVA_SCHEMA: &str = r#"
datasource db {
  provider = "sqlite"
  url      = env("DATABASE_URL")
}

generator client {
  provider    = "nautilus-client-java"
  output      = "./generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  interface   = "sync"
}

model User {
  id   Int    @id @default(autoincrement())
  name String
}
"#;

struct Fixture {
    _tempdir: TempDir,
    root: PathBuf,
    schema_path: PathBuf,
    bin_path: PathBuf,
    path_env: OsString,
}

fn validate(source: &str) -> SchemaIr {
    validate_schema_source(source)
        .expect("schema validation failed")
        .ir
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir should have a parent")
        .parent()
        .expect("workspace root should exist")
        .to_path_buf()
}

fn binary_name() -> &'static str {
    if cfg!(windows) {
        "nautilus.exe"
    } else {
        "nautilus"
    }
}

fn command_exists(name: &str) -> bool {
    Command::new(name)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

fn prefixed_path(dir: &Path) -> OsString {
    let mut paths = vec![dir.to_path_buf()];
    if let Some(current) = env::var_os("PATH") {
        paths.extend(env::split_paths(&current));
    }
    env::join_paths(paths).expect("failed to construct PATH")
}

fn format_name(index: usize) -> String {
    format!("User-{index:05}")
}

fn assert_common_output(values: &BTreeMap<String, String>) {
    let expected_count = BREAK_AFTER.to_string();
    let expected_first = format_name(1);
    let expected_tenth = format_name(BREAK_AFTER);

    assert_eq!(
        values.get("count").map(String::as_str),
        Some(expected_count.as_str())
    );
    assert_eq!(
        values.get("first").map(String::as_str),
        Some(expected_first.as_str())
    );
    assert_eq!(
        values.get("tenth").map(String::as_str),
        Some(expected_tenth.as_str())
    );
    assert_eq!(
        values.get("follow").map(String::as_str),
        Some(EXPECTED_FOLLOW)
    );
    assert_eq!(values.get("tail").map(String::as_str), Some(EXPECTED_TAIL));
}

fn parse_key_values(stdout: &str) -> BTreeMap<String, String> {
    stdout
        .lines()
        .filter_map(|line| line.trim().split_once('='))
        .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        .collect()
}

fn render_fixture(template: &str, replacements: &[(&str, &str)]) -> String {
    let mut rendered = template.to_string();
    for (placeholder, value) in replacements {
        rendered = rendered.replace(placeholder, value);
    }
    rendered
}

fn render_output(output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("stdout:\n{stdout}\n\nstderr:\n{stderr}")
}

fn run_checked(command: &mut Command, context: &str) -> String {
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("{context}: failed to execute command: {error}"));
    assert!(
        output.status.success(),
        "{context}: command failed\n{}",
        render_output(&output)
    );
    String::from_utf8(output.stdout).expect("stdout should be valid utf-8")
}

fn ensure_nautilus_binary() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();

    BIN.get_or_init(|| {
        let root = workspace_root();
        let path = root.join("target").join("debug").join(binary_name());
        if path.is_file() {
            return path;
        }

        let status = Command::new("cargo")
            .args([
                "build",
                "--quiet",
                "--offline",
                "--package",
                "nautilus-cli",
                "--bin",
                "nautilus",
            ])
            .current_dir(&root)
            .status()
            .expect("failed to run cargo build for nautilus binary");

        assert!(
            status.success(),
            "failed to build workspace nautilus binary"
        );
        assert!(
            path.is_file(),
            "nautilus binary was not produced at {path:?}"
        );
        path
    })
    .clone()
}

fn create_fixture(prefix: &str) -> Fixture {
    let root = workspace_root();
    let tempdir = tempfile::tempdir_in(&root).expect("failed to create tempdir in workspace");
    let fixture_root = tempdir.path().to_path_buf();
    let db_path = fixture_root.join("stream-many-e2e.sqlite");
    let schema_path = fixture_root.join("schema.nautilus");
    let db_url = format!("sqlite:///{}", db_path.to_string_lossy().replace('\\', "/"));
    let dot_env = fixture_root.join(".env");

    seed_sqlite_database(&db_path, prefix);
    fs::write(&schema_path, BASE_SCHEMA).expect("failed to write schema");
    fs::write(&dot_env, format!("DATABASE_URL={db_url}\n")).expect("failed to write .env");

    let bin_path = ensure_nautilus_binary();
    let bin_dir = bin_path
        .parent()
        .expect("nautilus binary should have a parent directory")
        .to_path_buf();

    Fixture {
        _tempdir: tempdir,
        root: fixture_root,
        schema_path,
        bin_path,
        path_env: prefixed_path(&bin_dir),
    }
}

fn seed_sqlite_database(db_path: &Path, label: &str) {
    let sql = format!(
        r#"
PRAGMA journal_mode = WAL;
CREATE TABLE "User" (
  id   INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL
);
WITH RECURSIVE seq(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM seq WHERE x < {ROW_COUNT}
)
INSERT INTO "User"(name)
SELECT printf('User-%05d', x) FROM seq;
"#
    );

    let mut child = Command::new("sqlite3")
        .arg(db_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("failed to spawn sqlite3 for {label}: {error}"));

    child
        .stdin
        .as_mut()
        .expect("sqlite3 stdin should be available")
        .write_all(sql.as_bytes())
        .expect("failed to write sqlite seed SQL");

    let output = child
        .wait_with_output()
        .expect("failed to wait for sqlite3");
    assert!(
        output.status.success(),
        "sqlite3 failed while creating fixture {label}\n{}",
        render_output(&output)
    );
}

fn generate_python_client_fixture(output_dir: &Path, schema_path: &str) {
    let ir = validate(BASE_SCHEMA);
    let models = generate_all_python_models(&ir, true, 0);
    let runtime_files = python_runtime_files();
    let client_code = Some(generate_python_client(&ir.models, schema_path, true));

    write_python_code(
        output_dir
            .to_str()
            .expect("python output path should be utf-8"),
        &models,
        None,
        None,
        &[],
        client_code,
        &runtime_files,
    )
    .expect("failed to write generated python client");
}

fn generate_js_client_fixture(output_dir: &Path, schema_path: &str) {
    let ir = validate(BASE_SCHEMA);
    let (js_models, dts_models) = generate_all_js_models(&ir);
    let (js_client, dts_client) = generate_js_client(&ir.models, schema_path);
    let (js_models_index, dts_models_index) = generate_js_models_index(&js_models);
    let runtime_files = js_runtime_files();

    write_js_code(
        output_dir.to_str().expect("js output path should be utf-8"),
        &js_models,
        &dts_models,
        None,
        None,
        None,
        &[],
        &[],
        Some(js_client),
        Some(dts_client),
        Some(js_models_index),
        Some(dts_models_index),
        &runtime_files,
    )
    .expect("failed to write generated js client");
}

fn java_test_classpath() -> Option<OsString> {
    if let Some(explicit) = env::var_os("NAUTILUS_JAVA_TEST_CLASSPATH") {
        if !explicit.is_empty() {
            return Some(explicit);
        }
    }

    let base = workspace_root()
        .join("target")
        .join("test-jars")
        .join(format!("jackson-{JAVA_JACKSON_VERSION}"));
    let jars = [
        base.join(format!("jackson-annotations-{JAVA_JACKSON_VERSION}.jar")),
        base.join(format!("jackson-core-{JAVA_JACKSON_VERSION}.jar")),
        base.join(format!("jackson-databind-{JAVA_JACKSON_VERSION}.jar")),
        base.join(format!(
            "jackson-datatype-jsr310-{JAVA_JACKSON_VERSION}.jar"
        )),
    ];

    if jars.iter().all(|jar| jar.is_file()) {
        Some(env::join_paths(jars).expect("failed to build Java classpath"))
    } else {
        None
    }
}

fn generate_java_client_fixture(output_dir: &Path, schema_path: &str) {
    let ir = validate(JAVA_SCHEMA);
    let files =
        generate_java_client(&ir, schema_path, false).expect("failed to generate Java client");
    write_java_code(
        output_dir
            .to_str()
            .expect("java output path should be utf-8"),
        &files,
    )
    .expect("failed to write generated Java client");
}

fn write_java_runner(output_dir: &Path) {
    let runner_path = output_dir.join("src/main/java/com/acme/db/e2e/StreamManyE2e.java");
    fs::create_dir_all(
        runner_path
            .parent()
            .expect("java runner should have a parent directory"),
    )
    .expect("failed to create Java runner directory");
    let break_after = BREAK_AFTER.to_string();
    let tail_skip = TAIL_SKIP.to_string();
    fs::write(
        &runner_path,
        render_fixture(
            JAVA_RUNNER_TEMPLATE,
            &[
                ("__BREAK_AFTER__", break_after.as_str()),
                ("__TAIL_SKIP__", tail_skip.as_str()),
            ],
        ),
    )
    .expect("failed to write Java runner");
}

fn collect_java_sources(root: &Path) -> Vec<PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    let mut sources = Vec::new();

    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("failed to read directory") {
            let entry = entry.expect("failed to read directory entry");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("java") {
                sources.push(path);
            }
        }
    }

    sources.sort();
    sources
}

#[test]
fn generated_python_stream_many_early_break_cleans_up() {
    if !command_exists("python3") {
        eprintln!("skipping python stream_many e2e test: python3 not available");
        return;
    }

    let fixture = create_fixture("python-stream-many-e2e");
    let package_root = fixture.root.join("pyclient");
    let schema_path = fixture.schema_path.to_string_lossy().replace('\\', "/");
    generate_python_client_fixture(&package_root, &schema_path);
    fs::write(fixture.root.join("pydantic.py"), PYDANTIC_STUB)
        .expect("failed to write local pydantic stub");

    let runner = fixture.root.join("run_python_stream_many.py");
    let break_after = BREAK_AFTER.to_string();
    let tail_skip = TAIL_SKIP.to_string();
    fs::write(
        &runner,
        render_fixture(
            PYTHON_RUNNER_TEMPLATE,
            &[
                ("__BREAK_AFTER__", break_after.as_str()),
                ("__TAIL_SKIP__", tail_skip.as_str()),
            ],
        ),
    )
    .expect("failed to write Python runner");

    let stdout = run_checked(
        Command::new("python3")
            .arg(&runner)
            .env("PYTHONPATH", &fixture.root)
            .env("PATH", &fixture.path_env),
        "python stream_many e2e",
    );
    let values = parse_key_values(&stdout);

    assert_common_output(&values);
    assert_eq!(values.get("partial_data").map(String::as_str), Some("0"));
    assert_eq!(values.get("stream_queues").map(String::as_str), Some("0"));
}

#[test]
fn generated_js_stream_many_early_break_cleans_up() {
    if !command_exists("node") {
        eprintln!("skipping js streamMany e2e test: node not available");
        return;
    }

    let fixture = create_fixture("js-stream-many-e2e");
    let package_root = fixture.root.join("jsclient");
    let schema_path = fixture.schema_path.to_string_lossy().replace('\\', "/");
    generate_js_client_fixture(&package_root, &schema_path);

    fs::write(fixture.root.join("package.json"), JS_PACKAGE_JSON)
        .expect("failed to write package.json for JS fixture");

    let runner = fixture.root.join("run_js_stream_many.mjs");
    let break_after = BREAK_AFTER.to_string();
    let tail_skip = TAIL_SKIP.to_string();
    fs::write(
        &runner,
        render_fixture(
            JS_RUNNER_TEMPLATE,
            &[
                ("__BREAK_AFTER__", break_after.as_str()),
                ("__TAIL_SKIP__", tail_skip.as_str()),
            ],
        ),
    )
    .expect("failed to write JS runner");

    let stdout = run_checked(
        Command::new("node")
            .arg(&runner)
            .env("PATH", &fixture.path_env),
        "js streamMany e2e",
    );
    let values = parse_key_values(&stdout);

    assert_common_output(&values);
    assert_eq!(values.get("partialData").map(String::as_str), Some("0"));
    assert_eq!(values.get("streams").map(String::as_str), Some("0"));
}

/// Java uses generated sources plus Jackson jars on the compile/runtime classpath.
/// The test is skipped when that classpath is not available locally so the
/// regular Rust suite stays offline-friendly.
#[test]
fn generated_java_stream_many_early_break_cleans_up() {
    if !command_exists("javac") || !command_exists("java") {
        eprintln!("skipping java streamMany e2e test: javac/java not available");
        return;
    }

    let Some(classpath) = java_test_classpath() else {
        eprintln!(
            "skipping java streamMany e2e test: set NAUTILUS_JAVA_TEST_CLASSPATH or cache Jackson jars under target/test-jars/jackson-{JAVA_JACKSON_VERSION}"
        );
        return;
    };

    let fixture = create_fixture("java-stream-many-e2e");
    let output_dir = fixture.root.join("javaclient");
    let schema_path = fixture.schema_path.to_string_lossy().replace('\\', "/");
    generate_java_client_fixture(&output_dir, &schema_path);
    write_java_runner(&output_dir);

    let classes_dir = fixture.root.join("javaclient-classes");
    fs::create_dir_all(&classes_dir).expect("failed to create javac output directory");

    let sources = collect_java_sources(&output_dir.join("src/main/java"));
    assert!(!sources.is_empty(), "expected generated Java sources");

    let mut javac = Command::new("javac");
    javac
        .arg("--release")
        .arg("21")
        .arg("-cp")
        .arg(&classpath)
        .arg("-d")
        .arg(&classes_dir);
    for source in &sources {
        javac.arg(source);
    }
    run_checked(&mut javac, "javac streamMany e2e");

    let runtime_classpath =
        env::join_paths(std::iter::once(classes_dir.clone()).chain(env::split_paths(&classpath)))
            .expect("failed to build Java runtime classpath");

    let stdout = run_checked(
        Command::new("java")
            .arg("-cp")
            .arg(runtime_classpath)
            .arg("com.acme.db.e2e.StreamManyE2e")
            .env("NAUTILUS_BIN", &fixture.bin_path),
        "java streamMany e2e",
    );
    let values = parse_key_values(&stdout);

    assert_common_output(&values);
    assert_eq!(values.get("partialData").map(String::as_str), Some("0"));
    assert_eq!(values.get("streams").map(String::as_str), Some("0"));
}

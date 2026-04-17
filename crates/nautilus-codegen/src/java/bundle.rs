use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::java::generator::JACKSON_VERSION;

const JAVA_RELEASE: &str = "21";

/// Per-connection timeout for Maven artifact downloads. Protects CI/build
/// runs from hanging indefinitely on a slow mirror without being so short
/// that legitimate cold-cache downloads fail.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);

struct MavenJarDependency {
    group_path: &'static str,
    artifact_id: &'static str,
    version: &'static str,
}

struct JavaTools {
    javac: PathBuf,
    jar: PathBuf,
}

const JAVA_RUNTIME_DEPS: &[MavenJarDependency] = &[
    MavenJarDependency {
        group_path: "com/fasterxml/jackson/core",
        artifact_id: "jackson-databind",
        version: JACKSON_VERSION,
    },
    MavenJarDependency {
        group_path: "com/fasterxml/jackson/core",
        artifact_id: "jackson-annotations",
        version: JACKSON_VERSION,
    },
    MavenJarDependency {
        group_path: "com/fasterxml/jackson/core",
        artifact_id: "jackson-core",
        version: JACKSON_VERSION,
    },
    MavenJarDependency {
        group_path: "com/fasterxml/jackson/datatype",
        artifact_id: "jackson-datatype-jsr310",
        version: JACKSON_VERSION,
    },
];

pub fn build_java_bundle(output_path: &str, artifact_id: &str) -> Result<PathBuf> {
    let output_root = Path::new(output_path);
    let source_root = output_root.join("src").join("main").join("java");
    if !source_root.is_dir() {
        bail!(
            "Java bundle mode expected generated sources under {}, but that directory does not exist",
            source_root.display()
        );
    }

    let tools = resolve_java_tools()?;
    let dist_dir = output_root.join("dist");
    let lib_dir = dist_dir.join("lib");
    let build_dir = output_root.join(".nautilus-build");
    let classes_dir = build_dir.join("classes");

    clear_dir(&dist_dir)?;
    clear_dir(&build_dir)?;
    fs::create_dir_all(&lib_dir)
        .with_context(|| format!("Failed to create {}", lib_dir.display()))?;
    fs::create_dir_all(&classes_dir)
        .with_context(|| format!("Failed to create {}", classes_dir.display()))?;

    let deps = materialize_runtime_dependencies(&lib_dir)?;
    let sources = collect_java_sources(&source_root)?;
    if sources.is_empty() {
        bail!(
            "No generated Java sources were found under {}",
            source_root.display()
        );
    }

    compile_java_sources(&tools.javac, &classes_dir, &deps, &sources)?;

    let jar_path = dist_dir.join(format!("{artifact_id}.jar"));
    create_jar(&tools.jar, &jar_path, &classes_dir)?;
    clear_dir(&build_dir)?;

    Ok(jar_path)
}

fn resolve_java_tools() -> Result<JavaTools> {
    let java_home = env::var_os("JAVA_HOME").map(PathBuf::from);
    let path_dirs = env::var_os("PATH")
        .map(|path| env::split_paths(&path).collect::<Vec<_>>())
        .unwrap_or_default();

    let javac = resolve_java_tool("javac", java_home.as_deref(), &path_dirs)?;
    let jar = resolve_companion_java_tool("jar", &javac, java_home.as_deref(), &path_dirs)?;

    Ok(JavaTools { javac, jar })
}

fn resolve_java_tool(
    tool_name: &str,
    java_home: Option<&Path>,
    path_dirs: &[PathBuf],
) -> Result<PathBuf> {
    if let Some(path) = java_home.and_then(|home| find_tool_in_dir(tool_name, &home.join("bin"))) {
        return Ok(path);
    }

    if let Some(path) = find_tool_in_directories(tool_name, path_dirs) {
        return Ok(path);
    }

    bail!(
        "Could not find `{}`. Java bundle mode requires a JDK with `javac` and `jar` available via JAVA_HOME or PATH.",
        tool_name
    )
}

fn resolve_companion_java_tool(
    tool_name: &str,
    primary_tool: &Path,
    java_home: Option<&Path>,
    path_dirs: &[PathBuf],
) -> Result<PathBuf> {
    if let Some(path) = java_home.and_then(|home| find_tool_in_dir(tool_name, &home.join("bin"))) {
        return Ok(path);
    }

    if let Some(bin_dir) = primary_tool.parent() {
        if let Some(path) = find_tool_in_dir(tool_name, bin_dir) {
            return Ok(path);
        }
    }

    if let Some(inferred_java_home) = infer_java_home_from_tool(primary_tool) {
        if let Some(path) = find_tool_in_dir(tool_name, &inferred_java_home.join("bin")) {
            return Ok(path);
        }
    }

    if let Some(path) = find_tool_in_directories(tool_name, path_dirs) {
        return Ok(path);
    }

    bail!(
        "Could not find `{}`. Java bundle mode requires a JDK with `javac` and `jar` available via JAVA_HOME or PATH.",
        tool_name
    )
}

fn find_tool_in_directories(tool_name: &str, directories: &[PathBuf]) -> Option<PathBuf> {
    directories
        .iter()
        .find_map(|directory| find_tool_in_dir(tool_name, directory))
}

fn find_tool_in_dir(tool_name: &str, directory: &Path) -> Option<PathBuf> {
    tool_candidates(tool_name)
        .into_iter()
        .map(|candidate| directory.join(candidate))
        .find(|path| path.is_file())
}

fn tool_candidates(tool_name: &str) -> Vec<OsString> {
    if cfg!(windows) {
        vec![
            OsString::from(format!("{tool_name}.exe")),
            OsString::from(format!("{tool_name}.cmd")),
            OsString::from(format!("{tool_name}.bat")),
            OsString::from(tool_name),
        ]
    } else {
        vec![OsString::from(tool_name)]
    }
}

fn infer_java_home_from_tool(tool_path: &Path) -> Option<PathBuf> {
    if let Some(java_home) = infer_java_home_from_path(tool_path) {
        return Some(java_home);
    }

    let canonical = tool_path.canonicalize().ok()?;
    if let Some(java_home) = infer_java_home_from_path(&canonical) {
        return Some(java_home);
    }

    infer_java_home_from_tool_output(tool_path)
}

fn infer_java_home_from_path(tool_path: &Path) -> Option<PathBuf> {
    let bin_dir = tool_path.parent()?;
    if bin_dir.file_name().is_some_and(|name| name == "bin") {
        return bin_dir.parent().map(Path::to_path_buf);
    }
    None
}

fn infer_java_home_from_tool_output(tool_path: &Path) -> Option<PathBuf> {
    let output = Command::new(tool_path)
        .arg("-J-XshowSettings:properties")
        .arg("-version")
        .output()
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    parse_java_home_from_settings_output(&stdout)
        .or_else(|| parse_java_home_from_settings_output(&stderr))
}

fn parse_java_home_from_settings_output(output: &str) -> Option<PathBuf> {
    output.lines().find_map(|line| {
        let trimmed = line.trim();
        let (_, value) = trimmed.split_once("java.home =")?;
        let path = value.trim();
        if path.is_empty() {
            None
        } else {
            Some(PathBuf::from(path))
        }
    })
}

fn materialize_runtime_dependencies(lib_dir: &Path) -> Result<Vec<PathBuf>> {
    let client = Client::builder()
        .timeout(DOWNLOAD_TIMEOUT)
        .build()
        .context("Failed to create HTTP client for Java bundle dependencies")?;

    let mut deps = Vec::with_capacity(JAVA_RUNTIME_DEPS.len());
    for dep in JAVA_RUNTIME_DEPS {
        deps.push(materialize_dependency(dep, lib_dir, &client)?);
    }
    Ok(deps)
}

fn materialize_dependency(
    dep: &MavenJarDependency,
    lib_dir: &Path,
    client: &Client,
) -> Result<PathBuf> {
    let gav = dep_gav(dep);
    let relative = repo_relative_path(dep);
    let file_name = format!("{}-{}.jar", dep.artifact_id, dep.version);
    let destination = lib_dir.join(&file_name);

    if let Some(local_repo) = maven_local_repo() {
        let local_path = local_repo.join(&relative);
        if local_path.is_file() {
            fs::copy(&local_path, &destination).with_context(|| {
                format!(
                    "Failed to copy {gav} from {} into {}",
                    local_path.display(),
                    destination.display()
                )
            })?;
            return Ok(destination);
        }
    }

    let cache_path = download_cache_dir().map(|dir| dir.join(&relative));
    if let Some(cache_path) = cache_path.as_ref() {
        if cache_path.is_file() {
            fs::copy(cache_path, &destination).with_context(|| {
                format!(
                    "Failed to copy cached {gav} from {} into {}",
                    cache_path.display(),
                    destination.display()
                )
            })?;
            return Ok(destination);
        }
    }

    let url = format!(
        "https://repo.maven.apache.org/maven2/{}",
        relative.replace('\\', "/")
    );
    let response = client
        .get(&url)
        .send()
        .with_context(|| format!("Failed to download {gav} from {url}"))?;

    let status = response.status();
    if !status.is_success() {
        bail!("Could not download {gav} from {url} (HTTP {status})",);
    }

    let bytes = response
        .bytes()
        .with_context(|| format!("Failed to read {gav} body from {url}"))?;
    fs::write(&destination, &bytes)
        .with_context(|| format!("Failed to write {gav} to {}", destination.display()))?;

    if let Some(cache_path) = cache_path {
        if let Some(parent) = cache_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        // Best-effort cache write: if it fails (e.g. read-only cache dir)
        // the build still succeeded, so don't surface the error.
        let _ = fs::write(&cache_path, &bytes);
    }

    Ok(destination)
}

/// Formats a dependency as `group:artifact:version` for diagnostic messages.
fn dep_gav(dep: &MavenJarDependency) -> String {
    format!(
        "{}:{}:{}",
        dep.group_path.replace('/', "."),
        dep.artifact_id,
        dep.version
    )
}

fn repo_relative_path(dep: &MavenJarDependency) -> String {
    format!(
        "{}/{}/{}/{}-{}.jar",
        dep.group_path, dep.artifact_id, dep.version, dep.artifact_id, dep.version
    )
}

fn maven_local_repo() -> Option<PathBuf> {
    if let Some(repo) = env::var_os("M2_REPO") {
        return Some(PathBuf::from(repo));
    }

    home_dir().map(|home| home.join(".m2").join("repository"))
}

/// Per-user download cache populated by `materialize_dependency` when a JAR
/// is fetched over the network. Sits at `~/.cache/nautilus/maven` on
/// Unix-like hosts and under `%LOCALAPPDATA%\nautilus\maven` on Windows so
/// subsequent offline builds (and CI caches keyed on that path) can skip
/// the download.
fn download_cache_dir() -> Option<PathBuf> {
    if let Some(override_path) = env::var_os("NAUTILUS_JAVA_CACHE") {
        return Some(PathBuf::from(override_path));
    }
    if cfg!(windows) {
        env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .map(|root| root.join("nautilus").join("maven"))
    } else {
        if let Some(xdg) = env::var_os("XDG_CACHE_HOME") {
            return Some(PathBuf::from(xdg).join("nautilus").join("maven"));
        }
        home_dir().map(|home| home.join(".cache").join("nautilus").join("maven"))
    }
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
        .or_else(
            || match (env::var_os("HOMEDRIVE"), env::var_os("HOMEPATH")) {
                (Some(drive), Some(path)) => {
                    let mut home = PathBuf::from(drive);
                    home.push(path);
                    Some(home)
                }
                _ => None,
            },
        )
}

fn collect_java_sources(source_root: &Path) -> Result<Vec<PathBuf>> {
    let mut sources = Vec::new();
    collect_java_sources_recursive(source_root, &mut sources)?;
    sources.sort();
    Ok(sources)
}

fn collect_java_sources_recursive(dir: &Path, sources: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("Failed to read directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("Failed to read entry in {}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_java_sources_recursive(&path, sources)?;
        } else if path.extension().is_some_and(|ext| ext == "java") {
            sources.push(path);
        }
    }
    Ok(())
}

fn compile_java_sources(
    javac: &Path,
    classes_dir: &Path,
    deps: &[PathBuf],
    sources: &[PathBuf],
) -> Result<()> {
    let classpath = env::join_paths(deps)
        .context("Failed to construct the classpath for generated Java sources")?;

    let output = Command::new(javac)
        .arg("--release")
        .arg(JAVA_RELEASE)
        .arg("-classpath")
        .arg(&classpath)
        .arg("-d")
        .arg(classes_dir)
        .args(sources)
        .output()
        .with_context(|| format!("Failed to run `{}`", javac.display()))?;

    if !output.status.success() {
        bail!(
            "Failed to compile generated Java sources with `{}`.\n{}",
            javac.display(),
            format_process_output(&output.stdout, &output.stderr)
        );
    }

    Ok(())
}

fn create_jar(jar_tool: &Path, jar_path: &Path, classes_dir: &Path) -> Result<()> {
    let output = Command::new(jar_tool)
        .arg("--create")
        .arg("--file")
        .arg(jar_path)
        .arg("-C")
        .arg(classes_dir)
        .arg(".")
        .output()
        .with_context(|| format!("Failed to run `{}`", jar_tool.display()))?;

    if !output.status.success() {
        bail!(
            "Failed to create Java bundle `{}` with `{}`.\n{}",
            jar_path.display(),
            jar_tool.display(),
            format_process_output(&output.stdout, &output.stderr)
        );
    }

    Ok(())
}

fn format_process_output(stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = String::from_utf8_lossy(stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();

    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => "The process did not produce any output.".to_string(),
        (false, true) => stdout,
        (true, false) => stderr,
        (false, false) => format!("{stdout}\n{stderr}"),
    }
}

fn clear_dir(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path).with_context(|| format!("Failed to remove {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        find_tool_in_dir, infer_java_home_from_path, parse_java_home_from_settings_output,
    };

    #[test]
    fn parses_java_home_from_launcher_settings_output() {
        let output = r#"
Property settings:
    file.encoding = UTF-8
    java.home = C:\Program Files\Java\jdk-24
    java.version = 24
"#;

        assert_eq!(
            parse_java_home_from_settings_output(output)
                .expect("java.home should be parsed")
                .to_string_lossy(),
            r"C:\Program Files\Java\jdk-24"
        );
    }

    #[test]
    fn infers_java_home_when_tool_already_lives_in_bin_directory() {
        let tool_path = std::path::Path::new(r"C:\Program Files\Java\jdk-24\bin\javac.exe");

        assert_eq!(
            infer_java_home_from_path(tool_path)
                .expect("java home should be inferred")
                .to_string_lossy(),
            r"C:\Program Files\Java\jdk-24"
        );
    }

    #[test]
    fn finds_tool_in_directory_with_windows_candidate_names() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let tool_path = temp.path().join("jar.exe");
        std::fs::write(&tool_path, b"stub").expect("write jar.exe");

        assert_eq!(
            find_tool_in_dir("jar", temp.path()).expect("jar should be found"),
            tool_path
        );
    }
}

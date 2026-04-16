use anyhow::{bail, Context, Result};
use clap::Args;
use reqwest::blocking::Client;
use serde::Deserialize;
use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock,
};
use std::time::Duration;
use zip::ZipArchive;

use crate::tui;

pub static STUDIO_GITHUB_REPO: &str = "TonnoBelloSnello/nautilus-orm-studio";
pub static STUDIO_RELEASE_ASSET_PREFIX: &str = "nautilus-orm-studio-";

const STUDIO_DIR_NAME: &str = "studio";
const STUDIO_INSTALL_DIR_NAME: &str = "app";
const STUDIO_ARCHIVE_FILE_NAME: &str = "release.zip";

#[derive(Args, Clone, Debug, Eq, PartialEq)]
pub struct StudioArgs {
    /// Download the latest GitHub release again before starting the app
    #[arg(long)]
    pub update: bool,

    /// Remove the locally cached Studio release files
    #[arg(long)]
    pub uninstall: bool,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    assets: Vec<GitHubReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct StudioPackageManifest {
    version: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct GitHubReleaseAsset {
    name: String,
    browser_download_url: String,
}

#[derive(Debug)]
struct StudioReleaseUnavailable {
    detail: String,
}

impl StudioReleaseUnavailable {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for StudioReleaseUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for StudioReleaseUnavailable {}

pub async fn run(args: StudioArgs) -> Result<()> {
    tokio::task::spawn_blocking(move || run_sync(args))
        .await
        .unwrap_or_else(|e| Err(anyhow::anyhow!("Task error: {}", e)))
}

fn run_sync(args: StudioArgs) -> Result<()> {
    validate_args(&args)?;
    tui::print_header("studio");

    let project_dir =
        std::env::current_dir().context("Failed to resolve the current project directory")?;
    let install_root = studio_install_root()?;
    let app_dir = install_root.join(STUDIO_INSTALL_DIR_NAME);
    let archive_path = install_root.join(STUDIO_ARCHIVE_FILE_NAME);

    if args.uninstall {
        uninstall_installation(&install_root)?;
        return Ok(());
    }

    ensure_command_available(
        "node",
        &["--version"],
        "Node.js is required to run Nautilus Studio",
    )?;
    ensure_command_available(
        npm_executable(),
        &["--version"],
        "npm is required to run Nautilus Studio",
    )?;

    let needs_download = args.update || resolve_app_root(&app_dir).is_none();
    if needs_download {
        match install_or_update_from_release(&install_root, &app_dir, &archive_path) {
            Ok(()) => {}
            Err(err) => {
                if let Some(unavailable) = err.downcast_ref::<StudioReleaseUnavailable>() {
                    tui::print_warning(&unavailable.detail);
                    return Ok(());
                }
                return Err(err);
            }
        }
    }

    let app_root = resolve_app_root(&app_dir).ok_or_else(|| {
        anyhow::anyhow!(
            "No Studio app root found under {}. Run `nautilus studio --update` after publishing a valid release.",
            app_dir.display()
        )
    })?;

    if !needs_download {
        check_for_update_tip(&app_root);
    }

    if !runtime_dependencies_installed(&app_root) {
        install_runtime_dependencies(&app_root)?;
    }

    launch_app(&app_root, &project_dir)
}

fn validate_args(args: &StudioArgs) -> Result<()> {
    if args.update && args.uninstall {
        bail!("`nautilus studio` does not support using --update and --uninstall together");
    }
    Ok(())
}

fn install_or_update_from_release(
    install_root: &Path,
    app_dir: &Path,
    archive_path: &Path,
) -> Result<()> {
    tui::print_section("Studio Download");

    std::fs::create_dir_all(install_root)
        .with_context(|| format!("Failed to create {}", install_root.display()))?;

    if app_dir.exists() {
        std::fs::remove_dir_all(app_dir)
            .with_context(|| format!("Failed to clear {}", app_dir.display()))?;
    }

    if archive_path.exists() {
        std::fs::remove_file(archive_path)
            .with_context(|| format!("Failed to remove {}", archive_path.display()))?;
    }

    let asset = latest_release_asset()?;
    download_release_archive(&asset.browser_download_url, archive_path)?;
    extract_release_archive(archive_path, app_dir)?;

    let app_root = resolve_app_root(app_dir).ok_or_else(|| {
        anyhow::anyhow!(
            "Studio release extracted successfully, but no package.json was found under {}",
            app_dir.display()
        )
    })?;

    install_runtime_dependencies(&app_root)?;

    tui::print_summary_ok("Studio ready", &format!("Downloaded {}", asset.name));
    Ok(())
}

fn fetch_latest_release() -> Result<GitHubRelease> {
    let client = Client::builder()
        .build()
        .context("Failed to create HTTP client for Studio release lookup")?;

    let response = client
        .get(format!(
            "https://api.github.com/repos/{}/releases/latest",
            STUDIO_GITHUB_REPO
        ))
        .header(reqwest::header::USER_AGENT, "nautilus-cli")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .context("Failed to request the latest Nautilus Studio release")?;

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(StudioReleaseUnavailable::new(format!(
            "No published Nautilus Studio release was found for https://github.com/{}/releases/latest. Publish a release first or update STUDIO_GITHUB_REPO.",
            STUDIO_GITHUB_REPO
        ))
        .into());
    }

    response
        .error_for_status()
        .context("Could not resolve the latest Nautilus Studio release")?
        .json()
        .context("Failed to decode the latest Nautilus Studio release metadata")
}

fn latest_release_asset() -> Result<GitHubReleaseAsset> {
    let release = fetch_latest_release()?;
    let asset = select_release_asset(&release)?;
    Ok(asset)
}

fn select_release_asset(release: &GitHubRelease) -> Result<GitHubReleaseAsset> {
    select_release_asset_for_platform(release, release_asset_platform())
}

fn select_release_asset_for_platform(
    release: &GitHubRelease,
    platform: &str,
) -> Result<GitHubReleaseAsset> {
    let expected_name = expected_release_asset_name_for_platform(&release.tag_name, platform);

    release
        .assets
        .iter()
        .find(|asset| asset.name == expected_name)
        .cloned()
        .ok_or_else(|| {
            StudioReleaseUnavailable::new(format!(
                "The latest Nautilus Studio release for {} does not include the expected asset `{}` for the current platform ({})",
                STUDIO_GITHUB_REPO, expected_name, platform,
            ))
            .into()
        })
}

fn expected_release_asset_name_for_platform(tag_name: &str, platform: &str) -> String {
    format!(
        "{}{}-{}.zip",
        STUDIO_RELEASE_ASSET_PREFIX, tag_name, platform
    )
}

fn release_asset_platform() -> &'static str {
    match std::env::consts::OS {
        "windows" => "windows",
        "macos" => "macos",
        "linux" => "linux",
        other => other,
    }
}

fn read_installed_version(app_root: &Path) -> Option<String> {
    read_app_package_version(app_root)
}

fn read_app_package_version(app_root: &Path) -> Option<String> {
    let manifest = std::fs::read_to_string(app_root.join("package.json")).ok()?;
    let package: StudioPackageManifest = serde_json::from_str(&manifest).ok()?;
    let version = package.version.trim().to_string();
    if version.is_empty() {
        None
    } else {
        Some(version)
    }
}

fn normalize_version_label(version: &str) -> &str {
    version.trim().trim_start_matches(['v', 'V'])
}

fn same_version(left: &str, right: &str) -> bool {
    normalize_version_label(left) == normalize_version_label(right)
}

fn fetch_latest_release_tag_silently() -> Option<String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;

    let response = client
        .get(format!(
            "https://api.github.com/repos/{}/releases/latest",
            STUDIO_GITHUB_REPO
        ))
        .header(reqwest::header::USER_AGENT, "nautilus-cli")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .ok()?;

    if !response.status().is_success() {
        return None;
    }

    response.json::<GitHubRelease>().ok().map(|r| r.tag_name)
}

fn check_for_update_tip(app_root: &Path) {
    let installed = match read_installed_version(app_root) {
        Some(v) => v,
        None => {
            tui::print_tip(
                "Could not determine the installed Nautilus Studio version from package.json. Run `nautilus studio --update` to refresh it.",
            );
            return;
        }
    };

    let latest = match fetch_latest_release_tag_silently() {
        Some(v) => v,
        None => return,
    };

    if !same_version(&installed, &latest) {
        tui::print_tip(&format!(
            "A newer version of Nautilus Studio is available ({latest}). Run `nautilus studio --update` to install it."
        ));
    }
}

fn uninstall_installation(install_root: &Path) -> Result<()> {
    tui::print_section("Studio Uninstall");

    if !install_root.exists() {
        tui::print_summary_ok(
            "Studio already absent",
            &format!("{}", install_root.display()),
        );
        return Ok(());
    }

    std::fs::remove_dir_all(install_root)
        .with_context(|| format!("Failed to remove {}", install_root.display()))?;
    tui::print_summary_ok("Studio uninstalled", &format!("{}", install_root.display()));
    Ok(())
}

fn download_release_archive(url: &str, archive_path: &Path) -> Result<()> {
    let client = Client::builder()
        .build()
        .context("Failed to create HTTP client for Studio download")?;

    let response = client
        .get(url)
        .header(reqwest::header::USER_AGENT, "nautilus-cli")
        .send()
        .with_context(|| format!("Failed to request {}", url))?;

    let status = response.status();
    if !status.is_success() {
        bail!(
            "Could not download Nautilus Studio from {} (HTTP {}). The release asset may not exist yet.",
            url,
            status
        );
    }

    let bytes = response
        .bytes()
        .with_context(|| format!("Failed to read response body from {}", url))?;

    std::fs::write(archive_path, &bytes)
        .with_context(|| format!("Failed to write {}", archive_path.display()))?;

    tui::print_ok(&format!("Downloaded {}", archive_path.display()));
    Ok(())
}

fn extract_release_archive(archive_path: &Path, app_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(app_dir)
        .with_context(|| format!("Failed to create {}", app_dir.display()))?;

    let file = File::open(archive_path)
        .with_context(|| format!("Failed to open {}", archive_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("Failed to read {}", archive_path.display()))?;

    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .with_context(|| format!("Failed to read ZIP entry {index}"))?;

        let relative = entry
            .enclosed_name()
            .map(|path| path.to_path_buf())
            .ok_or_else(|| anyhow::anyhow!("ZIP archive contains an invalid path"))?;
        let output_path = app_dir.join(relative);

        if entry.is_dir() {
            std::fs::create_dir_all(&output_path)
                .with_context(|| format!("Failed to create {}", output_path.display()))?;
            continue;
        }

        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create {}", parent.display()))?;
        }

        let mut output = File::create(&output_path)
            .with_context(|| format!("Failed to create {}", output_path.display()))?;
        io::copy(&mut entry, &mut output)
            .with_context(|| format!("Failed to extract {}", output_path.display()))?;
    }

    tui::print_ok(&format!("Extracted to {}", app_dir.display()));
    Ok(())
}

fn resolve_app_root(root: &Path) -> Option<PathBuf> {
    if root.join("package.json").is_file() {
        return Some(root.to_path_buf());
    }

    let release_package = root.join("release-package");
    if release_package.join("package.json").is_file() {
        return Some(release_package);
    }

    let mut discovered = Vec::new();
    collect_app_roots(root, &mut discovered);
    discovered.sort_by_key(|path| path.components().count());
    discovered.into_iter().next()
}

fn collect_app_roots(root: &Path, discovered: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.join("package.json").is_file() {
                discovered.push(path);
            } else {
                collect_app_roots(&path, discovered);
            }
        }
    }
}

fn runtime_dependencies_installed(app_root: &Path) -> bool {
    app_root.join("node_modules").exists()
}

fn install_runtime_dependencies(app_root: &Path) -> Result<()> {
    tui::print_section("Studio Runtime");

    let mut command = Command::new(npm_executable());
    command.current_dir(app_root);

    if app_root.join("package-lock.json").is_file() {
        command.args(["ci", "--omit=dev"]);
    } else {
        command.args(["install", "--omit=dev"]);
    }

    run_logged_command(
        &mut command,
        "Installing Nautilus Studio runtime dependencies",
    )
}

fn launch_app(app_root: &Path, project_dir: &Path) -> Result<()> {
    tui::print_section("Studio Start");
    tui::print_ok(&format!(
        "Release repo: https://github.com/{}",
        STUDIO_GITHUB_REPO
    ));
    tui::print_ok(&format!("Project directory: {}", project_dir.display()));
    tui::print_ok(&format!("Studio directory: {}", app_root.display()));

    let next_cli = next_cli_path(app_root);
    if !next_cli.is_file() {
        bail!(
            "Could not find the bundled Next.js CLI at {}",
            next_cli.display()
        );
    }

    let mut command = Command::new("node");
    command
        .current_dir(project_dir)
        .arg(&next_cli)
        .arg("start")
        .arg(app_root);

    run_logged_command(&mut command, "Starting Nautilus Studio")
}

fn next_cli_path(app_root: &Path) -> PathBuf {
    app_root
        .join("node_modules")
        .join("next")
        .join("dist")
        .join("bin")
        .join("next")
}

fn npm_executable() -> &'static str {
    if cfg!(windows) {
        "npm.cmd"
    } else {
        "npm"
    }
}

fn ensure_command_available(program: &str, args: &[&str], message: &str) -> Result<()> {
    let available = Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false);

    if !available {
        bail!("{message}");
    }

    Ok(())
}

fn run_logged_command(command: &mut Command, description: &str) -> Result<()> {
    let rendered = format_command(command);
    let interrupted = studio_interrupt_flag()?;
    interrupted.store(false, Ordering::SeqCst);

    let mut child = command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("Failed to run `{rendered}`"))?;

    let status = wait_for_child(&mut child, &interrupted)
        .with_context(|| format!("Failed while running `{rendered}`"))?;

    if interrupted.load(Ordering::SeqCst) || is_interrupt_exit_status(&status) {
        let display_name = description
            .strip_prefix("Starting ")
            .unwrap_or(description)
            .to_string();
        tui::print_summary_ok(&format!("{display_name} stopped"), "Interrupted by Ctrl+C");
        return Ok(());
    }

    if !status.success() {
        bail!("{description} failed while running `{rendered}`");
    }

    Ok(())
}

fn studio_interrupt_flag() -> Result<Arc<AtomicBool>> {
    static FLAG: OnceLock<std::result::Result<Arc<AtomicBool>, String>> = OnceLock::new();

    match FLAG.get_or_init(|| {
        let flag = Arc::new(AtomicBool::new(false));
        let handler_flag = Arc::clone(&flag);

        ctrlc::set_handler(move || {
            handler_flag.store(true, Ordering::SeqCst);
        })
        .map(|_| flag)
        .map_err(|error| error.to_string())
    }) {
        Ok(flag) => Ok(Arc::clone(flag)),
        Err(error) => Err(anyhow::anyhow!(
            "Failed to install Ctrl+C handler for Nautilus Studio: {}",
            error
        )),
    }
}

fn wait_for_child(child: &mut Child, interrupted: &Arc<AtomicBool>) -> Result<ExitStatus> {
    loop {
        if let Some(status) = child.try_wait().context("Failed to query child status")? {
            return Ok(status);
        }

        if interrupted.load(Ordering::SeqCst) {
            terminate_child(child)?;
            return child.wait().context("Failed to wait for interrupted child");
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}

fn terminate_child(child: &mut Child) -> Result<()> {
    if child
        .try_wait()
        .context("Failed to query child status before termination")?
        .is_some()
    {
        return Ok(());
    }

    match child.kill() {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput => Ok(()),
        Err(error) => Err(error).context("Failed to terminate interrupted child process"),
    }
}

fn is_interrupt_exit_status(status: &ExitStatus) -> bool {
    if let Some(code) = status.code() {
        return is_interrupt_exit_code(code);
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        return status.signal() == Some(2);
    }

    #[cfg(not(unix))]
    {
        false
    }
}

fn is_interrupt_exit_code(code: i32) -> bool {
    #[cfg(windows)]
    {
        code == 0xC000_013A_u32 as i32
    }

    #[cfg(not(windows))]
    {
        code == 130
    }
}

fn format_command(command: &Command) -> String {
    let program = command.get_program().to_string_lossy();
    let args = command
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    if args.is_empty() {
        program.into_owned()
    } else {
        format!("{program} {}", args.join(" "))
    }
}

fn studio_install_root() -> Result<PathBuf> {
    Ok(nautilus_home()?.join(STUDIO_DIR_NAME))
}

fn nautilus_home() -> Result<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        let base = std::env::var("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|_| dirs_home().expect("home dir required").join(".nautilus"));
        Ok(base.join("nautilus"))
    }
    #[cfg(not(target_os = "windows"))]
    {
        Ok(dirs_home()?.join(".nautilus"))
    }
}

fn dirs_home() -> Result<PathBuf> {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(PathBuf::from)
        .map_err(|_| {
            anyhow::anyhow!("Could not determine home directory (HOME / USERPROFILE not set)")
        })
}

#[cfg(test)]
mod tests {
    use super::{
        collect_app_roots, expected_release_asset_name_for_platform, is_interrupt_exit_code,
        next_cli_path, read_installed_version, release_asset_platform, resolve_app_root,
        same_version, select_release_asset, select_release_asset_for_platform, GitHubRelease,
        GitHubReleaseAsset, StudioReleaseUnavailable, STUDIO_GITHUB_REPO,
    };
    use std::path::{Path, PathBuf};

    #[test]
    fn expected_release_asset_name_matches_workflow() {
        assert_eq!(
            expected_release_asset_name_for_platform("v0.1.0", "windows"),
            "nautilus-orm-studio-v0.1.0-windows.zip"
        );
    }

    #[test]
    fn release_asset_selection_matches_release_workflow_naming() {
        let release = GitHubRelease {
            tag_name: "v0.1.0".to_string(),
            assets: vec![
                GitHubReleaseAsset {
                    name: "checksums.txt".to_string(),
                    browser_download_url: "https://example.com/checksums.txt".to_string(),
                },
                GitHubReleaseAsset {
                    name: "nautilus-orm-studio-v0.1.0-windows.zip".to_string(),
                    browser_download_url:
                        "https://example.com/nautilus-orm-studio-v0.1.0-windows.zip".to_string(),
                },
            ],
        };

        let asset = select_release_asset_for_platform(&release, "windows").expect("studio asset");
        assert_eq!(asset.name, "nautilus-orm-studio-v0.1.0-windows.zip");
    }

    #[test]
    fn release_asset_selection_uses_current_platform() {
        let release = GitHubRelease {
            tag_name: "v0.1.0".to_string(),
            assets: vec![
                GitHubReleaseAsset {
                    name: "nautilus-orm-studio-v0.1.0-linux.zip".to_string(),
                    browser_download_url:
                        "https://example.com/nautilus-orm-studio-v0.1.0-linux.zip".to_string(),
                },
                GitHubReleaseAsset {
                    name: "nautilus-orm-studio-v0.1.0-macos.zip".to_string(),
                    browser_download_url:
                        "https://example.com/nautilus-orm-studio-v0.1.0-macos.zip".to_string(),
                },
                GitHubReleaseAsset {
                    name: "nautilus-orm-studio-v0.1.0-windows.zip".to_string(),
                    browser_download_url:
                        "https://example.com/nautilus-orm-studio-v0.1.0-windows.zip".to_string(),
                },
            ],
        };

        let asset = select_release_asset(&release).expect("studio asset");
        assert_eq!(
            asset.name,
            format!(
                "nautilus-orm-studio-v0.1.0-{}.zip",
                release_asset_platform()
            )
        );
    }

    #[test]
    fn release_asset_selection_reports_missing_zip() {
        let release = GitHubRelease {
            tag_name: "v0.1.0".to_string(),
            assets: vec![GitHubReleaseAsset {
                name: "checksums.txt".to_string(),
                browser_download_url: "https://example.com/checksums.txt".to_string(),
            }],
        };

        let err = select_release_asset(&release).expect_err("missing asset should fail");
        assert!(err.downcast_ref::<StudioReleaseUnavailable>().is_some());
        assert!(err.to_string().contains(STUDIO_GITHUB_REPO));
    }

    #[test]
    fn resolve_app_root_prefers_release_package_directory() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let app_root = temp_dir.path().join("release-package");
        std::fs::create_dir_all(&app_root).expect("create dirs");
        std::fs::write(app_root.join("package.json"), "{}").expect("package.json");

        let resolved = resolve_app_root(temp_dir.path()).expect("app root");
        assert_eq!(resolved, app_root);
    }

    #[test]
    fn resolve_app_root_falls_back_to_recursive_search() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let nested = temp_dir.path().join("artifact").join("bundle");
        std::fs::create_dir_all(&nested).expect("create dirs");
        std::fs::write(nested.join("package.json"), "{}").expect("package.json");

        let resolved = resolve_app_root(temp_dir.path()).expect("app root");
        assert_eq!(resolved, nested);
    }

    #[test]
    fn recursive_app_root_collection_finds_nested_package_json() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let nested = temp_dir.path().join("a").join("b");
        std::fs::create_dir_all(&nested).expect("create dirs");
        std::fs::write(nested.join("package.json"), "{}").expect("package.json");

        let mut discovered = Vec::new();
        collect_app_roots(temp_dir.path(), &mut discovered);

        assert_eq!(discovered, vec![nested]);
    }

    #[test]
    fn installed_version_is_read_from_package_manifest() {
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let app_root = temp_dir.path().join("app");

        std::fs::create_dir_all(&app_root).expect("create dirs");
        std::fs::write(
            app_root.join("package.json"),
            r#"{"name":"nautilus-studio","version":"0.1.0"}"#,
        )
        .expect("package.json");

        let version = read_installed_version(&app_root).expect("version");

        assert_eq!(version, "0.1.0");
    }

    #[test]
    fn version_comparison_ignores_optional_v_prefix() {
        assert!(same_version("0.1.0", "v0.1.0"));
        assert!(same_version("V0.1.0", "0.1.0"));
        assert!(!same_version("0.1.0", "v0.2.0"));
    }

    #[test]
    fn next_cli_path_points_to_bundled_next_binary() {
        let root = Path::new("C:/studio/release-package");
        let cli = next_cli_path(root);

        assert_eq!(
            cli,
            PathBuf::from("C:/studio/release-package/node_modules/next/dist/bin/next")
        );
    }

    #[test]
    fn interrupt_exit_codes_are_treated_as_clean_shutdowns() {
        #[cfg(windows)]
        assert!(is_interrupt_exit_code(0xC000_013A_u32 as i32));

        #[cfg(not(windows))]
        assert!(is_interrupt_exit_code(130));
    }
}

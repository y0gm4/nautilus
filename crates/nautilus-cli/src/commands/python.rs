use anyhow::{Context, Result};
use clap::Subcommand;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Subcommand)]
pub enum PythonCommand {
    /// Install a .pth file so `import nautilus` works without pip
    Install {
        /// Python executable to use (default: python / python3)
        #[arg(long, default_value = "")]
        python: String,
    },
    /// Remove the nautilus .pth file from site-packages
    Uninstall {
        /// Python executable to use (default: python / python3)
        #[arg(long, default_value = "")]
        python: String,
    },
}

pub async fn run(cmd: PythonCommand) -> Result<()> {
    match cmd {
        PythonCommand::Install { python } => install(&python),
        PythonCommand::Uninstall { python } => uninstall(&python),
    }
}

const SHIM_INIT: &str = "";

const SHIM_MAIN: &str = r#"import os
import subprocess
import shutil
import sys


def main() -> None:
    binary_name = "nautilus.exe" if sys.platform == "win32" else "nautilus"
    binary = shutil.which(binary_name)
    if binary is None:
        print(f"Error: '{binary_name}' not found in PATH.", file=sys.stderr)
        print("Install it with: cargo install nautilus-cli", file=sys.stderr)
        sys.exit(1)
    env = dict(os.environ)
    env["NAUTILUS_PYTHON_WRAPPER"] = "1"
    argv = [binary] + sys.argv[1:]
    if sys.platform == "win32":
        raise SystemExit(subprocess.run(argv, env=env).returncode)
    os.execve(binary, argv, env)


if __name__ == "__main__":
    main()
"#;

fn install(python_hint: &str) -> Result<()> {
    let python = resolve_python(python_hint)?;
    let site_packages = query_site_packages(&python)?;
    let shim_dir = write_shim_package()?;

    let pth_path = site_packages.join("nautilus.pth");
    std::fs::write(&pth_path, format!("{}\n", shim_dir.display()))
        .with_context(|| format!("Failed to write {}", pth_path.display()))?;

    println!("✓ Installed nautilus shim to {}", shim_dir.display());
    println!("✓ Created {}", pth_path.display());
    println!();
    println!("You can now run `python -m nautilus` or `import nautilus` in Python.");
    Ok(())
}

fn uninstall(python_hint: &str) -> Result<()> {
    let python = resolve_python(python_hint)?;
    let site_packages = query_site_packages(&python)?;
    let pth_path = site_packages.join("nautilus.pth");

    if pth_path.exists() {
        std::fs::remove_file(&pth_path)
            .with_context(|| format!("Failed to remove {}", pth_path.display()))?;
        println!("✓ Removed {}", pth_path.display());
    } else {
        println!("Nothing to remove: {} not found.", pth_path.display());
    }
    Ok(())
}

/// Resolve which Python executable to use.
fn resolve_python(hint: &str) -> Result<String> {
    if !hint.is_empty() {
        return Ok(hint.to_string());
    }
    for candidate in &["python", "python3"] {
        if which_exists(candidate) {
            return Ok(candidate.to_string());
        }
    }
    anyhow::bail!(
        "No Python interpreter found in PATH. \
         Pass --python /path/to/python to specify one explicitly."
    )
}

fn which_exists(name: &str) -> bool {
    Command::new(name)
        .args(["--version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Ask Python where its purelib site-packages directory is.
fn query_site_packages(python: &str) -> Result<PathBuf> {
    let out = Command::new(python)
        .args([
            "-c",
            "import sysconfig; print(sysconfig.get_path('purelib'))",
        ])
        .output()
        .with_context(|| format!("Failed to run `{python}`"))?;

    if !out.status.success() {
        anyhow::bail!(
            "`{python}` exited with status {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let path_str = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let path = PathBuf::from(&path_str);

    if !path.exists() {
        anyhow::bail!(
            "site-packages directory does not exist: {}.\n\
             Make sure the Python interpreter is fully installed.",
            path.display()
        );
    }

    Ok(path)
}

/// Write the shim package to `~/.nautilus/python/nautilus/` and return the
/// parent directory (`~/.nautilus/python/`) that the .pth file should point to.
fn write_shim_package() -> Result<PathBuf> {
    let base = nautilus_home()?.join("python");
    let pkg = base.join("nautilus");
    std::fs::create_dir_all(&pkg).with_context(|| format!("Failed to create {}", pkg.display()))?;

    write_if_changed(&pkg.join("__init__.py"), SHIM_INIT)?;
    write_if_changed(&pkg.join("__main__.py"), SHIM_MAIN)?;

    Ok(base)
}

fn write_if_changed(path: &Path, content: &str) -> Result<()> {
    let existing = std::fs::read_to_string(path).unwrap_or_default();
    if existing != content {
        std::fs::write(path, content)
            .with_context(|| format!("Failed to write {}", path.display()))?;
    }
    Ok(())
}

/// Platform-specific base directory for nautilus data.
fn nautilus_home() -> anyhow::Result<PathBuf> {
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

fn dirs_home() -> anyhow::Result<PathBuf> {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(PathBuf::from)
        .map_err(|_| {
            anyhow::anyhow!("Could not determine home directory (HOME / USERPROFILE not set)")
        })
}

#[cfg(test)]
mod tests {
    use super::SHIM_MAIN;

    #[test]
    fn shim_main_marks_wrapped_processes_and_avoids_windows_execv() {
        assert!(SHIM_MAIN.contains("NAUTILUS_PYTHON_WRAPPER"));
        assert!(SHIM_MAIN.contains("subprocess.run"));
        assert!(SHIM_MAIN.contains("os.execve"));
        assert!(!SHIM_MAIN.contains("os.execv("));
    }
}

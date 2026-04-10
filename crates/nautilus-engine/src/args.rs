use std::env;

/// CLI arguments for the Nautilus engine
#[derive(Debug)]
pub struct CliArgs {
    pub schema_path: String,
    /// Database URL from the `--database-url` flag.
    /// If `None`, the engine resolves runtime/admin URLs from `DATABASE_URL`
    /// and the schema datasource (`url` / `direct_url`).
    pub database_url: Option<String>,
    /// If true, run CREATE TABLE migrations before entering the request loop.
    pub migrate: bool,
}

impl CliArgs {
    pub fn parse() -> Result<Self, String> {
        let args: Vec<String> = env::args().collect();
        Self::parse_from(&args[1..])
    }

    /// Parse from an explicit slice of argument strings (without the program name).
    pub fn parse_from(args: &[String]) -> Result<Self, String> {
        let mut schema_path = None;
        let mut database_url = None;
        let mut migrate = false;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--schema" => {
                    if i + 1 >= args.len() {
                        return Err("--schema requires a path argument".to_string());
                    }
                    schema_path = Some(args[i + 1].clone());
                    i += 2;
                }
                "--database-url" => {
                    if i + 1 >= args.len() {
                        return Err("--database-url requires a URL argument".to_string());
                    }
                    database_url = Some(args[i + 1].clone());
                    i += 2;
                }
                "--migrate" => {
                    migrate = true;
                    i += 1;
                }
                arg => {
                    return Err(format!("Unknown argument: {}", arg));
                }
            }
        }

        let schema_path = schema_path.ok_or("--schema is required")?;

        Ok(CliArgs {
            schema_path,
            database_url,
            migrate,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(strs: &[&str]) -> Vec<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn minimal_args() {
        let cli = CliArgs::parse_from(&args(&["--schema", "path.nautilus"])).unwrap();
        assert_eq!(cli.schema_path, "path.nautilus");
        assert_eq!(cli.database_url, None);
        assert!(!cli.migrate);
    }

    #[test]
    fn all_flags() {
        let cli = CliArgs::parse_from(&args(&[
            "--schema",
            "s.nautilus",
            "--database-url",
            "postgres://localhost/db",
            "--migrate",
        ]))
        .unwrap();
        assert_eq!(cli.schema_path, "s.nautilus");
        assert_eq!(
            cli.database_url,
            Some("postgres://localhost/db".to_string())
        );
        assert!(cli.migrate);
    }

    #[test]
    fn missing_schema() {
        let err = CliArgs::parse_from(&args(&["--migrate"])).unwrap_err();
        assert!(err.contains("--schema is required"), "got: {err}");
    }

    #[test]
    fn schema_missing_value() {
        let err = CliArgs::parse_from(&args(&["--schema"])).unwrap_err();
        assert!(err.contains("requires a path"), "got: {err}");
    }

    #[test]
    fn unknown_arg() {
        let err = CliArgs::parse_from(&args(&["--schema", "s.n", "--verbose"])).unwrap_err();
        assert!(err.contains("Unknown argument"), "got: {err}");
    }
}

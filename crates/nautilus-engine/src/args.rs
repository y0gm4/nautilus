use std::env;

use crate::EnginePoolOptions;

/// CLI arguments for the Nautilus engine
#[derive(Debug)]
pub struct CliArgs {
    pub schema_path: Option<String>,
    /// Database URL from the `--database-url` flag.
    /// If `None`, the engine resolves runtime/admin URLs from `DATABASE_URL`
    /// and the schema datasource (`url` / `direct_url`).
    pub database_url: Option<String>,
    /// If true, run CREATE TABLE migrations before entering the request loop.
    pub migrate: bool,
    /// Engine-level connection-pool overrides for the runtime database client.
    pub pool_options: EnginePoolOptions,
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
        let mut pool_options = EnginePoolOptions::new();

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
                "--max-connections" => {
                    if i + 1 >= args.len() {
                        return Err("--max-connections requires a numeric argument".to_string());
                    }
                    let value = args[i + 1].parse::<u32>().map_err(|_| {
                        "--max-connections requires a valid u32 argument".to_string()
                    })?;
                    pool_options = pool_options.max_connections(value);
                    i += 2;
                }
                "--min-connections" => {
                    if i + 1 >= args.len() {
                        return Err("--min-connections requires a numeric argument".to_string());
                    }
                    let value = args[i + 1].parse::<u32>().map_err(|_| {
                        "--min-connections requires a valid u32 argument".to_string()
                    })?;
                    pool_options = pool_options.min_connections(value);
                    i += 2;
                }
                "--acquire-timeout-ms" => {
                    if i + 1 >= args.len() {
                        return Err("--acquire-timeout-ms requires a numeric argument".to_string());
                    }
                    let value = args[i + 1].parse::<u64>().map_err(|_| {
                        "--acquire-timeout-ms requires a valid u64 argument".to_string()
                    })?;
                    pool_options = pool_options.acquire_timeout_ms(value);
                    i += 2;
                }
                "--idle-timeout-ms" => {
                    if i + 1 >= args.len() {
                        return Err("--idle-timeout-ms requires a numeric argument".to_string());
                    }
                    if matches!(pool_options.get_idle_timeout(), Some(None)) {
                        return Err(
                            "--idle-timeout-ms conflicts with --disable-idle-timeout".to_string()
                        );
                    }
                    let value = args[i + 1].parse::<u64>().map_err(|_| {
                        "--idle-timeout-ms requires a valid u64 argument".to_string()
                    })?;
                    pool_options = pool_options.idle_timeout_ms(value);
                    i += 2;
                }
                "--disable-idle-timeout" => {
                    if matches!(pool_options.get_idle_timeout(), Some(Some(_))) {
                        return Err(
                            "--disable-idle-timeout conflicts with --idle-timeout-ms".to_string()
                        );
                    }
                    pool_options = pool_options.disable_idle_timeout();
                    i += 1;
                }
                "--test-before-acquire" => {
                    if i + 1 >= args.len() {
                        return Err("--test-before-acquire requires a boolean argument".to_string());
                    }
                    let value = args[i + 1]
                        .parse::<bool>()
                        .map_err(|_| "--test-before-acquire requires true or false".to_string())?;
                    pool_options = pool_options.test_before_acquire(value);
                    i += 2;
                }
                "--statement-cache-capacity" => {
                    if i + 1 >= args.len() {
                        return Err(
                            "--statement-cache-capacity requires a numeric argument".to_string()
                        );
                    }
                    let value = args[i + 1].parse::<usize>().map_err(|_| {
                        "--statement-cache-capacity requires a valid usize argument".to_string()
                    })?;
                    pool_options = pool_options.statement_cache_capacity(value);
                    i += 2;
                }
                arg => {
                    return Err(format!("Unknown argument: {}", arg));
                }
            }
        }

        Ok(CliArgs {
            schema_path,
            database_url,
            migrate,
            pool_options,
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
        assert_eq!(cli.schema_path.as_deref(), Some("path.nautilus"));
        assert_eq!(cli.database_url, None);
        assert!(!cli.migrate);
        assert_eq!(cli.pool_options, EnginePoolOptions::default());
    }

    #[test]
    fn all_flags() {
        let cli = CliArgs::parse_from(&args(&[
            "--schema",
            "s.nautilus",
            "--database-url",
            "postgres://localhost/db",
            "--migrate",
            "--max-connections",
            "16",
            "--min-connections",
            "2",
            "--acquire-timeout-ms",
            "1500",
            "--idle-timeout-ms",
            "30000",
            "--test-before-acquire",
            "false",
            "--statement-cache-capacity",
            "64",
        ]))
        .unwrap();
        assert_eq!(cli.schema_path.as_deref(), Some("s.nautilus"));
        assert_eq!(
            cli.database_url,
            Some("postgres://localhost/db".to_string())
        );
        assert!(cli.migrate);
        assert_eq!(cli.pool_options.get_max_connections(), Some(16));
        assert_eq!(cli.pool_options.get_min_connections(), Some(2));
        assert_eq!(
            cli.pool_options.get_acquire_timeout(),
            Some(std::time::Duration::from_millis(1500))
        );
        assert_eq!(
            cli.pool_options.get_idle_timeout(),
            Some(Some(std::time::Duration::from_millis(30000)))
        );
        assert_eq!(cli.pool_options.get_test_before_acquire(), Some(false));
        assert_eq!(cli.pool_options.get_statement_cache_capacity(), Some(64));
    }

    #[test]
    fn schema_is_optional() {
        let cli = CliArgs::parse_from(&args(&["--migrate"])).unwrap();
        assert_eq!(cli.schema_path, None);
        assert!(cli.migrate);
    }

    #[test]
    fn disable_idle_timeout_is_parsed() {
        let cli = CliArgs::parse_from(&args(&["--disable-idle-timeout"])).unwrap();
        assert_eq!(cli.pool_options.get_idle_timeout(), Some(None));
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

    #[test]
    fn idle_timeout_flags_conflict() {
        let err = CliArgs::parse_from(&args(&[
            "--idle-timeout-ms",
            "1000",
            "--disable-idle-timeout",
        ]))
        .unwrap_err();
        assert!(err.contains("conflicts"), "got: {err}");
    }
}

use anyhow::Context;
use nautilus_schema::{format_schema, parse_schema_source_with_recovery, LineIndex};

use crate::commands::db::connection::resolve_schema_path;
use crate::tui;

/// Execute `nautilus format` — reformat a `.nautilus` schema file in-place.
///
/// The file is parsed to AST (syntax only, no semantic validation) and
/// re-emitted in the canonical Nautilus style: 2-space indentation, blank
/// lines between blocks, and aligned field columns within each model.
pub async fn run(schema_arg: Option<String>) -> anyhow::Result<()> {
    tui::print_header("format");

    let schema_path = resolve_schema_path(schema_arg)?;

    let sp = tui::spinner(&format!("Parsing {}…", schema_path.display()));
    let schema_source = std::fs::read_to_string(&schema_path)
        .with_context(|| format!("Cannot read schema file: {}", schema_path.display()))?;

    let parsed = parse_schema_source_with_recovery(&schema_source)
        .context("Parse error — fix syntax errors before formatting")?;
    let recovered = parsed.recovered_errors;
    if !recovered.is_empty() {
        tui::spinner_err(sp, "Syntax errors found — formatting aborted");
        let line_index = LineIndex::new(&schema_source);
        let schema_label = schema_path.display().to_string();
        for e in &recovered {
            tui::print_err_line(&e.format_with_file_indexed(
                &schema_label,
                &schema_source,
                &line_index,
            ));
        }
        anyhow::bail!(
            "{} syntax error{} — fix them before formatting",
            recovered.len(),
            if recovered.len() == 1 { "" } else { "s" }
        );
    }

    tui::spinner_ok(
        sp,
        &format!(
            "Parsed  ({} declaration{})",
            parsed.ast.declarations.len(),
            if parsed.ast.declarations.len() == 1 {
                ""
            } else {
                "s"
            },
        ),
    );

    let formatted = format_schema(&parsed.ast, &schema_source);

    if formatted == schema_source {
        tui::print_summary_ok(
            "Already formatted",
            &format!("{} — no changes needed", schema_path.display()),
        );
        return Ok(());
    }

    std::fs::write(&schema_path, &formatted)
        .with_context(|| format!("Cannot write schema file: {}", schema_path.display()))?;

    tui::print_summary_ok("Formatted", &format!("{}", schema_path.display()));

    Ok(())
}

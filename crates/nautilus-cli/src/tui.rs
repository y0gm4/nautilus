//! Pretty-printed terminal helpers for the Nautilus CLI.
//!
//! Every user-facing message (headers, spinners, diff summaries, interactive
//! menus, confirmation prompts) goes through this module so the output style
//! stays consistent.

use anyhow::Error;
use console::{style, Emoji, Term};
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    io::{self, Write},
    time::Duration,
};

pub static CHECK: Emoji<'_, '_> = Emoji("✔ ", "+");
pub static CROSS: Emoji<'_, '_> = Emoji("✖ ", "x");
pub static ARROW: Emoji<'_, '_> = Emoji("◈ ", "*");
pub static WARN: Emoji<'_, '_> = Emoji("⚠ ", "!");

const PYTHON_WRAPPER_ENV: &str = "NAUTILUS_PYTHON_WRAPPER";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConfirmationResponse {
    Accept,
    Reject,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectionResponse {
    Selected(usize),
    Cancelled,
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RawSelection {
    Selected(usize),
    Cancelled,
    Fallback,
}

fn term_width() -> usize {
    Term::stdout().size().1 as usize
}

fn rule(width: usize) -> String {
    "─".repeat(width.min(72))
}

fn wrapped_by_python() -> bool {
    std::env::var_os(PYTHON_WRAPPER_ENV).is_some()
}

fn read_stdin_line() -> io::Result<Option<String>> {
    let mut line = String::new();
    let bytes_read = io::stdin().read_line(&mut line)?;
    if bytes_read == 0 {
        Ok(None)
    } else {
        Ok(Some(line))
    }
}

fn parse_confirmation_response(input: &str) -> ConfirmationResponse {
    match input.trim().to_ascii_lowercase().as_str() {
        "y" | "yes" => ConfirmationResponse::Accept,
        "" | "n" | "no" => ConfirmationResponse::Reject,
        _ => ConfirmationResponse::Invalid,
    }
}

fn parse_selection_response(input: &str, options_len: usize) -> SelectionResponse {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return SelectionResponse::Cancelled;
    }

    match trimmed.parse::<usize>() {
        Ok(choice) if (1..=options_len).contains(&choice) => {
            SelectionResponse::Selected(choice - 1)
        }
        _ => SelectionResponse::Invalid,
    }
}

pub fn print_header(command: &str) {
    let title = format!("  {}  nautilus  {}  ", ARROW, style(command).bold().cyan());
    println!();
    println!("{}", style(&title).bold());
    println!("  {}", style(rule(term_width().saturating_sub(2))).dim());
    println!();
}

pub fn spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("  {spinner:.cyan}  {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    pb.set_message(msg.to_string());
    pb.enable_steady_tick(Duration::from_millis(80));
    pb
}

pub fn spinner_ok(pb: ProgressBar, msg: &str) {
    pb.finish_and_clear();
    println!("  {}  {}", style(CHECK).green(), style(msg).dim());
}

pub fn spinner_err(pb: ProgressBar, msg: &str) {
    pb.finish_and_clear();
    println!("  {}  {}", style(CROSS).red(), style(msg).red());
}

pub fn print_ok(msg: &str) {
    println!("  {}  {}", style(CHECK).green(), style(msg).dim());
}

pub fn print_err_line(msg: &str) {
    println!("  {}  {}", style(CROSS).red(), style(msg).red());
}

pub fn print_section(label: &str) {
    println!();
    println!("  {}", style(label).bold().underlined());
    println!();
}

pub fn print_table_err(table: &str, err: &str) {
    println!(
        "  {}  {}  {}",
        style(CROSS).red(),
        style(table).white(),
        style(err).dim().red()
    );
}

pub fn print_summary_ok(label: &str, detail: &str) {
    println!();
    println!("  {}", style(rule(term_width().saturating_sub(2))).dim());
    println!(
        "  {}  {}  {}",
        style(CHECK).green(),
        style(label).bold().green(),
        style(detail).dim(),
    );
    println!();
}

pub fn print_summary_err(label: &str, detail: &str) {
    println!();
    println!("  {}", style(rule(term_width().saturating_sub(2))).dim());
    println!(
        "  {}  {}  {}",
        style(CROSS).red(),
        style(label).bold().red(),
        style(detail).dim(),
    );
    println!();
}

pub fn print_fatal_error(msg: &str) {
    eprintln!();
    let mut lines = msg.lines();
    if let Some(first_line) = lines.next() {
        eprintln!(
            "  {}  {}",
            style(CROSS).red().bold(),
            style(first_line).red()
        );
        for line in lines {
            eprintln!("     {}", style(line).red());
        }
    } else {
        eprintln!("  {}", style(CROSS).red().bold());
    }
    eprintln!();
}

pub fn print_warning(msg: &str) {
    println!("  {}  {}", style(WARN).yellow(), style(msg).yellow());
}

pub fn print_tip(msg: &str) {
    println!(
        "  {}  {}",
        style("TIP:").yellow().bold(),
        style(msg).yellow()
    );
}

/// Render an [`anyhow::Error`] and its causes in a multi-line, user-friendly form.
pub fn format_error_chain(err: &Error) -> String {
    let mut chain = err.chain();
    let mut lines = Vec::new();

    if let Some(top) = chain.next() {
        lines.push(top.to_string());
    }

    let causes: Vec<String> = chain.map(|cause| cause.to_string()).collect();
    if !causes.is_empty() {
        lines.push("Caused by:".to_string());
        for cause in causes {
            lines.push(format!("  - {}", cause));
        }
    }

    lines.join("\n")
}

/// Describe a single [`Change`] as `(sigil, subject, annotation)`.
fn describe_change(change: &nautilus_migrate::Change) -> (&'static str, String, String) {
    use nautilus_migrate::Change;
    match change {
        Change::NewTable(model) => ("+", model.db_name.clone(), "CREATE TABLE (safe)".into()),
        Change::DroppedTable { name } => (
            "-",
            name.clone(),
            "DROP TABLE (destructive — data will be lost)".into(),
        ),
        Change::AddedColumn { table, field } => (
            "+",
            format!("{}.{}", table, field.db_name),
            "ADD COLUMN (safe)".into(),
        ),
        Change::DroppedColumn { table, column } => (
            "-",
            format!("{}.{}", table, column),
            "DROP COLUMN (destructive — data will be lost)".into(),
        ),
        Change::TypeChanged {
            table,
            column,
            from,
            to,
        } => (
            "~",
            format!("{}.{}", table, column),
            format!("TYPE {} -> {} (destructive — may truncate data)", from, to),
        ),
        Change::NullabilityChanged {
            table,
            column,
            now_required: true,
        } => (
            "~",
            format!("{}.{}", table, column),
            "NOT NULL (destructive — column may contain NULLs)".into(),
        ),
        Change::NullabilityChanged {
            table,
            column,
            now_required: false,
        } => ("~", format!("{}.{}", table, column), "NULL (safe)".into()),
        Change::DefaultChanged {
            table,
            column,
            from,
            to,
        } => (
            "~",
            format!("{}.{}", table, column),
            format!(
                "DEFAULT {} -> {} (safe)",
                from.as_deref().unwrap_or("none"),
                to.as_deref().unwrap_or("none"),
            ),
        ),
        Change::PrimaryKeyChanged { table } => (
            "~",
            table.clone(),
            "PRIMARY KEY changed (destructive — requires rebuild)".into(),
        ),
        Change::IndexAdded {
            table,
            columns,
            unique,
            index_type,
            ..
        } => ("+", format!("{} ({})", table, columns.join(", ")), {
            let type_str = index_type
                .map(|t| format!(" {} ", t.as_str()))
                .unwrap_or_default();
            if *unique {
                format!("ADD UNIQUE{}INDEX (safe)", type_str)
            } else {
                format!("ADD{}INDEX (safe)", type_str)
            }
        }),
        Change::IndexDropped { table, columns, .. } => (
            "-",
            format!("{} ({})", table, columns.join(", ")),
            "DROP INDEX (safe)".into(),
        ),
        Change::ComputedExprChanged { table, column, .. } => (
            "~",
            format!("{}.{}", table, column),
            "COMPUTED expression changed (safe — DROP + ADD COLUMN)".into(),
        ),
        Change::CheckChanged {
            table,
            column,
            from,
            to,
        } => (
            "~",
            match column {
                Some(col) => format!("{}.{}", table, col),
                None => table.clone(),
            },
            format!(
                "CHECK {} -> {} (safe)",
                from.as_deref().unwrap_or("none"),
                to.as_deref().unwrap_or("none"),
            ),
        ),
        Change::CreateCompositeType { name } => (
            "+",
            format!("type:{}", name),
            "CREATE TYPE composite (safe)".into(),
        ),
        Change::DropCompositeType { name } => (
            "-",
            format!("type:{}", name),
            "DROP TYPE composite (destructive — data will be lost)".into(),
        ),
        Change::AlterCompositeType {
            name,
            added_fields,
            dropped_fields,
            type_changed_fields,
        } => {
            let annotation = if dropped_fields.is_empty() && type_changed_fields.is_empty() {
                format!("ADD ATTRIBUTE {} field(s) (safe)", added_fields.len())
            } else {
                format!(
                    "ALTER TYPE: +{} ~{} -{} field(s) (destructive)",
                    added_fields.len(),
                    type_changed_fields.len(),
                    dropped_fields.len(),
                )
            };
            ("~", format!("type:{}", name), annotation)
        }
        Change::CreateEnum { name, .. } => (
            "+",
            format!("enum:{}", name),
            "CREATE TYPE enum (safe)".into(),
        ),
        Change::DropEnum { name } => (
            "-",
            format!("enum:{}", name),
            "DROP TYPE enum (destructive — data will be lost)".into(),
        ),
        Change::AlterEnum {
            name,
            added_variants,
            removed_variants,
        } => {
            let annotation = if removed_variants.is_empty() {
                format!("ADD VALUE {} variant(s) (safe)", added_variants.len())
            } else {
                format!(
                    "ALTER ENUM: +{} -{} variant(s) (destructive — drop + recreate)",
                    added_variants.len(),
                    removed_variants.len(),
                )
            };
            ("~", format!("enum:{}", name), annotation)
        }
        Change::ForeignKeyAdded {
            table,
            columns,
            referenced_table,
            ..
        } => (
            "+",
            format!("{} ({})", table, columns.join(", ")),
            format!(
                "ADD FOREIGN KEY -> {} (destructive — may fail on existing data)",
                referenced_table,
            ),
        ),
        Change::ForeignKeyDropped {
            table,
            constraint_name,
        } => (
            "-",
            table.clone(),
            format!("DROP FOREIGN KEY {} (safe)", constraint_name),
        ),
    }
}

/// Print a formatted diff summary table.
pub fn print_diff_summary(changes: &[(nautilus_migrate::Change, nautilus_migrate::ChangeRisk)]) {
    use nautilus_migrate::ChangeRisk;
    let w = term_width().saturating_sub(2);
    println!();
    println!("  {}  {}", ARROW, style("Changes detected").bold());
    println!("  {}", style(rule(w)).dim());
    for (change, risk) in changes {
        let (sigil, subject, annotation) = describe_change(change);
        match risk {
            ChangeRisk::Safe => {
                println!(
                    "  {}  {}  {}",
                    style(sigil).green(),
                    style(&subject).white(),
                    style(&annotation).dim(),
                );
            }
            ChangeRisk::Destructive => {
                println!(
                    "  {}  {}  {}",
                    style(sigil).red().bold(),
                    style(&subject).white(),
                    style(&annotation).red(),
                );
            }
        }
    }
    println!("  {}", style(rule(w)).dim());
    println!();
}

/// Print a prominent bordered warning box (red).
pub fn print_warning_box(msg: &str) {
    let w = term_width().saturating_sub(2);
    let border = style(rule(w)).red().bold();
    println!();
    println!("  {}", border);
    println!(
        "  {}  {}",
        style(WARN).red().bold(),
        style(msg).red().bold()
    );
    println!("  {}", style(rule(w)).red().bold());
    println!();
}

fn select_option_line(prompt: &str, options: &[&str]) -> Option<usize> {
    println!();
    println!("  {}", style(prompt).bold());
    println!();
    for (index, option) in options.iter().enumerate() {
        println!("  {}. {}", index + 1, option);
    }
    println!();

    loop {
        print!(
            "  ? Select an option [1-{}] (press Enter to cancel): ",
            options.len()
        );
        let _ = io::stdout().flush();

        let line = match read_stdin_line() {
            Ok(Some(line)) => line,
            Ok(None) | Err(_) => return None,
        };

        match parse_selection_response(&line, options.len()) {
            SelectionResponse::Selected(index) => return Some(index),
            SelectionResponse::Cancelled => return None,
            SelectionResponse::Invalid => {
                print_warning(&format!(
                    "Enter a number between 1 and {}, or press Enter to cancel.",
                    options.len()
                ));
            }
        }
    }
}

fn select_option_raw(prompt: &str, options: &[&str]) -> RawSelection {
    use console::Key;
    let term = Term::stdout();
    let mut selected = 0usize;

    let total_lines = 4 + options.len();

    loop {
        println!();
        println!("  {}", style(prompt).bold());
        println!();
        for (i, opt) in options.iter().enumerate() {
            if i == selected {
                println!(
                    "  {}  {}",
                    style(">").cyan().bold(),
                    style(opt).bold().white()
                );
            } else {
                println!("     {}", style(opt).dim());
            }
        }
        println!();

        match term.read_key() {
            Ok(Key::ArrowUp) if selected > 0 => {
                selected -= 1;
            }
            Ok(Key::ArrowDown) if selected < options.len() - 1 => {
                selected += 1;
            }
            Ok(Key::Enter) => {
                term.clear_last_lines(total_lines).ok();
                return RawSelection::Selected(selected);
            }
            Ok(Key::Escape) => {
                term.clear_last_lines(total_lines).ok();
                return RawSelection::Cancelled;
            }
            Ok(_) | Err(_) => {
                term.clear_last_lines(total_lines).ok();
                return RawSelection::Fallback;
            }
        }

        term.clear_last_lines(total_lines).ok();
    }
}

/// Show an interactive selection menu.
///
/// The native arrow-key UI is kept for direct terminal launches. When Nautilus
/// is started through the Python wrapper, the prompt falls back to a numbered
/// selection flow, which is more reliable on Windows console shims.
pub fn select_option(prompt: &str, options: &[&str]) -> Option<usize> {
    if options.is_empty() {
        return None;
    }

    if wrapped_by_python() || !Term::stdout().is_term() {
        return select_option_line(prompt, options);
    }

    match select_option_raw(prompt, options) {
        RawSelection::Selected(index) => Some(index),
        RawSelection::Cancelled => None,
        RawSelection::Fallback => select_option_line(prompt, options),
    }
}

/// Prompt for explicit confirmation before a destructive operation.
/// Returns `true` only if the user types `y` or `yes`.
pub fn confirm_destructive() -> bool {
    loop {
        print!("  ? Apply destructive changes? This will cause data loss. [y/N] ");
        let _ = io::stdout().flush();

        let line = match read_stdin_line() {
            Ok(Some(line)) => line,
            Ok(None) | Err(_) => return false,
        };

        match parse_confirmation_response(&line) {
            ConfirmationResponse::Accept => return true,
            ConfirmationResponse::Reject => return false,
            ConfirmationResponse::Invalid => {
                print_warning("Please answer with `y` or `n`.");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        format_error_chain, parse_confirmation_response, parse_selection_response,
        ConfirmationResponse, SelectionResponse,
    };
    use anyhow::anyhow;

    #[test]
    fn format_error_chain_includes_nested_causes() {
        let err = anyhow!("inner failure").context("outer failure");
        let rendered = format_error_chain(&err);

        assert!(rendered.contains("outer failure"));
        assert!(rendered.contains("Caused by:"));
        assert!(rendered.contains("inner failure"));
    }

    #[test]
    fn confirmation_parser_accepts_yes_variants() {
        assert_eq!(
            parse_confirmation_response("y"),
            ConfirmationResponse::Accept
        );
        assert_eq!(
            parse_confirmation_response("YES"),
            ConfirmationResponse::Accept
        );
    }

    #[test]
    fn confirmation_parser_rejects_default_and_no_variants() {
        assert_eq!(
            parse_confirmation_response(""),
            ConfirmationResponse::Reject
        );
        assert_eq!(
            parse_confirmation_response("n"),
            ConfirmationResponse::Reject
        );
        assert_eq!(
            parse_confirmation_response("No"),
            ConfirmationResponse::Reject
        );
    }

    #[test]
    fn confirmation_parser_flags_invalid_input() {
        assert_eq!(
            parse_confirmation_response("maybe"),
            ConfirmationResponse::Invalid
        );
    }

    #[test]
    fn selection_parser_accepts_valid_numeric_choices() {
        assert_eq!(
            parse_selection_response("2", 3),
            SelectionResponse::Selected(1)
        );
    }

    #[test]
    fn selection_parser_treats_blank_as_cancel() {
        assert_eq!(
            parse_selection_response("", 3),
            SelectionResponse::Cancelled
        );
    }

    #[test]
    fn selection_parser_rejects_out_of_range_values() {
        assert_eq!(parse_selection_response("4", 3), SelectionResponse::Invalid);
        assert_eq!(
            parse_selection_response("abc", 3),
            SelectionResponse::Invalid
        );
    }
}

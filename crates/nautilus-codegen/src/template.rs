//! Shared helpers for rendering Tera templates across codegen backends.

use tera::{Context, Tera};

/// Render `template` from `tera` with `ctx`, normalizing CRLF line endings.
///
/// Tera on Windows can emit `\r\n` depending on how template strings are
/// embedded at build time. All codegen backends want LF-only output so
/// generated sources hash consistently across platforms.
pub(crate) fn render(tera: &Tera, template: &str, ctx: &Context) -> String {
    tera.render(template, ctx)
        .unwrap_or_else(|error| panic!("template rendering failed for '{}': {:?}", template, error))
        .replace("\r\n", "\n")
}

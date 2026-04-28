//! Compile-time exhaustiveness check for the projection registry.
//!
//! Parses `crates/cairn-domain/src/events.rs` for every `RuntimeEvent`
//! variant, then parses `src/projection_registry.rs` for every
//! `variant: "..."` entry. If the two sets differ, the build fails with a
//! list of the missing or orphaned variants.
//!
//! This replaces a proc-macro + derive pair (which would require a
//! separate `cairn-store-derive` crate) with a much simpler static
//! build-script check. The guarantee is the same: no new `RuntimeEvent`
//! variant can land without a registry entry, and no registry entry can
//! reference a variant that no longer exists in the domain.
//!
//! RFC-025 Phase 0.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

fn main() {
    // Locate sibling crate files. `CARGO_MANIFEST_DIR` is
    // `crates/cairn-store`; domain sits alongside at `crates/cairn-domain`.
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let events_rs = manifest_dir.join("../cairn-domain/src/events.rs");
    let registry_rs = manifest_dir.join("src/projection_registry.rs");

    println!("cargo:rerun-if-changed={}", events_rs.display());
    println!("cargo:rerun-if-changed={}", registry_rs.display());

    let domain_variants = match extract_domain_variants(&events_rs) {
        Ok(v) => v,
        Err(e) => {
            // Upstream events.rs missing/unreadable would be a serious
            // repo-layout issue; surface it as a structured cargo error
            // so the message renders cleanly in PR diagnostics rather
            // than as a raw build-script panic stack trace.
            emit_error(&format!(
                "build.rs: failed to parse {}: {e}",
                events_rs.display()
            ));
            std::process::exit(1);
        }
    };
    let registry_variants = match extract_registry_variants(&registry_rs) {
        Ok(v) => v,
        Err(e) => {
            emit_error(&format!(
                "build.rs: failed to parse {}: {e}",
                registry_rs.display()
            ));
            std::process::exit(1);
        }
    };

    let missing: BTreeSet<&String> = domain_variants.difference(&registry_variants).collect();
    let orphaned: BTreeSet<&String> = registry_variants.difference(&domain_variants).collect();

    if !missing.is_empty() || !orphaned.is_empty() {
        // Emit each drift bucket as its own cargo error/warning line so
        // contributors see the concrete variant list in the PR check
        // annotations without hunting through a multi-line panic.
        emit_error("projection registry is out of sync with RuntimeEvent (RFC-025 Phase 0)");
        if !missing.is_empty() {
            emit_error(&format!(
                "missing from REGISTRY ({} variant(s)): {} — add each to \
                 crates/cairn-store/src/projection_registry.rs with \
                 ProjectionStatus::Projected / Stubbed / Ephemeral",
                missing.len(),
                missing
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !orphaned.is_empty() {
            emit_error(&format!(
                "orphaned REGISTRY entries ({}): {} — either restore the variant in \
                 crates/cairn-domain/src/events.rs or delete the registry entry",
                orphaned.len(),
                orphaned
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        std::process::exit(1);
    }
}

/// Emit a structured cargo warning directive plus a stderr line. The
/// stable build-script directive is `cargo:warning=<msg>` (single
/// colon, `cargo:<key>=...` shape); the newer `cargo::warning=<msg>`
/// double-colon syntax was added in Rust 1.77 but the single-colon
/// form is backward-compatible and renders identically in GitHub
/// Actions PR annotations. `cargo:error=` is NOT a stable directive —
/// cargo would silently ignore it — so we combine the warning line
/// (for annotation rendering) with an explicit stderr `error:` prefix
/// and a non-zero exit from the caller to fail the build.
fn emit_error(msg: &str) {
    println!("cargo:warning={msg}");
    eprintln!("error: {msg}");
}

/// Extract every `RuntimeEvent` variant name from `events.rs`.
///
/// Strategy: find the `pub enum RuntimeEvent {` block, then scan forward
/// until the matching closing brace (depth-0), pulling out every line of
/// the shape `    <PascalCase>(<PascalCase>),`. Comments (`///`, `//`)
/// and blank lines are skipped.
fn extract_domain_variants(path: &std::path::Path) -> Result<BTreeSet<String>, String> {
    let contents = fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let start = contents
        .find("pub enum RuntimeEvent {")
        .ok_or_else(|| "pub enum RuntimeEvent { not found".to_string())?;
    // Walk from `start` counting braces until depth returns to 0.
    let bytes = contents.as_bytes();
    let mut depth: i32 = 0;
    let mut end: Option<usize> = None;
    let mut seen_open = false;
    for (i, &b) in bytes[start..].iter().enumerate() {
        if b == b'{' {
            depth += 1;
            seen_open = true;
        } else if b == b'}' {
            depth -= 1;
            if seen_open && depth == 0 {
                end = Some(start + i);
                break;
            }
        }
    }
    let end = end.ok_or_else(|| "RuntimeEvent enum not closed".to_string())?;
    let block = &contents[start..=end];

    let mut variants = BTreeSet::new();
    for raw in block.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with("//") || line.starts_with("///") {
            continue;
        }
        // Match `Variant(...)`, possibly trailing with `,`.
        // Skip the `pub enum RuntimeEvent {` header itself.
        if line.starts_with("pub enum") {
            continue;
        }
        // A variant line looks like `SessionCreated(SessionCreated),` or
        // `SessionCreated(SessionCreated)`. Strip the payload type.
        let before_paren = match line.split_once('(') {
            Some((name, _)) => name.trim(),
            None => continue,
        };
        if before_paren.is_empty() {
            continue;
        }
        // Skip anything that isn't a bare PascalCase identifier (e.g.
        // `pub enum`, `#[...]`, etc. — defense-in-depth).
        if !before_paren
            .chars()
            .next()
            .map(|c| c.is_ascii_uppercase())
            .unwrap_or(false)
        {
            continue;
        }
        if !before_paren
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            continue;
        }
        variants.insert(before_paren.to_string());
    }
    if variants.is_empty() {
        return Err("no RuntimeEvent variants parsed (scanner regression?)".into());
    }
    Ok(variants)
}

/// Extract every `variant: "..."` literal from the projection registry.
fn extract_registry_variants(path: &std::path::Path) -> Result<BTreeSet<String>, String> {
    let contents = fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut variants = BTreeSet::new();
    // Look for `variant: "Name"` — simple but strict enough that it
    // won't match comments of the same shape. We additionally require
    // `variant` is at the start of a trimmed line (registry entries are
    // indented inside `ProjectionEntry { ... }`).
    for raw in contents.lines() {
        let line = raw.trim_start();
        if !line.starts_with("variant:") {
            continue;
        }
        let after = &line["variant:".len()..].trim_start();
        if !after.starts_with('"') {
            continue;
        }
        let rest = &after[1..];
        let end = match rest.find('"') {
            Some(i) => i,
            None => continue,
        };
        let name = &rest[..end];
        if name.is_empty() {
            continue;
        }
        variants.insert(name.to_string());
    }
    if variants.is_empty() {
        return Err("no registry entries parsed (format regression?)".into());
    }
    Ok(variants)
}

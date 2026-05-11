//! Layer-ordering invariant for #440.
//!
//! CLAUDE.md declares the crate ordering:
//!
//! ```text
//! domain -> store -> runtime -> {memory, graph, evals, tools, agent,
//!     signal, channels} -> api/plugin-proto -> app
//! ```
//!
//! cairn-api sits above cairn-memory, so cairn-memory must not depend
//! on cairn-api — direct or transitive — at runtime. We previously had
//! a `cairn-api = { path = "../cairn-api" }` production dep in
//! `cairn-memory/Cargo.toml` plus a `cairn-memory = { … }` dev-dep in
//! `cairn-api/Cargo.toml`, which both inverted the ordering and left a
//! latent cycle risk.
//!
//! This test asserts the regression-proof shape directly on the source
//! tree: no cairn-memory Rust file may `use cairn_api::` anything, and
//! no cairn-memory Cargo manifest line may resolve `cairn-api`. The
//! legitimate replacement is `cairn_api_contracts::…`, which lives
//! below cairn-memory and is therefore dependency-safe.

use std::path::{Path, PathBuf};

fn cairn_memory_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_owned()
}

fn visit_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            if p.file_name().map(|n| n == "target").unwrap_or(false) {
                continue;
            }
            visit_rs_files(&p, out);
        } else if p.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(p);
        }
    }
}

#[test]
fn cairn_memory_does_not_import_cairn_api() {
    let root = cairn_memory_root();
    let mut rs_files = Vec::new();
    visit_rs_files(&root.join("src"), &mut rs_files);
    visit_rs_files(&root.join("tests"), &mut rs_files);

    // Skip this file itself — the assertion's diagnostic strings
    // intentionally contain `cairn_api::` literals to explain the
    // violation to humans; scanning them would false-positive.
    let self_path = Path::new(file!()).file_name().unwrap_or_default();

    let mut violations: Vec<String> = Vec::new();
    for f in &rs_files {
        if f.file_name().unwrap_or_default() == self_path {
            continue;
        }
        let Ok(contents) = std::fs::read_to_string(f) else {
            continue;
        };
        for (line_no, line) in contents.lines().enumerate() {
            let trimmed = line.trim_start();
            // Match `use cairn_api::` or `cairn_api::` in a non-comment line.
            // Skip ordinary comment lines (// …); doc-comments and other
            // referencing text is fine.
            if trimmed.starts_with("//") {
                continue;
            }
            // Detect the crate path in isolation — `cairn_api_contracts`
            // starts with `cairn_api_` so guard against matching it.
            let pat = "cairn_api::";
            let mut idx = 0;
            while let Some(hit) = line[idx..].find(pat) {
                let absolute = idx + hit;
                let before = line[..absolute].chars().last();
                // Reject `foo_cairn_api::` or similar composites;
                // `cairn_api::` must appear at a word boundary.
                let boundary = match before {
                    None => true,
                    Some(c) => !c.is_alphanumeric() && c != '_',
                };
                if boundary {
                    violations.push(format!(
                        "{}:{}: `use cairn_api::…` inverts layer ordering (use `cairn_api_contracts::…` instead): {}",
                        f.display(),
                        line_no + 1,
                        line.trim()
                    ));
                    break;
                }
                idx = absolute + pat.len();
            }
        }
    }

    assert!(
        violations.is_empty(),
        "cairn-memory must not depend on cairn-api (see #440 — layering rule):\n{}",
        violations.join("\n")
    );
}

#[test]
fn cairn_memory_cargo_manifest_does_not_list_cairn_api() {
    let manifest = std::fs::read_to_string(cairn_memory_root().join("Cargo.toml"))
        .expect("cairn-memory/Cargo.toml must be readable");

    for (line_no, line) in manifest.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            // Comments may reference the historical `cairn-api` dep for
            // context; they are not a real dep line.
            continue;
        }
        // Real dep lines look like `cairn-api = { path = "…" }` or
        // `cairn-api = "0.1"`. We match on the leading identifier only.
        if trimmed.starts_with("cairn-api ") || trimmed.starts_with("cairn-api=") {
            panic!(
                "Cargo.toml:{}: cairn-memory must not list cairn-api as a dep (see #440 — layering rule): {}",
                line_no + 1,
                line
            );
        }
    }
}

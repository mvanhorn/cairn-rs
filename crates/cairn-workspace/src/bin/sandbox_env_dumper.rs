//! Test-only helper binary for the `#527` env-isolation integration tests.
//!
//! Purpose: simulate a sandboxed child and report every environment variable
//! it can observe. The parent test spawns this binary via
//! [`cairn_workspace::sandbox::spawn::spawn_sandboxed_agent`], then reads the
//! output and asserts:
//!
//! 1. No secret from the parent's environment (tokens, API keys, DB URL)
//!    appears in the child — the `env_clear` path works.
//! 2. The documented baseline (PATH, HOME, LANG, …) AND the cairn-specific
//!    contract vars (CAIRN_SESSION_ID, CAIRN_SANDBOX_BASE_DIR) DID make it
//!    through.
//! 3. Explicit `extra_env` declarations reach the child verbatim.
//!
//! The child writes its output over fd 3 (the tool-bridge socketpair the
//! parent set up) so stdout/stderr inheritance does not affect the test.
//! Output is a single line of JSON — one frame, newline-terminated, matching
//! the line-delimited convention [`bin_sandboxed_agent`] uses.
//!
//! The binary parses `/proc/self/environ` — the kernel's ground-truth view
//! of the process's env block — rather than `std::env::vars()`. Reading
//! `/proc` makes the assertion robust to any parent-side std::env
//! manipulation during spawn: what we check is the final exec(2) env.
//!
//! Non-Linux builds compile a `main` that hard-errors (prints a diagnostic
//! to stderr and exits with status 2) so the crate still builds but a stray
//! invocation of the helper off-platform fails loudly rather than silently
//! succeeding. The integration test that runs this binary is itself gated on
//! `target_os = "linux"`.

#![forbid(unsafe_code)]

#[cfg(target_os = "linux")]
fn main() {
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("sandbox_env_dumper: only supported on Linux");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::BTreeMap;
    use std::io::Write;

    pub fn run() {
        // Read the kernel's ground-truth env block. Entries are NUL-separated
        // `KEY=VALUE` records.
        let bytes = match std::fs::read("/proc/self/environ") {
            Ok(b) => b,
            Err(err) => {
                eprintln!("[env-dumper] failed to read /proc/self/environ: {err}");
                std::process::exit(20);
            }
        };

        let mut env: BTreeMap<String, String> = BTreeMap::new();
        for record in bytes.split(|&b| b == 0) {
            if record.is_empty() {
                continue;
            }
            let text = match std::str::from_utf8(record) {
                Ok(s) => s,
                Err(_) => continue, // non-UTF-8 env var — ignored; shouldn't happen here
            };
            let Some((k, v)) = text.split_once('=') else {
                continue;
            };
            env.insert(k.to_string(), v.to_string());
        }

        // Serialize via serde_json (already a crate dep): correct RFC 8259
        // escape handling with no hand-rolled encoder to maintain.
        let payload = match serde_json::to_string(&serde_json::json!({ "env": &env })) {
            Ok(mut s) => {
                s.push('\n');
                s
            }
            Err(err) => {
                eprintln!("[env-dumper] JSON serialize failed: {err}");
                std::process::exit(22);
            }
        };

        // Write to fd 3 (the tool-bridge socketpair). Using the `File` wrapper
        // around a raw fd via `std::os::fd::FromRawFd` would require `unsafe`
        // (which this binary forbids); instead go through `nix::sys::socket`,
        // same pattern `bin_sandboxed_agent` uses.
        //
        // Guard: the test helper also prints to stderr so a test-harness
        // failure mode (fd 3 closed, wrong wiring) surfaces a readable
        // diagnostic rather than exiting silently.
        eprintln!("[env-dumper] writing {} env entries to fd 3", env.len());
        match write_to_fd3(payload.as_bytes()) {
            Ok(()) => {}
            Err(err) => {
                // If fd 3 isn't wired (shouldn't happen in the test), fall
                // back to stdout so the test at least has something to read.
                eprintln!("[env-dumper] fd 3 write failed ({err}); falling back to stdout");
                let _ = std::io::stdout().write_all(payload.as_bytes());
                let _ = std::io::stdout().flush();
                std::process::exit(21);
            }
        }
    }

    fn write_to_fd3(mut data: &[u8]) -> std::io::Result<()> {
        use nix::sys::socket::MsgFlags;
        while !data.is_empty() {
            let written = nix::sys::socket::send(3, data, MsgFlags::empty())
                .map_err(|err| std::io::Error::other(format!("send: {err}")))?;
            if written == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "socket closed mid-write",
                ));
            }
            data = &data[written..];
        }
        Ok(())
    }
}

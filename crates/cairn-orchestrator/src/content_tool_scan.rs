//! #697 R5-B: content-scan fallback parser for pseudo-XML tool calls.
//!
//! # Problem
//!
//! Dogfood R5 surfaced that NVIDIA Nemotron-3-Super-120B emits
//! tool-calls as Nvidia-Nemo pseudo-XML (`<function=NAME>\n<parameter=x>
//! value</parameter>\n</function>`) into the assistant message's
//! `content` field instead of populating `tool_calls[]` — even when a
//! proper native `tools[]` schema is declared.
//!
//! R5-A (native `spawn_subagent` tool_def) mitigates this for the
//! happy path: with a well-shaped schema, most models emit clean
//! JSON in `tool_calls[]`. But the industry research in
//! `agent-knowledge/multi-provider-tool-call-quirks.md` confirms
//! there are at least 3 distinct content-leak formats in production
//! use across different model families, and R6 dogfood will almost
//! certainly surface at least one of them:
//!
//!   1. **Nemotron-Nemo**: `<function=NAME><parameter=key>value</parameter>…</function>`
//!   2. **Hermes / Qwen (with thinking enabled)**:
//!      `<tool_call>{"name":"...","arguments":{...}}</tool_call>`
//!   3. **Llama-3.2 small / Llama-4 with pythonic off**:
//!      `<|python_tag|>{"name":"...","parameters":{...}}`
//!
//! This module detects those shapes in message content and returns
//! synthetic OpenAI-shaped `tool_calls[]` entries that the existing
//! `tool_calls_to_proposals` pipeline can consume without any
//! downstream changes.
//!
//! # Non-goals
//!
//! - Full format coverage. vLLM ships 15+ parsers and SGLang ships
//!   20+. We cover the 3 shapes most likely to hit cairn's production
//!   providers (OpenAI-compat via OpenRouter / Z.ai / Ollama / direct
//!   Anthropic + OpenAI). The others (DeepSeek UTF-8 delimiters,
//!   GLM-4-MoE XML, Step3/MiniMax steptml, Kimi-K2, Jamba, Granite,
//!   xLAM, InternLM, pythonic, etc.) are filed as follow-ups once we
//!   see them in dogfood.
//! - Streaming deltas. This module operates on the full concatenated
//!   `content` string; streaming reassembly lives upstream in the
//!   providers crate.
//! - Schema validation of extracted arguments. The downstream
//!   `tool_calls_to_proposals` path already validates; we just hand
//!   off the synthetic tool_calls in the canonical shape.
//!
//! # Shape of the synthetic output
//!
//! Every detected pattern is converted to:
//!
//! ```json
//! {
//!   "type": "function",
//!   "function": {
//!     "name": "<extracted_name>",
//!     "arguments": "<json_string>"
//!   }
//! }
//! ```
//!
//! matching the OpenAI shape the rest of cairn consumes. IDs are NOT
//! synthesized here — the cairn pipeline doesn't yet depend on
//! `tool_calls[].id` for anything observable to the run. If a future
//! change needs IDs, `format!("scan_{uuid}")` is the LiteLLM
//! convention to copy.

use std::sync::LazyLock;

use regex::Regex;
use serde_json::Value;

// ── Detectors ────────────────────────────────────────────────────────────

/// Nemotron-Nemo: `<function=NAME>\n<parameter=key>value</parameter>\n</function>`.
///
/// Captures the function name + the inner body. The body is then
/// re-scanned with [`NEMO_PARAMETER_RE`] for key/value pairs.
static NEMO_FUNCTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    // `.+?` for name (non-greedy — names are short, stop at `>`).
    // `(?s)` lets `.` match newlines inside the body.
    Regex::new(r"(?s)<function=([A-Za-z0-9_\-]+)>(.*?)</function>")
        .expect("NEMO_FUNCTION_RE compiles")
});

static NEMO_PARAMETER_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)<parameter=([A-Za-z0-9_\-]+)>(.*?)</parameter>")
        .expect("NEMO_PARAMETER_RE compiles")
});

/// Hermes / Qwen: `<tool_call>{…}</tool_call>`. The body is raw JSON
/// with `{"name": "...", "arguments": {...}}`.
static HERMES_TOOL_CALL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)<tool_call>(.*?)</tool_call>").expect("HERMES_TOOL_CALL_RE compiles")
});

/// Llama-3.2: `<|python_tag|>{…}` — no closing tag, JSON runs to end
/// of line (or string).
///
/// Anchoring: `\{.*?\}` non-greedy-matches the first balanced-looking
/// JSON object, then the terminator `(?:\s*\n|\s*$)` lets optional
/// trailing whitespace (spaces / tabs / carriage-return) land before
/// the newline or string-end. Without the `\s*` the regex missed
/// lines emitted by providers that rstrip to a single trailing space
/// or emit `\r\n`. A proper JSON-depth tracker would be more robust
/// for nested braces inside strings, but `.*?` matches the shortest
/// balanced shape so this handles the 3 observed Llama-3.2 emissions
/// without false-positives on surrounding prose.
static LLAMA_PYTHON_TAG_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?s)<\|python_tag\|>(\{.*?\})(?:\s*\n|\s*$)")
        .expect("LLAMA_PYTHON_TAG_RE compiles")
});

// ── Public API ───────────────────────────────────────────────────────────

/// Scan `content` for pseudo-XML tool calls and convert each match to
/// a synthetic OpenAI-shaped `tool_calls[]` entry. Returns entries in
/// source order.
///
/// Empty content, or content with no matches, returns `vec![]` —
/// callers can safely chain this into their existing native
/// `tool_calls[]` handling without guarding.
///
/// # Ordering
///
/// Matches are returned in the order they appear in the source. Mixed
/// formats (e.g. a Nemotron `<function=>` tag followed by a
/// Hermes `<tool_call>`) are all captured; the caller decides how to
/// prioritise. In practice a single model session only emits one
/// format, so mixed runs are extremely rare.
///
/// # Limitations
///
/// - No schema validation of extracted arguments.
/// - No streaming support (operates on concatenated content).
/// - Regex-based: pathological inputs (deeply nested mis-matched
///   tags) can fall back to the last valid match rather than parse
///   into structured proposals. This is acceptable because the
///   downstream `tool_calls_to_proposals` path handles malformed
///   JSON gracefully by surfacing the problem to R2-A's
///   retry-with-feedback loop.
pub fn scan_content_for_tool_calls(content: &str) -> Vec<Value> {
    if content.is_empty() {
        return Vec::new();
    }

    let mut out: Vec<ScanMatch> = Vec::new();

    // Nemotron-Nemo function tags.
    for cap in NEMO_FUNCTION_RE.captures_iter(content) {
        let start = cap.get(0).map(|m| m.start()).unwrap_or(0);
        let name = cap.get(1).map(|m| m.as_str()).unwrap_or("").to_owned();
        let body = cap.get(2).map(|m| m.as_str()).unwrap_or("");
        let args = parse_nemo_parameters(body);
        let arguments_str = serde_json::to_string(&args).unwrap_or_else(|_| "{}".to_owned());
        out.push(ScanMatch {
            start,
            call: build_synthetic_tool_call(&name, &arguments_str),
        });
    }

    // Hermes / Qwen <tool_call> blocks.
    for cap in HERMES_TOOL_CALL_RE.captures_iter(content) {
        let start = cap.get(0).map(|m| m.start()).unwrap_or(0);
        let body = cap.get(1).map(|m| m.as_str()).unwrap_or("").trim();
        if let Some((name, args_str)) = extract_hermes_name_and_args(body) {
            out.push(ScanMatch {
                start,
                call: build_synthetic_tool_call(&name, &args_str),
            });
        }
    }

    // Llama <|python_tag|> blocks.
    for cap in LLAMA_PYTHON_TAG_RE.captures_iter(content) {
        let start = cap.get(0).map(|m| m.start()).unwrap_or(0);
        let body = cap.get(1).map(|m| m.as_str()).unwrap_or("").trim();
        if let Some((name, args_str)) = extract_llama_name_and_args(body) {
            out.push(ScanMatch {
                start,
                call: build_synthetic_tool_call(&name, &args_str),
            });
        }
    }

    // Sort by source-order so concurrent-format output stays
    // deterministic.
    out.sort_by_key(|m| m.start);
    out.into_iter().map(|m| m.call).collect()
}

// ── Internals ────────────────────────────────────────────────────────────

struct ScanMatch {
    start: usize,
    call: Value,
}

/// Parse `<parameter=key>value</parameter>` pairs out of a
/// Nemotron-Nemo function body. Values are best-effort parsed as JSON
/// first (so numbers and booleans land as their typed variants); if
/// that fails the raw string is used.
///
/// Example body:
/// ```text
///   <parameter=role>researcher</parameter>
///   <parameter=goal>Find 3 Rust patterns</parameter>
/// ```
/// → `{"role": "researcher", "goal": "Find 3 Rust patterns"}`.
fn parse_nemo_parameters(body: &str) -> Value {
    let mut obj = serde_json::Map::new();
    for cap in NEMO_PARAMETER_RE.captures_iter(body) {
        let key = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let raw_value = cap.get(2).map(|m| m.as_str()).unwrap_or("").trim();
        let value = serde_json::from_str::<Value>(raw_value)
            .unwrap_or_else(|_| Value::String(raw_value.to_owned()));
        obj.insert(key.to_owned(), value);
    }
    Value::Object(obj)
}

/// Parse the body of a `<tool_call>…</tool_call>` block. Expected
/// shape: `{"name": "...", "arguments": {...}}`. Returns
/// `(name, arguments_as_json_string)` or `None` if either field is
/// missing.
///
/// `arguments` is re-serialized to a string (not returned as the
/// original raw slice) so the output is byte-canonical regardless of
/// whitespace / ordering in the source.
fn extract_hermes_name_and_args(body: &str) -> Option<(String, String)> {
    let parsed: Value = serde_json::from_str(body).ok()?;
    let obj = parsed.as_object()?;
    let name = obj.get("name")?.as_str()?.to_owned();
    let args = obj
        .get("arguments")
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()));
    let args_str = serde_json::to_string(&args).ok()?;
    Some((name, args_str))
}

/// Parse the body of a `<|python_tag|>{…}` block. Llama emits
/// `{"name": "...", "parameters": {...}}` — note `parameters` not
/// `arguments`. Normalize to `arguments` on the synthetic tool_call.
fn extract_llama_name_and_args(body: &str) -> Option<(String, String)> {
    let parsed: Value = serde_json::from_str(body).ok()?;
    let obj = parsed.as_object()?;
    let name = obj.get("name")?.as_str()?.to_owned();
    // Accept both `parameters` (Llama canonical) and `arguments`
    // (some Llama-compat serving stacks rename).
    let args = obj
        .get("parameters")
        .or_else(|| obj.get("arguments"))
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()));
    let args_str = serde_json::to_string(&args).ok()?;
    Some((name, args_str))
}

/// Wrap an extracted `(name, args_json_string)` into an OpenAI-shaped
/// tool_call entry that downstream `tool_calls_to_proposals`
/// consumes unchanged.
fn build_synthetic_tool_call(name: &str, arguments_json_string: &str) -> Value {
    serde_json::json!({
        "type": "function",
        "function": {
            "name": name,
            "arguments": arguments_json_string,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_content_returns_empty() {
        assert!(scan_content_for_tool_calls("").is_empty());
    }

    #[test]
    fn content_with_no_tool_call_returns_empty() {
        assert!(scan_content_for_tool_calls("Just some prose with no tags.").is_empty());
    }

    #[test]
    fn nemotron_function_tags_produce_synthetic_tool_call() {
        // The exact pseudo-XML shape observed in dogfood R5 from
        // nvidia/nemotron-3-super-120b-a12b:free.
        let content = "\
            Reasoning about what to do...\n\
            <function=spawn_subagent>\n\
            <parameter=role>researcher</parameter>\n\
            <parameter=goal>Identify 3 Rust circuit breaker best practices</parameter>\n\
            </function>\n";
        let calls = scan_content_for_tool_calls(content);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["function"]["name"], "spawn_subagent");
        let args_str = calls[0]["function"]["arguments"].as_str().unwrap();
        let args: Value = serde_json::from_str(args_str).unwrap();
        assert_eq!(args["role"], "researcher");
        assert_eq!(
            args["goal"],
            "Identify 3 Rust circuit breaker best practices"
        );
    }

    #[test]
    fn hermes_tool_call_block_produces_synthetic_tool_call() {
        let content = r#"
            Thinking...
            <tool_call>
            {"name": "spawn_subagent", "arguments": {"role": "researcher", "goal": "X"}}
            </tool_call>
        "#;
        let calls = scan_content_for_tool_calls(content);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["function"]["name"], "spawn_subagent");
        let args_str = calls[0]["function"]["arguments"].as_str().unwrap();
        let args: Value = serde_json::from_str(args_str).unwrap();
        assert_eq!(args["role"], "researcher");
        assert_eq!(args["goal"], "X");
    }

    #[test]
    fn llama_python_tag_produces_synthetic_tool_call() {
        // Llama uses `parameters` not `arguments`; the extractor
        // normalizes to `arguments` on the synthetic output while
        // keeping the inner key-value structure intact.
        let content = "<|python_tag|>{\"name\": \"spawn_subagent\", \"parameters\": {\"role\": \"researcher\", \"goal\": \"X\"}}\n";
        let calls = scan_content_for_tool_calls(content);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["function"]["name"], "spawn_subagent");
        let args_str = calls[0]["function"]["arguments"].as_str().unwrap();
        let args: Value = serde_json::from_str(args_str).unwrap();
        assert_eq!(args["role"], "researcher");
        assert_eq!(args["goal"], "X");
    }

    #[test]
    fn llama_python_tag_handles_trailing_whitespace_variants() {
        // Gemini review on PR #699 caught that the regex missed
        // content with trailing whitespace before the newline (space,
        // tab, CRLF) or content that ended at EOF with trailing
        // spaces but no newline. The terminator `(?:\s*\n|\s*$)`
        // absorbs those variants without consuming bytes from the
        // JSON body.
        let cases = [
            // Trailing space before newline.
            "<|python_tag|>{\"name\":\"t\",\"parameters\":{}} \n",
            // CRLF.
            "<|python_tag|>{\"name\":\"t\",\"parameters\":{}}\r\n",
            // Tab + newline.
            "<|python_tag|>{\"name\":\"t\",\"parameters\":{}}\t\n",
            // Trailing spaces at end-of-string (no newline at all).
            "<|python_tag|>{\"name\":\"t\",\"parameters\":{}}   ",
            // End-of-string immediately after JSON.
            "<|python_tag|>{\"name\":\"t\",\"parameters\":{}}",
        ];
        for (i, content) in cases.iter().enumerate() {
            let calls = scan_content_for_tool_calls(content);
            assert_eq!(calls.len(), 1, "case {i} failed: no synthetic call");
            assert_eq!(calls[0]["function"]["name"], "t", "case {i} name mismatch");
        }
    }

    #[test]
    fn multiple_nemotron_functions_in_one_content_all_extracted() {
        // Rare shape (models usually emit one tool_call per turn) but
        // the extractor must handle it deterministically.
        let content = "\
            <function=tool_a><parameter=x>1</parameter></function>\n\
            prose in between\n\
            <function=tool_b><parameter=y>2</parameter></function>\n";
        let calls = scan_content_for_tool_calls(content);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0]["function"]["name"], "tool_a");
        assert_eq!(calls[1]["function"]["name"], "tool_b");
    }

    #[test]
    fn nemotron_number_parameter_preserves_type() {
        // Nemo parameter values that are valid JSON (number, bool,
        // null, array) get typed correctly so downstream tools see
        // `42` instead of `"42"`.
        let content =
            "<function=tool><parameter=count>42</parameter><parameter=flag>true</parameter></function>";
        let calls = scan_content_for_tool_calls(content);
        assert_eq!(calls.len(), 1);
        let args_str = calls[0]["function"]["arguments"].as_str().unwrap();
        let args: Value = serde_json::from_str(args_str).unwrap();
        assert_eq!(args["count"], 42);
        assert_eq!(args["flag"], true);
    }

    #[test]
    fn malformed_hermes_block_skipped_cleanly() {
        // Invalid JSON inside <tool_call> — no panic, just no
        // synthetic entry for that block. Downstream R2-A retry
        // surfaces the shape problem to the model.
        let content = "<tool_call>{\"this is not valid json</tool_call>";
        let calls = scan_content_for_tool_calls(content);
        assert!(calls.is_empty());
    }

    #[test]
    fn mixed_formats_produce_entries_in_source_order() {
        // Defense test for the sort_by_key — if a content has both a
        // <function=> and a <tool_call> block, output order matches
        // source order regardless of which detector ran first
        // internally.
        let content = "\
            <tool_call>{\"name\":\"first\",\"arguments\":{}}</tool_call>\n\
            <function=second><parameter=k>v</parameter></function>\n";
        let calls = scan_content_for_tool_calls(content);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0]["function"]["name"], "first");
        assert_eq!(calls[1]["function"]["name"], "second");
    }

    #[test]
    fn nemo_body_without_parameters_yields_empty_args() {
        // A bare `<function=NAME></function>` with no parameters should
        // still yield a synthetic tool_call — the downstream parser
        // will surface "missing required field" via R2-A.
        let content = "<function=spawn_subagent></function>";
        let calls = scan_content_for_tool_calls(content);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["function"]["name"], "spawn_subagent");
        let args_str = calls[0]["function"]["arguments"].as_str().unwrap();
        let args: Value = serde_json::from_str(args_str).unwrap();
        assert!(args.as_object().unwrap().is_empty());
    }

    #[test]
    fn content_with_thinking_tag_noise_still_finds_tool_call() {
        // Qwen/Nemotron "thinking" outputs wrap reasoning in noise tags.
        // The detector should ignore everything outside the function
        // / tool_call blocks.
        let content = "\
            <think>\n\
            We need to call spawn_subagent with these args...\n\
            </think>\n\
            <tool_call>{\"name\":\"spawn_subagent\",\"arguments\":{\"role\":\"researcher\",\"goal\":\"X\"}}</tool_call>\n";
        let calls = scan_content_for_tool_calls(content);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["function"]["name"], "spawn_subagent");
    }
}

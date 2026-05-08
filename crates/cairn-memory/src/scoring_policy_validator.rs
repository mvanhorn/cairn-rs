//! RFC 029 PR-B2: validate a proposed `ScoringPolicy` against the
//! resolved provider's surfaced dimensions.
//!
//! A policy is rejected when it assigns a *non-zero* weight to a
//! provider-required dimension the resolved provider declared
//! `not_supported`. The three runtime-owned dimensions
//! (graph_proximity, source_credibility, corroboration) are always
//! available — cairn computes them post-hoc regardless of provider —
//! so weights on those dims are never rejected.
//!
//! Per RFC: "Scoring-policy validation at `PUT /v1/projects/:id/
//! scoring-policy` time — rejects writes referencing unavailable
//! dimensions".
//!
//! The caller supplies the resolved provider snapshot
//! (`ResolvedProviderSnapshot.scoring_dimensions_surfaced`). Policy
//! validation is pure — no I/O, no event emission here. The HTTP
//! handler does the snapshot resolution + policy storage.

use crate::retrieval::{ScoringPolicy, ScoringWeights};

/// Known scoring-dimension names, split into the two RFC 029 families.
pub const PROVIDER_REQUIRED_DIMS: &[&str] = &[
    "semantic_relevance",
    "lexical_relevance",
    "freshness_decay",
    "staleness_penalty",
    "recency_of_use",
];

/// Runtime-owned dimensions are unconditionally available — cairn
/// computes them post-hoc. A policy can always carry non-zero weights
/// on these.
pub const RUNTIME_OWNED_DIMS: &[&str] = &["graph_proximity", "source_credibility", "corroboration"];

/// Validation result.
#[derive(Debug)]
pub enum ScoringPolicyValidationError {
    /// Policy weights reference provider-required dimensions the
    /// resolved provider declared not supported. Carries the offending
    /// dimension names so the HTTP handler can surface them verbatim
    /// in the 400 response body.
    UnavailableDimensions {
        provider_id: String,
        unsupported: Vec<String>,
    },
}

impl std::fmt::Display for ScoringPolicyValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnavailableDimensions {
                provider_id,
                unsupported,
            } => write!(
                f,
                "scoring policy references dimension(s) not surfaced by provider {provider_id}: {}",
                unsupported.join(", ")
            ),
        }
    }
}

impl std::error::Error for ScoringPolicyValidationError {}

/// Validate `policy` against the set of provider-required dimensions
/// the resolved provider actually surfaces.
///
/// `provider_id` is surfaced in the error message so operator UI can
/// render the offending provider name — no other behaviour difference.
/// `surfaced_dims` is
/// `ResolvedProviderSnapshot.scoring_dimensions_surfaced` (the
/// handshake snapshot's surfaced dimension list for the project's
/// resolved provider).
///
/// Returns `Ok(())` if every provider-required dimension whose weight
/// is non-zero in `policy` is present in `surfaced_dims`. When the
/// resolved provider is `None` (runtime couldn't resolve — e.g. brand-
/// new project configured with a plugin that hasn't handshaked yet),
/// validation passes: operator intent should not be blocked by a
/// transient resolver failure.
pub fn validate_scoring_policy(
    policy: &ScoringPolicy,
    provider_id: Option<&str>,
    surfaced_dims: Option<&[String]>,
) -> Result<(), ScoringPolicyValidationError> {
    let (provider_id, surfaced) = match (provider_id, surfaced_dims) {
        (Some(id), Some(dims)) => (id, dims),
        _ => return Ok(()),
    };

    let surfaced_set: std::collections::HashSet<&str> =
        surfaced.iter().map(String::as_str).collect();

    let offenders: Vec<String> = weights_by_dim(&policy.weights)
        .into_iter()
        .filter_map(|(dim, weight)| {
            if !PROVIDER_REQUIRED_DIMS.contains(&dim) {
                return None;
            }
            if weight == 0.0 {
                return None;
            }
            if surfaced_set.contains(dim) {
                return None;
            }
            Some(dim.to_owned())
        })
        .collect();

    if offenders.is_empty() {
        Ok(())
    } else {
        Err(ScoringPolicyValidationError::UnavailableDimensions {
            provider_id: provider_id.to_owned(),
            unsupported: offenders,
        })
    }
}

/// Enumerate non-runtime-owned dimension → weight pairs so the
/// validator can walk them in a single loop. Keeps the dimension
/// names stable across `ScoringWeights` structural changes: renames
/// land here, not in every validator caller.
fn weights_by_dim(w: &ScoringWeights) -> Vec<(&'static str, f64)> {
    vec![
        ("semantic_relevance", w.semantic_weight),
        ("lexical_relevance", w.lexical_weight),
        ("freshness_decay", w.freshness_weight),
        ("staleness_penalty", w.staleness_weight),
        ("recency_of_use", w.recency_weight),
        // The three runtime-owned dims are listed for completeness but
        // excluded from rejection by the `PROVIDER_REQUIRED_DIMS` check
        // above.
        ("source_credibility", w.credibility_weight),
        ("corroboration", w.corroboration_weight),
        ("graph_proximity", w.graph_proximity_weight),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider_surfacing(dims: &[&str]) -> Vec<String> {
        dims.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn default_policy_with_fully_surfacing_provider_passes() {
        let policy = ScoringPolicy::default();
        let surfaced = provider_surfacing(PROVIDER_REQUIRED_DIMS);
        assert!(validate_scoring_policy(&policy, Some("cairn-default"), Some(&surfaced)).is_ok());
    }

    #[test]
    fn none_snapshot_passes_unconditionally() {
        let policy = ScoringPolicy::default();
        assert!(validate_scoring_policy(&policy, None, None).is_ok());
    }

    #[test]
    fn rejects_weight_on_unsurfaced_dimension() {
        // Provider surfaces only semantic_relevance.
        let policy = ScoringPolicy::default();
        let surfaced = provider_surfacing(&["semantic_relevance"]);
        match validate_scoring_policy(&policy, Some("plugin:bedrock-kb"), Some(&surfaced)) {
            Err(ScoringPolicyValidationError::UnavailableDimensions {
                provider_id,
                unsupported,
            }) => {
                assert_eq!(provider_id, "plugin:bedrock-kb");
                // Default policy carries non-zero weights on lexical,
                // freshness, staleness, recency — all of which the
                // single-dim provider doesn't surface.
                for missing in [
                    "lexical_relevance",
                    "freshness_decay",
                    "staleness_penalty",
                    "recency_of_use",
                ] {
                    assert!(
                        unsupported.iter().any(|d| d == missing),
                        "expected {missing} in unsupported, got {unsupported:?}"
                    );
                }
            }
            other => panic!("expected UnavailableDimensions, got {other:?}"),
        }
    }

    #[test]
    fn zero_weight_on_unsurfaced_dimension_is_ok() {
        let mut policy = ScoringPolicy::default();
        policy.weights.lexical_weight = 0.0;
        policy.weights.freshness_weight = 0.0;
        policy.weights.staleness_weight = 0.0;
        policy.weights.recency_weight = 0.0;
        let surfaced = provider_surfacing(&["semantic_relevance"]);
        assert!(
            validate_scoring_policy(&policy, Some("plugin:bedrock-kb"), Some(&surfaced)).is_ok()
        );
    }

    #[test]
    fn runtime_owned_weights_never_trigger_rejection() {
        // Only the three runtime-owned dims get weight; the five
        // provider-required dims have zero weights. Even with an empty
        // surfaced set, validation passes.
        let policy = ScoringPolicy {
            weights: ScoringWeights {
                semantic_weight: 0.0,
                lexical_weight: 0.0,
                freshness_weight: 0.0,
                staleness_weight: 0.0,
                credibility_weight: 0.5,
                corroboration_weight: 0.3,
                graph_proximity_weight: 0.2,
                recency_weight: 0.0,
            },
            ..ScoringPolicy::default()
        };
        let surfaced = provider_surfacing(&[]);
        assert!(validate_scoring_policy(&policy, Some("plugin:readonly"), Some(&surfaced)).is_ok());
    }
}

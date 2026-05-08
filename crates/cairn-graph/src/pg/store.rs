use async_trait::async_trait;
use sqlx::PgPool;

use crate::projections::{
    EdgeKind, GraphEdge, GraphNode, GraphProjection, GraphProjectionError, NodeKind,
};
use crate::queries::{
    GraphQuery, GraphQueryError, GraphQueryService, Subgraph, TraversalDirection,
};

/// Postgres-backed graph store implementing projection and query traits.
///
/// Stores nodes and edges in the shared cairn-store schema
/// (V012/V013 migrations).
pub struct PgGraphStore {
    pool: PgPool,
}

impl PgGraphStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl GraphProjection for PgGraphStore {
    async fn add_node(&self, node: GraphNode) -> Result<(), GraphProjectionError> {
        let kind_str = node_kind_str(node.kind);

        sqlx::query(
            "INSERT INTO graph_nodes (node_id, kind, created_at)
             VALUES ($1, $2, $3)
             ON CONFLICT (node_id) DO NOTHING",
        )
        .bind(&node.node_id)
        .bind(kind_str)
        .bind(node.created_at as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| GraphProjectionError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn add_edge(&self, edge: GraphEdge) -> Result<(), GraphProjectionError> {
        let kind_str = edge_kind_str(edge.kind);

        sqlx::query(
            "INSERT INTO graph_edges (source_node_id, target_node_id, kind, created_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (source_node_id, target_node_id, kind) DO NOTHING",
        )
        .bind(&edge.source_node_id)
        .bind(&edge.target_node_id)
        .bind(kind_str)
        .bind(edge.created_at as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| GraphProjectionError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn node_exists(&self, node_id: &str) -> Result<bool, GraphProjectionError> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM graph_nodes WHERE node_id = $1")
            .bind(node_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| GraphProjectionError::StorageError(e.to_string()))?;

        Ok(row.is_some())
    }
}

#[async_trait]
impl GraphQueryService for PgGraphStore {
    async fn query(&self, query: GraphQuery) -> Result<Subgraph, GraphQueryError> {
        match query {
            GraphQuery::ExecutionTrace {
                root_node_id,
                max_depth,
                ..
            } => self.traverse_downstream(&root_node_id, max_depth).await,
            GraphQuery::DependencyPath {
                node_id,
                direction,
                max_depth,
            } => match direction {
                TraversalDirection::Upstream => self.traverse_upstream(&node_id, max_depth).await,
                TraversalDirection::Downstream => {
                    self.traverse_downstream(&node_id, max_depth).await
                }
            },
            GraphQuery::PromptProvenance { outcome_node_id } => {
                self.traverse_upstream(&outcome_node_id, 10).await
            }
            GraphQuery::RetrievalProvenance { answer_node_id } => {
                self.traverse_upstream(&answer_node_id, 5).await
            }
            GraphQuery::DecisionInvolvement { decision_node_id } => {
                self.traverse_upstream(&decision_node_id, 5).await
            }
            GraphQuery::EvalLineage { eval_run_node_id } => {
                self.traverse_upstream(&eval_run_node_id, 10).await
            }
            // MultiHop requires edge-confidence filtering, but V013 graph_edges
            // has no confidence column yet. Return Internal until the schema
            // migration lands. The in-memory backend supports it today.
            GraphQuery::MultiHop { .. } => Err(GraphQueryError::Internal(
                "MultiHop traversal is not yet supported by the Postgres graph backend \
                 (edge confidence column pending schema migration)"
                    .to_string(),
            )),
        }
    }

    async fn neighbors(
        &self,
        node_id: &str,
        edge_filter: Option<EdgeKind>,
        direction: TraversalDirection,
        limit: usize,
    ) -> Result<Vec<(GraphEdge, GraphNode)>, GraphQueryError> {
        let (edge_col, join_col) = match direction {
            TraversalDirection::Downstream => ("source_node_id", "target_node_id"),
            TraversalDirection::Upstream => ("target_node_id", "source_node_id"),
        };

        let (edges, nodes) = if let Some(kind) = edge_filter {
            let kind_str = edge_kind_str(kind);
            let sql = format!(
                "SELECT e.source_node_id, e.target_node_id, e.kind, e.created_at,
                        n.node_id, n.kind AS node_kind, n.created_at AS node_created_at
                 FROM graph_edges e
                 JOIN graph_nodes n ON n.node_id = e.{join_col}
                 WHERE e.{edge_col} = $1 AND e.kind = $2
                 LIMIT $3"
            );
            fetch_neighbor_rows(&self.pool, &sql, node_id, Some(kind_str), limit).await?
        } else {
            let sql = format!(
                "SELECT e.source_node_id, e.target_node_id, e.kind, e.created_at,
                        n.node_id, n.kind AS node_kind, n.created_at AS node_created_at
                 FROM graph_edges e
                 JOIN graph_nodes n ON n.node_id = e.{join_col}
                 WHERE e.{edge_col} = $1
                 LIMIT $2"
            );
            fetch_neighbor_rows(&self.pool, &sql, node_id, None, limit).await?
        };

        Ok(edges.into_iter().zip(nodes).collect())
    }

    async fn find_edges_by_source(
        &self,
        source_node_id: &str,
        edge_filter: Option<EdgeKind>,
        limit: usize,
    ) -> Result<Vec<GraphEdge>, GraphQueryError> {
        let rows: Vec<EdgeRow> = if let Some(kind) = edge_filter {
            sqlx::query_as(
                "SELECT source_node_id, target_node_id, kind, created_at
                 FROM graph_edges
                 WHERE source_node_id = $1 AND kind = $2
                 LIMIT $3",
            )
            .bind(source_node_id)
            .bind(edge_kind_str(kind))
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as(
                "SELECT source_node_id, target_node_id, kind, created_at
                 FROM graph_edges
                 WHERE source_node_id = $1
                 LIMIT $2",
            )
            .bind(source_node_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| GraphQueryError::StorageError(e.to_string()))?;

        Ok(rows.into_iter().map(|r| r.into_graph_edge()).collect())
    }

    async fn find_edges_by_target(
        &self,
        target_node_id: &str,
        edge_filter: Option<EdgeKind>,
        limit: usize,
    ) -> Result<Vec<GraphEdge>, GraphQueryError> {
        let rows: Vec<EdgeRow> = if let Some(kind) = edge_filter {
            sqlx::query_as(
                "SELECT source_node_id, target_node_id, kind, created_at
                 FROM graph_edges
                 WHERE target_node_id = $1 AND kind = $2
                 LIMIT $3",
            )
            .bind(target_node_id)
            .bind(edge_kind_str(kind))
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as(
                "SELECT source_node_id, target_node_id, kind, created_at
                 FROM graph_edges
                 WHERE target_node_id = $1
                 LIMIT $2",
            )
            .bind(target_node_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| GraphQueryError::StorageError(e.to_string()))?;

        Ok(rows.into_iter().map(|r| r.into_graph_edge()).collect())
    }

    async fn shortest_path(
        &self,
        from_node_id: &str,
        to_node_id: &str,
        edge_filter: Option<EdgeKind>,
        max_depth: u32,
    ) -> Result<Option<Subgraph>, GraphQueryError> {
        // Trivial case: same node.
        if from_node_id == to_node_id {
            if let Some(node) = fetch_node(&self.pool, from_node_id).await? {
                return Ok(Some(Subgraph {
                    nodes: vec![node],
                    edges: vec![],
                }));
            }
            return Ok(None);
        }

        // Layered BFS — matches the in-memory semantics: edges are treated
        // as **undirected** so a query with `from=A, to=B` finds a path via
        // either `A -> … -> B` or `A <- … <- B`. Using `source_node_id OR
        // target_node_id` mirrors the in-memory helper `bfs_shortest_path`
        // which walks either side of every edge.
        //
        // One SQL query per BFS layer (not per node): fetch every edge
        // whose source *or* target is in the current frontier set in a
        // single `= ANY($1)` query. On path hit, batch-fetch every node
        // on the reconstructed path with a second `= ANY($1)` query.
        // That keeps the query count at O(depth + 1) instead of O(V+E).
        use std::collections::{HashMap, HashSet};

        let mut parent: HashMap<String, (String, GraphEdge)> = HashMap::new();
        let mut visited: HashSet<String> = HashSet::new();
        visited.insert(from_node_id.to_owned());

        let mut frontier: Vec<String> = vec![from_node_id.to_owned()];
        let filter_kind_str = edge_filter.map(edge_kind_str);
        let target = to_node_id.to_owned();

        for _depth in 0..max_depth {
            if frontier.is_empty() {
                break;
            }

            // Fetch every edge with either endpoint in the frontier.
            let edges: Vec<EdgeRow> = if let Some(k) = filter_kind_str {
                sqlx::query_as(
                    "SELECT source_node_id, target_node_id, kind, created_at
                     FROM graph_edges
                     WHERE (source_node_id = ANY($1) OR target_node_id = ANY($1))
                       AND kind = $2",
                )
                .bind(&frontier)
                .bind(k)
                .fetch_all(&self.pool)
                .await
            } else {
                sqlx::query_as(
                    "SELECT source_node_id, target_node_id, kind, created_at
                     FROM graph_edges
                     WHERE source_node_id = ANY($1) OR target_node_id = ANY($1)",
                )
                .bind(&frontier)
                .fetch_all(&self.pool)
                .await
            }
            .map_err(|e| GraphQueryError::StorageError(e.to_string()))?;

            let frontier_set: HashSet<&String> = frontier.iter().collect();
            let mut next_frontier: Vec<String> = Vec::new();

            for row in edges {
                // Figure out which endpoint is the "current" side and
                // which is the "neighbour". Both-in-frontier is fine —
                // deterministic tie-break to source keeps the parent
                // pointer well-defined.
                let (current, neighbour) = if frontier_set.contains(&row.source_node_id) {
                    (row.source_node_id.clone(), row.target_node_id.clone())
                } else if frontier_set.contains(&row.target_node_id) {
                    (row.target_node_id.clone(), row.source_node_id.clone())
                } else {
                    // Shouldn't happen given the WHERE clause, but handle
                    // it gracefully rather than trusting the query.
                    continue;
                };

                if !visited.insert(neighbour.clone()) {
                    continue;
                }

                let edge = row.into_graph_edge();
                parent.insert(neighbour.clone(), (current, edge));

                if neighbour == target {
                    // Reconstruct path from target back to source.
                    let mut path_edges: Vec<GraphEdge> = Vec::new();
                    let mut path_node_ids: Vec<String> = vec![neighbour.clone()];
                    let mut cursor = neighbour.clone();
                    while let Some((prev, edge)) = parent.remove(&cursor) {
                        path_edges.push(edge);
                        path_node_ids.push(prev.clone());
                        cursor = prev;
                    }
                    path_edges.reverse();
                    path_node_ids.reverse();

                    // Batch-fetch every node on the path in a single query.
                    let node_rows: Vec<NodeRow> = sqlx::query_as(
                        "SELECT node_id, kind, created_at
                         FROM graph_nodes WHERE node_id = ANY($1)",
                    )
                    .bind(&path_node_ids)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| GraphQueryError::StorageError(e.to_string()))?;

                    // Preserve path order (the SELECT doesn't guarantee it).
                    let by_id: HashMap<String, GraphNode> = node_rows
                        .into_iter()
                        .map(|r| (r.node_id.clone(), r.into_graph_node()))
                        .collect();
                    let path_nodes: Vec<GraphNode> = path_node_ids
                        .iter()
                        .filter_map(|nid| by_id.get(nid).cloned())
                        .collect();

                    return Ok(Some(Subgraph {
                        nodes: path_nodes,
                        edges: path_edges,
                    }));
                }

                next_frontier.push(neighbour);
            }

            frontier = next_frontier;
        }

        Ok(None)
    }

    /// RFC 029 PR-B2: batched neighbor lookup. One `WHERE source_node_id
    /// = ANY($1) OR target_node_id = ANY($1)` query returns every
    /// adjacent edge for the entire input batch in a single round-trip;
    /// results are then regrouped per input id so the caller sees the
    /// same `(id, Vec<GraphEdge>)` shape the default impl provides.
    /// Self-loops appear once in the owning bucket (the default impl
    /// elsewhere preserves this invariant).
    async fn multi_neighbors(
        &self,
        node_ids: &[String],
    ) -> Result<Vec<(String, Vec<GraphEdge>)>, GraphQueryError> {
        use std::collections::{HashMap, HashSet};

        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows: Vec<EdgeRow> = sqlx::query_as(
            "SELECT source_node_id, target_node_id, kind, created_at
               FROM graph_edges
              WHERE source_node_id = ANY($1) OR target_node_id = ANY($1)",
        )
        .bind(node_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| GraphQueryError::StorageError(e.to_string()))?;

        let wanted: HashSet<&str> = node_ids.iter().map(String::as_str).collect();
        let mut per_id: HashMap<String, Vec<GraphEdge>> =
            node_ids.iter().map(|id| (id.clone(), Vec::new())).collect();

        for row in rows {
            let edge = row.into_graph_edge();
            let src_wanted = wanted.contains(edge.source_node_id.as_str());
            let tgt_wanted = wanted.contains(edge.target_node_id.as_str());
            if src_wanted {
                if let Some(bucket) = per_id.get_mut(&edge.source_node_id) {
                    bucket.push(edge.clone());
                }
            }
            if tgt_wanted && edge.source_node_id != edge.target_node_id {
                if let Some(bucket) = per_id.get_mut(&edge.target_node_id) {
                    bucket.push(edge.clone());
                }
            }
        }

        // `.get().cloned()` rather than `.remove()` so duplicate input
        // ids still map to the same edge list (remove would return
        // `None` on the second occurrence).
        Ok(node_ids
            .iter()
            .map(|id| (id.clone(), per_id.get(id).cloned().unwrap_or_default()))
            .collect())
    }
}

impl PgGraphStore {
    /// BFS traversal downstream from a root node.
    async fn traverse_downstream(
        &self,
        root_id: &str,
        max_depth: u32,
    ) -> Result<Subgraph, GraphQueryError> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut frontier = vec![root_id.to_owned()];
        let mut visited = std::collections::HashSet::new();

        for _depth in 0..max_depth {
            if frontier.is_empty() {
                break;
            }

            let mut next_frontier = Vec::new();

            for node_id in &frontier {
                if !visited.insert(node_id.clone()) {
                    continue;
                }

                // Fetch the node itself.
                if let Some(node) = fetch_node(&self.pool, node_id).await? {
                    nodes.push(node);
                }

                // Fetch outgoing edges.
                let out_edges = sqlx::query_as::<_, EdgeRow>(
                    "SELECT source_node_id, target_node_id, kind, created_at
                     FROM graph_edges WHERE source_node_id = $1",
                )
                .bind(node_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| GraphQueryError::StorageError(e.to_string()))?;

                for row in out_edges {
                    next_frontier.push(row.target_node_id.clone());
                    edges.push(row.into_graph_edge());
                }
            }

            frontier = next_frontier;
        }

        Ok(Subgraph { nodes, edges })
    }

    /// BFS traversal upstream from a leaf node.
    async fn traverse_upstream(
        &self,
        leaf_id: &str,
        max_depth: u32,
    ) -> Result<Subgraph, GraphQueryError> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut frontier = vec![leaf_id.to_owned()];
        let mut visited = std::collections::HashSet::new();

        for _depth in 0..max_depth {
            if frontier.is_empty() {
                break;
            }

            let mut next_frontier = Vec::new();

            for node_id in &frontier {
                if !visited.insert(node_id.clone()) {
                    continue;
                }

                if let Some(node) = fetch_node(&self.pool, node_id).await? {
                    nodes.push(node);
                }

                let in_edges = sqlx::query_as::<_, EdgeRow>(
                    "SELECT source_node_id, target_node_id, kind, created_at
                     FROM graph_edges WHERE target_node_id = $1",
                )
                .bind(node_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| GraphQueryError::StorageError(e.to_string()))?;

                for row in in_edges {
                    next_frontier.push(row.source_node_id.clone());
                    edges.push(row.into_graph_edge());
                }
            }

            frontier = next_frontier;
        }

        Ok(Subgraph { nodes, edges })
    }
}

// --- Row types and helpers ---

#[derive(sqlx::FromRow)]
struct NodeRow {
    node_id: String,
    kind: String,
    created_at: i64,
}

impl NodeRow {
    fn into_graph_node(self) -> GraphNode {
        GraphNode {
            node_id: self.node_id,
            kind: parse_node_kind(&self.kind).unwrap_or(NodeKind::Session),
            project: None,
            created_at: self.created_at as u64,
        }
    }
}

#[derive(sqlx::FromRow)]
struct EdgeRow {
    source_node_id: String,
    target_node_id: String,
    kind: String,
    created_at: i64,
}

impl EdgeRow {
    fn into_graph_edge(self) -> GraphEdge {
        GraphEdge {
            source_node_id: self.source_node_id,
            target_node_id: self.target_node_id,
            kind: parse_edge_kind(&self.kind).unwrap_or(EdgeKind::Triggered),
            created_at: self.created_at as u64,
            confidence: None,
        }
    }
}

async fn fetch_node(pool: &PgPool, node_id: &str) -> Result<Option<GraphNode>, GraphQueryError> {
    let row: Option<NodeRow> =
        sqlx::query_as("SELECT node_id, kind, created_at FROM graph_nodes WHERE node_id = $1")
            .bind(node_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| GraphQueryError::StorageError(e.to_string()))?;

    Ok(row.map(|r| r.into_graph_node()))
}

async fn fetch_neighbor_rows(
    pool: &PgPool,
    sql: &str,
    node_id: &str,
    kind_filter: Option<&str>,
    limit: usize,
) -> Result<(Vec<GraphEdge>, Vec<GraphNode>), GraphQueryError> {
    #[derive(sqlx::FromRow)]
    struct NeighborRow {
        source_node_id: String,
        target_node_id: String,
        kind: String,
        created_at: i64,
        node_id: String,
        node_kind: String,
        node_created_at: i64,
    }

    let rows: Vec<NeighborRow> = if let Some(k) = kind_filter {
        sqlx::query_as(sql)
            .bind(node_id)
            .bind(k)
            .bind(limit as i64)
            .fetch_all(pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(node_id)
            .bind(limit as i64)
            .fetch_all(pool)
            .await
    }
    .map_err(|e| GraphQueryError::StorageError(e.to_string()))?;

    let mut edges = Vec::with_capacity(rows.len());
    let mut nodes = Vec::with_capacity(rows.len());

    for r in rows {
        edges.push(GraphEdge {
            source_node_id: r.source_node_id,
            target_node_id: r.target_node_id,
            kind: parse_edge_kind(&r.kind).unwrap_or(EdgeKind::Triggered),
            created_at: r.created_at as u64,
            confidence: None,
        });
        nodes.push(GraphNode {
            node_id: r.node_id,
            kind: parse_node_kind(&r.node_kind).unwrap_or(NodeKind::Session),
            project: None,
            created_at: r.node_created_at as u64,
        });
    }

    Ok((edges, nodes))
}

fn node_kind_str(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Session => "session",
        NodeKind::Run => "run",
        NodeKind::Task => "task",
        NodeKind::Approval => "approval",
        NodeKind::Checkpoint => "checkpoint",
        NodeKind::Trigger => "trigger",
        NodeKind::MailboxMessage => "mailbox_message",
        NodeKind::ToolInvocation => "tool_invocation",
        NodeKind::Memory => "memory",
        NodeKind::Document => "document",
        NodeKind::Chunk => "chunk",
        NodeKind::Source => "source",
        NodeKind::PromptAsset => "prompt_asset",
        NodeKind::PromptVersion => "prompt_version",
        NodeKind::PromptRelease => "prompt_release",
        NodeKind::EvalRun => "eval_run",
        NodeKind::Skill => "skill",
        NodeKind::ChannelTarget => "channel_target",
        NodeKind::Signal => "signal",
        NodeKind::IngestJob => "ingest_job",
        NodeKind::RouteDecision => "route_decision",
        NodeKind::ProviderCall => "provider_call",
    }
}

fn parse_node_kind(s: &str) -> Option<NodeKind> {
    match s {
        "session" => Some(NodeKind::Session),
        "run" => Some(NodeKind::Run),
        "task" => Some(NodeKind::Task),
        "approval" => Some(NodeKind::Approval),
        "checkpoint" => Some(NodeKind::Checkpoint),
        "trigger" => Some(NodeKind::Trigger),
        "mailbox_message" => Some(NodeKind::MailboxMessage),
        "tool_invocation" => Some(NodeKind::ToolInvocation),
        "memory" => Some(NodeKind::Memory),
        "document" => Some(NodeKind::Document),
        "chunk" => Some(NodeKind::Chunk),
        "source" => Some(NodeKind::Source),
        "prompt_asset" => Some(NodeKind::PromptAsset),
        "prompt_version" => Some(NodeKind::PromptVersion),
        "prompt_release" => Some(NodeKind::PromptRelease),
        "eval_run" => Some(NodeKind::EvalRun),
        "skill" => Some(NodeKind::Skill),
        "channel_target" => Some(NodeKind::ChannelTarget),
        "signal" => Some(NodeKind::Signal),
        "ingest_job" => Some(NodeKind::IngestJob),
        "route_decision" => Some(NodeKind::RouteDecision),
        "provider_call" => Some(NodeKind::ProviderCall),
        _ => None,
    }
}

fn edge_kind_str(kind: EdgeKind) -> &'static str {
    match kind {
        EdgeKind::Triggered => "triggered",
        EdgeKind::MatchedBy => "matched_by",
        EdgeKind::Fired => "fired",
        EdgeKind::Spawned => "spawned",
        EdgeKind::DependedOn => "depended_on",
        EdgeKind::ApprovedBy => "approved_by",
        EdgeKind::ResumedFrom => "resumed_from",
        EdgeKind::SentTo => "sent_to",
        EdgeKind::ReadFrom => "read_from",
        EdgeKind::Cited => "cited",
        EdgeKind::DerivedFrom => "derived_from",
        EdgeKind::EmbeddedAs => "embedded_as",
        EdgeKind::EvaluatedBy => "evaluated_by",
        EdgeKind::ReleasedAs => "released_as",
        EdgeKind::RolledBackTo => "rolled_back_to",
        EdgeKind::RoutedTo => "routed_to",
        EdgeKind::UsedPrompt => "used_prompt",
        EdgeKind::UsedTool => "used_tool",
        EdgeKind::CalledProvider => "called_provider",
    }
}

fn parse_edge_kind(s: &str) -> Option<EdgeKind> {
    match s {
        "triggered" => Some(EdgeKind::Triggered),
        "matched_by" => Some(EdgeKind::MatchedBy),
        "fired" => Some(EdgeKind::Fired),
        "spawned" => Some(EdgeKind::Spawned),
        "depended_on" => Some(EdgeKind::DependedOn),
        "approved_by" => Some(EdgeKind::ApprovedBy),
        "resumed_from" => Some(EdgeKind::ResumedFrom),
        "sent_to" => Some(EdgeKind::SentTo),
        "read_from" => Some(EdgeKind::ReadFrom),
        "cited" => Some(EdgeKind::Cited),
        "derived_from" => Some(EdgeKind::DerivedFrom),
        "embedded_as" => Some(EdgeKind::EmbeddedAs),
        "evaluated_by" => Some(EdgeKind::EvaluatedBy),
        "released_as" => Some(EdgeKind::ReleasedAs),
        "rolled_back_to" => Some(EdgeKind::RolledBackTo),
        "routed_to" => Some(EdgeKind::RoutedTo),
        "used_prompt" => Some(EdgeKind::UsedPrompt),
        "used_tool" => Some(EdgeKind::UsedTool),
        "called_provider" => Some(EdgeKind::CalledProvider),
        _ => None,
    }
}

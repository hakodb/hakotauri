//! The single-command bridge between a Tauri frontend and HakoDB.
//!
//! # How it works
//!
//! The frontend speaks to exactly one Tauri command, [`hako_exec`], sending a
//! [`HakoOp`] (JSON, snake_case) and receiving a [`HakoResponse`] as
//! MessagePack bytes. One command keeps the IPC surface small: new database
//! features arrive as new `HakoOp` variants, never as new commands.
//!
//! # Threading
//!
//! The engine is synchronous, so [`hako_exec`] runs every op on a
//! `spawn_blocking` worker and subscriptions on a dedicated blocking loop per
//! listener. Tauri events (`window.emit`) carry MessagePack bytes, matching
//! the request path.
//!
//! # Conventions developers should know
//!
//! - **snake_case wire**: `HakoOp` uses `#[serde(tag = "op",
//!   rename_all = "snake_case")]`; flags a client omits default to `false`
//!   (eager blobs, replicated writes). The `casing_tests` module locks this.
//! - **ids ride along**: query rows and point reads inject the doc id as an
//!   `"id"` field in the returned JSON — storage rows don't carry it.
//! - **JSON `<->` Value**: request data crosses as JSON and is converted to
//!   engine [`Value`](hakodb::document::value::Value); responses convert
//!   back, with `Binary` as a number array, references as `{"__ref__":
//!   "col/id"}`, and unresolved blobs as `{"__blob__": {offset, len}}`.
//! - **raw bytes are opaque**: [`HakoOp::QueryRaw`] rows cross as storage
//!   bytes the client must never hand-build; [`HakoOp::DecodeRaw`] turns
//!   them back into documents server-side.

use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tauri::{command, Emitter, Runtime, State, Window};

use hakodb::config::DurabilityMode;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{AuditEntry, BatchMutation, ChangeKind, Hako};
use hakodb::error::HakoError;
use hakodb::index::composite::definition::SortDirection;
use hakodb::index::vector::{decode_f32s, distance, Metric};
use hakodb::query::filter::{Filter, Operator};
use hakodb::query::query::{AggregateOp, Query};
use hakodb::util::clock::unix_millis;

/// Encodes any response as MessagePack bytes for the trip back to the
/// frontend. Raw row bytes inside stay `bin` (Uint8Array on the TS side).
fn to_binary_payload<S: serde::Serialize>(val: &S) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(val).map_err(|e| e.to_string())
}

/// Every operation the frontend can ask for, as one tagged enum.
///
/// Serialization is the wire contract: `{"op": "query", ...}` with
/// snake_case fields. Fields a client omits fall back to serde defaults
/// (empty filters, `defer_blobs: false`, `local_only: false), so old
/// clients keep working when new flags are added — see `casing_tests`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum HakoOp {
    Get { collection: String, doc_id: String },
    Set { collection: String, doc_id: String, data: serde_json::Value },
    Patch { collection: String, doc_id: String, data: serde_json::Value },
    Delete {
        collection: String,
        doc_id: String,
        /// When true the tombstone stays on this device (never replicates).
        #[serde(default)]
        local_only: bool,
        /// Hard delete: skip the soft-delete retention/restore window.
        #[serde(default)]
        force: bool,
    },
    /// Restore a soft-deleted doc from its retention record (fresh version).
    Restore {
        collection: String,
        doc_id: String,
    },
    /// List restoreable soft-deleted docs; omitted collection lists all.
    RestoreAvailable {
        /// Omitted by old clients -> every collection.
        #[serde(default)]
        collection: Option<String>,
    },
    /// List soft-deleted docs due for the sweeper purge.
    DeleteQueue {
        /// Omitted by old clients -> every collection.
        #[serde(default)]
        collection: Option<String>,
    },
    /// Run one sweeper pass now; returns the purged record count.
    Sweep,
    Vacuum { collection: String },
    CreateIndex { collection: String, field: String },
    CreateFtsIndex { collection: String, field: String },
    CreateCompositeIndex { collection: String, fields: Vec<CompositeFieldInput> },
    CreateVectorIndex {
        collection: String,
        field: String,
        dim: u32,
        /// Omitted by old clients -> cosine (the RAG default).
        #[serde(default)]
        metric: VectorMetric,
        /// HNSW knobs (#141-1), all optional: omitted/0 ride the per-dim
        /// derived defaults. Persist in the index definition, survive reopen.
        #[serde(default)]
        m: Option<usize>,
        #[serde(default)]
        ef_construction: Option<usize>,
        #[serde(default)]
        ef_search: Option<usize>,
        /// Popcount stage-1 (#190); omitted by old clients -> int8 stage-1.
        #[serde(default)]
        binary_stage1: bool,
    },
    CreateTraverseIndex {
        collection: String,
    },
    /// Phase B: register an edge collection (tracks + `_from`/`_to`).
    CreateEdgeIndex {
        collection: String,
    },
    FindPaths {
        collection: String,
        doc_id: String,
        /// Omitted by old clients -> outgoing.
        #[serde(default)]
        direction: TraverseDirection,
        /// Omitted -> any ref field.
        edge: Option<String>,
        /// Omitted by old clients -> 10.
        #[serde(default = "default_traverse_depth")]
        max_depth: usize,
        /// Omitted by old clients -> 100.
        #[serde(default = "default_traverse_k")]
        k: usize,
        /// Edge collection (docs with _from/_to join the walk).
        via: Option<String>,
        /// Numeric prop on edge docs for Dijkstra ordering.
        weight: Option<String>,
        /// Node predicate: prune-at-expansion (spec.node_match).
        #[serde(default)]
        node_match: Option<PropMatch>,
        /// Edge-collection relationship routing: ("type", value)-style.
        #[serde(default)]
        edge_type: Option<PropMatch>,
        /// Edge-collection property filter (ANDs with edge_type).
        #[serde(default)]
        edge_match: Option<PropMatch>,
        /// Route projections are opt-in (0.19.5): false keeps the v0.19.3
        /// engine cost; true fills PathHit.path/via.
        #[serde(default)]
        include_path: bool,
        #[serde(default)]
        include_via: bool,
    },
    /// Cycle-safe simple-path enumeration: like FindPaths but never
    /// revisits a node (k-shortest paths without cycles).
    FindSimplePaths {
        collection: String,
        doc_id: String,
        #[serde(default)]
        direction: TraverseDirection,
        edge: Option<String>,
        #[serde(default = "default_traverse_depth")]
        max_depth: usize,
        #[serde(default = "default_traverse_k")]
        k: usize,
        via: Option<String>,
        weight: Option<String>,
        #[serde(default)]
        node_match: Option<PropMatch>,
        #[serde(default)]
        edge_type: Option<PropMatch>,
        #[serde(default)]
        edge_match: Option<PropMatch>,
        #[serde(default)]
        include_path: bool,
        #[serde(default)]
        include_via: bool,
    },
    FindNear {
        collection: String,
        field: String,
        query: Vec<f32>,
        /// Omitted by old clients -> 10.
        #[serde(default = "default_near_k")]
        k: usize,
        /// Distance cutoff (#141-11): keep only hits at or under the bound.
        #[serde(default)]
        max_distance: Option<f64>,
        /// Hybrid search (#141): extra filters ANDed with the near
        /// predicate — the planner ANN-pre-filters or over-fetches.
        #[serde(default)]
        filters: Vec<FilterInput>,
    },
    /// Batched KNN (#141-5): one readiness cycle for many query vectors.
    /// Results align 1:1 with `queries`, each nearest-first like FindNear.
    FindNearBatch {
        collection: String,
        field: String,
        queries: Vec<Vec<f32>>,
        #[serde(default = "default_near_k")]
        k: usize,
    },
    Query {
        collection: String,
        #[serde(default)]
        action: Option<QueryAction>,
        doc_id_filter: Option<String>,
        #[serde(default)]
        filters: Vec<FilterInput>,
        or_groups: Option<Vec<Vec<FilterInput>>>,
        order_by: Option<Vec<OrderByInput>>,
        limit: Option<usize>,
        offset: Option<usize>,
        projection: Option<Vec<String>>,
        start_at: Option<Vec<serde_json::Value>>,
        start_after: Option<Vec<serde_json::Value>>,
        end_at: Option<Vec<serde_json::Value>>,
        end_before: Option<Vec<serde_json::Value>>,
        // Opt-in per-query blob deferral: list views skip blob-file reads
        // and get `__blob__` placeholders instead.
        #[serde(default)]
        defer_blobs: bool,
        // Local-only scope for the Delete action (Fetch ignores it).
        #[serde(default)]
        local_only: bool,
    },
    /// Raw scan: pinned storage bytes per row, no decode, no JSON. Bytes
    /// cross msgpack as bin (Uint8Array on the TS side) and stay opaque
    /// there: hash/count/export them, or send selected rows back through
    /// DecodeRaw. Page with start_after: `[lastId]` under an id order; ids
    /// ride along in the clear.
    QueryRaw {
        collection: String,
        doc_id_filter: Option<String>,
        #[serde(default)]
        filters: Vec<FilterInput>,
        or_groups: Option<Vec<Vec<FilterInput>>>,
        order_by: Option<Vec<OrderByInput>>,
        limit: Option<usize>,
        offset: Option<usize>,
        start_at: Option<Vec<serde_json::Value>>,
        start_after: Option<Vec<serde_json::Value>>,
        end_at: Option<Vec<serde_json::Value>>,
        end_before: Option<Vec<serde_json::Value>>,
    },
    /// Lazy typed field pull: point view + one field, no decode, no JSON
    /// document. Scalars cross as JSON values; missing/wrong-type reads
    /// null. BlobLink fields surface their placeholder (resolve the doc
    /// via DecodeRaw when needed).
    ViewGetField {
        collection: String,
        doc_id: String,
        field: String,
    },
    /// Decode one raw row back into a Document (blobs inflated). The bytes
    /// must be an exact stored row (e.g. from QueryRaw) — never hand-built.
    DecodeRaw {
        collection: String,
        doc_id: String,
        bytes: Vec<u8>,
    },
    Batch { mutations: Vec<BatchInput> },
    Aggregate {
        collection: String,
        #[serde(default)]
        filters: Vec<FilterInput>,
        #[serde(default)]
        or_groups: Option<Vec<Vec<FilterInput>>>,
        kind: AggregateKind,
        field: Option<String>,
    },
    /// Cheap existence probe: no owned decode and no collection creation.
    Exists { collection: String, doc_id: String },
    Subscribe {
        listener_id: String,
        doc_id_filter: Option<String>,
        collection: String,
        #[serde(default)]
        filters: Vec<FilterInput>,
        or_groups: Option<Vec<Vec<FilterInput>>>,
        order_by: Option<Vec<OrderByInput>>,
        limit: Option<usize>,
        offset: Option<usize>,
        projection: Option<Vec<String>>,
        event_name: Option<String>,
        start_at: Option<Vec<serde_json::Value>>,
        start_after: Option<Vec<serde_json::Value>>,
        end_at: Option<Vec<serde_json::Value>>,
        end_before: Option<Vec<serde_json::Value>>,
    },
    Unsubscribe { listener_id: String },
    Backup { path: String },
    Compact,
    /// Compact one resident shard; absent/unloaded collections are a no-op.
    CompactCollection { collection: String },
    /// Flush WAL + segments to disk (close path, deterministic tests).
    Flush,
    /// Doc ids in a collection (key scans for seeding, archive passes,
    /// stats — no decode, no filters).
    ListStorageKeys { collection: String },
    GetStats,
    /// True once background index recovery finishes. Poll after open
    /// before cursor-paged queries (pre-readiness plans ignore bounds).
    IndexesReady,
    ListCollections,
    ListIndexes { collection: Option<String> },
    SnapshotIndices,
    GetAuditLog,
    SetDurability { mode: i32 },
    SetCompression { enabled: bool, level: i32 },
    SetGroupCommitInterval { ms: u64 },
    /// Move docs between collections (archive/restore): timestamp-
    /// preserving put at dst first, fresh tombstone at src. Returns
    /// moved ids + ids missing at src; refuses excluded sides.
    RelocateDocs { src: String, dst: String, ids: Vec<String> },
    /// Explicit load of one (usually lazy archive) collection.
    LoadCollection { collection: String },
    /// Explicit evict of one lazy collection (refuses non-lazy).
    UnloadCollection { collection: String },
    /// Archive-group collections on disk but not loaded.
    UnloadedCollections,
    /// Serializable read-modify-write with bounded retry: steps run in
    /// order inside one engine transaction; a read-set conflict retries
    /// the WHOLE step list on a fresh snapshot. Get steps collect their
    /// documents into the response (aligned, in step order).
    Transaction {
        steps: Vec<TxnStep>,
        /// Omitted by old clients -> 3 (engine default attempts).
        #[serde(default = "default_tx_attempts")]
        max_attempts: u32,
    },
    /// Multi-stage pipeline over one collection: match replaces the row
    /// set, every other stage transforms it (seeding from a full scan
    /// when it runs before any match).
    Pipeline {
        collection: String,
        stages: Vec<StageInput>,
    },
    /// Drop one index definition (and its structures). Kind selects the
    /// manager surface: simple/fts/vector take `field`, composite takes
    /// `id` (from ListIndexes), traverse/edge take the collection only.
    DropIndex {
        kind: DropIndexKind,
        collection: String,
        #[serde(default)]
        field: Option<String>,
        #[serde(default)]
        id: Option<u32>,
    },
    /// Whole-graph algorithms (0.19.4): connected components, one directed
    /// cycle, topological sort, degree centrality — over the spec's
    /// adjacency. Result rides ValueResult (arrays of [collection, id]).
    GraphAnalysis {
        collection: String,
        kind: GraphAnalysisKind,
        #[serde(default)]
        direction: TraverseDirection,
        #[serde(default)]
        edge: Option<String>,
        #[serde(default)]
        via: Option<String>,
        /// Optional node predicate limiting the graph's node set.
        #[serde(default)]
        node_match: Option<PropMatch>,
    },
    /// Export one collection's graph as viz-ready JSON (nodes + edges).
    ExportGraph {
        collection: String,
        #[serde(default)]
        direction: TraverseDirection,
        #[serde(default)]
        edge: Option<String>,
        #[serde(default)]
        via: Option<String>,
    },
    /// Personalized PageRank (#196): teleport mass `1 - damping` on the
    /// seed set; omitted/empty seeds = classic PageRank. damping 0 rides
    /// the engine default (0.85); top 0 = all nodes. Rows {node, score}
    /// score desc then node asc.
    GraphPagerank {
        collection: String,
        /// Seeds as `["col/id", ...]`.
        #[serde(default)]
        seeds: Option<Vec<String>>,
        #[serde(default)]
        direction: TraverseDirection,
        #[serde(default)]
        edge: Option<String>,
        #[serde(default)]
        via: Option<String>,
        #[serde(default)]
        node_match: Option<PropMatch>,
        #[serde(default)]
        damping: f64,
        #[serde(default)]
        top: usize,
    },
    /// Seed-scoped N-hop subgraph (#198): the export-graph node/edge
    /// shape scoped to one seed. Missing seed = empty graph.
    Subgraph {
        collection: String,
        seed_id: String,
        #[serde(default = "default_traverse_depth")]
        max_depth: usize,
        #[serde(default)]
        direction: TraverseDirection,
        #[serde(default)]
        edge: Option<String>,
        #[serde(default)]
        via: Option<String>,
        #[serde(default)]
        node_match: Option<PropMatch>,
    },
    /// Hybrid retrieval (#193): FTS (sparse) + KNN (dense) fused with
    /// RRF. Rows {id, score (fused, desc), doc}; rrf_k 0 rides the
    /// engine default (60).
    HybridSearch {
        collection: String,
        text_field: String,
        text_query: String,
        vec_field: String,
        query: Vec<f32>,
        k: usize,
        #[serde(default)]
        rrf_k: usize,
    },
    /// Point read with the storage version attached (optimistic
    /// concurrency on the client).
    GetVersioned { collection: String, doc_id: String },
    /// Resolve one `{"__ref__": "col/id"}` reference value to its doc.
    GetByRef { reference: serde_json::Value },
    /// Write one document under the subcollection namespace of a parent
    /// doc (`{col}/{id}/{subcollection}` storage prefix).
    PutSubdocument {
        collection: String,
        doc_id: String,
        subcollection: String,
        subdoc_id: String,
        data: serde_json::Value,
    },
    /// Rename a collection (storage prefix move; returns the new version).
    RenameCollection { from: String, to: String },
    /// Hard-drop a whole collection: tombstones every live doc, unlinks
    /// the shard directory. Idempotent (absent = empty report).
    DropCollection { collection: String },
    /// Un-withhold from replication: a doc_id scopes to one key,
    /// otherwise the whole collection's local-only marks clear.
    Replicate {
        collection: String,
        #[serde(default)]
        doc_id: Option<String>,
    },
    /// Operational visibility: what background work is outstanding
    /// (index recovery, pending index ops, backfills, blob bytes).
    Quiescence,
    /// Pin/unpin a collection to this instance only (never replicates).
    SetCollectionLocal { collection: String, local: bool },
    /// Whole-engine read-only toggle (rejects writes, serves reads).
    SetReadOnly { enabled: bool },
}

/// One raw row over the bridge: id in the clear, storage bytes as
/// msgpack bin. Bytes are opaque: decode server-side via DecodeRaw.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawRow {
    pub id: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HakoResponse {
    Ok,
    Document { data: Option<serde_json::Value> },
    QueryResult { rows: Vec<serde_json::Value> },
    /// Raw rows: ids in the clear, bytes as msgpack bin (Uint8Array).
    RawResult { rows: Vec<RawRow> },
    /// Single lazy field value (or null when missing/wrong-type).
    ValueResult { value: Option<serde_json::Value> },
    /// Index readiness probe.
    Ready { ready: bool },
    AggregateResult { value: Option<f64> },
    /// Cheap existence probe (Exists).
    Exists { exists: bool },
    SubscriptionAck { listener_id: String },
    Unsubscribed { listener_id: String },
    Collections { names: Vec<String> },
    /// Doc ids in a collection (ListStorageKeys).
    StorageKeys { ids: Vec<String> },
    Stats { details: serde_json::Value },
    Indexes { list: serde_json::Value },
    AuditLog { entries: Vec<AuditEntry> },
    /// Soft-delete retention records (restore_available / delete_queue).
    Records { list: serde_json::Value },
    BulkActionResult { count: usize },
    /// Relocate report: moved ids + ids missing at source.
    RelocateResult { moved: Vec<String>, missing: Vec<String> },
    /// KNN hits: nearest-first, exact score per hit (same `distance()` the
    /// executor rescore uses — the graph only prunes, never orders).
    NearResult { hits: Vec<NearHit> },
    /// Batched KNN: one result set per query vector, aligned 1:1.
    NearBatchResult { results: Vec<Vec<NearHit>> },
    /// Hybrid RRF hits: fused score desc (higher is better, unlike
    /// NearHit's distance).
    HybridResult { hits: Vec<HybridHit> },
    /// Graph BFS hits in walk order (collection + id: paths cross
    /// collections; depth = hops from the anchor).
    PathResult { hits: Vec<PathHit> },
    /// Transaction commit: documents captured by Get steps, aligned in
    /// step order (null = absent at snapshot).
    TransactionResult { docs: Vec<Option<serde_json::Value>> },
    /// Point read with its storage version (GetVersioned).
    DocumentVersion { data: Option<serde_json::Value>, version: u64 },
    /// Drop report: tombstoned live docs + reclaimed shard bytes.
    DropResult { docs_removed: u64, bytes_reclaimed: u64, collections_dropped: Vec<String> },
}

/// One path hit: where it lives, its id, BFS depth, and the doc itself
/// (with `"id"` injected, like every other row shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PathHit {
    pub collection: String,
    pub id: String,
    pub depth: u64,
    pub cost: f64,
    pub doc: serde_json::Value,
    /// Full discovery path start->hit, inclusive both ends (FindPaths /
    /// FindSimplePaths with include_path; absent otherwise).
    #[serde(default)]
    pub path: Option<Vec<PathNode>>,
    /// Edge doc per hop, positionally aligned with `path` hops
    /// (include_via; absent otherwise).
    #[serde(default)]
    pub via: Option<Vec<Option<String>>>,
}

/// One (collection, doc id) node on a traversal route.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathNode {
    pub collection: String,
    pub id: String,
}

/// One KNN hit: doc id, exact metric distance, and the doc itself (with
/// `"id"` injected, like every other row shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct NearHit {
    pub id: String,
    pub score: f64,
    pub doc: serde_json::Value,
}

/// Hybrid retrieval row (#193): fused RRF score, higher is better.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridHit {
    pub id: String,
    pub score: f64,
    pub doc: serde_json::Value,
}

/// Wire metric for vector indexes. Serializes as `"cosine"` / `"l2"`
/// (matches `Metric::from_u32`: 0 = cosine, 1 = l2 on the FFI side).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum VectorMetric {
    #[default]
    Cosine,
    L2,
}

impl VectorMetric {
    fn metric(self) -> Metric {
        match self {
            VectorMetric::Cosine => Metric::Cosine,
            VectorMetric::L2 => Metric::L2,
        }
    }
}

fn default_near_k() -> usize {
    10
}

/// Wire direction for graph BFS (mirrors engine TraverseDir).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TraverseDirection {
    #[default]
    Outgoing,
    Incoming,
    Both,
}

impl TraverseDirection {
    fn engine(self) -> hakodb::index::traverse::TraverseDir {
        match self {
            TraverseDirection::Outgoing => hakodb::index::traverse::TraverseDir::Outgoing,
            TraverseDirection::Incoming => hakodb::index::traverse::TraverseDir::Incoming,
            TraverseDirection::Both => hakodb::index::traverse::TraverseDir::Both,
        }
    }
}

fn default_traverse_depth() -> usize {
    10
}

fn default_traverse_k() -> usize {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FilterInput {
    pub field: String,
    pub op: FilterOperator,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OrderByInput {
    pub field: String,
    pub ascending: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BatchInput {
    pub mutation: BatchMutationKind,
    pub collection: String,
    pub doc_id: String,
    pub data: Option<serde_json::Value>,
    /// Local-only scope for Delete items (Set/Patch ignore it).
    #[serde(default)]
    pub local_only: bool,
    /// Hard-delete scope for Delete items (Set/Patch ignore it); soft is the default.
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchMutationKind { Set, Patch, Delete }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Eq, Ne, Gt, Gte, Lt, Lte, Match, MatchPrefix, Contains, StartsWith, In, NotIn, ArrayContains, ArrayContainsAny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateKind { Count, Sum, Avg, Min, Max }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CompositeFieldInput {
    pub field: String,
    #[serde(default)]
    pub desc: bool,
}

/// One (field, value) predicate for traversal routing/filtering:
/// node_match prunes at expansion, edge_type routes edge-collection
/// hops, edge_match filters edge properties (ANDs with edge_type).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropMatch {
    pub field: String,
    pub value: serde_json::Value,
}

/// One step inside a Transaction op, in execution order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnStep {
    pub kind: TxnStepKind,
    pub collection: String,
    pub doc_id: String,
    /// Set/Patch payload (ignored by Get/Delete).
    #[serde(default)]
    pub data: Option<serde_json::Value>,
    /// Delete only: true = legacy hard delete, false = soft (default).
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxnStepKind { Get, Set, Patch, Delete }

fn default_tx_attempts() -> u32 {
    3
}

/// One pipeline stage, wire-tagged ("match"/"project"/...). Group
/// accumulators mirror the Aggregate op's kinds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum StageInput {
    Match {
        #[serde(default)]
        filters: Vec<FilterInput>,
        #[serde(default)]
        or_groups: Option<Vec<Vec<FilterInput>>>,
        order_by: Option<Vec<OrderByInput>>,
        limit: Option<usize>,
        offset: Option<usize>,
        projection: Option<Vec<String>>,
        start_at: Option<Vec<serde_json::Value>>,
        start_after: Option<Vec<serde_json::Value>>,
        end_at: Option<Vec<serde_json::Value>>,
        end_before: Option<Vec<serde_json::Value>>,
        doc_id_filter: Option<String>,
    },
    Project { fields: Vec<String> },
    Sort { field: String, #[serde(default)] ascending: bool },
    Limit { n: usize },
    Offset { n: usize },
    Unwind { field: String },
    Group {
        key: Option<String>,
        accums: Vec<AccumInput>,
    },
}

/// One accumulator in a Group stage: `out` names the output field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccumInput {
    pub out: String,
    pub kind: AggregateKind,
    /// Ignored by count.
    #[serde(default)]
    pub field: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropIndexKind { Simple, Fts, Vector, Traverse, Edge, Composite }

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphAnalysisKind { Components, Cycle, Topo, Degrees }

/// Wire spec shared by FindPaths / FindSimplePaths.
#[derive(Debug, Clone)]
struct TraverseInput {
    direction: TraverseDirection,
    edge: Option<String>,
    max_depth: usize,
    k: usize,
    via: Option<String>,
    weight: Option<String>,
    node_match: Option<PropMatch>,
    edge_type: Option<PropMatch>,
    edge_match: Option<PropMatch>,
    include_path: bool,
    include_via: bool,
}

/// Shared handle the consumer manages with Tauri (`app.manage(...)`) and
/// every command below borrows via `State`. `db` is public so out-of-tree
/// crates (sync commands, admin tools) can reuse the same engine handle.
#[derive(Clone)]
pub struct HakoGateway {
    pub db: Arc<Hako>,
    subscriptions: Arc<Mutex<HashMap<String, SubscriptionEntry>>>,
}

/// What changed for one document inside a subscription delta.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaKind {
    /// Initial bootstrap snapshot.
    Full,
    /// Added or modified (and still matches the query).
    Update,
    /// Removed, or updated so it no longer matches the query.
    Delete,
}

/// One document change inside a [`DeltaPayload`].
#[derive(Debug, Clone, serde::Serialize)]
pub struct DocumentChange {
    pub kind: DeltaKind,
    pub doc_id: String,
    pub data: Option<serde_json::Value>,
}

/// The batch container emitted per subscription event: every change since
/// the last emit. The frontend merges these into its local cache
/// (see `onSnapshot` in `@hakodb/tauri`).
#[derive(Debug, Clone, serde::Serialize)]
pub struct DeltaPayload {
    pub listener_id: String,
    pub changes: Vec<DocumentChange>,
}

struct SubscriptionEntry {
    stop_tx: Sender<()>,
    window_label: String,
}

impl HakoGateway {
    pub fn new(db: Hako) -> Self {
        Self {
            db: Arc::new(db),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Drops every subscription owned by a closed window. Call it from the
    /// window's destroy hook so dead listeners stop waking the event loop.
    pub fn cleanup_window_subscriptions(&self, window_label: &str) {
        let mut subs = self.subscriptions.lock();

        let ids_to_remove: Vec<String> = subs
            .iter()
            .filter(|(_, entry)| entry.window_label == window_label)
            .map(|(id, _)| id.clone())
            .collect();

        for id in ids_to_remove {
            if let Some(entry) = subs.remove(&id) {
                // The loop below breaks on this signal.
                let _ = entry.stop_tx.send(());
            }
        }
    }

    pub fn unsubscribe(&self, listener_id: &str) {
        if let Some(entry) = self.subscriptions.lock().remove(listener_id) {
            let _ = entry.stop_tx.send(());
        }
    }

    /// Starts (or replaces) a live query: sends one `Full` bootstrap with
    /// the current rows, then streams `Update`/`Delete` deltas as storage
    /// events arrive. Matching runs on raw bytes without decoding
    /// (`plan_for_watch` + `matches_watch`), so the hot path only decodes
    /// rows that actually match.
    fn register_subscription<R: Runtime>(
        &self,
        window: Window<R>,
        listener_id: String,
        query_template: QueryInput,
        event_name: String,
    ) -> Result<(), String> {
        self.unsubscribe(&listener_id);

        let rx = self.db.watch_collection(&query_template.collection);
        let (stop_tx, stop_rx) = mpsc::channel::<()>();
        
        self.subscriptions.lock().insert(
            listener_id.clone(),
            SubscriptionEntry { stop_tx, window_label: window.label().to_string() },
        );

        let db = Arc::clone(&self.db);
        let lid = listener_id.clone();
        let ename = event_name.clone();
        let subscriptions = Arc::clone(&self.subscriptions);
        
        tokio::task::spawn_blocking(move || {
            // 1. Bootstrap: the current rows as one `Full` change.
            let initial_rows = execute_query_input(&db, &query_template).unwrap_or_default();

            let bootstrap_payload = DeltaPayload {
                listener_id: lid.clone(),
                changes: vec![DocumentChange {
                    kind: DeltaKind::Full,
                    doc_id: "_all_".into(),
                    data: Some(serde_json::Value::Array(initial_rows)),
                }],
            };

            // Events must cross as MessagePack bytes, like `hako_exec`.
            if let Ok(bin) = to_binary_payload(&bootstrap_payload) {
                let _ = window.emit(&ename, bin);
            }

            // 2. Prebuild the filter plan once; every live event reuses it.
            let query_obj = build_query_from_input(&query_template).unwrap_or_else(|_| {
                Query::new(&query_template.collection)
            });

            let filter_plan = db.plan_for_watch(&query_obj);

            // 3. Event loop: drain pending events, emit one payload.
            loop {
                if stop_rx.try_recv().is_ok() { break; }

                match rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(first_event) => {
                        let mut events = vec![first_event];
                        while let Ok(extra) = rx.try_recv() { events.push(extra); }

                        let mut changes = Vec::new();
                        for event in events {
                            // In Hako the storage path is the doc id.
                            let doc_id: String = event.path.to_string();

                            // Cheap pre-filter before touching disk.
                            if let Some(want) = query_template.doc_id_filter.as_deref() {
                                if want != doc_id { continue; }
                            }

                            match event.kind {
                                ChangeKind::Delete => {
                                    // Deletes carry a timestamp so the client
                                    // can ignore stale updates (LWW).
                                    let mut meta = serde_json::Map::new();
                                    meta.insert("_time".to_string(), serde_json::json!(unix_millis() * 1000));

                                    changes.push(DocumentChange {
                                        kind: DeltaKind::Delete,
                                        doc_id,
                                        data: Some(serde_json::Value::Object(meta))
                                    });
                                }
                                ChangeKind::Put => {
                                    // Raw bytes, no decoded point-get.
                                    let bytes_res = db.get_raw_bytes(&query_template.collection, &event.path);

                                    if let Ok(Some(bytes)) = bytes_res {
                                        // Zero-decode match, same cost as in-tree.
                                        // Row-header timestamp at bytes 2..10
                                        // (mirrors the storage layout).
                                        let doc_time = i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0;8]));
                                        if Hako::matches_watch(&doc_id, &bytes, &filter_plan) {
                                            let doc = if let Some(ref p) = query_template.projection {
                                                HakoDoc::decode_projected(&bytes, p)
                                            } else {
                                                HakoDoc::decode(&bytes)
                                            };

                                            if let Some(mut d) = doc {
                                                let _ = db.resolve_document_blobs(&mut d, &query_template.collection);
                                                changes.push(DocumentChange {
                                                    kind: DeltaKind::Update,
                                                    doc_id: doc_id.to_string(),
                                                    data: doc_to_json_value(&doc_id,&d).ok()
                                                });
                                            }
                                        } else {
                                            // Exit case: the doc matched before
                                            // but the update moved it out.
                                            let mut meta = serde_json::Map::new();
                                            meta.insert("_time".to_string(), serde_json::json!(doc_time));
                                            changes.push(DocumentChange {
                                                kind: DeltaKind::Delete,
                                                doc_id: doc_id.to_string(),
                                                data: Some(serde_json::Value::Object(meta))
                                            });
                                        }
                                    }
                                }
                            }
                        }

                        if !changes.is_empty() {
                            let payload = DeltaPayload { listener_id: lid.clone(), changes };
                            if let Ok(bin) = to_binary_payload(&payload) {
                                let _ = window.emit(&ename, bin);
                            }
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
            subscriptions.lock().remove(&lid);
        });

        Ok(())
    }
}

/// The decoded shape of a `query` / `query_raw` / `subscribe` payload:
/// everything `build_query_from_input` needs, without the action tag.
#[derive(Debug, Clone)]
struct QueryInput {
    collection: String,
    filters: Vec<FilterInput>,
    or_groups: Option<Vec<Vec<FilterInput>>>,
    order_by: Option<Vec<OrderByInput>>,
    limit: Option<usize>,
    offset: Option<usize>,
    projection: Option<Vec<String>>,
    start_at: Option<Vec<serde_json::Value>>,
    start_after: Option<Vec<serde_json::Value>>,
    end_at: Option<Vec<serde_json::Value>>,
    end_before: Option<Vec<serde_json::Value>>,
    doc_id_filter: Option<String>,
    defer_blobs: bool,
    local_only: bool,
}

/// What a `query` op does besides fetching: bulk delete or bulk patch over
/// the same filter set.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryAction {
    Fetch,
    Delete,
    Patch { data: serde_json::Value },
    /// Plan-only: no execution, returns the explain report.
    Explain,
}

/// The one Tauri command. Every op runs on a blocking worker (the engine
/// is synchronous) and the [`HakoResponse`] is returned as MessagePack
/// bytes. Register the module path, not the root re-export:
///
/// ```ignore
/// tauri::generate_handler![hakotauri::gateway::hako_exec]
/// ```
#[command]
pub async fn hako_exec<R: Runtime>(
    _window: Window<R>,
    state: State<'_, HakoGateway>,
    op: HakoOp,
) -> Result<Vec<u8>, String> {
    let gateway = state.inner().clone();

    let res = tokio::task::spawn_blocking(move || -> Result<HakoResponse, String> {
        match op {
            HakoOp::Get { collection, doc_id } => {
                let doc = gateway.db.get(&collection, &doc_id).map_err(|e| e.to_string())?;
                let data = doc.map(|d| doc_to_json_value(&doc_id, &d)).transpose()?;
                Ok(HakoResponse::Document { data })
            }
            HakoOp::Set { collection, doc_id, data } => {
                let doc = json_to_doc(&data)?;
                // `doc` is freshly built from JSON — move it in instead of
                // deep-cloning every field via `put`.
                gateway.db.put_owned(&collection, &doc_id, doc).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::Patch { collection, doc_id, data } => {
                let updates = json_to_vec(&data)?;
                gateway.db.patch(&collection, &doc_id, updates).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::Delete { collection, doc_id, local_only, force } => {
                if local_only {
                    gateway.db.delete_local(&collection, &doc_id).map_err(|e| e.to_string())?;
                } else if force {
                    gateway.db.delete_force(&collection, &doc_id).map_err(|e| e.to_string())?;
                } else {
                    gateway.db.delete(&collection, &doc_id).map_err(|e| e.to_string())?;
                }
                Ok(HakoResponse::Ok)
            }
            HakoOp::Restore { collection, doc_id } => {
                gateway.db.restore(&collection, &doc_id).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::RestoreAvailable { collection } => {
                let records = gateway
                    .db
                    .restore_available(collection.as_deref())
                    .map_err(|e| e.to_string())?;
                let list = serde_json::to_value(records).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Records { list })
            }
            HakoOp::DeleteQueue { collection } => {
                let records = gateway
                    .db
                    .delete_queue(collection.as_deref())
                    .map_err(|e| e.to_string())?;
                let list = serde_json::to_value(records).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Records { list })
            }
            HakoOp::Sweep => {
                let count = gateway.db.sweep_now().map_err(|e| e.to_string())?;
                Ok(HakoResponse::BulkActionResult { count: count as usize })
            }
            HakoOp::Vacuum { collection } => {
                let count = gateway.db.vacuum_collection(&collection).map_err(|e| e.to_string())?;
                Ok(HakoResponse::BulkActionResult { count })
            }
            HakoOp::CreateIndex { collection, field } => {
                gateway.db.create_index(&collection, &field).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::CreateFtsIndex { collection, field } => {
                gateway.db.create_fts_index(&collection, &field).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::CreateCompositeIndex { collection, fields } => {
                let parsed_fields = fields.into_iter().map(|f| (f.field, if f.desc { SortDirection::Desc } else { SortDirection::Asc })).collect();
                let _ = gateway.db.create_composite_index(&collection, parsed_fields);
                gateway.db.persist_index_defs().map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::CreateVectorIndex { collection, field, dim, metric, m, ef_construction, ef_search, binary_stage1 } => {
                let params = hakodb::index::vector::HnswParams {
                    m,
                    ef_construction,
                    ef_search,
                    binary_stage1,
                };
                gateway.db.create_vector_index_with_params(&collection, &field, dim, metric.metric(), params).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::CreateTraverseIndex { collection } => {
                gateway.db.create_traverse_index(&collection).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::FindPaths { collection, doc_id, direction, edge, max_depth, k, via, weight, node_match, edge_type, edge_match, include_path, include_via } => {
                let input = TraverseInput { direction, edge, max_depth, k, via, weight, node_match, edge_type, edge_match, include_path, include_via };
                run_find_paths(&gateway.db, collection, doc_id, input, false)
            }
            HakoOp::FindSimplePaths { collection, doc_id, direction, edge, max_depth, k, via, weight, node_match, edge_type, edge_match, include_path, include_via } => {
                let input = TraverseInput { direction, edge, max_depth, k, via, weight, node_match, edge_type, edge_match, include_path, include_via };
                run_find_paths(&gateway.db, collection, doc_id, input, true)
            }
            HakoOp::CreateEdgeIndex { collection } => {
                gateway.db.create_edge_index(&collection).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::FindNear { collection, field, query, k, max_distance, filters } => {
                // Exact rescore per hit from the stored document vector —
                // twin of `hk_engine_find_near` in hakodb's ffi.rs (kept
                // inline: one shared helper would couple this crate's
                // release train to core's; promote it if a third consumer
                // appears). Hybrid filters ride the query planner
                // (index-satisfied pre-filter or over-fetch + post-filter).
                let rows = if filters.is_empty() {
                    // Exact scores come back pre-computed; drop them here —
                    // the rescore loop below recomputes from the stored
                    // document either way.
                    gateway.db
                        .find_near_scored(&collection, &field, &query, k, max_distance)
                        .map_err(|e| e.to_string())?
                        .into_iter()
                        .map(|(id, _score, doc)| (id, doc))
                        .collect::<Vec<_>>()
                } else {
                    let q = build_query_from_input(&QueryInput {
                        collection: collection.clone(),
                        doc_id_filter: None,
                        or_groups: None,
                        order_by: None,
                        limit: Some(k),
                        offset: None,
                        projection: None,
                        start_at: None, start_after: None, end_at: None, end_before: None,
                        defer_blobs: false,
                        local_only: false,
                        filters,
                    })?;
                    // The near predicate itself: engine ANN lane + rescore.
                    let bytes = hakodb::index::vector::encode_f32s(&query);
                    let q = q.where_filter(&field, Operator::Near, Value::Binary(bytes));
                    gateway.db.query(q).map_err(|e| e.to_string())?
                };
                let metric = gateway.db.vector_metric(&collection, &field).unwrap_or(Metric::Cosine);
                let mut hits = Vec::with_capacity(rows.len());
                for (id, doc) in rows {
                    let score = match doc.get(&field) {
                        Some(Value::Binary(b)) => decode_f32s(b)
                            .ok()
                            .filter(|v| v.len() == query.len())
                            .map(|v| distance(metric, &query, &v))
                            .unwrap_or(f64::INFINITY),
                        _ => f64::INFINITY,
                    };
                    if let Some(max) = max_distance {
                        if score > max { continue; }
                    }
                    let json = doc_to_json_value(&id, &doc)?;
                    hits.push(NearHit { id, score, doc: json });
                }
                Ok(HakoResponse::NearResult { hits })
            }
            HakoOp::FindNearBatch { collection, field, queries, k } => {
                let refs: Vec<&[f32]> = queries.iter().map(|q| q.as_slice()).collect();
                let batch = gateway.db.find_near_batch(&collection, &field, &refs, k).map_err(|e| e.to_string())?;
                let metric = gateway.db.vector_metric(&collection, &field).unwrap_or(Metric::Cosine);
                let mut results = Vec::with_capacity(batch.len());
                for (qi, rows) in batch.into_iter().enumerate() {
                    let query = &queries[qi];
                    let mut hits = Vec::with_capacity(rows.len());
                    for (id, doc) in rows {
                        let score = match doc.get(&field) {
                            Some(Value::Binary(b)) => decode_f32s(b)
                                .ok()
                                .filter(|v| v.len() == query.len())
                                .map(|v| distance(metric, query, &v))
                                .unwrap_or(f64::INFINITY),
                            _ => f64::INFINITY,
                        };
                        let json = doc_to_json_value(&id, &doc)?;
                        hits.push(NearHit { id, score, doc: json });
                    }
                    results.push(hits);
                }
                Ok(HakoResponse::NearBatchResult { results })
            }
            HakoOp::Query { collection, action, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before, defer_blobs, local_only } => {
                let input = QueryInput { 
                    collection, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before, defer_blobs, local_only 
                };
                let query_obj = build_query_from_input(&input)?;

                match action.unwrap_or(QueryAction::Fetch) {
                    QueryAction::Fetch => {
                        let rows = execute_query_input(&gateway.db, &input)?;
                        Ok(HakoResponse::QueryResult { rows })
                    }
                    QueryAction::Delete => {
                        let count = if input.local_only {
                            gateway.db.delete_where_local(query_obj).map_err(|e| e.to_string())?
                        } else {
                            gateway.db.delete_where(query_obj).map_err(|e| e.to_string())?
                        };
                        Ok(HakoResponse::BulkActionResult { count })
                    }
                    QueryAction::Patch { data } => {
                        let updates = json_to_vec(&data)?;
                        let count = gateway.db.patch_where(query_obj, updates).map_err(|e| e.to_string())?;
                        Ok(HakoResponse::BulkActionResult { count })
                    }
                    QueryAction::Explain => {
                        let report = gateway.db.explain_report(query_obj).map_err(|e| e.to_string())?;
                        Ok(HakoResponse::ValueResult {
                            value: Some(serde_json::to_value(report).map_err(|e| e.to_string())?),
                        })
                    }
                }
            }
            HakoOp::QueryRaw { collection, doc_id_filter, filters, or_groups, order_by, limit, offset, start_at, start_after, end_at, end_before } => {
                // Same builder as Query; raw mode is forced inside
                // `query_raw`. Projection/defer/local-only don't apply.
                let input = QueryInput {
                    collection, doc_id_filter, filters, or_groups, order_by, limit, offset,
                    projection: None, start_at, start_after, end_at, end_before,
                    defer_blobs: false, local_only: false,
                };
                let query_obj = build_query_from_input(&input)?;
                let rows = gateway.db.query_raw(query_obj).map_err(|e| e.to_string())?
                    .into_iter()
                    .map(|(id, bytes)| RawRow { id, bytes: bytes.as_ref().clone() })
                    .collect();
                Ok(HakoResponse::RawResult { rows })
            }
            HakoOp::DecodeRaw { collection, doc_id, bytes } => {
                let mut doc = HakoDoc::decode(&bytes).ok_or("raw bytes do not decode")?;
                gateway.db.resolve_document_blobs(&mut doc, &collection).map_err(|e| e.to_string())?;
                let data = doc_to_json_value(&doc_id, &doc)?;
                Ok(HakoResponse::Document { data: Some(data) })
            }
            HakoOp::ViewGetField { collection, doc_id, field } => {
                // Stateless lazy pull — view borrowed, one field decoded,
                // nothing owned except the JSON value itself.
                let value = match gateway.db.get_view(&collection, &doc_id).map_err(|e| e.to_string())? {
                    Some(view) => view.get(&field).map(|v| v.to_json()),
                    None => None,
                };
                Ok(HakoResponse::ValueResult { value })
            }
            HakoOp::Batch { mutations } => {
                let mut batch = Vec::with_capacity(mutations.len());
                // Local-only deletes bypass the shared batch so their marks
                // persist once per collection, not per item.
                let mut local_dels: HashMap<String, Vec<String>> = HashMap::new();
                for item in mutations {
                    match item.mutation {
                        BatchMutationKind::Set => {
                            let data = item.data.ok_or("missing data")?;
                            batch.push(BatchMutation::Put { collection: item.collection, doc_id: item.doc_id, doc: json_to_doc(&data)? });
                        }
                        BatchMutationKind::Patch => {
                            let data = item.data.ok_or("missing data")?;
                            batch.push(BatchMutation::Patch { collection: item.collection, doc_id: item.doc_id, updates: json_to_vec(&data)? });
                        }
                        BatchMutationKind::Delete if item.local_only => {
                            local_dels.entry(item.collection).or_default().push(item.doc_id);
                        }
                        // Soft by default; `Delete` is the legacy hard variant.
                        BatchMutationKind::Delete if item.force => {
                            batch.push(BatchMutation::Delete { collection: item.collection, doc_id: item.doc_id });
                        }
                        BatchMutationKind::Delete => {
                            batch.push(BatchMutation::SoftDelete { collection: item.collection, doc_id: item.doc_id });
                        }
                    }
                }
                for (col, ids) in local_dels {
                    gateway.db.delete_ids_local(&col, &ids).map_err(|e| e.to_string())?;
                }
                gateway.db.write_batch(batch).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::Aggregate { collection, filters, or_groups, kind, field } => {
                let mut query = Query::new(&collection);
                for filter in filters {
                    query = query.where_filter(&filter.field, map_operator(&filter.op), json_value_to_value(&filter.value)?);
                }
                if let Some(groups) = or_groups {
                    for group in groups {
                        let filters: Vec<Filter> = group.iter()
                            .map(|f: &FilterInput| -> Result<Filter, String> {
                                Ok(Filter {
                                    field: f.field.clone(),
                                    op: map_operator(&f.op),
                                    value: json_value_to_value(&f.value)?
                                })
                            })
                            .collect::<Result<Vec<_>, String>>()?;
                        query.or_groups.push(filters);
                    }
                }
                query = match kind {
                    AggregateKind::Count => query.aggregate(AggregateOp::Count),
                    AggregateKind::Sum => query.aggregate(AggregateOp::Sum(field.ok_or("missing field")?)),
                    AggregateKind::Avg => query.aggregate(AggregateOp::Avg(field.ok_or("missing field")?)),
                    AggregateKind::Min => query.aggregate(AggregateOp::Min(field.ok_or("missing field")?)),
                    AggregateKind::Max => query.aggregate(AggregateOp::Max(field.ok_or("missing field")?)),
                };
                let result = gateway.db.execute_aggregation(query).map_err(|e| e.to_string())?;
                // min/max over an empty set return no entry at all.
                let value = result.into_values().next();
                Ok(HakoResponse::AggregateResult { value })
            }
            HakoOp::Subscribe { listener_id, collection, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, event_name, start_at, start_after, end_at, end_before } => {
                gateway.register_subscription(
                    _window,
                    listener_id.clone(),
                    QueryInput { collection, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before, defer_blobs: false, local_only: false },
                    event_name.unwrap_or_else(|| "hako://snapshot".to_string()),
                )?;
                Ok(HakoResponse::SubscriptionAck { listener_id })
            }
            HakoOp::Unsubscribe { listener_id } => {
                gateway.unsubscribe(&listener_id);
                Ok(HakoResponse::Unsubscribed { listener_id })
            }
            HakoOp::GetStats => {
                let stats = gateway.db.get_stats();
                Ok(HakoResponse::Stats { details: serde_json::to_value(stats).unwrap() })
            }
            HakoOp::IndexesReady => {
                Ok(HakoResponse::Ready { ready: gateway.db.is_indexes_ready() })
            }
            HakoOp::ListCollections => {
                let names = gateway.db.list_collections().map_err(|e| e.to_string())?;
                Ok(HakoResponse::Collections { names })
            }
            HakoOp::Compact => {
                gateway.db.compact().map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::CompactCollection { collection } => {
                gateway.db.compact_collection(&collection).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::Exists { collection, doc_id } => {
                let exists = gateway.db.exists(&collection, &doc_id).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Exists { exists })
            }
            HakoOp::Flush => {
                gateway.db.flush().map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::ListStorageKeys { collection } => {
                let ids = gateway.db.list_storage_keys(&collection).map_err(|e| e.to_string())?;
                Ok(HakoResponse::StorageKeys { ids })
            }
            HakoOp::Backup { path } => {
                gateway.db.backup(path).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::ListIndexes { collection } => {
                let list = gateway.db.list_indexes(collection.as_deref());
                Ok(HakoResponse::Indexes { list: serde_json::to_value(list).unwrap() })
            }
            HakoOp::SnapshotIndices => {
                gateway.db.save_index_snapshots().map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::GetAuditLog => {
                let entries = gateway.db.audit_entries();
                Ok(HakoResponse::AuditLog { entries })
            }
            HakoOp::SetDurability { mode } => {
                let d_mode = match mode {
                    1 => DurabilityMode::Interval,
                    2 => DurabilityMode::Manual,
                    3 => DurabilityMode::OnCommit,
                    _ => DurabilityMode::Always,
                };
                gateway.db.set_durability_mode_all(d_mode);
                Ok(HakoResponse::Ok)
            }
            HakoOp::SetCompression { enabled: _, level: _ } => {
                // No-op: compression is engine-managed in current versions.
                Ok(HakoResponse::Ok)
            }
            HakoOp::SetGroupCommitInterval { ms } => {
                gateway.db.set_group_commit_interval_ms_all(ms);
                Ok(HakoResponse::Ok)
            }
            HakoOp::RelocateDocs { src, dst, ids } => {
                let rep = gateway.db.relocate_docs(&src, &dst, &ids).map_err(|e| e.to_string())?;
                Ok(HakoResponse::RelocateResult { moved: rep.moved, missing: rep.missing })
            }
            HakoOp::LoadCollection { collection } => {
                gateway.db.load_collection(&collection).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::UnloadCollection { collection } => {
                gateway.db.unload_collection(&collection).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::UnloadedCollections => {
                Ok(HakoResponse::Collections { names: gateway.db.unloaded_lazy_collections() })
            }
            HakoOp::Transaction { steps, max_attempts } => {
                let policy = hakodb::engine::RetryPolicy::exponential(
                    max_attempts.max(1),
                    std::time::Duration::from_millis(25),
                    std::time::Duration::from_millis(400),
                );
                let db = Arc::clone(&gateway.db);
                // Get-step docs are captured INSIDE the body: a retry reruns
                // the whole step list on a fresh snapshot, so only the docs
                // read by the attempt that finally commits are returned.
                let gets: Vec<(String, String)> = steps.iter()
                    .filter(|s| matches!(s.kind, TxnStepKind::Get))
                    .map(|s| (s.collection.clone(), s.doc_id.clone()))
                    .collect();
                let docs = gateway.db.transaction(policy, |tx| {
                    let mut docs = Vec::with_capacity(gets.len());
                    for step in &steps {
                        match step.kind {
                            TxnStepKind::Get => {
                                docs.push(tx.get(&db, &step.collection, &step.doc_id)?);
                            }
                            TxnStepKind::Set => {
                                let data = step.data.as_ref()
                                    .ok_or_else(|| HakoError::InvalidInput("missing data".into()))?;
                                let doc = json_to_doc(data).map_err(HakoError::InvalidInput)?;
                                tx.put(&step.collection, &step.doc_id, doc);
                            }
                            TxnStepKind::Patch => {
                                let data = step.data.as_ref()
                                    .ok_or_else(|| HakoError::InvalidInput("missing data".into()))?;
                                let updates = json_to_vec(data).map_err(HakoError::InvalidInput)?;
                                tx.patch(&db, &step.collection, &step.doc_id, updates)?;
                            }
                            TxnStepKind::Delete => {
                                if step.force { tx.delete_force(&step.collection, &step.doc_id); }
                                else { tx.delete(&step.collection, &step.doc_id); }
                            }
                        }
                    }
                    Ok(docs)
                }).map_err(|e| e.to_string())?;
                let mut out = Vec::with_capacity(docs.len());
                for (i, doc) in docs.into_iter().enumerate() {
                    let id = &gets[i].1;
                    out.push(match doc {
                        Some(d) => Some(doc_to_json_value(id, &d)?),
                        None => None,
                    });
                }
                Ok(HakoResponse::TransactionResult { docs: out })
            }
            HakoOp::Pipeline { collection, stages } => {
                use hakodb::query::pipeline::{Accum, AccumSpec, Pipeline as EnginePipeline, Stage};
                let mut pipeline = EnginePipeline::new(&collection);
                for stage in stages {
                    pipeline = pipeline.stage(match stage {
                        StageInput::Match { filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before, doc_id_filter } => {
                            let input = QueryInput {
                                collection: collection.clone(),
                                doc_id_filter, filters, or_groups, order_by,
                                limit, offset, projection,
                                start_at, start_after, end_at, end_before,
                                defer_blobs: false,
                                local_only: false,
                            };
                            Stage::Match(build_query_from_input(&input)?)
                        }
                        StageInput::Project { fields } => Stage::Project(fields),
                        StageInput::Sort { field, ascending } => Stage::Sort { field, ascending },
                        StageInput::Limit { n } => Stage::Limit(n),
                        StageInput::Offset { n } => Stage::Offset(n),
                        StageInput::Unwind { field } => Stage::Unwind { field },
                        StageInput::Group { key, accums } => {
                            let mut specs = Vec::with_capacity(accums.len());
                            for a in accums {
                                let op = match a.kind {
                                    AggregateKind::Count => Accum::Count,
                                    AggregateKind::Sum => Accum::Sum(a.field.ok_or("missing field")?),
                                    AggregateKind::Avg => Accum::Avg(a.field.ok_or("missing field")?),
                                    AggregateKind::Min => Accum::Min(a.field.ok_or("missing field")?),
                                    AggregateKind::Max => Accum::Max(a.field.ok_or("missing field")?),
                                };
                                specs.push(AccumSpec { out: a.out, op });
                            }
                            Stage::Group { key, accums: specs }
                        }
                    });
                }
                let rows = gateway.db.run_pipeline(pipeline).map_err(|e| e.to_string())?;
                let out = rows.into_par_iter()
                    .map(|(id, doc)| doc_to_json_value(&id, &doc).unwrap_or(serde_json::Value::Null))
                    .collect();
                Ok(HakoResponse::QueryResult { rows: out })
            }
            HakoOp::DropIndex { kind, collection, field, id } => {
                match kind {
                    DropIndexKind::Simple => gateway.db.drop_index(&collection, field.as_deref().ok_or("missing field")?).map_err(|e| e.to_string())?,
                    DropIndexKind::Fts => gateway.db.drop_fts_index(&collection, field.as_deref().ok_or("missing field")?).map_err(|e| e.to_string())?,
                    DropIndexKind::Vector => gateway.db.drop_vector_index(&collection, field.as_deref().ok_or("missing field")?).map_err(|e| e.to_string())?,
                    DropIndexKind::Traverse => gateway.db.drop_traverse_index(&collection).map_err(|e| e.to_string())?,
                    DropIndexKind::Edge => gateway.db.drop_edge_index(&collection).map_err(|e| e.to_string())?,
                    DropIndexKind::Composite => gateway.db.drop_composite_index(&collection, id.ok_or("missing id")?).map_err(|e| e.to_string())?,
                }
                gateway.db.persist_index_defs().map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::GetVersioned { collection, doc_id } => {
                match gateway.db.get_versioned(&collection, &doc_id).map_err(|e| e.to_string())? {
                    Some((doc, version)) => {
                        let data = doc_to_json_value(&doc_id, &doc)?;
                        Ok(HakoResponse::DocumentVersion { data: Some(data), version })
                    }
                    None => Ok(HakoResponse::DocumentVersion { data: None, version: 0 }),
                }
            }
            HakoOp::GetByRef { reference } => {
                // Wire shape: `{"__ref__": "col/id"}` — the exact form
                // doc_to_json_value emits for Value::Reference.
                let (ref_col, ref_id) = match &reference {
                    serde_json::Value::Object(map) => match map.get("__ref__")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.split_once('/'))
                    {
                        Some((c, i)) => (c.to_string(), i.to_string()),
                        None => return Ok(HakoResponse::Document { data: None }),
                    },
                    _ => return Ok(HakoResponse::Document { data: None }),
                };
                let doc = gateway.db
                    .get_by_reference(&Value::Reference { collection: ref_col, doc_id: ref_id.clone() })
                    .map_err(|e| e.to_string())?;
                let data = doc.map(|d| doc_to_json_value(&ref_id, &d)).transpose()?;
                Ok(HakoResponse::Document { data })
            }
            HakoOp::PutSubdocument { collection, doc_id, subcollection, subdoc_id, data } => {
                let doc = json_to_doc(&data)?;
                let id = gateway.db.put_subdocument(&collection, &doc_id, &subcollection, &subdoc_id, &doc).map_err(|e| e.to_string())?;
                Ok(HakoResponse::ValueResult { value: Some(serde_json::Value::String(id)) })
            }
            HakoOp::RenameCollection { from, to } => {
                gateway.db.rename_collection(&from, &to).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::DropCollection { collection } => {
                let rep = gateway.db.drop_collection(&collection).map_err(|e| e.to_string())?;
                Ok(HakoResponse::DropResult {
                    docs_removed: rep.docs_removed,
                    bytes_reclaimed: rep.bytes_reclaimed,
                    collections_dropped: rep.collections_dropped,
                })
            }
            HakoOp::Replicate { collection, doc_id } => {
                match doc_id {
                    Some(id) => gateway.db.replicate_key(&collection, &id),
                    None => gateway.db.replicate_collection(&collection),
                }
                Ok(HakoResponse::Ok)
            }
            HakoOp::Quiescence => {
                // QuiescenceStatus is a plain C-facing struct (no Serialize);
                // hand-build the JSON so the wire shape is explicit.
                let s = gateway.db.quiescence_status();
                let value = serde_json::json!({
                    "indexes_ready": s.indexes_ready,
                    "pending_index_ops": s.pending_index_ops,
                    "index_backfills": s.index_backfills,
                    "pending_blob_bytes": s.pending_blob_bytes,
                    "queued_blob_items": s.queued_blob_items,
                    "maintenance_running": s.maintenance_running,
                });
                Ok(HakoResponse::ValueResult { value: Some(value) })
            }
            HakoOp::SetCollectionLocal { collection, local } => {
                gateway.db.set_collection_local(&collection, local);
                Ok(HakoResponse::Ok)
            }
            HakoOp::SetReadOnly { enabled } => {
                gateway.db.set_read_only(enabled);
                Ok(HakoResponse::Ok)
            }
            HakoOp::GraphAnalysis { collection, kind, direction, edge, via, node_match } => {
                let spec = build_traverse_spec(&TraverseInput {
                    direction, edge, via, node_match,
                    max_depth: default_traverse_depth(),
                    k: default_traverse_k(),
                    weight: None,
                    edge_type: None,
                    edge_match: None,
                    include_path: false,
                    include_via: false,
                })?;
                let value = match kind {
                    GraphAnalysisKind::Components => {
                        let comps = gateway.db.graph_components(&collection, &spec).map_err(|e| e.to_string())?;
                        serde_json::to_value(comps).map_err(|e| e.to_string())?
                    }
                    GraphAnalysisKind::Cycle => {
                        let cycle = gateway.db.graph_find_cycle(&collection, &spec).map_err(|e| e.to_string())?;
                        serde_json::to_value(cycle).map_err(|e| e.to_string())?
                    }
                    GraphAnalysisKind::Topo => {
                        let order = gateway.db.graph_topo_sort(&collection, &spec).map_err(|e| e.to_string())?;
                        serde_json::to_value(order).map_err(|e| e.to_string())?
                    }
                    GraphAnalysisKind::Degrees => {
                        let rows = gateway.db.graph_degree_centrality(&collection, &spec).map_err(|e| e.to_string())?;
                        serde_json::to_value(rows).map_err(|e| e.to_string())?
                    }
                };
                Ok(HakoResponse::ValueResult { value: Some(value) })
            }
            HakoOp::ExportGraph { collection, direction, edge, via } => {
                let spec = build_traverse_spec(&TraverseInput {
                    direction, edge, via, node_match: None,
                    max_depth: default_traverse_depth(),
                    k: default_traverse_k(),
                    weight: None,
                    edge_type: None,
                    edge_match: None,
                    include_path: false,
                    include_via: false,
                })?;
                let json = gateway.db.export_graph_json(&collection, &spec).map_err(|e| e.to_string())?;
                let value = serde_json::from_str(&json).map_err(|e| e.to_string())?;
                Ok(HakoResponse::ValueResult { value: Some(value) })
            }
            HakoOp::GraphPagerank { collection, seeds, direction, edge, via, node_match, damping, top } => {
                let spec = build_traverse_spec(&TraverseInput {
                    direction, edge, via, node_match,
                    max_depth: default_traverse_depth(),
                    k: default_traverse_k(),
                    weight: None,
                    edge_type: None,
                    edge_match: None,
                    include_path: false,
                    include_via: false,
                })?;
                let seed_nodes: Vec<(String, String)> = seeds
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|s| s.split_once('/').map(|(c, id)| (c.to_string(), id.to_string())))
                    .collect();
                // 0 rides the engine default, mirroring the FFI contract.
                let damping = if damping == 0.0 { 0.85 } else { damping };
                let rows = gateway.db.graph_pagerank(&collection, &seed_nodes, &spec, damping, top).map_err(|e| e.to_string())?;
                let value = serde_json::to_value(
                    rows.into_iter()
                        .map(|((col, id), score)| serde_json::json!({"node": format!("{col}/{id}"), "score": score}))
                        .collect::<Vec<_>>(),
                )
                .map_err(|e| e.to_string())?;
                Ok(HakoResponse::ValueResult { value: Some(value) })
            }
            HakoOp::Subgraph { collection, seed_id, max_depth, direction, edge, via, node_match } => {
                let spec = build_traverse_spec(&TraverseInput {
                    direction, edge, via, node_match,
                    max_depth,
                    k: default_traverse_k(),
                    weight: None,
                    edge_type: None,
                    edge_match: None,
                    include_path: false,
                    include_via: false,
                })?;
                let json = gateway.db.subgraph_json(&collection, &seed_id, &spec).map_err(|e| e.to_string())?;
                let value = serde_json::from_str(&json).map_err(|e| e.to_string())?;
                Ok(HakoResponse::ValueResult { value: Some(value) })
            }
            HakoOp::HybridSearch { collection, text_field, text_query, vec_field, query, k, rrf_k } => {
                // 0 rides the engine default, mirroring the FFI contract.
                let rrf_k = if rrf_k == 0 { 60 } else { rrf_k };
                let rows = gateway.db.hybrid_search(&collection, &text_field, &text_query, &vec_field, &query, k, rrf_k).map_err(|e| e.to_string())?;
                let mut hits = Vec::with_capacity(rows.len());
                for (id, score, doc) in rows {
                    let json = doc_to_json_value(&id, &doc)?;
                    hits.push(HybridHit { id, score, doc: json });
                }
                Ok(HakoResponse::HybridResult { hits })
            }
        }
    })
    .await
    .map_err(|e| e.to_string())??; // Join error, then the op error.
    to_binary_payload(&res)
}


/// Wire traversal input -> engine [`TraverseSpec`]. Shared by FindPaths,
/// FindSimplePaths, GraphAnalysis and ExportGraph.
fn build_traverse_spec(input: &TraverseInput) -> Result<hakodb::index::traverse::TraverseSpec, String> {
    let prop = |m: &Option<PropMatch>| -> Result<Option<(String, Value)>, String> {
        m.as_ref().map(|pm| Ok((pm.field.clone(), json_value_to_value(&pm.value)?)))
            .transpose()
    };
    Ok(hakodb::index::traverse::TraverseSpec {
        direction: input.direction.engine(),
        edge_field: input.edge.clone(),
        max_depth: input.max_depth,
        limit: input.k,
        via_edge: input.via.clone(),
        weight_field: input.weight.clone(),
        node_match: prop(&input.node_match)?,
        edge_type: prop(&input.edge_type)?,
        edge_match: prop(&input.edge_match)?,
        include_path: input.include_path,
        include_via: input.include_via,
    })
}

/// Runs one traversal (BFS/weighted via `find_paths`, or the cycle-safe
/// simple-path enumeration) and maps hits to the wire shape.
fn run_find_paths(
    db: &Hako,
    collection: String,
    doc_id: String,
    input: TraverseInput,
    simple: bool,
) -> Result<HakoResponse, String> {
    let spec = build_traverse_spec(&input)?;
    let rows = if simple {
        db.find_simple_paths(&collection, &doc_id, &spec).map_err(|e| e.to_string())?
    } else {
        db.find_paths(&collection, &doc_id, &spec).map_err(|e| e.to_string())?
    };
    let mut hits = Vec::with_capacity(rows.len());
    for h in &rows {
        hits.push(PathHit {
            collection: h.collection.clone(),
            id: h.id.clone(),
            depth: h.depth as u64,
            cost: h.cost,
            doc: doc_to_json_value(&h.id, &h.doc)?,
            path: input.include_path.then(|| {
                h.path.iter().map(|(c, i)| PathNode { collection: c.clone(), id: i.clone() }).collect()
            }),
            via: input.include_via.then(|| h.via.clone()),
        });
    }
    Ok(HakoResponse::PathResult { hits })
}

/// Builds an engine [`Query`] from a decoded [`QueryInput`].
fn build_query_from_input(input: &QueryInput) -> Result<Query, String> {
    let mut query = Query::new(&input.collection);
    // Per-query blob deferral (list views skip blob-file reads).
    query.defer_blobs = input.defer_blobs;

    // A doc_id_filter narrows the whole query to one document; without it
    // a targeted request would return the entire collection.
    if let Some(ref id) = input.doc_id_filter {
        if !id.is_empty() {
            query = query.where_filter("id", Operator::Eq, Value::String(id.clone()));
        }
    }

    for filter in &input.filters {
        let val = json_value_to_value(&filter.value)?;
        query = query.where_filter(
            &filter.field,
            map_operator(&filter.op),
            val
        );
    }

    if let Some(groups) = &input.or_groups {
        for group in groups {
            let filters: Vec<Filter> = group.iter()
                .map(|f: &FilterInput| -> Result<Filter, String> {
                    Ok(Filter {
                        field: f.field.clone(),
                        op: map_operator(&f.op),
                        value: json_value_to_value(&f.value)?
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            query.or_groups.push(filters);
        }
    }

    if let Some(ref orders) = &input.order_by {
        for order in orders {
            query = query.order_by(&order.field, order.ascending);
        }
    }

    if let Some(limit) = input.limit { query = query.limit(limit); }
    if let Some(offset) = input.offset { query = query.offset(offset); }

    if let Some(proj) = &input.projection { query = query.select_fields(proj.clone()); }

    if let Some(v) = &input.start_at { query.start_at = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.start_after { query.start_after = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.end_at { query.end_at = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.end_before { query.end_before = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }

    Ok(query)
}


/// Runs a query and converts every row to JSON in parallel. With a
/// projection this takes the zero-copy path (only projected fields leave
/// storage); otherwise full documents are decoded and converted.
fn execute_query_input(db: &Hako, input: &QueryInput) -> Result<Vec<serde_json::Value>, String> {
    let query = build_query_from_input(input)?;

    if let Some(projection) = &input.projection {
        if !projection.is_empty() {
            let rows = db.query_projected_zero_copy(query.clone(), projection).map_err(|e| e.to_string())?;
            return Ok(rows.into_par_iter()
                .map(|(id, fields)| projection_fields_to_json(&id, fields).unwrap_or(serde_json::Value::Null))
                .collect());
        }
    }

    let rows = db.query(query).map_err(|e| e.to_string())?;

    Ok(rows.into_par_iter()
        .map(|(id, doc)| doc_to_json_value(&id, &doc).unwrap_or(serde_json::Value::Null))
        .collect())
}

/// Frontend filter op to engine [`Operator`]. The TS side sends snake_case
/// (see `symToOp`); unknown strings never reach here.
fn map_operator(op: &FilterOperator) -> Operator {
    match op {
        FilterOperator::Eq => Operator::Eq,
        FilterOperator::Ne => Operator::Ne,
        FilterOperator::Gt => Operator::Gt,
        FilterOperator::Gte => Operator::Gte,
        FilterOperator::Lt => Operator::Lt,
        FilterOperator::Lte => Operator::Lte,
        FilterOperator::Match => Operator::Match,
        FilterOperator::MatchPrefix => Operator::MatchPrefix,
        FilterOperator::Contains => Operator::Contains,
        FilterOperator::StartsWith => Operator::StartsWith,
        FilterOperator::In => Operator::In,
        FilterOperator::NotIn => Operator::NotIn,
        FilterOperator::ArrayContains => Operator::ArrayContains,
        FilterOperator::ArrayContainsAny => Operator::ArrayContainsAny,
    }
}

/// Request JSON object to a stored document (Set / Batch-Set).
fn json_to_doc(v: &serde_json::Value) -> Result<HakoDoc, String> {
    let obj = v.as_object().ok_or("document must be object")?;
    let mut doc = HakoDoc::default();
    for (k, val) in obj {
        doc.insert(k.clone(), json_value_to_value(val)?);
    }
    Ok(doc)
}

/// Request JSON object to patch pairs (Patch / Batch-Patch / Query-Patch).
fn json_to_vec(v: &serde_json::Value) -> Result<Vec<(String, Value)>, String> {
    let obj = v.as_object().ok_or("updates must be object")?;
    let mut out = Vec::new();
    for (k, val) in obj {
        out.push((k.clone(), json_value_to_value(val)?));
    }
    Ok(out)
}

/// Request JSON value to an engine [`Value`] (filters, cursors, writes).
fn json_value_to_value(v: &serde_json::Value) -> Result<Value, String> {
    Value::from_json(v.clone())
}

/// Converts a stored document to JSON, injecting the doc id (storage rows
/// don't carry it, but every client expects an `"id"` field).
fn doc_to_json_value(id: &str, doc: &HakoDoc) -> Result<serde_json::Value, String> {
    let mut json = doc.to_json();
    if let Some(obj) = json.as_object_mut() {
        obj.insert("id".to_string(), serde_json::Value::String(id.to_string()));
    }
    Ok(json)
}

/// Zero-copy projected row to JSON: only the requested fields plus `"id"`.
fn projection_fields_to_json(id: &str, fields: Vec<(String, Value)>) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
    map.insert("id".to_string(), serde_json::Value::String(id.to_string()));
    for (k, v) in fields {
        map.insert(k, value_to_json(&v)?);
    }
    Ok(serde_json::Value::Object(map))
}

/// Engine [`Value`] back to JSON for the frontend. Three shapes need
/// knowing: `Binary` crosses as a number array, references as
/// `{"__ref__": "col/id"}`, and unresolved blobs as
/// `{"__blob__": {offset, len}}` (decode the row when the bytes are needed).
fn value_to_json(v: &Value) -> Result<serde_json::Value, String> {
    match v {
        Value::Null | Value::ServerTimestamp => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int(i) => Ok(serde_json::Value::Number((*i).into())),
        Value::Float(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number).ok_or("invalid float".into()),
        Value::String(s) => Ok(serde_json::Value::String(s.clone())),
        Value::Binary(bytes) => Ok(serde_json::json!(bytes)),
        Value::Timestamp(micros) => Ok(serde_json::Value::Number((*micros).into())),
        Value::Reference { collection, doc_id } => {
            let mut map = serde_json::Map::new();
            map.insert("__ref__".to_string(), serde_json::Value::String(format!("{collection}/{doc_id}")));
            Ok(serde_json::Value::Object(map))
        }
        Value::Map(fields) => {
            let mut map = serde_json::Map::new();
            for (k, v) in fields {
                map.insert(k.to_string(), value_to_json(v)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Value::BlobLink { offset, len } => {
            let mut map = serde_json::Map::new();
            let mut meta = serde_json::Map::new();
            meta.insert("offset".to_string(), serde_json::json!(offset));
            meta.insert("len".to_string(), serde_json::json!(len));
            map.insert("__blob__".to_string(), serde_json::Value::Object(meta));
            Ok(serde_json::Value::Object(map))
        }
        Value::Array(values) => Ok(serde_json::Value::Array(values.iter().map(value_to_json).collect::<Result<Vec<_>, _>>()?)),
    }
}

#[cfg(test)]
mod casing_tests {
    use super::*;

    #[test]
    fn query_op_field_casing_contract() {
        // Locks the wire contract the JS clients depend on: struct-variant
        // fields are snake_case (rename_all applies to fields, not just the
        // op tag), unknown fields are ignored, and bool flags default off.
        let snake = r#"{"op":"query","collection":"c","order_by":[{"field":"x","ascending":true}],"defer_blobs":true,"local_only":true}"#;
        let op: HakoOp = serde_json::from_str(snake).expect("snake_case must parse");
        match op {
            HakoOp::Query { order_by, defer_blobs, local_only, .. } => {
                assert!(order_by.is_some());
                assert!(defer_blobs);
                assert!(local_only);
            }
            _ => panic!("wrong variant"),
        }

        let minimal: HakoOp =
            serde_json::from_str(r#"{"op":"query","collection":"c"}"#).expect("minimal must parse");
        match minimal {
            HakoOp::Query { defer_blobs, local_only, filters, .. } => {
                assert!(!defer_blobs, "old clients omit the flag -> eager");
                assert!(!local_only, "old clients omit the flag -> replicated");
                assert!(filters.is_empty());
            }
            _ => panic!("wrong variant"),
        }

        let del: HakoOp = serde_json::from_str(
            r#"{"op":"delete","collection":"c","doc_id":"a","local_only":true}"#,
        )
        .expect("delete must parse");
        match del {
            HakoOp::Delete { local_only, .. } => assert!(local_only),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn soft_delete_op_wire_shapes() {
        // force and the queue filter default off/every-collection for old clients.
        let op: HakoOp =
            serde_json::from_str(r#"{"op":"delete","collection":"c","doc_id":"a"}"#)
                .expect("old clients omit both flags");
        match op {
            HakoOp::Delete { local_only, force, .. } => {
                assert!(!local_only);
                assert!(!force, "old clients omit force -> soft");
            }
            _ => panic!("wrong variant"),
        }

        let op: HakoOp =
            serde_json::from_str(r#"{"op":"restore","collection":"c","doc_id":"a"}"#)
                .expect("restore must parse");
        assert!(matches!(op, HakoOp::Restore { .. }));

        let op: HakoOp =
            serde_json::from_str(r#"{"op":"restore_available"}"#).expect("collection omittable");
        match op {
            HakoOp::RestoreAvailable { collection } => assert_eq!(collection, None),
            _ => panic!("wrong variant"),
        }

        let op: HakoOp =
            serde_json::from_str(r#"{"op":"delete_queue","collection":"c"}"#).expect("must parse");
        assert!(matches!(op, HakoOp::DeleteQueue { .. }));

        let op: HakoOp = serde_json::from_str(r#"{"op":"sweep"}"#).expect("sweep must parse");
        assert!(matches!(op, HakoOp::Sweep));

        let res = HakoResponse::Records {
            list: serde_json::json!([{ "col": "c", "id": "a" }]),
        };
        let v = serde_json::to_value(&res).expect("serializes");
        assert_eq!(
            v,
            serde_json::json!({"records": { "list": [{ "col": "c", "id": "a" }]}})
        );
    }

    #[test]
    fn parity_ops_wire_shapes() {
        // 0.19.4-parity ops: snake_case tags, optional fields omitted by old
        // clients ride their defaults, and bool flags default off.
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"transaction","steps":[
                {"kind":"get","collection":"c","doc_id":"a"},
                {"kind":"set","collection":"c","doc_id":"b","data":{"x":1}},
                {"kind":"delete","collection":"c","doc_id":"a"}]}"#,
        )
        .expect("minimal transaction must parse");
        match op {
            HakoOp::Transaction { steps, max_attempts } => {
                assert_eq!(steps.len(), 3);
                assert!(!steps[2].force, "old clients omit force -> soft delete");
                assert_eq!(max_attempts, 3, "default attempts");
            }
            _ => panic!("wrong variant"),
        }

        let op: HakoOp = serde_json::from_str(
            r#"{"op":"pipeline","collection":"c","stages":[{"stage":"match","filters":[{"field":"x","op":"eq","value":1}]},{"stage":"sort","field":"y"},{"stage":"group","key":"k","accums":[{"out":"n","kind":"count"}]}]}"#,
        )
        .expect("pipeline must parse");
        assert!(matches!(op, HakoOp::Pipeline { .. }));

        let op: HakoOp = serde_json::from_str(
            r#"{"op":"drop_index","kind":"composite","collection":"c","id":7}"#,
        )
        .expect("drop_index must parse");
        match op {
            HakoOp::DropIndex { kind, field, id, .. } => {
                assert!(matches!(kind, DropIndexKind::Composite));
                assert_eq!(field, None);
                assert_eq!(id, Some(7));
            }
            _ => panic!("wrong variant"),
        }

        let op: HakoOp = serde_json::from_str(
            r#"{"op":"find_near","collection":"c","field":"v","query":[1,2],"k":5,
                "max_distance":0.5,
                "filters":[{"field":"tag","op":"eq","value":"x"}]}"#,
        )
        .expect("hybrid find_near must parse");
        match op {
            HakoOp::FindNear { max_distance, filters, .. } => {
                assert_eq!(max_distance, Some(0.5));
                assert_eq!(filters.len(), 1);
            }
            _ => panic!("wrong variant"),
        }
        let minimal: HakoOp = serde_json::from_str(
            r#"{"op":"find_near","collection":"c","field":"v","query":[1]}"#,
        )
        .expect("old find_near must parse");
        match minimal {
            HakoOp::FindNear { max_distance, filters, k, .. } => {
                assert_eq!(max_distance, None);
                assert!(filters.is_empty());
                assert_eq!(k, 10);
            }
            _ => panic!("wrong variant"),
        }

        let op: HakoOp = serde_json::from_str(
            r#"{"op":"find_simple_paths","collection":"c","doc_id":"a","include_path":true}"#,
        )
        .expect("find_simple_paths must parse");
        match op {
            HakoOp::FindSimplePaths { include_path, node_match, .. } => {
                assert!(include_path);
                assert!(node_match.is_none());
            }
            _ => panic!("wrong variant"),
        }

        for (json, tag) in [
            (r#"{"op":"get_versioned","collection":"c","doc_id":"a"}"#, "get_versioned"),
            (r#"{"op":"get_by_ref","reference":{"__ref__":"c/a"}}"#, "get_by_ref"),
            (r#"{"op":"quiescence"}"#, "quiescence"),
            (r#"{"op":"set_read_only","enabled":true}"#, "set_read_only"),
            (r#"{"op":"replicate","collection":"c"}"#, "replicate"),
        ] {
            serde_json::from_str::<HakoOp>(json).unwrap_or_else(|e| panic!("{tag}: {e}"));
        }
        let op: HakoOp = serde_json::from_str(r#"{"op":"replicate","collection":"c","doc_id":"a"}"#)
            .expect("replicate with doc_id must parse");
        assert!(matches!(op, HakoOp::Replicate { doc_id: Some(_), .. }));

        let res = HakoResponse::TransactionResult {
            docs: vec![Some(serde_json::json!({"id": "a"})), None],
        };
        let v = serde_json::to_value(&res).expect("serializes");
        assert_eq!(v, serde_json::json!({"transaction_result": {"docs": [{"id": "a"}, null]}}));

        let res = HakoResponse::DocumentVersion {
            data: Some(serde_json::json!({"id": "a"})),
            version: 9,
        };
        let v = serde_json::to_value(&res).expect("serializes");
        assert_eq!(v, serde_json::json!({"document_version": {"data": {"id": "a"}, "version": 9}}));

        let res = HakoResponse::DropResult {
            docs_removed: 3,
            bytes_reclaimed: 4096,
            collections_dropped: vec!["c".into()],
        };
        let v = serde_json::to_value(&res).expect("serializes");
        assert_eq!(
            v,
            serde_json::json!({"drop_result": {"docs_removed": 3, "bytes_reclaimed": 4096, "collections_dropped": ["c"]}})
        );

        let res = HakoResponse::NearBatchResult {
            results: vec![vec![NearHit { id: "a".into(), score: 0.5, doc: serde_json::json!({}) }]],
        };
        let v = serde_json::to_value(&res).expect("serializes");
        assert_eq!(v, serde_json::json!({"near_batch_result": {"results": [[{"id": "a", "score": 0.5, "doc": {}}]]}}));
    }

    #[test]
    fn set_group_commit_interval_wire_shape() {
        // tag + snake_case field, matching the TS wrapper's payload.
        let op: HakoOp =
            serde_json::from_str(r#"{"op":"set_group_commit_interval","ms":50}"#)
                .expect("must parse");
        match op {
            HakoOp::SetGroupCommitInterval { ms } => assert_eq!(ms, 50),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn relocate_wire_shapes() {
        let op: HakoOp =
            serde_json::from_str(r#"{"op":"relocate_docs","src":"a","dst":"b","ids":["k1"]}"#)
                .expect("must parse");
        match op {
            HakoOp::RelocateDocs { src, dst, ids } => {
                assert_eq!(src, "a");
                assert_eq!(dst, "b");
                assert_eq!(ids, vec!["k1".to_string()]);
            }
            _ => panic!("wrong variant"),
        }
        let op: HakoOp = serde_json::from_str(r#"{"op":"load_collection","collection":"a"}"#)
            .expect("must parse");
        assert!(matches!(op, HakoOp::LoadCollection { .. }));
        let op: HakoOp = serde_json::from_str(r#"{"op":"unload_collection","collection":"a"}"#)
            .expect("must parse");
        assert!(matches!(op, HakoOp::UnloadCollection { .. }));
        let op: HakoOp = serde_json::from_str(r#"{"op":"unloaded_collections"}"#)
            .expect("must parse");
        assert!(matches!(op, HakoOp::UnloadedCollections));
    }

    #[test]
    fn consumer_ops_wire_shapes() {
        // Flush is a bare tag; ListStorageKeys carries the collection.
        // Response side stays externally-tagged snake_case like the rest.
        let op: HakoOp = serde_json::from_str(r#"{"op":"flush"}"#).expect("must parse");
        assert!(matches!(op, HakoOp::Flush));
        let op: HakoOp = serde_json::from_str(r#"{"op":"list_storage_keys","collection":"c"}"#)
            .expect("must parse");
        assert!(matches!(op, HakoOp::ListStorageKeys { .. }));
        let res = HakoResponse::StorageKeys { ids: vec!["a".into()] };
        let v = serde_json::to_value(&res).expect("serializes");
        assert_eq!(v, serde_json::json!({"storage_keys": {"ids": ["a"]}}));
    }

    #[test]
    fn graph_wire_shapes() {
        // Direction omittable -> outgoing; depth/k omittable -> 10/100.
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"create_traverse_index","collection":"g"}"#,
        )
        .expect("must parse");
        assert!(matches!(op, HakoOp::CreateTraverseIndex { .. }));
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"find_paths","collection":"g","doc_id":"a"}"#,
        )
        .expect("must parse");
        match op {
            HakoOp::FindPaths { direction, max_depth, k, edge, .. } => {
                assert!(matches!(direction, TraverseDirection::Outgoing));
                assert_eq!(max_depth, 10);
                assert_eq!(k, 100);
                assert_eq!(edge, None);
            }
            _ => panic!("wrong variant"),
        }
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"find_paths","collection":"g","doc_id":"a","direction":"both","edge":"link","max_depth":3,"k":5}"#,
        )
        .expect("must parse");
        assert!(matches!(
            op,
            HakoOp::FindPaths { direction: TraverseDirection::Both, .. }
        ));
    }

    #[test]
    fn vector_wire_shapes() {
        // Metric omittable -> cosine; k omittable -> 10 (old clients).
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"create_vector_index","collection":"docs","field":"emb","dim":384}"#,
        )
        .expect("must parse");
        match op {
            HakoOp::CreateVectorIndex { dim, metric, .. } => {
                assert_eq!(dim, 384);
                assert!(matches!(metric, VectorMetric::Cosine));
            }
            _ => panic!("wrong variant"),
        }
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"create_vector_index","collection":"docs","field":"emb","dim":2,"metric":"l2"}"#,
        )
        .expect("must parse");
        assert!(matches!(
            op,
            HakoOp::CreateVectorIndex { metric: VectorMetric::L2, .. }
        ));
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"find_near","collection":"docs","field":"emb","query":[1.0,0.5]}"#,
        )
        .expect("must parse");
        match op {
            HakoOp::FindNear { k, query, .. } => {
                assert_eq!(k, 10);
                assert_eq!(query, vec![1.0, 0.5]);
            }
            _ => panic!("wrong variant"),
        }
        // binary_stage1 omittable -> false (old clients keep int8).
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"create_vector_index","collection":"docs","field":"emb","dim":2}"#,
        )
        .expect("must parse");
        assert!(matches!(
            op,
            HakoOp::CreateVectorIndex { binary_stage1: false, .. }
        ));
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"create_vector_index","collection":"docs","field":"emb","dim":2,"binary_stage1":true}"#,
        )
        .expect("must parse");
        assert!(matches!(
            op,
            HakoOp::CreateVectorIndex { binary_stage1: true, .. }
        ));
    }

    #[test]
    fn graphrag_wire_shapes() {
        // Pagerank: everything but collection omittable; seeds parse.
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"graph_pagerank","collection":"g"}"#,
        )
        .expect("must parse");
        assert!(matches!(
            op,
            HakoOp::GraphPagerank { seeds: None, damping: 0.0, top: 0, .. }
        ));
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"graph_pagerank","collection":"g","seeds":["g/a","g/b"],"damping":0.9,"top":5}"#,
        )
        .expect("must parse");
        match op {
            HakoOp::GraphPagerank { seeds, damping, top, .. } => {
                assert_eq!(seeds, Some(vec!["g/a".into(), "g/b".into()]));
                assert!((damping - 0.9).abs() < 1e-12);
                assert_eq!(top, 5);
            }
            _ => panic!("wrong variant"),
        }
        // Subgraph: max_depth omittable -> 10; seed_id required.
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"subgraph","collection":"g","seed_id":"a"}"#,
        )
        .expect("must parse");
        match op {
            HakoOp::Subgraph { seed_id, max_depth, .. } => {
                assert_eq!(seed_id, "a");
                assert_eq!(max_depth, 10);
            }
            _ => panic!("wrong variant"),
        }
        // HybridSearch: rrf_k omittable -> 0 (engine default 60).
        let op: HakoOp = serde_json::from_str(
            r#"{"op":"hybrid_search","collection":"docs","text_field":"title","text_query":"cat","vec_field":"emb","query":[1.0],"k":5}"#,
        )
        .expect("must parse");
        match op {
            HakoOp::HybridSearch { k, rrf_k, .. } => {
                assert_eq!(k, 5);
                assert_eq!(rrf_k, 0);
            }
            _ => panic!("wrong variant"),
        }
        let res = HakoResponse::HybridResult {
            hits: vec![HybridHit { id: "a".into(), score: 0.5, doc: serde_json::json!({"id": "a"}) }],
        };
        let v = serde_json::to_value(&res).expect("serializes");
        assert_eq!(
            v,
            serde_json::json!({"hybrid_result": {"hits": [{"id": "a", "score": 0.5, "doc": {"id": "a"}}]}})
        );
    }

    #[test]
    fn minmax_exists_and_compact_collection_wire_shapes() {
        // Aggregate kinds share one op; min/max add no new field.
        for (kind, wire) in [("min", "min"), ("max", "max")] {
            let op: HakoOp = serde_json::from_str(&format!(
                r#"{{"op":"aggregate","collection":"c","kind":"{wire}","field":"n"}}"#
            ))
            .expect("must parse");
            match op {
                HakoOp::Aggregate { kind: parsed, field, .. } => {
                    assert_eq!(field.as_deref(), Some("n"));
                    assert!(matches!(
                        (kind, parsed),
                        ("min", AggregateKind::Min) | ("max", AggregateKind::Max)
                    ));
                }
                _ => panic!("wrong variant"),
            }
        }

        // Empty min/max -> absent entry -> null on the wire, not 0.0.
        let res = HakoResponse::AggregateResult { value: None };
        assert_eq!(
            serde_json::to_value(&res).expect("serializes"),
            serde_json::json!({ "aggregate_result": { "value": null } })
        );

        let op: HakoOp = serde_json::from_str(
            r#"{"op":"exists","collection":"c","doc_id":"d"}"#,
        )
        .expect("must parse");
        assert!(matches!(op, HakoOp::Exists { .. }));
        let res = HakoResponse::Exists { exists: true };
        assert_eq!(
            serde_json::to_value(&res).expect("serializes"),
            serde_json::json!({ "exists": { "exists": true } })
        );

        let op: HakoOp = serde_json::from_str(
            r#"{"op":"compact_collection","collection":"c"}"#,
        )
        .expect("must parse");
        assert!(matches!(op, HakoOp::CompactCollection { .. }));
    }
}

use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tauri::{command, Emitter, Runtime, State, Window};

use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::composite::definition::SortDirection;
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn to_binary_payload<S: serde::Serialize>(val: &S) -> Result<Vec<u8>, String> {
    // let json = serde_json::to_value(val).map_err(|e| e.to_string())?;
    // let flat_value = json_to_rmpv(json);
    // rmp_serde::to_vec(&flat_value).map_err(|e| e.to_string())
    rmp_serde::to_vec_named(val).map_err(|e| e.to_string())
}

// fn json_to_rmpv(json: serde_json::Value) -> rmpv::Value {
//     match json {
//         serde_json::Value::Null => rmpv::Value::Nil,
//         serde_json::Value::Bool(b) => rmpv::Value::Boolean(b),
//         serde_json::Value::Number(n) => {
//             if let Some(i) = n.as_i64() { rmpv::Value::Integer(i.into()) }
//             else { rmpv::Value::F64(n.as_f64().unwrap_or(0.0)) }
//         }
//         serde_json::Value::String(s) => rmpv::Value::String(s.into()),
//         serde_json::Value::Array(arr) => rmpv::Value::Array(arr.into_iter().map(json_to_rmpv).collect()),
//         serde_json::Value::Object(obj) => rmpv::Value::Map(
//             obj.into_iter()
//                .map(|(k, v)| (rmpv::Value::String(k.into()), json_to_rmpv(v)))
//                .collect()
//         ),
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum HakoOp {
    Get { collection: String, doc_id: String },
    Set { collection: String, doc_id: String, data: serde_json::Value },
    Patch { collection: String, doc_id: String, data: serde_json::Value },
    Delete {
        collection: String,
        doc_id: String,
        // ponytail: old clients omit it and get replicated behavior via default.
        #[serde(default)]
        local_only: bool,
    },
    Vacuum { collection: String },
    CreateIndex { collection: String, field: String },
    CreateFtsIndex { collection: String, field: String },
    CreateCompositeIndex { collection: String, fields: Vec<CompositeFieldInput> },
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
        // ponytail: opt-in per-query blob deferral (old clients omit it and
        // get eager behavior via serde default).
        #[serde(default)]
        defer_blobs: bool,
        // Local-only scope for the Delete action (Fetch ignores it).
        #[serde(default)]
        local_only: bool,
    },
    /// Raw scan (v0.8.7+): pinned storage bytes per row, no decode, no
    /// JSON. Bytes cross msgpack as bin (Uint8Array on the TS side) and
    /// stay opaque there G�� hash/count/export them, or send selected rows
    /// back through DecodeRaw. Page with start_after: [lastId] under an
    /// id order; ids ride along in the clear.
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
    /// Lazy typed field pull (v0.8.13+): point view + one field, no
    /// decode, no JSON document. Scalars cross as JSON values;
    /// missing/wrong-type reads null. BlobLink fields surface their
    /// placeholder (resolve the doc via DecodeRaw when needed).
    ViewGetField {
        collection: String,
        doc_id: String,
        field: String,
    },
    /// Decode one raw row back into a Document (blobs inflated). The bytes
    /// must be an exact stored row (e.g. from QueryRaw) G�� never hand-built.
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
}

/// One raw row over the bridge: id in the clear, storage bytes as
/// msgpack bin. Bytes are opaque G�� decode server-side via DecodeRaw.
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
    AggregateResult { value: f64 },
    Aggregate(f64), 
    SubscriptionAck { listener_id: String },
    Unsubscribed { listener_id: String },
    Collections { names: Vec<String> },
    Stats { details: serde_json::Value },
    Indexes { list: serde_json::Value },
    AuditLog { entries: Vec<hakodb::engine::AuditEntry> },
    BulkActionResult { count: usize },
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
    // Local-only scope for Delete items (Set/Patch ignore it).
    #[serde(default)]
    pub local_only: bool,
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
pub enum AggregateKind { Count, Sum, Avg }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CompositeFieldInput {
    pub field: String,
    #[serde(default)]
    pub desc: bool,
}

#[derive(Clone)]
pub struct HakoGateway {
    pub db: Arc<Hako>,
    subscriptions: Arc<Mutex<HashMap<String, SubscriptionEntry>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaKind {
    Full,    // Initial bootstrap
    Update,  // Add or Modify
    Delete,  // Removed or no longer matches filter
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DocumentChange {
    pub kind: DeltaKind,
    pub doc_id: String,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DeltaPayload {
    pub listener_id: String,
    pub changes: Vec<DocumentChange>, // The batch container
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

    // --- ADD THIS METHOD ---
    pub fn cleanup_window_subscriptions(&self, window_label: &str) {
        let mut subs = self.subscriptions.lock();
        
        // Find all listener IDs belonging to the closed window
        let ids_to_remove: Vec<String> = subs
            .iter()
            .filter(|(_, entry)| entry.window_label == window_label)
            .map(|(id, _)| id.clone())
            .collect();

        // Stop the threads and remove from the map
        for id in ids_to_remove {
            if let Some(entry) = subs.remove(&id) {
                // Sending this signal causes the loop in the thread to break
                let _ = entry.stop_tx.send(());
            }
        }
    }

    pub fn unsubscribe(&self, listener_id: &str) {
        if let Some(entry) = self.subscriptions.lock().remove(listener_id) {
            let _ = entry.stop_tx.send(());
        }
    }

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
            // --- 1. INITIAL BOOTSTRAP (The "Full" Snapshot) ---
            let initial_rows = match execute_query_input(&db, &query_template) {
                Ok(rows) => rows,
                Err(_) => Vec::new(),
            };

            let bootstrap_payload = DeltaPayload {
                listener_id: lid.clone(),
                changes: vec![DocumentChange {
                    kind: DeltaKind::Full,
                    doc_id: "_all_".into(),
                    data: Some(serde_json::Value::Array(initial_rows)),
                }],
            };

            // CRITICAL: Must convert to binary before emitting!
            if let Ok(bin) = to_binary_payload(&bootstrap_payload) {
                let _ = window.emit(&ename, bin);
            }
            
            // --- 2. PREPARE MATCHER PLAN FOR LIVE UPDATES ---
            // (via the public watch API — same plan, zero-decode match.)
            let query_obj = build_query_from_input(&query_template).unwrap_or_else(|_| {
                hakodb::query::query::Query::new(&query_template.collection)
            });

            let filter_plan = db.plan_for_watch(&query_obj);

            // --- 3. EVENT LOOP ---
            loop {
                // Check if unsubscribed
                if stop_rx.try_recv().is_ok() { break; }

                match rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(first_event) => {
                        let mut events = vec![first_event];
                        while let Ok(extra) = rx.try_recv() { events.push(extra); }

                        let mut changes = Vec::new();
                        for event in events {
                            let doc_id: String = event.path.to_string(); // The path is the ID in Hako
                            
                            // ID Filtering (Optimization: check before reading disk)
                            if let Some(ref target_id) = query_template.doc_id_filter {
                                if target_id.as_str() != &doc_id[..] { continue; }
                            }

                            match event.kind {
                                hakodb::engine::ChangeKind::Delete => {
                                    // Provide a timestamp for the delete so client can ignore stale updates
                                    let mut meta = serde_json::Map::new();
                                    meta.insert("_time".to_string(), serde_json::json!(hakodb::util::clock::unix_millis() * 1000));
                                    
                                    changes.push(DocumentChange { 
                                        kind: DeltaKind::Delete, 
                                        doc_id, 
                                        data: Some(serde_json::Value::Object(meta)) 
                                    });
                                }
                                hakodb::engine::ChangeKind::Put => {
                                    // Raw bytes via the public watch API (no
                                    // decoded point-get on the hot path).
                                    let bytes_res = db.get_raw_bytes(&query_template.collection, &event.path);

                                    if let Ok(Some(bytes)) = bytes_res {
                                        // Complex Query Filtering (zero-decode
                                        // view match — same cost as in-tree).
                                        let doc_time = i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0;8]));
                                        if hakodb::engine::Hako::matches_watch(&doc_id, &bytes, &filter_plan) {
                                            // Handle Projection
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
                                            // This handles the "Exit" case: 
                                            // Doc existed and matched, but was updated to no longer match.
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

#[derive(Debug, Clone)]
struct QueryInput {    collection: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryAction {
    Fetch,
    Delete,
    Patch { data: serde_json::Value },
}

#[command]
pub async fn firelite_exec<R: Runtime>(
    _window: Window<R>,
    state: State<'_, HakoGateway>,
    op: HakoOp,
) -> Result<Vec<u8>, String> {
    let gateway = state.inner().clone();

    // 1. We wrap the logic in spawn_blocking. 
    // The closure return type is Result<HakoResponse, String>
    let res = tokio::task::spawn_blocking(move || -> Result<HakoResponse, String> {
        match op {
            HakoOp::Get { collection, doc_id } => {
                let doc = gateway.db.get(&collection, &doc_id).map_err(|e| e.to_string())?;
                let data = doc.map(|d| doc_to_json_value(&doc_id, &d)).transpose()?;
                Ok(HakoResponse::Document { data })
            }
            HakoOp::Set { collection, doc_id, data } => {
                let doc = json_to_doc(&data)?;
                // ponytail: `doc` is freshly built from JSON G�� move it in
                // instead of deep-cloning every field via `put`.
                gateway.db.put_owned(&collection, &doc_id, doc).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::Patch { collection, doc_id, data } => {
                let updates = json_to_vec(&data)?;
                gateway.db.patch(&collection, &doc_id, updates).map_err(|e| e.to_string())?;
                Ok(HakoResponse::Ok)
            }
            HakoOp::Delete { collection, doc_id, local_only } => {
                if local_only {
                    gateway.db.delete_local(&collection, &doc_id).map_err(|e| e.to_string())?;
                } else {
                    gateway.db.delete(&collection, &doc_id).map_err(|e| e.to_string())?;
                }
                Ok(HakoResponse::Ok)
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
                }
            }
            HakoOp::QueryRaw { collection, doc_id_filter, filters, or_groups, order_by, limit, offset, start_at, start_after, end_at, end_before } => {
                // ponytail: same builder as Query (raw forced inside
                // query_raw), defaults for the non-raw knobs.
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
                // ponytail: stateless lazy pull G�� view borrowed, one field
                // decoded, nothing owned except the JSON value itself.
                let value = match gateway.db.get_view(&collection, &doc_id).map_err(|e| e.to_string())? {
                    Some(view) => view.get(&field).map(|v| v.to_json()),
                    None => None,
                };
                Ok(HakoResponse::ValueResult { value })
            }
            HakoOp::Batch { mutations } => {
                let mut batch = Vec::with_capacity(mutations.len());
                // ponytail: local-only deletes bypass the shared batch so
                // their marks persist once per collection, not per item.
                let mut local_dels: std::collections::HashMap<String, Vec<String>> =
                    std::collections::HashMap::new();
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
                        BatchMutationKind::Delete => {
                            batch.push(BatchMutation::Delete { collection: item.collection, doc_id: item.doc_id });
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
                        let filters: Vec<hakodb::query::filter::Filter> = group.iter()
                            .map(|f: &FilterInput| -> Result<hakodb::query::filter::Filter, String> { 
                                Ok(hakodb::query::filter::Filter { 
                                    field: f.field.clone(), 
                                    op: map_operator(&f.op), 
                                    value: json_value_to_value(&f.value)? 
                                })
                            })
                            .collect::<Result<Vec<_>, String>>()?;
                        query.or_groups.push(filters);
                    }
                }
                use hakodb::query::query::AggregateOp;
                query = match kind {
                    AggregateKind::Count => query.aggregate(AggregateOp::Count),
                    AggregateKind::Sum => query.aggregate(AggregateOp::Sum(field.ok_or("missing field")?)),
                    AggregateKind::Avg => query.aggregate(AggregateOp::Avg(field.ok_or("missing field")?)),
                };
                let result = gateway.db.execute_aggregation(query).map_err(|e| e.to_string())?;
                let val = *result.values().next().unwrap_or(&0.0);
                Ok(HakoResponse::AggregateResult { value: val })
            }
            HakoOp::Subscribe { listener_id, collection, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, event_name, start_at, start_after, end_at, end_before } => {
                gateway.register_subscription(
                    _window,
                    listener_id.clone(),
                    QueryInput { collection, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before, defer_blobs: false, local_only: false },
                    event_name.unwrap_or_else(|| "firelite://snapshot".to_string()),
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
                use hakodb::config::DurabilityMode;
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
                Ok(HakoResponse::Ok)
            }
        }
    })
    .await
    .map_err(|e| e.to_string())??; // First '?' handles spawn_blocking error, second handles inner String error

    // 2. We now have 'res' as HakoResponse. Serialize it to MessagePack.
    // rmp_serde::to_vec_named(&res).map_err(|e| format!("Serialization error: {}", e))
    to_binary_payload(&res)
}


fn build_query_from_input(input: &QueryInput) -> Result<Query, String> {
    let mut query = Query::new(&input.collection);
    // ponytail: per-query blob deferral (list views skip image reads).
    query.defer_blobs = input.defer_blobs;

    // 1. CRITICAL FIX: Inject doc_id_filter into the Query filters.
    // The TS client often sends this for single-document snapshots or targeted queries.
    // If we don't add this here, targeted queries return the whole collection.
    if let Some(ref id) = input.doc_id_filter {
        if !id.is_empty() {
            query = query.where_filter("id", Operator::Eq, Value::String(id.clone()));
        }
    }

    // 2. Add Standard Filters (This handles the 'in' operator values)
    for filter in &input.filters {
        let val = json_value_to_value(&filter.value)?;
        query = query.where_filter(
            &filter.field, 
            map_operator(&filter.op), 
            val
        );
    }

    // 2. Add OR groups
    if let Some(groups) = &input.or_groups {
        for group in groups {
            let filters: Vec<hakodb::query::filter::Filter> = group.iter()
                .map(|f: &FilterInput| -> Result<hakodb::query::filter::Filter, String> { 
                    Ok(hakodb::query::filter::Filter { 
                        field: f.field.clone(), 
                        op: map_operator(&f.op), 
                        value: json_value_to_value(&f.value)? 
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            query.or_groups.push(filters);
        }
    }

    // 3. Sorting and Pagination
    // if let Some(order) = &input.order_by { query = query.order_by(&order.field, order.ascending); }
    if let Some(ref orders) = &input.order_by {
        for order in orders {
            query = query.order_by(&order.field, order.ascending);
        }
    }
    
    if let Some(limit) = input.limit { query = query.limit(limit); }
    if let Some(offset) = input.offset { query = query.offset(offset); }

    // 5. Select/Projection
    if let Some(proj) = &input.projection { query = query.select_fields(proj.clone()); }

    // 4. Cursor support
    if let Some(v) = &input.start_at { query.start_at = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.start_after { query.start_after = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.end_at { query.end_at = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.end_before { query.end_before = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }

    Ok(query)
}


fn execute_query_input(db: &Hako, input: &QueryInput) -> Result<Vec<serde_json::Value>, String> {
    // USE THE NEW HELPER
    let query = build_query_from_input(input)?;

    // 1. Parallel Zero-Copy Projection Path
    if let Some(projection) = &input.projection {
        if !projection.is_empty() {
            let rows = db.query_projected_zero_copy(query.clone(), projection).map_err(|e| e.to_string())?;
            // Use rayon to parallelize JSON construction
            use rayon::prelude::*;
            return Ok(rows.into_par_iter()
                .map(|(id, fields)| projection_fields_to_json(&id, fields).unwrap_or(serde_json::Value::Null))
                .collect());
        }
    }

    // 2. Parallel Standard Query Path
    let rows = db.query(query).map_err(|e| e.to_string())?;
    
    use rayon::prelude::*;
    Ok(rows.into_par_iter()
        .map(|(id, doc)| doc_to_json_value(&id, &doc).unwrap_or(serde_json::Value::Null))
        .collect())
}

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

fn json_to_doc(v: &serde_json::Value) -> Result<HakoDoc, String> {
    let obj = v.as_object().ok_or("document must be object")?;
    let mut doc = HakoDoc::default();
    for (k, val) in obj {
        doc.insert(k.clone(), json_value_to_value(val)?);
    }
    Ok(doc)
}

fn json_to_vec(v: &serde_json::Value) -> Result<Vec<(String, Value)>, String> {
    let obj = v.as_object().ok_or("updates must be object")?;
    let mut out = Vec::new();
    for (k, val) in obj {
        out.push((k.clone(), json_value_to_value(val)?));
    }
    Ok(out)
}

fn json_value_to_value(v: &serde_json::Value) -> Result<Value, String> {
    Value::from_json(v.clone())
}

fn doc_to_json_value(id: &str, doc: &HakoDoc) -> Result<serde_json::Value, String> {
    // Ok(doc.to_json())
    let mut json = doc.to_json();
    if let Some(obj) = json.as_object_mut() {
        // Force the ID into the JSON response
        obj.insert("id".to_string(), serde_json::Value::String(id.to_string()));
    }
    Ok(json)
}

fn projection_fields_to_json(id: &str, fields: Vec<(String, Value)>) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
    map.insert("id".to_string(), serde_json::Value::String(id.to_string()));
    for (k, v) in fields {
        map.insert(k, value_to_json(&v)?);
    }
    Ok(serde_json::Value::Object(map))
}

fn value_to_json(v: &Value) -> Result<serde_json::Value, String> {
    match v {
        Value::Null | Value::ServerTimestamp => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int(i) => Ok(serde_json::Value::Number((*i).into())),
        Value::Float(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number).ok_or("invalid float".into()),
        Value::String(s) => Ok(serde_json::Value::String(s.clone())),
        Value::Binary(bytes) => {
            Ok(serde_json::json!(bytes)) 
        }
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
        // ADD THIS ARM:
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
}

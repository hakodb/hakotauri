//! Mesh (LAN) sync Tauri commands: start/stop/status/peers/restore.
//!
//! This is the SDK home of the toggle flow consumers used to hand-roll
//! against `hakodb::net_sync` directly (e.g. tokocepat-tauri `sync.rs`).
//! The two consumer-owned answers the old code reached for elsewhere are
//! now command arguments, so the SDK never touches app code:
//!
//! - `self_id`: the device identity (the old code read it from the license
//!   HWID module — pass it in).
//! - `room_key` / `excluded`: `None` falls back to the on-disk prefs
//!   (`app_state/sync_group.room_key`, default `"default"`) and
//!   `["app_state"]` respectively.
//!
//! Register in the consumer:
//! ```ignore
//! tauri::generate_handler![
//!     hakotauri::net_sync::toggle_net_sync,
//!     hakotauri::net_sync::get_sync_status,
//!     // ...
//! ]
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex;

use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

use crate::gateway::HakoGateway;

// ponytail: re-export, not wrapper types — one NetSyncer everywhere.
pub use hakodb::net_sync::{NetSyncer, NetworkStatus, SyncStatus};

/// Prefs live in ordinary docs so they survive restarts; the collections
/// themselves stay sync-excluded (see `Hako::sync_collections`), so prefs
/// never leak to peers.
pub const APP_STATE_COLLECTION: &str = "app_state";
pub const NET_PREFS_DOC: &str = "sync_prefs";
pub const NET_GROUP_DOC: &str = "sync_group";
pub const FALLBACK_ROOM_KEY: &str = "default";

pub const EVENT_SYNC_ON: &str = "sync_on";
pub const EVENT_SYNC_OFF: &str = "sync_off";

pub struct NetSyncState {
    pub syncer: Arc<Mutex<Option<NetSyncer>>>,
    pub app_handle: AppHandle,
}

impl NetSyncState {
    pub fn new(app_handle: AppHandle) -> Self {
        Self {
            syncer: Arc::new(Mutex::new(None)),
            app_handle,
        }
    }
}

fn read_room_key(gateway: &HakoGateway) -> Option<String> {
    gateway
        .db
        .get(APP_STATE_COLLECTION, NET_GROUP_DOC)
        .ok()
        .flatten()
        .and_then(|d| d.get("room_key").map(|v| v.to_json()))
        .and_then(|j| j.as_str().map(|s| s.to_string()))
        .filter(|k| !k.is_empty())
}

#[tauri::command]
pub async fn toggle_net_sync(
    enabled: bool,
    port: u16,
    self_id: String,
    room_key: Option<String>,
    excluded: Option<Vec<String>>,
    state: State<'_, NetSyncState>,
    gateway: State<'_, HakoGateway>,
) -> Result<String, String> {
    let mut syncer_lock = state.syncer.lock().await;
    let app = state.app_handle.clone();

    // OPTIMIZATION: Check if the state is already the same
    let current_config = gateway
        .db
        .get(APP_STATE_COLLECTION, NET_PREFS_DOC)
        .ok()
        .flatten();
    let doc = current_config.unwrap_or_default();
    let current_enabled = doc
        .get("enabled")
        .and_then(|v| v.to_json().as_bool())
        .unwrap_or(false);

    if enabled != current_enabled {
        let mut prefs = HakoDoc::default();
        prefs.insert("enabled", Value::Bool(enabled));
        let _ = gateway.db.put(APP_STATE_COLLECTION, NET_PREFS_DOC, &prefs);
    }

    if !enabled {
        if !syncer_lock.is_none() {
            if let Some(s) = syncer_lock.take() {
                s.stop();
            }
        }
        let _ = app.emit(EVENT_SYNC_OFF, ());
        return Ok("OFF".into());
    }

    // Only start if not already started
    if syncer_lock.is_none() {
        // 1. Acquire Multicast Lock ONLY when starting
        #[cfg(target_os = "android")]
        acquire_multicast_lock();

        let db = Arc::clone(&gateway.db);
        // Room key: explicit arg wins, else the group room persisted by the
        // app's Create/Join Group flow, else the fallback. app_state is
        // excluded from sync and encrypted at rest, so the key never leaves
        // this device through replication (room members know it by design —
        // knowing the PIN *is* the join credential, enforced by room_hash).
        let room_key = room_key
            .filter(|k| !k.is_empty())
            .or_else(|| read_room_key(&gateway))
            .unwrap_or_else(|| FALLBACK_ROOM_KEY.to_string());
        let excluded = excluded.unwrap_or_else(|| vec![APP_STATE_COLLECTION.to_string()]);

        let new_syncer = NetSyncer::new(db, &self_id, &room_key, excluded);
        new_syncer.start(port).await.map_err(|e| e.to_string())?;

        *syncer_lock = Some(new_syncer);
        let _ = app.emit(EVENT_SYNC_ON, ());
    }

    Ok("ON".into())
}

#[tauri::command]
pub async fn get_sync_status(
    state: State<'_, NetSyncState>,
) -> Result<Option<NetworkStatus>, String> {
    let lock = state.syncer.lock().await;
    match lock.as_ref() {
        // NetSyncer::status() is public in the library implementation
        Some(s) => Ok(Some(s.status())),
        None => Ok(None),
    }
}

#[tauri::command]
pub async fn check_sync_security_exists(
    collection: String,
    gateway: State<'_, HakoGateway>,
) -> Result<bool, String> {
    Ok(gateway
        .db
        .get(&collection, "config")
        .map_err(|e| e.to_string())?
        .is_some())
}

#[tauri::command]
pub async fn list_network_peers(
    security_collection: String,
    state: State<'_, NetSyncState>,
    gateway: State<'_, HakoGateway>,
) -> Result<Vec<serde_json::Value>, String> {
    // 1. Get the Live Peers (raw HWIDs)
    let syncer_guard = state.syncer.lock().await;
    let live_ids = match syncer_guard.as_ref() {
        Some(s) => s.status().known_peers,
        None => Vec::new(),
    };

    if live_ids.is_empty() {
        return Ok(Vec::new());
    }

    // 2. Map the Vec<String> of IDs to Vec<Value> for the query
    let id_values: Vec<Value> = live_ids
        .iter()
        .map(|id| Value::String(id.clone()))
        .collect();

    // 3. Get Authorized Peers using the "id" virtual field
    let query =
        Query::new(&security_collection).where_filter("id", Operator::In, Value::Array(id_values));

    let db_results = gateway.db.query(query).map_err(|e| e.to_string())?;

    // 4. Create a lookup map for the results found in the database
    let mut db_map = HashMap::new();
    for (id, doc) in db_results {
        db_map.insert(id, doc);
    }

    // 5. Construct the final list based ONLY on live_ids
    let mut final_list = Vec::new();
    for id in live_ids {
        let db_doc = db_map.get(&id);

        final_list.push(serde_json::json!({
            "id": id,
            "is_online": true,
            // Status: use DB value if exists, otherwise "new_device"
            "status": db_doc.and_then(|d| d.get("status"))
                .map(|v| v.to_json())
                .unwrap_or(serde_json::json!("new_device")),
            // Name: use DB name if exists, otherwise "Perangkat Baru"
            "name": db_doc.and_then(|d| d.get("name"))
                .map(|v| v.to_json())
                .unwrap_or(serde_json::json!("Perangkat Baru"))
        }));
    }

    Ok(final_list)
}

/// Restore half of a local reset. For each collection wiped by local-only
/// deletes: purge tombstones (drops the collection version so the next
/// handshake pulls peer state) and clear local-only marks (so restored docs
/// replicate normally again). Emits nothing — peers are untouched. Call
/// BEFORE toggle_net_sync(true) when re-enabling sync after a reset.
/// No-op on collections without tombstones/marks.
#[tauri::command]
pub async fn prepare_sync_restore(
    collections: Vec<String>,
    gateway: State<'_, HakoGateway>,
) -> Result<Vec<String>, String> {
    let mut out = Vec::with_capacity(collections.len());
    for col in collections {
        let purged = gateway
            .db
            .vacuum_collection(&col)
            .map_err(|e| e.to_string())?;
        gateway.db.replicate_collection(&col);
        out.push(format!("{}:{}", col, purged));
    }
    Ok(out)
}

#[tauri::command]
pub async fn bootstrap_sync(
    port: u16,
    self_id: String,
    excluded: Option<Vec<String>>,
    state: State<'_, NetSyncState>,
    gateway: State<'_, HakoGateway>,
) -> Result<(), String> {
    // 1. Check DB if sync was previously enabled
    let config = gateway
        .db
        .get(APP_STATE_COLLECTION, NET_PREFS_DOC)
        .ok()
        .flatten();
    if let Some(doc) = config {
        if let Some(Value::Bool(true)) = doc.get("enabled") {
            // Pass through to the existing toggle logic
            let _ = toggle_net_sync(true, port, self_id, None, excluded, state, gateway).await;
        }
    }
    Ok(())
}

#[cfg(target_os = "android")]
pub fn acquire_multicast_lock() {
    use jni::objects::JObject;

    let res = (|| {
        let ctx = ndk_context::android_context();
        let vm = unsafe { jni::JavaVM::from_raw(ctx.vm().cast()) }.ok()?;
        let mut env = vm.attach_current_thread().ok()?;

        let context = unsafe { JObject::from_raw(ctx.context().cast()) };
        let wifi_service_str = env.new_string("wifi").ok()?;

        let wifi_manager = env.call_method(
            &context,
            "getSystemService",
            "(Ljava/lang/String;)Ljava/lang/Object;",
            &[(&wifi_service_str).into()]
        ).ok()?.l().ok()?;

        let lock_tag = env.new_string("tokocepat_sync_lock").ok()?;

        let mcast_lock = env.call_method(
            &wifi_manager,
            "createMulticastLock",
            "(Ljava/lang/String;)Landroid/net/wifi/WifiManager$MulticastLock;",
            &[(&lock_tag).into()]
        ).ok()?.l().ok()?;

        let _ = env.call_method(&mcast_lock, "acquire", "()V", &[]);
        Some(())
    })();

    if res.is_none() {
        eprintln!("Failed to acquire Multicast Lock. Sync may not work on this WiFi.");
    }
}

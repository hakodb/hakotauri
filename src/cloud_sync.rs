//! Cloud sync Tauri commands: toggle/status/peers/config/groups/bootstrap.
//!
//! This is the SDK home of the cloud flow consumers used to hand-roll
//! against `hakodb::cloud_sync` directly (e.g. tokocepat-tauri
//! `cloud_sync.rs`). The one consumer-owned answer — `self_id`, the device
//! identity the old code read from the license HWID module — is now a
//! command argument, so the SDK never touches app code.
//!
//! Prefs layout is unchanged (`app_state/cloud_sync_prefs` + the
//! non-replicating `__cloud_client` creds doc), so existing installs keep
//! their saved config across the migration.
//!
//! Register in the consumer:
//! ```ignore
//! tauri::generate_handler![
//!     hakotauri::cloud_sync::toggle_cloud_sync,
//!     hakotauri::cloud_sync::get_cloud_sync_status,
//!     // ...
//! ]
//! ```

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex;

use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;

use crate::gateway::HakoGateway;

// ponytail: re-export, not wrapper types — one CloudSync everywhere.
pub use hakodb::cloud_sync::{
    hash_api_key, CloudStatus, CloudSync, CloudSyncMode, PeerView, GROUPS_COLLECTION,
};

pub const APP_STATE_COLLECTION: &str = "app_state";
pub const CLOUD_PREFS_DOC: &str = "cloud_sync_prefs";

pub const EVENT_CLOUD_SYNC_ON: &str = "cloud_sync_on";
pub const EVENT_CLOUD_SYNC_OFF: &str = "cloud_sync_off";

pub struct CloudSyncState {
    pub syncer: Arc<Mutex<Option<CloudSync>>>,
    pub app_handle: AppHandle,
}

impl CloudSyncState {
    pub fn new(app_handle: AppHandle) -> Self {
        Self {
            syncer: Arc::new(Mutex::new(None)),
            app_handle,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudSyncConfig {
    pub enabled: bool,
    pub mode: CloudSyncMode,
    pub server_url: Option<String>,
    pub bind_addr: Option<String>,
    pub room_name: Option<String>,
    pub room_key: Option<String>,
    pub auth_token: String,
    /// Group API key presented at handshake for `registered` groups.
    /// Stored in app_state (encrypted cols) — never logged.
    #[serde(default)]
    pub api_key: Option<String>,
}

fn mode_to_str(mode: &CloudSyncMode) -> String {
    match mode {
        CloudSyncMode::Server => "server".to_string(),
        CloudSyncMode::Client => "client".to_string(),
    }
}

fn parse_mode(mode: &str) -> CloudSyncMode {
    if mode.eq_ignore_ascii_case("server") {
        CloudSyncMode::Server
    } else {
        CloudSyncMode::Client
    }
}

/// Client-side cloud credentials. `_`-prefixed so it never replicates:
/// `list_collections()` (which feeds the cloud outbound tailers) skips
/// underscore collections. The group API key must not leave the device —
/// unlike `app_state`, which replicates per room by design.
pub const CLOUD_KEYS_COLLECTION: &str = "__cloud_client";

fn save_api_key(gateway: &HakoGateway, api_key: Option<&str>) {
    let mut doc = HakoDoc::default();
    if let Some(k) = api_key.filter(|k| !k.is_empty()) {
        doc.insert("api_key", Value::String(k.to_string()));
    }
    let _ = gateway.db.put(CLOUD_KEYS_COLLECTION, "creds", &doc);
}

fn load_api_key(gateway: &HakoGateway) -> Option<String> {
    // One-time migration from the old app_state location.
    if let Ok(Some(old)) = gateway.db.get(APP_STATE_COLLECTION, CLOUD_PREFS_DOC) {
        if let Some(k) = old
            .get("api_key")
            .map(|v| v.to_json())
            .as_ref()
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|k| !k.is_empty())
        {
            save_api_key(gateway, Some(&k));
            // Scrub the replicating copy.
            let mut prefs = HakoDoc::default();
            for (field, value) in &old.fields {
                if field.as_ref() != "api_key" {
                    prefs.insert(field.to_string(), value.clone());
                }
            }
            let _ = gateway.db.put(APP_STATE_COLLECTION, CLOUD_PREFS_DOC, &prefs);
            return Some(k);
        }
    }
    gateway
        .db
        .get(CLOUD_KEYS_COLLECTION, "creds")
        .ok()
        .flatten()
        .and_then(|doc| {
            doc.get("api_key")
                .map(|v| v.to_json())
                .as_ref()
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .filter(|k| !k.is_empty())
}

fn save_config(gateway: &HakoGateway, config: &CloudSyncConfig) {
    let mut prefs = HakoDoc::default();
    prefs.insert("enabled", Value::Bool(config.enabled));
    prefs.insert("mode", Value::String(mode_to_str(&config.mode)));
    if let Some(v) = &config.server_url {
        prefs.insert("server_url", Value::String(v.clone()));
    }
    if let Some(v) = &config.bind_addr {
        prefs.insert("bind_addr", Value::String(v.clone()));
    }
    if let Some(v) = &config.room_name {
        prefs.insert("room_name", Value::String(v.clone()));
    }
    if let Some(v) = &config.room_key {
        prefs.insert("room_key", Value::String(v.clone()));
    }
    prefs.insert("auth_token", Value::String(config.auth_token.clone()));
    // NOTE: api_key is NOT stored here — app_state replicates per room.
    // It lives in __cloud_client (see save_api_key/load_api_key).
    let _ = gateway.db.put(APP_STATE_COLLECTION, CLOUD_PREFS_DOC, &prefs);
}

/// Start/stop Cloud Sync. Mirrors the net_sync toggle flow. In
/// client mode, `server_url` points to the central server (ws:// or wss://).
/// In server mode, `bind_addr` is the bind address to host the cloud hub.
#[tauri::command]
pub async fn toggle_cloud_sync(
    enabled: bool,
    mode: String,
    server_url: Option<String>,
    bind_addr: Option<String>,
    room_name: Option<String>,
    room_key: Option<String>,
    auth_token: String,
    api_key: Option<String>,
    self_id: String,
    state: State<'_, CloudSyncState>,
    gateway: State<'_, HakoGateway>,
) -> Result<String, String> {
    let mut syncer_lock = state.syncer.lock().await;
    let app = state.app_handle.clone();
    let mode = parse_mode(&mode);

    // Effective API key: freshly supplied wins, otherwise the stored one.
    // Stored outside app_state so it never replicates to the room/server.
    if let Some(k) = api_key.as_deref().filter(|k| !k.is_empty()) {
        save_api_key(&gateway, Some(k));
    }
    let api_key = api_key
        .filter(|k| !k.is_empty())
        .or_else(|| load_api_key(&gateway));

    let config = CloudSyncConfig {
        enabled,
        mode: mode.clone(),
        server_url: server_url.clone(),
        bind_addr: bind_addr.clone(),
        room_name: room_name.clone(),
        room_key: room_key.clone(),
        auth_token: auth_token.clone(),
        api_key: api_key.clone().filter(|k| !k.is_empty()),
    };

    // OPTIMIZATION: Check if the state is already the same
    let current_config = gateway
        .db
        .get(APP_STATE_COLLECTION, CLOUD_PREFS_DOC)
        .ok()
        .flatten();
    let doc = current_config.unwrap_or_default();
    let current_enabled = doc
        .get("enabled")
        .and_then(|v| v.to_json().as_bool())
        .unwrap_or(false);

    if enabled != current_enabled {
        save_config(&gateway, &config);
    }

    if !enabled {
        if !syncer_lock.is_none() {
            if let Some(s) = syncer_lock.take() {
                s.stop();
            }
        }
        let _ = app.emit(EVENT_CLOUD_SYNC_OFF, ());
        return Ok("OFF".into());
    }

    // Only start if not already started
    if syncer_lock.is_none() {
        let db = Arc::clone(&gateway.db);
        let api_key = config.api_key.clone();

        let new_syncer = match mode {
            CloudSyncMode::Server => {
                let bind = bind_addr
                    .clone()
                    .unwrap_or_else(|| "0.0.0.0:8056".to_string());
                let cs = CloudSync::server(db, &self_id, &auth_token);
                // Presented at every (re)connect handshake; set before start
                // so the first handshake already carries it.
                cs.set_api_key(api_key.clone());
                cs.start(&bind)
                    .await
                    .map_err(|e| format!("cloud sync server start failed: {}", e))?;
                cs
            }
            CloudSyncMode::Client => {
                let url = server_url
                    .clone()
                    .unwrap_or_else(|| "ws://127.0.0.1:8056".to_string());
                let room_name = room_name.unwrap_or_else(|| "default".to_string());
                let room_key = room_key.unwrap_or_default();
                let cs = CloudSync::client(db, &self_id, &room_name, &room_key, &auth_token);
                cs.set_api_key(api_key.clone());
                cs.start(&url)
                    .await
                    .map_err(|e| format!("cloud sync client start failed: {}", e))?;
                cs
            }
        };

        *syncer_lock = Some(new_syncer);
        let _ = app.emit(EVENT_CLOUD_SYNC_ON, ());
    }

    Ok("ON".into())
}

#[tauri::command]
pub async fn get_cloud_sync_status(
    state: State<'_, CloudSyncState>,
) -> Result<Option<CloudStatus>, String> {
    let lock = state.syncer.lock().await;
    Ok(lock.as_ref().map(|s| s.status()))
}

/// Connected peers (server: all room members; client: the uplink if tracked).
#[tauri::command]
pub async fn get_cloud_peers(state: State<'_, CloudSyncState>) -> Result<Vec<PeerView>, String> {
    let lock = state.syncer.lock().await;
    Ok(lock.as_ref().map(|s| s.peer_list()).unwrap_or_default())
}

/// Saved prefs for prefilling the settings UI. Secrets included: the UI
/// needs the stored API key to re-present it for editing.
#[tauri::command]
pub async fn get_cloud_config(
    gateway: State<'_, HakoGateway>,
) -> Result<Option<CloudSyncConfig>, String> {
    let doc = match gateway.db.get(APP_STATE_COLLECTION, CLOUD_PREFS_DOC) {
        Ok(Some(d)) => d,
        _ => return Ok(None),
    };
    let str_field = |k: &str| {
        doc.get(k)
            .map(|v| v.to_json())
            .as_ref()
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };
    Ok(Some(CloudSyncConfig {
        enabled: doc
            .get("enabled")
            .map(|v| v.to_json())
            .as_ref()
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        mode: parse_mode(str_field("mode").as_deref().unwrap_or("client")),
        server_url: str_field("server_url"),
        bind_addr: str_field("bind_addr"),
        room_name: str_field("room_name"),
        room_key: str_field("room_key"),
        auth_token: str_field("auth_token").unwrap_or_default(),
        api_key: load_api_key(&gateway),
    }))
}

// --- Group security API (server-side `__groups` policy) ---
//
// A room whose name has no policy row (or `mode: "open"`) admits everyone
// (historic behavior). `mode: "registered"` admits only clients
// presenting a valid API key (+ listed members when non-empty).
// Only the key HASH is stored; the raw key is shown once at creation.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupPolicy {
    pub room_name: String,
    pub mode: String,
    pub has_key: bool,
    pub members: Vec<String>,
}

fn read_group(gateway: &HakoGateway, room_name: &str) -> GroupPolicy {
    let (mode, has_key, members) = match gateway.db.get(GROUPS_COLLECTION, room_name) {
        Ok(Some(doc)) => {
            let mode = doc
                .get("mode")
                .map(|v| v.to_json())
                .as_ref()
                .and_then(|v| v.as_str())
                .unwrap_or("open")
                .to_string();
            let has_key = doc
                .get("api_key_hash")
                .map(|v| v.to_json())
                .as_ref()
                .and_then(|v| v.as_str())
                .map(|s| !s.is_empty())
                .unwrap_or(false);
            let members = match doc.get("members") {
                Some(Value::Array(items)) => items
                    .iter()
                    .filter_map(|v| {
                        v.to_json()
                            .as_str()
                            .map(|s| s.to_string())
                    })
                    .collect(),
                _ => Vec::new(),
            };
            (mode, has_key, members)
        }
        _ => ("open".to_string(), false, Vec::new()),
    };
    GroupPolicy {
        room_name: room_name.to_string(),
        mode,
        has_key,
        members,
    }
}

#[tauri::command]
pub async fn cloud_group_get(
    room_name: String,
    gateway: State<'_, HakoGateway>,
) -> Result<GroupPolicy, String> {
    let room_name = room_name.trim().to_string();
    if room_name.is_empty() {
        return Err("Nama room tidak boleh kosong.".to_string());
    }
    Ok(read_group(gateway.inner(), &room_name))
}

/// Creates a fresh 256-bit API key (64 hex chars). The caller must store the
/// returned raw key somewhere safe — only its hash is persisted.
#[tauri::command]
pub async fn cloud_group_new_key() -> Result<String, String> {
    use uuid::Uuid;
    Ok(format!(
        "{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    ))
}

/// Sets a room's policy. `api_key`: raw key to hash+store (rotation), or None
/// to keep the existing hash. Switching to `registered` requires a usable key
/// (passed now or already stored).
#[tauri::command]
pub async fn cloud_group_set(
    room_name: String,
    mode: String,
    api_key: Option<String>,
    members: Option<Vec<String>>,
    gateway: State<'_, HakoGateway>,
) -> Result<GroupPolicy, String> {
    group_set_inner(
        gateway.inner(),
        &room_name,
        &mode,
        api_key,
        members,
    )
}

fn group_set_inner(
    gateway: &HakoGateway,
    room_name: &str,
    mode: &str,
    api_key: Option<String>,
    members: Option<Vec<String>>,
) -> Result<GroupPolicy, String> {
    let room_name = room_name.trim().to_string();
    if room_name.is_empty() {
        return Err("Nama room tidak boleh kosong.".to_string());
    }
    let mode = mode.trim().to_lowercase();
    if mode != "open" && mode != "registered" {
        return Err("Mode harus \"open\" atau \"registered\".".to_string());
    }
    let current = read_group(gateway, &room_name);

    let mut doc = HakoDoc::default();
    doc.insert("mode", Value::String(mode.clone()));
    // Keep existing hash unless a new raw key is supplied.
    let mut has_key = current.has_key;
    if let Some(raw) = api_key.map(|k| k.trim().to_string()).filter(|k| !k.is_empty()) {
        if raw.len() < 16 {
            return Err("API key minimal 16 karakter.".to_string());
        }
        doc.insert("api_key_hash", Value::String(hash_api_key(&raw)));
        has_key = true;
    } else if let Ok(Some(old)) = gateway.db.get(GROUPS_COLLECTION, &room_name) {
        if let Some(h) = old.get("api_key_hash") {
            doc.insert("api_key_hash", h.clone());
        }
    }
    if mode == "registered" && !has_key {
        return Err("Mode registered butuh API key — buat/generate dulu.".to_string());
    }
    let members: Vec<String> = members
        .unwrap_or(current.members)
        .into_iter()
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty())
        .collect();
    doc.insert(
        "members",
        Value::Array(members.iter().map(|m| Value::String(m.clone())).collect()),
    );
    gateway
        .db
        .put(GROUPS_COLLECTION, &room_name, &doc)
        .map_err(|e| e.to_string())?;
    Ok(read_group(gateway, &room_name))
}

/// Adds a client id to a room's member allowlist (registered groups only
/// enforce it when the list is non-empty).
#[tauri::command]
pub async fn cloud_group_add_member(
    room_name: String,
    client_id: String,
    gateway: State<'_, HakoGateway>,
) -> Result<GroupPolicy, String> {
    let client_id = client_id.trim().to_string();
    if client_id.is_empty() {
        return Err("Client ID tidak boleh kosong.".to_string());
    }
    let gw = gateway.inner();
    let mut policy = read_group(gw, room_name.trim());
    if !policy.members.iter().any(|m| m == &client_id) {
        policy.members.push(client_id);
    }
    Ok(group_set_inner(
        gw,
        &policy.room_name,
        &policy.mode,
        None,
        Some(policy.members),
    )?)
}

/// Removes a client id from a room's member allowlist.
#[tauri::command]
pub async fn cloud_group_remove_member(
    room_name: String,
    client_id: String,
    gateway: State<'_, HakoGateway>,
) -> Result<GroupPolicy, String> {
    let gw = gateway.inner();
    let policy = read_group(gw, room_name.trim());
    let members: Vec<String> = policy
        .members
        .into_iter()
        .filter(|m| m != client_id.trim())
        .collect();
    Ok(group_set_inner(
        gw,
        &policy.room_name,
        &policy.mode,
        None,
        Some(members),
    )?)
}

/// Re-enable Cloud Sync on startup if it was previously enabled.
#[tauri::command]
pub async fn bootstrap_cloud_sync(
    self_id: String,
    state: State<'_, CloudSyncState>,
    gateway: State<'_, HakoGateway>,
) -> Result<(), String> {
    let config = gateway
        .db
        .get(APP_STATE_COLLECTION, CLOUD_PREFS_DOC)
        .ok()
        .flatten();
    if let Some(doc) = config {
        let enabled = doc
            .get("enabled")
            .and_then(|v| v.to_json().as_bool())
            .unwrap_or(false);
        if enabled {
            let str_field = |k: &str| {
                doc.get(k)
                    .map(|v| v.to_json())
                    .as_ref()
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            };
            let mode = str_field("mode").unwrap_or_else(|| "client".to_string());
            let server_url = str_field("server_url");
            let bind_addr = str_field("bind_addr");
            let room_name = str_field("room_name");
            let room_key = str_field("room_key");
            let auth_token = str_field("auth_token").unwrap_or_default();
            // load_api_key also migrates the old app_state location.
            let api_key = load_api_key(&gateway);

            let _ = toggle_cloud_sync(
                true,
                mode,
                server_url,
                bind_addr,
                room_name,
                room_key,
                auth_token,
                api_key,
                self_id,
                state,
                gateway,
            )
            .await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod cloud_group_tests {
    use super::*;
    use hakodb::config::HakoConfig;
    use hakodb::engine::Hako;

    fn temp_gateway(name: &str) -> (HakoGateway, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("hakotauri-cloud-test-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = Hako::open(dir.join("t.db"), HakoConfig::default()).unwrap();
        (HakoGateway::new(db), dir)
    }

    #[test]
    fn registered_group_roundtrip() {
        let (gw, dir) = temp_gateway("reg");
        // registered without key must fail
        assert!(group_set_inner(&gw, "toko", "registered", None, None).is_err());
        // with key: ok, hash stored (never raw), members kept
        let p = group_set_inner(
            &gw,
            "toko",
            "registered",
            Some("kunci-rahasia-123456".to_string()),
            Some(vec!["cabang-1".to_string()]),
        )
        .unwrap();
        assert_eq!(p.mode, "registered");
        assert!(p.has_key);
        assert_eq!(p.members, vec!["cabang-1".to_string()]);
        // stored doc holds the hash, not the raw key
        let doc = gw.db.get(GROUPS_COLLECTION, "toko").unwrap().unwrap();
        let stored = doc
            .get("api_key_hash")
            .map(|v| v.to_json())
            .as_ref()
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        assert_eq!(stored, hash_api_key("kunci-rahasia-123456"));
        assert!(!stored.contains("rahasia"));
        // rotation keeps members when omitted
        let p2 = group_set_inner(&gw, "toko", "open", None, None).unwrap();
        assert_eq!(p2.mode, "open");
        assert!(p2.has_key);
        assert_eq!(p2.members, vec!["cabang-1".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_group_needs_nothing() {
        let (gw, dir) = temp_gateway("open");
        let p = group_set_inner(&gw, "bebas", "open", None, None).unwrap();
        assert_eq!(p.mode, "open");
        assert!(!p.has_key);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn api_key_migrates_out_of_app_state() {
        let (gw, dir) = temp_gateway("mig");
        // Old layout: key inside replicating app_state prefs.
        let mut prefs = HakoDoc::default();
        prefs.insert("enabled", Value::Bool(true));
        prefs.insert("api_key", Value::String("lama-1234567890".to_string()));
        gw.db.put(APP_STATE_COLLECTION, CLOUD_PREFS_DOC, &prefs).unwrap();
        // First load migrates + scrubs.
        assert_eq!(load_api_key(&gw).as_deref(), Some("lama-1234567890"));
        let after = gw.db.get(APP_STATE_COLLECTION, CLOUD_PREFS_DOC).unwrap().unwrap();
        assert!(after.get("api_key").is_none());
        assert!(load_api_key(&gw).is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

# hakotauri

> Part of [**HakoDB**](https://github.com/hakodb/hakodb) — embedded Firestore-style document DB in Rust. The engine + C ABI live in `hakodb/hakodb`; this repo holds the Tauri gateway crate (pairs with [`hakotaurits`](https://github.com/hakodb/hakotaurits)).

Tauri gateway commands for
HakoDB: exposes the engine
(get/set/query/watch/index admin) as binary (MessagePack) Tauri commands
for the `@hakodb/tauri` TypeScript client.

## Compatibility

| hakotauri | hako core | tauri |
|---|---|---|
| 0.2.0 | `hakodb 0.8.23+` (crates.io) | =2.10.3 (pinned triple, see below) |

## Register

```rust
tauri::Builder::default()
    .manage(hakotauri::HakoGateway::new(db))
    .manage(hakotauri::net_sync::NetSyncState::new(app_handle.clone()))
    .manage(hakotauri::cloud_sync::CloudSyncState::new(app_handle.clone()))
    .invoke_handler(tauri::generate_handler![
        hakotauri::gateway::hako_exec,
        hakotauri::net_sync::toggle_net_sync,
        hakotauri::net_sync::get_sync_status,
        hakotauri::net_sync::list_network_peers,
        hakotauri::net_sync::prepare_sync_restore,
        hakotauri::net_sync::bootstrap_sync,
        hakotauri::cloud_sync::toggle_cloud_sync,
        hakotauri::cloud_sync::get_cloud_sync_status,
        hakotauri::cloud_sync::get_cloud_peers,
        hakotauri::cloud_sync::get_cloud_config,
        hakotauri::cloud_sync::cloud_group_get,
        hakotauri::cloud_sync::cloud_group_new_key,
        hakotauri::cloud_sync::cloud_group_set,
        hakotauri::cloud_sync::cloud_group_add_member,
        hakotauri::cloud_sync::cloud_group_remove_member,
        hakotauri::cloud_sync::bootstrap_cloud_sync,
    ])
```

Use the **module path** (`gateway::hako_exec`), not the root
re-export: the `#[command]` wrapper macro resolves through the same
module as the function. (The root re-export covers types and direct
calls.)

## Watch hot path

Subscriptions prebuild a filter plan once (`Hako::plan_for_watch`)
and match per-event bytes with zero decode
(`Hako::matches_watch`) — the same cost as the former in-tree
implementation; only the location moved.

## Pinned triple

`tauri =2.10.3` + `tauri-macros =2.5.5` + `tauri-runtime =2.10.1` — the
exact set the in-tree gateway build was green on. Newer macros (2.6.x)
emit a doubled `#[macro_export]` that current rustc rejects (E0255),
and a newer runtime breaks tauri 2.10.3 (Send vs Send+Sync drift).
Do not bump one without rebuilding against all three.

## Build

```sh
cargo build --offline  # all deps in the shared cargo cache
```

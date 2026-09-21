# hako-tauri

Tauri gateway commands for
HakoDB: exposes the engine
(get/set/query/watch/index admin) as binary (MessagePack) Tauri commands
for the `@hakodb/tauri` TypeScript client.

## Compatibility

| hako-tauri | hako core | tauri |
|---|---|---|
| 0.1.1 | `cloud_sync` branch / `v0.8.20`+ release asset | =2.10.3 (pinned triple, see below) |

## Register

```rust
tauri::Builder::default()
    .manage(hako_tauri::HakoGateway::new(db))
    .invoke_handler(tauri::generate_handler![
        hako_tauri::gateway::hako_exec
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

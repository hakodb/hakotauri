//! Tauri gateway commands for the HakoDB embedded document engine.
//!
//! The command itself lives in the `gateway` module (a submodule on purpose: the
//! `#[command]` macro emits a same-named helper macro that collides at the
//! crate root on current rustc — see the submodule note there). Re-exported
//! here for ergonomics, but register the **module path** with Tauri:
//!
//! ```ignore
//! tauri::generate_handler![hakotauri::gateway::hako_exec]
//! ```
//!
//! The `#[command]` wrapper macro resolves through the same module path,
//! so the root re-export below is for types and direct calls only.
//!
//! [`net_sync`] and [`cloud_sync`] hold the mesh/cloud toggle flows, so
//! consumers register SDK commands instead of driving `hakodb::net_sync` /
//! `hakodb::cloud_sync` directly.

mod gateway;
#[cfg(feature = "cloud-sync")]
pub mod cloud_sync;
#[cfg(feature = "net-sync")]
pub mod net_sync;

pub use gateway::{
    hako_exec, AggregateKind, BatchInput, BatchMutationKind, CompositeFieldInput,
    DeltaKind, DeltaPayload, DocumentChange, FilterInput, FilterOperator, HakoGateway,
    HakoOp, HakoResponse, OrderByInput, QueryAction, RawRow,
};

/// Engine types re-exported so consumers don't need a direct `hakodb`
/// dependency for the in-process API (bootstrap `Hako::open`, backend-owned
/// handle, tests). Versions unify through this crate's pin; features are
/// chosen via `[features]` (default = net-sync + cloud-sync, never
/// socket-sync unless asked).
pub use hakodb::{
    config::{DurabilityMode, HakoConfig},
    document::{hako_doc::HakoDoc, value::Value},
    engine::{BatchMutation, Hako},
    error::HakoError,
    index::composite::definition::SortDirection,
    query::{filter::Operator, query::Query},
};
/// Unix-socket peering (opt-in via `socket-sync`; unix-only like the
/// engine module — absent on Windows by design, not by omission).
#[cfg(all(unix, feature = "socket-sync"))]
pub use hakodb::socket_sync::SocketSync;

//! Tauri gateway commands for the HakoDB embedded document engine.
//!
//! The command itself lives in [`gateway`] (a submodule on purpose: the
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
pub mod cloud_sync;
pub mod net_sync;

pub use gateway::{
    hako_exec, AggregateKind, BatchInput, BatchMutationKind, CompositeFieldInput,
    DeltaKind, DeltaPayload, DocumentChange, FilterInput, FilterOperator, HakoGateway,
    HakoOp, HakoResponse, OrderByInput, QueryAction, RawRow,
};

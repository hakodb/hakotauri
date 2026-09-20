//! Tauri gateway commands for the FireLite embedded document engine.
//!
//! The command itself lives in [`gateway`] (a submodule on purpose: the
//! `#[command]` macro emits a same-named helper macro that collides at the
//! crate root on current rustc — see the submodule note there). Re-exported
//! here for ergonomics, but register the **module path** with Tauri:
//!
//! ```ignore
//! tauri::generate_handler![firelite_tauri::gateway::firelite_exec]
//! ```
//!
//! The `#[command]` wrapper macro resolves through the same module path,
//! so the root re-export below is for types and direct calls only.

mod gateway;

pub use gateway::{
    firelite_exec, AggregateKind, BatchInput, BatchMutationKind, CompositeFieldInput,
    DeltaKind, DeltaPayload, DocumentChange, FilterInput, FilterOperator, FireLiteGateway,
    FireLiteOp, FireLiteResponse, OrderByInput, QueryAction, RawRow,
};

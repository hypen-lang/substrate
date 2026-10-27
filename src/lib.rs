#![doc = include_str!("../README.md")]
//! Liquid SQL: typed relation occurrences, additive schemas, durable snapshot transactions.
pub mod auth;
pub mod capability;
pub mod catalog;
mod codec;
mod codegen;
mod compiled;
pub mod demo;
pub mod disk_snapshot;
mod durable;
mod dx;
mod engine;
#[cfg(unix)]
pub mod local;
pub mod remote;
pub mod replication;
pub mod schema_control;
pub mod service;
mod session_frame;
pub mod views;
mod visibility;
pub use codegen::GeneratedCode;
pub use visibility::Visibility;
mod manifest;
pub use dx::{Fields, Record, RetryPolicy, RowPage, TypedId};
pub use manifest::{SchemaManifest, SchemaPlan};
mod model;
pub mod nested;
pub mod paged;
pub mod partition;
pub mod profiling;
mod query;
mod runtime;
pub use runtime::RuntimeDescriptor;
mod datafusion_sql;
mod sql;
mod storage;
mod wire;

pub use durable::{
    CheckpointInfo, CheckpointPolicy, DatabaseOptions, PersistenceStats, RecoveryInfo, StorageInfo,
    StorageOptions,
};
pub use engine::{
    CheckpointCompletion, Checkpointer, CheckpointerOptions, CheckpointerStats, Database,
    GroupCommitOptions, GroupCommitStats, Session, Snapshot, Stats, Transaction,
};
pub use model::*;
pub use query::Table;
pub use sql::{
    QueryLimits, SqlQuery, SqlSnapshot, SqlTable, SqlTransaction, SqliteQuery, SqliteSnapshot,
};

#[cfg(feature = "consensus")]
pub mod consensus;

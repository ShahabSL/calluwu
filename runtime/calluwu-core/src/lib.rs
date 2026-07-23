//! Provider-neutral realtime execution engine for Calluwu voice agents.
//!
//! The crate keeps the latency-sensitive session path separate from control-plane
//! persistence. A [`supervisor::ShardSupervisor`] multiplexes bounded
//! [`session::SessionActor`] instances, while provider and event interfaces make
//! external services replaceable without changing the wire protocol.

pub mod domain;
pub mod error;
pub mod event;
pub mod manifest;
pub mod protocol;
pub mod provider;
pub mod server;
pub mod session;
pub mod supervisor;
pub mod tool;

pub use error::{Result, RuntimeError};

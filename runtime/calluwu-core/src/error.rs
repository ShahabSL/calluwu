use std::borrow::Cow;

use serde_json::{Map, Value};
use thiserror::Error;

/// Failures that can cross a runtime component boundary.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("invalid request: {0}")]
    InvalidRequest(Cow<'static, str>),
    #[error("protocol violation: {0}")]
    Protocol(Cow<'static, str>),
    #[error("runtime generation mismatch: expected {expected}, received {received}")]
    GenerationMismatch { expected: u64, received: u64 },
    #[error("session state does not permit this operation: {0}")]
    InvalidState(Cow<'static, str>),
    #[error("runtime shard is at capacity")]
    AtCapacity,
    #[error("runtime shard is draining")]
    Draining,
    #[error("session already admitted")]
    SessionConflict,
    #[error("bounded {lane} lane is full")]
    MailboxFull { lane: &'static str },
    #[error("event spool is full")]
    EventSpoolFull,
    #[error("outbound client queue is full")]
    OutputBackpressure,
    #[error("provider stage {stage} failed: {message}")]
    Provider {
        stage: &'static str,
        message: String,
    },
    #[error("tool execution failed: {0}")]
    Tool(String),
    #[error("event delivery failed: {0}")]
    EventDelivery(String),
    #[error("I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("internal runtime invariant failed: {0}")]
    Internal(Cow<'static, str>),
}

impl RuntimeError {
    /// Stable machine-readable code used in realtime error messages.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) => "invalid_request",
            Self::Protocol(_) => "protocol_error",
            Self::GenerationMismatch { .. } => "runtime_generation_mismatch",
            Self::InvalidState(_) => "invalid_session_state",
            Self::AtCapacity => "runtime_at_capacity",
            Self::Draining => "runtime_draining",
            Self::SessionConflict => "session_conflict",
            Self::MailboxFull { .. } => "runtime_overloaded",
            Self::EventSpoolFull => "event_spool_full",
            Self::OutputBackpressure => "client_backpressure",
            Self::Provider { .. } => "provider_error",
            Self::Tool(_) => "tool_error",
            Self::EventDelivery(_) => "event_delivery_error",
            Self::Io(_) | Self::Json(_) | Self::Internal(_) => "internal_error",
        }
    }

    /// Safe, bounded metadata for the protocol error envelope.
    #[must_use]
    pub fn details(&self) -> Option<Map<String, Value>> {
        match self {
            Self::GenerationMismatch { expected, received } => Some(Map::from_iter([
                ("expected".into(), Value::from(*expected)),
                ("received".into(), Value::from(*received)),
            ])),
            Self::MailboxFull { lane } => Some(Map::from_iter([(
                "lane".into(),
                Value::String((*lane).into()),
            )])),
            _ => None,
        }
    }

    /// Client-safe message. Internal I/O and serialization context is withheld.
    #[must_use]
    pub fn public_message(&self) -> Cow<'_, str> {
        match self {
            Self::Io(_) | Self::Json(_) | Self::Internal(_) => {
                Cow::Borrowed("The runtime encountered an internal error")
            }
            Self::Provider { .. } => Cow::Borrowed("A provider stage failed"),
            Self::Tool(_) => Cow::Borrowed("Tool execution failed"),
            Self::EventDelivery(_) => Cow::Borrowed("Runtime event delivery failed"),
            _ => Cow::Owned(self.to_string()),
        }
    }
}

/// Runtime result alias.
pub type Result<T> = std::result::Result<T, RuntimeError>;

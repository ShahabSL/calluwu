use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::future::join_all;
use serde::Serialize;
use tokio::{sync::mpsc, task::AbortHandle, time::Instant};
use uuid::Uuid;

use crate::{
    Result, RuntimeError,
    domain::TenantContext,
    event::{
        EventPipeline, EventPrivacy, EventRetryPolicy, EventSink, EventSource, EventType,
        PendingRuntimeEvent,
    },
    manifest::AgentManifest,
    protocol::RealtimeOutput,
    provider::{
        DeploymentProviderResolver, ProviderSet, RuntimeServiceAccess, ScriptedProviderResolver,
    },
    session::{SessionActor, SessionConfig, SessionHandle},
};

/// Leaves explicit margin beyond the control plane's 90-second attachment window.
pub const DEFAULT_PREPARED_SESSION_TTL: Duration = Duration::from_secs(120);

/// Capacity and spool budgets for one warm multiplexed runtime shard.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    pub max_sessions: usize,
    pub session: SessionConfig,
    pub event_spool_capacity: usize,
    pub event_batch_size: usize,
    pub event_retry: EventRetryPolicy,
    pub shutdown_grace: Duration,
    pub prepared_session_ttl: Duration,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            max_sessions: 128,
            session: SessionConfig::default(),
            event_spool_capacity: 512,
            event_batch_size: 32,
            event_retry: EventRetryPolicy::default(),
            shutdown_grace: Duration::from_secs(10),
            prepared_session_ttl: DEFAULT_PREPARED_SESSION_TTL,
        }
    }
}

impl ShardConfig {
    pub fn validate(&self) -> Result<()> {
        self.session.validate()?;
        if self.max_sessions == 0
            || self.event_spool_capacity == 0
            || self.event_batch_size == 0
            || self.event_batch_size > self.event_spool_capacity
            || self.event_retry.max_attempts == 0
            || self.event_retry.initial_backoff.is_zero()
            || self.event_retry.initial_backoff > self.event_retry.max_backoff
            || self.shutdown_grace.is_zero()
            || self.prepared_session_ttl.is_zero()
        {
            return Err(RuntimeError::InvalidRequest(
                "invalid runtime shard budget".into(),
            ));
        }
        Ok(())
    }
}

/// One admitted session's realtime endpoints.
pub struct SessionLease {
    pub handle: SessionHandle,
    pub output: mpsc::Receiver<RealtimeOutput>,
}

/// Snapshot consumed by allocator health/load polling.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadSnapshot {
    pub boot_id: String,
    pub accepting: bool,
    pub draining: bool,
    pub active_sessions: usize,
    pub prepared_sessions: usize,
    pub max_sessions: usize,
    pub available_sessions: usize,
    pub utilization: f64,
}

/// Result of draining all actors during SIGTERM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainReport {
    pub graceful: usize,
    pub forced: usize,
    pub aborted: usize,
}

/// Immutable, trusted control-plane snapshot awaiting WebSocket attachment.
pub struct SessionPreparation {
    pub context: TenantContext,
    pub manifest: AgentManifest,
    pub event_sink: Arc<dyn EventSink>,
    pub runtime_service_access: Option<RuntimeServiceAccess>,
    pub fingerprint: [u8; 32],
    pub attachment_fingerprint: [u8; 32],
}

/// Idempotent pre-admission result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparationStatus {
    Created,
    Existing,
}

struct PreparedSession {
    context: TenantContext,
    session_config: SessionConfig,
    providers: ProviderSet,
    event_sink: Arc<dyn EventSink>,
    fingerprint: [u8; 32],
    attachment_fingerprint: [u8; 32],
    expires_at: Instant,
}

struct ActiveSession {
    handle: SessionHandle,
    fingerprint: Option<[u8; 32]>,
    attachment_fingerprint: Option<[u8; 32]>,
    abort: AbortHandle,
}

/// Warm-shard owner of bounded session actors.
pub struct ShardSupervisor {
    config: ShardConfig,
    providers: ProviderSet,
    deployment_resolver: Arc<dyn DeploymentProviderResolver>,
    boot_id: String,
    accepting: AtomicBool,
    sessions: Mutex<HashMap<String, ActiveSession>>,
    prepared: Mutex<HashMap<String, PreparedSession>>,
}

impl ShardSupervisor {
    pub fn new(config: ShardConfig, providers: ProviderSet) -> Result<Arc<Self>> {
        Self::new_with_resolver(config, providers, Arc::new(ScriptedProviderResolver))
    }

    pub fn new_with_resolver(
        config: ShardConfig,
        providers: ProviderSet,
        deployment_resolver: Arc<dyn DeploymentProviderResolver>,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        Ok(Arc::new(Self {
            config,
            providers,
            deployment_resolver,
            boot_id: format!("boot_{}", Uuid::now_v7()),
            accepting: AtomicBool::new(true),
            sessions: Mutex::new(HashMap::new()),
            prepared: Mutex::new(HashMap::new()),
        }))
    }

    /// Validate and reserve a deployment snapshot before the realtime upgrade.
    pub fn prepare(self: &Arc<Self>, preparation: SessionPreparation) -> Result<PreparationStatus> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Draining);
        }
        preparation.context.validate()?;
        preparation.manifest.validate()?;
        let providers = self.deployment_resolver.resolve(
            &preparation.manifest,
            preparation.runtime_service_access.as_ref(),
        )?;
        let session_config = SessionConfig {
            max_session_duration: Duration::from_secs(
                preparation.manifest.definition.limits.max_session_seconds,
            ),
            max_history_messages: preparation.manifest.definition.limits.max_history_messages,
            sample_rate_hz: preparation.manifest.definition.voice.sample_rate_hz,
            voice_id: preparation.manifest.definition.voice.id.clone(),
            instructions: preparation.manifest.definition.instructions.clone(),
            required_capabilities: preparation.manifest.required_capabilities.clone(),
            ..self.config.session.clone()
        };
        session_config.validate()?;

        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| RuntimeError::Internal("session registry lock poisoned".into()))?;
        sessions.retain(|_, entry| !entry.handle.is_finished());
        if let Some(active) = sessions.get(&preparation.context.session_id) {
            return if active.fingerprint == Some(preparation.fingerprint)
                && active.handle.context().runtime_generation
                    == preparation.context.runtime_generation
            {
                Ok(PreparationStatus::Existing)
            } else {
                Err(RuntimeError::SessionConflict)
            };
        }

        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| RuntimeError::Internal("prepared registry lock poisoned".into()))?;
        let now = Instant::now();
        if let Some(existing) = prepared.get(&preparation.context.session_id) {
            return if existing.fingerprint == preparation.fingerprint
                && existing.context.runtime_generation == preparation.context.runtime_generation
            {
                Ok(PreparationStatus::Existing)
            } else {
                Err(RuntimeError::SessionConflict)
            };
        }
        if sessions.len().saturating_add(prepared.len()) >= self.config.max_sessions {
            return Err(RuntimeError::AtCapacity);
        }
        let prepared_session_id = preparation.context.session_id.clone();
        let prepared_fingerprint = preparation.fingerprint;
        prepared.insert(
            prepared_session_id.clone(),
            PreparedSession {
                context: preparation.context,
                session_config,
                providers,
                event_sink: preparation.event_sink,
                fingerprint: preparation.fingerprint,
                attachment_fingerprint: preparation.attachment_fingerprint,
                expires_at: now + self.config.prepared_session_ttl,
            },
        );
        drop(prepared);
        self.schedule_preparation_expiry(prepared_session_id, prepared_fingerprint);
        Ok(PreparationStatus::Created)
    }

    /// Atomically consume a prepared, generation-fenced snapshot and spawn its actor.
    pub fn validate_prepared_attachment(
        &self,
        context: &TenantContext,
        attachment_fingerprint: [u8; 32],
    ) -> Result<()> {
        context.validate()?;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| RuntimeError::Internal("session registry lock poisoned".into()))?;
        sessions.retain(|_, entry| !entry.handle.is_finished());
        if sessions.contains_key(&context.session_id) {
            return Err(RuntimeError::SessionConflict);
        }
        drop(sessions);
        let prepared = self
            .prepared
            .lock()
            .map_err(|_| RuntimeError::Internal("prepared registry lock poisoned".into()))?;
        let entry = prepared
            .get(&context.session_id)
            .ok_or_else(|| RuntimeError::InvalidState("session was not prepared".into()))?;
        validate_prepared_entry(entry, context, attachment_fingerprint)
    }

    /// Atomically consume a prepared, generation-fenced snapshot and spawn its actor.
    pub fn attach_prepared(
        &self,
        context: TenantContext,
        attachment_fingerprint: [u8; 32],
    ) -> Result<SessionLease> {
        context.validate()?;
        let mut prepared_registry = self
            .prepared
            .lock()
            .map_err(|_| RuntimeError::Internal("prepared registry lock poisoned".into()))?;
        let prepared = prepared_registry
            .get(&context.session_id)
            .ok_or_else(|| RuntimeError::InvalidState("session was not prepared".into()))?;
        validate_prepared_entry(prepared, &context, attachment_fingerprint)?;
        let prepared = prepared_registry
            .remove(&context.session_id)
            .ok_or_else(|| RuntimeError::Internal("prepared session disappeared".into()))?;
        drop(prepared_registry);
        self.admit_internal(
            context,
            prepared.session_config,
            prepared.providers,
            prepared.event_sink,
            Some(prepared.fingerprint),
            Some(prepared.attachment_fingerprint),
        )
    }

    /// Admit with the shard's immutable provider/session defaults.
    pub fn admit(
        &self,
        context: TenantContext,
        event_sink: Arc<dyn EventSink>,
    ) -> Result<SessionLease> {
        self.admit_internal(
            context,
            self.config.session.clone(),
            self.providers.clone(),
            event_sink,
            None,
            None,
        )
    }

    /// Admission hook used by deployment-aware allocators and local simulation.
    pub fn admit_with(
        &self,
        context: TenantContext,
        session_config: SessionConfig,
        providers: ProviderSet,
        event_sink: Arc<dyn EventSink>,
    ) -> Result<SessionLease> {
        self.admit_internal(context, session_config, providers, event_sink, None, None)
    }

    fn admit_internal(
        &self,
        context: TenantContext,
        session_config: SessionConfig,
        providers: ProviderSet,
        event_sink: Arc<dyn EventSink>,
        fingerprint: Option<[u8; 32]>,
        attachment_fingerprint: Option<[u8; 32]>,
    ) -> Result<SessionLease> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(RuntimeError::Draining);
        }
        context.validate()?;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| RuntimeError::Internal("session registry lock poisoned".into()))?;
        sessions.retain(|_, entry| !entry.handle.is_finished());
        if sessions.contains_key(&context.session_id) {
            return Err(RuntimeError::SessionConflict);
        }
        if sessions.len() >= self.config.max_sessions {
            return Err(RuntimeError::AtCapacity);
        }

        let events = EventPipeline::spawn_with_retry(
            event_sink,
            self.config.event_spool_capacity,
            self.config.event_batch_size,
            self.config.event_retry,
        );
        let spawned = SessionActor::spawn(session_config, context.clone(), providers, events)?;
        let abort = spawned.task.abort_handle();
        sessions.insert(
            context.session_id.clone(),
            ActiveSession {
                handle: spawned.handle.clone(),
                fingerprint,
                attachment_fingerprint,
                abort,
            },
        );
        drop(sessions);

        tokio::spawn(async move {
            match spawned.task.await {
                Ok(Ok(())) => tracing::info!(
                    session_id = %context.session_id,
                    runtime_generation = context.runtime_generation,
                    "session actor stopped"
                ),
                Ok(Err(error)) => tracing::error!(
                    session_id = %context.session_id,
                    runtime_generation = context.runtime_generation,
                    error_code = error.code(),
                    "session actor failed"
                ),
                Err(_) => tracing::error!(
                    session_id = %context.session_id,
                    runtime_generation = context.runtime_generation,
                    "session actor panicked"
                ),
            }
        });

        Ok(SessionLease {
            handle: spawned.handle,
            output: spawned.output,
        })
    }

    /// Stable for this process lifetime and changed on every runtime restart.
    #[must_use]
    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    fn schedule_preparation_expiry(self: &Arc<Self>, session_id: String, fingerprint: [u8; 32]) {
        let weak = Arc::downgrade(self);
        let ttl = self.config.prepared_session_ttl;
        tokio::spawn(async move {
            tokio::time::sleep(ttl).await;
            let Some(supervisor) = weak.upgrade() else {
                return;
            };
            let expired = {
                let Ok(mut prepared) = supervisor.prepared.lock() else {
                    tracing::error!(%session_id, "prepared registry lock poisoned during expiry");
                    return;
                };
                let should_expire = prepared.get(&session_id).is_some_and(|entry| {
                    entry.fingerprint == fingerprint && entry.expires_at <= Instant::now()
                });
                should_expire
                    .then(|| prepared.remove(&session_id))
                    .flatten()
            };
            if let Some(prepared) = expired {
                publish_prepared_failure(prepared, "preparation_expired").await;
            }
        });
    }

    /// Cancel a prepared or active cloud session after generation and binding validation.
    /// Missing state is a successful replay; stale state can never cancel a newer actor.
    pub async fn cancel_session(
        &self,
        context: &TenantContext,
        attachment_fingerprint: [u8; 32],
    ) -> Result<()> {
        self.stop_session(context, attachment_fingerprint, "control_plane_cancel")
            .await
    }

    /// Reap an attached actor when its sole realtime transport disconnects.
    pub async fn disconnect_session(
        &self,
        context: &TenantContext,
        attachment_fingerprint: [u8; 32],
    ) -> Result<()> {
        self.stop_session(context, attachment_fingerprint, "transport_closed")
            .await
    }

    async fn stop_session(
        &self,
        context: &TenantContext,
        attachment_fingerprint: [u8; 32],
        reason: &'static str,
    ) -> Result<()> {
        context.validate()?;
        let active = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| RuntimeError::Internal("session registry lock poisoned".into()))?;
            sessions.retain(|_, entry| !entry.handle.is_finished());
            if let Some(entry) = sessions.get(&context.session_id) {
                validate_active_entry(entry, context, attachment_fingerprint)?;
                Some((entry.handle.clone(), entry.abort.clone()))
            } else {
                None
            }
        };

        let Some((handle, abort)) = active else {
            let mut prepared = self
                .prepared
                .lock()
                .map_err(|_| RuntimeError::Internal("prepared registry lock poisoned".into()))?;
            if let Some(entry) = prepared.get(&context.session_id) {
                validate_prepared_entry(entry, context, attachment_fingerprint)?;
                prepared.remove(&context.session_id);
            }
            return Ok(());
        };

        match handle.cancel(reason) {
            Ok(()) | Err(RuntimeError::InvalidState(_)) => {}
            Err(RuntimeError::MailboxFull { .. }) => handle.force_cancel(),
            Err(error) => return Err(error),
        }
        let mut waiter = handle.clone();
        if tokio::time::timeout(self.config.shutdown_grace, waiter.wait_finished())
            .await
            .is_err()
        {
            handle.force_cancel();
            let mut forced_waiter = handle.clone();
            if tokio::time::timeout(Duration::from_secs(1), forced_waiter.wait_finished())
                .await
                .is_err()
            {
                abort.abort();
                let mut aborted_waiter = handle.clone();
                tokio::time::timeout(Duration::from_secs(1), aborted_waiter.wait_finished())
                    .await
                    .map_err(|_| {
                        RuntimeError::Internal("session actor did not stop after abort".into())
                    })?;
            }
        }
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| RuntimeError::Internal("session registry lock poisoned".into()))?;
        if sessions.get(&context.session_id).is_some_and(|entry| {
            entry.handle.context().runtime_generation == context.runtime_generation
        }) {
            sessions.remove(&context.session_id);
        }
        Ok(())
    }

    /// Current capacity after reaping completed actors.
    pub fn load(&self) -> Result<LoadSnapshot> {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| RuntimeError::Internal("session registry lock poisoned".into()))?;
        sessions.retain(|_, entry| !entry.handle.is_finished());
        let active_sessions = sessions.len();
        let prepared = self
            .prepared
            .lock()
            .map_err(|_| RuntimeError::Internal("prepared registry lock poisoned".into()))?;
        let prepared_sessions = prepared.len();
        let reserved_sessions = active_sessions.saturating_add(prepared_sessions);
        let accepting = self.accepting.load(Ordering::Acquire);
        Ok(LoadSnapshot {
            boot_id: self.boot_id.clone(),
            accepting,
            draining: !accepting,
            active_sessions,
            prepared_sessions,
            max_sessions: self.config.max_sessions,
            available_sessions: self.config.max_sessions.saturating_sub(reserved_sessions),
            utilization: reserved_sessions as f64 / self.config.max_sessions as f64,
        })
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
            && self.load().is_ok_and(|load| load.available_sessions > 0)
    }

    /// Stop new admission immediately while existing sessions continue draining.
    pub fn begin_draining(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    /// Stop admission, request normal completion, then force only after the deadline.
    pub async fn drain(&self) -> Result<DrainReport> {
        self.begin_draining();
        let prepared: Vec<_> = self
            .prepared
            .lock()
            .map_err(|_| RuntimeError::Internal("prepared registry lock poisoned".into()))?
            .drain()
            .map(|(_, prepared)| prepared)
            .collect();
        join_all(
            prepared.into_iter().map(|prepared| {
                publish_prepared_failure(prepared, "runtime_draining_before_attach")
            }),
        )
        .await;
        let handles: Vec<_> = self
            .sessions
            .lock()
            .map_err(|_| RuntimeError::Internal("session registry lock poisoned".into()))?
            .values()
            .filter(|entry| !entry.handle.is_finished())
            .map(|entry| entry.handle.clone())
            .collect();
        for handle in &handles {
            if let Err(error) = handle.cancel("runtime_draining") {
                tracing::warn!(
                    session_id = %handle.context().session_id,
                    error_code = error.code(),
                    "graceful session drain enqueue failed"
                );
            }
        }

        let waits = handles.iter().cloned().map(|mut handle| async move {
            handle.wait_finished().await;
        });
        if tokio::time::timeout(self.config.shutdown_grace, join_all(waits))
            .await
            .is_ok()
        {
            return Ok(DrainReport {
                graceful: handles.len(),
                forced: 0,
                aborted: 0,
            });
        }

        let mut forced = 0;
        for handle in &handles {
            if !handle.is_finished() {
                forced += 1;
                handle.force_cancel();
            }
        }
        let forced_waits = handles.iter().cloned().map(|mut handle| async move {
            handle.wait_finished().await;
        });
        let forced_completed = tokio::time::timeout(Duration::from_secs(1), join_all(forced_waits))
            .await
            .is_ok();
        let mut aborted = 0;
        if !forced_completed {
            let sessions = self
                .sessions
                .lock()
                .map_err(|_| RuntimeError::Internal("session registry lock poisoned".into()))?;
            for entry in sessions
                .values()
                .filter(|entry| !entry.handle.is_finished())
            {
                aborted += 1;
                entry.abort.abort();
            }
        }
        Ok(DrainReport {
            graceful: handles.len().saturating_sub(forced),
            forced,
            aborted,
        })
    }
}

async fn publish_prepared_failure(prepared: PreparedSession, code: &'static str) {
    let mut pipeline = EventPipeline::spawn(prepared.event_sink, 2, 1);
    let event = PendingRuntimeEvent::new(
        EventType::SessionFailed,
        prepared.context.correlation_id,
        None,
        EventSource::Runtime,
        EventPrivacy::Internal,
        std::collections::BTreeMap::from([
            ("code".into(), serde_json::Value::String(code.into())),
            (
                "runtimeGeneration".into(),
                serde_json::Value::from(prepared.context.runtime_generation),
            ),
        ]),
    );
    if let Err(error) = pipeline
        .publish_terminal(event, Duration::from_secs(1))
        .await
    {
        tracing::error!(
            error_code = error.code(),
            "failed to enqueue preparation failure"
        );
        return;
    }
    if let Err(error) = pipeline.close().await {
        tracing::error!(
            error_code = error.code(),
            "failed to deliver preparation failure"
        );
    }
}

fn validate_active_entry(
    active: &ActiveSession,
    context: &TenantContext,
    attachment_fingerprint: [u8; 32],
) -> Result<()> {
    let active_context = active.handle.context();
    if active_context.runtime_generation != context.runtime_generation {
        return Err(RuntimeError::GenerationMismatch {
            expected: active_context.runtime_generation,
            received: context.runtime_generation,
        });
    }
    if active_context.organization_id != context.organization_id
        || active_context.project_id != context.project_id
        || active_context.deployment_id != context.deployment_id
    {
        return Err(RuntimeError::InvalidRequest(
            "active session tenant context does not match cancellation".into(),
        ));
    }
    if active.attachment_fingerprint != Some(attachment_fingerprint) {
        return Err(RuntimeError::InvalidRequest(
            "active session ingest binding does not match cancellation".into(),
        ));
    }
    Ok(())
}

fn validate_prepared_entry(
    prepared: &PreparedSession,
    context: &TenantContext,
    attachment_fingerprint: [u8; 32],
) -> Result<()> {
    if prepared.expires_at <= Instant::now() {
        return Err(RuntimeError::InvalidState(
            "prepared session expired".into(),
        ));
    }
    if prepared.context.runtime_generation != context.runtime_generation {
        return Err(RuntimeError::GenerationMismatch {
            expected: prepared.context.runtime_generation,
            received: context.runtime_generation,
        });
    }
    if prepared.context.organization_id != context.organization_id
        || prepared.context.project_id != context.project_id
        || prepared.context.deployment_id != context.deployment_id
    {
        return Err(RuntimeError::InvalidRequest(
            "prepared session tenant context does not match attachment".into(),
        ));
    }
    if prepared.attachment_fingerprint != attachment_fingerprint {
        return Err(RuntimeError::InvalidRequest(
            "prepared session ingest binding does not match attachment".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::{EventType, MemoryEventSink},
        manifest::{AgentManifest, CONTRACT_VERSION},
        provider::ProviderSet,
    };

    fn context(id: &str) -> TenantContext {
        TenantContext {
            organization_id: "org_12345678".into(),
            project_id: "prj_12345678".into(),
            deployment_id: "dep_12345678".into(),
            session_id: id.into(),
            runtime_generation: 1,
            correlation_id: "test".into(),
        }
    }

    #[test]
    fn default_prepared_session_ttl_has_margin_over_api_attach_window() {
        const API_ATTACH_WINDOW: Duration = Duration::from_secs(90);

        assert_eq!(
            ShardConfig::default().prepared_session_ttl,
            DEFAULT_PREPARED_SESSION_TTL
        );
        assert_eq!(DEFAULT_PREPARED_SESSION_TTL, Duration::from_secs(120));
        assert!(DEFAULT_PREPARED_SESSION_TTL > API_ATTACH_WINDOW);
    }

    fn manifest() -> AgentManifest {
        AgentManifest::parse_json(
            &serde_json::json!({
                "contractVersion": CONTRACT_VERSION,
                "definition": {
                    "name": "test-agent",
                    "instructions": "Help.",
                    "providers": {
                        "speechToText": {"provider":"scripted", "model":"scripted-v1"},
                        "reasoning": {"provider":"scripted", "model":"scripted-v1"},
                        "textToSpeech": {"provider":"scripted", "model":"scripted-v1"}
                    },
                    "voice": {"id":"test"}
                },
                "requiredCapabilities": [
                    "batch-stt", "streaming-reasoning", "streaming-tts"
                ],
                "artifact": {
                    "sha256":"0".repeat(64), "sizeBytes":1, "format":"javascript-esm"
                }
            })
            .to_string(),
        )
        .expect("manifest")
    }

    #[tokio::test]
    async fn enforces_shard_capacity_and_drain() {
        let supervisor = ShardSupervisor::new(
            ShardConfig {
                max_sessions: 1,
                shutdown_grace: Duration::from_secs(1),
                ..ShardConfig::default()
            },
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let _lease = supervisor
            .admit(
                context("ses_12345678"),
                Arc::new(MemoryEventSink::default()),
            )
            .expect("first session");
        let second = supervisor.admit(
            context("ses_87654321"),
            Arc::new(MemoryEventSink::default()),
        );
        assert!(matches!(second, Err(RuntimeError::AtCapacity)));
        let report = supervisor.drain().await.expect("drain");
        assert_eq!(report.forced, 0);
        assert!(!supervisor.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn prepared_session_ttl_releases_capacity_and_emits_failure() {
        let supervisor = ShardSupervisor::new(
            ShardConfig {
                max_sessions: 1,
                prepared_session_ttl: Duration::from_secs(10),
                ..ShardConfig::default()
            },
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let sink = Arc::new(MemoryEventSink::default());
        supervisor
            .prepare(SessionPreparation {
                context: context("ses_12345678"),
                manifest: manifest(),
                event_sink: sink.clone(),
                runtime_service_access: None,
                fingerprint: [1; 32],
                attachment_fingerprint: [2; 32],
            })
            .expect("prepare");
        assert_eq!(supervisor.load().expect("load").prepared_sessions, 1);

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert_eq!(supervisor.load().expect("load").prepared_sessions, 0);
        assert!(sink.events().expect("events").iter().any(|event| {
            event.event_type == EventType::SessionFailed
                && event.payload.get("code") == Some(&serde_json::json!("preparation_expired"))
        }));
    }

    #[tokio::test]
    async fn active_cancellation_waits_for_actor_and_is_idempotent() {
        let supervisor = ShardSupervisor::new(
            ShardConfig {
                shutdown_grace: Duration::from_secs(1),
                ..ShardConfig::default()
            },
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let context = context("ses_12345678");
        let sink = Arc::new(MemoryEventSink::default());
        supervisor
            .prepare(SessionPreparation {
                context: context.clone(),
                manifest: manifest(),
                event_sink: sink.clone(),
                runtime_service_access: None,
                fingerprint: [1; 32],
                attachment_fingerprint: [2; 32],
            })
            .expect("prepare");
        let lease = supervisor
            .attach_prepared(context.clone(), [2; 32])
            .expect("attach");
        let handle = lease.handle.clone();
        supervisor
            .cancel_session(&context, [2; 32])
            .await
            .expect("cancel");
        assert!(handle.is_finished());
        assert_eq!(supervisor.load().expect("load").active_sessions, 0);
        supervisor
            .cancel_session(&context, [2; 32])
            .await
            .expect("idempotent replay");
        let events = sink.events().expect("events");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == EventType::SessionCanceled)
                .count(),
            1
        );
        assert!(
            !events
                .iter()
                .any(|event| event.event_type == EventType::SessionCompleted)
        );
    }

    #[test]
    fn boot_id_is_stable_per_supervisor_and_distinct_across_process_incarnations() {
        let first = ShardSupervisor::new(
            ShardConfig::default(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("first");
        let second = ShardSupervisor::new(
            ShardConfig::default(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("second");
        assert_eq!(first.boot_id(), first.load().expect("load").boot_id);
        assert_ne!(first.boot_id(), second.boot_id());
    }
}

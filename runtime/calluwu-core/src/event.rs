use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
};

use crate::{Result, RuntimeError};

/// Version of all initial semantic event payload envelopes.
pub const EVENT_VERSION: u8 = 1;

/// Stable semantic event names shared with the control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    #[serde(rename = "session.created")]
    SessionCreated,
    #[serde(rename = "session.provisioning")]
    SessionProvisioning,
    #[serde(rename = "session.ready")]
    SessionReady,
    #[serde(rename = "session.started")]
    SessionStarted,
    #[serde(rename = "session.interrupted")]
    SessionInterrupted,
    #[serde(rename = "session.completed")]
    SessionCompleted,
    #[serde(rename = "session.canceled")]
    SessionCanceled,
    #[serde(rename = "session.failed")]
    SessionFailed,
    #[serde(rename = "speech.started")]
    SpeechStarted,
    #[serde(rename = "speech.partial")]
    SpeechPartial,
    #[serde(rename = "speech.final")]
    SpeechFinal,
    #[serde(rename = "reasoning.started")]
    ReasoningStarted,
    #[serde(rename = "reasoning.delta")]
    ReasoningDelta,
    #[serde(rename = "reasoning.completed")]
    ReasoningCompleted,
    #[serde(rename = "tool.started")]
    ToolStarted,
    #[serde(rename = "tool.completed")]
    ToolCompleted,
    #[serde(rename = "tool.failed")]
    ToolFailed,
    #[serde(rename = "tts.started")]
    TtsStarted,
    #[serde(rename = "tts.first_audio")]
    TtsFirstAudio,
    #[serde(rename = "tts.completed")]
    TtsCompleted,
    #[serde(rename = "audio.overrun")]
    AudioOverrun,
}

/// Component that originated an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    Control,
    Runtime,
    Provider,
    Tool,
}

/// Data classification carried independently from retention policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventPrivacy {
    Internal,
    Pii,
    Sensitive,
}

/// Runtime event before the authoritative session coordinator assigns sequence/ID.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingRuntimeEvent {
    #[serde(rename = "type")]
    pub event_type: EventType,
    pub version: u8,
    /// Strictly increasing within one session/runtime generation.
    pub producer_sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    pub correlation_id: String,
    pub occurred_at: String,
    pub source: EventSource,
    pub privacy: EventPrivacy,
    pub payload: BTreeMap<String, Value>,
}

impl PendingRuntimeEvent {
    #[must_use]
    pub fn new(
        event_type: EventType,
        correlation_id: String,
        causation_id: Option<String>,
        source: EventSource,
        privacy: EventPrivacy,
        payload: BTreeMap<String, Value>,
    ) -> Self {
        Self {
            event_type,
            version: EVENT_VERSION,
            producer_sequence: 0,
            causation_id,
            correlation_id,
            occurred_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            source,
            privacy,
            payload,
        }
    }
}

/// Event after the single-writer coordinator assigns identity and order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEnvelope {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: EventType,
    pub version: u8,
    pub organization_id: String,
    pub project_id: String,
    pub deployment_id: String,
    pub session_id: String,
    pub sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    pub correlation_id: String,
    pub occurred_at: String,
    pub source: EventSource,
    pub privacy: EventPrivacy,
    pub payload: BTreeMap<String, Value>,
}

/// Whether an event delivery failure is safe to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSinkErrorKind {
    Transient,
    Permanent,
}

/// Sanitized event sink failure.
#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct EventSinkError {
    pub kind: EventSinkErrorKind,
    pub message: String,
}

/// Durable-ingest adapter. Implementations receive bounded batches off the fast path.
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn publish(
        &self,
        events: &[PendingRuntimeEvent],
    ) -> std::result::Result<(), EventSinkError>;
}

/// Session-scoped HTTP sink for the control-plane runtime-ingest endpoint.
pub struct HttpEventSink {
    client: reqwest::Client,
    url: Url,
    bearer_token: String,
    runtime_generation: u64,
}

impl HttpEventSink {
    pub fn new(url: &str, bearer_token: String, runtime_generation: u64) -> Result<Self> {
        if bearer_token.is_empty() || bearer_token.len() > 4_096 {
            return Err(RuntimeError::InvalidRequest(
                "runtime ingest token is missing or too large".into(),
            ));
        }
        let url = Url::parse(url)
            .map_err(|_| RuntimeError::InvalidRequest("runtime ingest URL is invalid".into()))?;
        let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"));
        if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
            return Err(RuntimeError::InvalidRequest(
                "runtime ingest URL must use HTTPS (HTTP is loopback-only)".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(500))
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|_| RuntimeError::Internal("failed to initialize HTTP client".into()))?;
        Ok(Self {
            client,
            url,
            bearer_token,
            runtime_generation,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IngestRequest<'a> {
    runtime_generation: u64,
    events: &'a [PendingRuntimeEvent],
}

#[async_trait]
impl EventSink for HttpEventSink {
    async fn publish(
        &self,
        events: &[PendingRuntimeEvent],
    ) -> std::result::Result<(), EventSinkError> {
        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(&self.bearer_token)
            .json(&IngestRequest {
                runtime_generation: self.runtime_generation,
                events,
            })
            .send()
            .await
            .map_err(|error| EventSinkError {
                kind: if error.is_timeout() || error.is_connect() {
                    EventSinkErrorKind::Transient
                } else {
                    EventSinkErrorKind::Permanent
                },
                message: "runtime event ingest request failed".into(),
            })?;

        if response.status() == StatusCode::NO_CONTENT {
            return Ok(());
        }
        let status = response.status();
        Err(EventSinkError {
            kind: if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                EventSinkErrorKind::Transient
            } else {
                EventSinkErrorKind::Permanent
            },
            message: format!("runtime event ingest returned HTTP {status}"),
        })
    }
}

/// Local sink that traces metadata only—never transcript/tool payloads.
#[derive(Debug, Default)]
pub struct TracingEventSink;

#[async_trait]
impl EventSink for TracingEventSink {
    async fn publish(
        &self,
        events: &[PendingRuntimeEvent],
    ) -> std::result::Result<(), EventSinkError> {
        for event in events {
            tracing::info!(
                event_type = ?event.event_type,
                event_version = event.version,
                correlation_id = %event.correlation_id,
                privacy = ?event.privacy,
                "runtime semantic event"
            );
        }
        Ok(())
    }
}

/// Test/simulation sink retaining exact pending events.
#[derive(Debug, Default)]
pub struct MemoryEventSink {
    events: Mutex<Vec<PendingRuntimeEvent>>,
}

impl MemoryEventSink {
    pub fn events(&self) -> Result<Vec<PendingRuntimeEvent>> {
        self.events
            .lock()
            .map(|events| events.clone())
            .map_err(|_| RuntimeError::Internal("memory event sink lock poisoned".into()))
    }
}

#[async_trait]
impl EventSink for MemoryEventSink {
    async fn publish(
        &self,
        events: &[PendingRuntimeEvent],
    ) -> std::result::Result<(), EventSinkError> {
        self.events
            .lock()
            .map_err(|_| EventSinkError {
                kind: EventSinkErrorKind::Permanent,
                message: "memory event sink unavailable".into(),
            })?
            .extend_from_slice(events);
        Ok(())
    }
}

/// Bounds and batches session events independently of the realtime actor.
pub struct EventPipeline {
    sender: Option<mpsc::Sender<QueuedRuntimeEvent>>,
    worker: Option<JoinHandle<Result<()>>>,
    ordinary_slots: Arc<Semaphore>,
    next_producer_sequence: AtomicU64,
    #[cfg(test)]
    terminal_enqueued: Arc<Notify>,
}

struct QueuedRuntimeEvent {
    event: PendingRuntimeEvent,
    // Ordinary events are limited to the configured spool size. The underlying
    // channel has one additional slot reserved exclusively for a terminal event.
    _ordinary_slot: Option<OwnedSemaphorePermit>,
}

impl QueuedRuntimeEvent {
    fn into_event(self) -> PendingRuntimeEvent {
        self.event
    }
}

/// Bounded retry budget for at-least-once event ingest.
#[derive(Debug, Clone, Copy)]
pub struct EventRetryPolicy {
    pub max_attempts: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for EventRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            initial_backoff: Duration::from_millis(25),
            max_backoff: Duration::from_secs(1),
        }
    }
}

impl EventPipeline {
    #[must_use]
    pub fn spawn(sink: Arc<dyn EventSink>, capacity: usize, batch_size: usize) -> Self {
        Self::spawn_with_retry(sink, capacity, batch_size, EventRetryPolicy::default())
    }

    #[must_use]
    pub fn spawn_with_retry(
        sink: Arc<dyn EventSink>,
        capacity: usize,
        batch_size: usize,
        retry: EventRetryPolicy,
    ) -> Self {
        assert!(capacity > 0, "event spool capacity must be positive");
        let channel_capacity = capacity
            .checked_add(1)
            .expect("event spool capacity must leave room for a terminal slot");
        assert!(batch_size > 0, "event batch size must be positive");
        assert!(
            retry.max_attempts > 0,
            "event retry attempts must be positive"
        );
        assert!(
            !retry.initial_backoff.is_zero() && retry.initial_backoff <= retry.max_backoff,
            "event retry backoff is invalid"
        );
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let worker = tokio::spawn(run_event_worker(receiver, sink, batch_size, retry));
        Self {
            sender: Some(sender),
            worker: Some(worker),
            ordinary_slots: Arc::new(Semaphore::new(capacity)),
            next_producer_sequence: AtomicU64::new(1),
            #[cfg(test)]
            terminal_enqueued: Arc::new(Notify::new()),
        }
    }

    /// Non-blocking fast-path enqueue. Full means session audit integrity is at risk.
    pub fn try_publish(&self, mut event: PendingRuntimeEvent) -> Result<()> {
        let ordinary_slot = Arc::clone(&self.ordinary_slots)
            .try_acquire_owned()
            .map_err(|_| RuntimeError::EventSpoolFull)?;
        let permit = self
            .sender
            .as_ref()
            .ok_or_else(|| RuntimeError::EventDelivery("event pipeline is closed".into()))?
            .try_reserve()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(()) => RuntimeError::EventSpoolFull,
                mpsc::error::TrySendError::Closed(()) => {
                    RuntimeError::EventDelivery("event pipeline worker stopped".into())
                }
            })?;
        let sequence = self
            .next_producer_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| RuntimeError::Internal("producer event sequence exhausted".into()))?;
        event.producer_sequence = sequence;
        permit.send(QueuedRuntimeEvent {
            event,
            _ordinary_slot: Some(ordinary_slot),
        });
        Ok(())
    }

    /// Enqueue the one terminal audit event without competing for ordinary spool capacity.
    /// Sequence allocation occurs only after the reserved channel slot is acquired.
    pub async fn publish_terminal(
        &self,
        mut event: PendingRuntimeEvent,
        enqueue_timeout: Duration,
    ) -> Result<()> {
        if !matches!(
            event.event_type,
            EventType::SessionCompleted | EventType::SessionCanceled | EventType::SessionFailed
        ) {
            return Err(RuntimeError::InvalidRequest(
                "reserved event slot only accepts terminal session events".into(),
            ));
        }
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| RuntimeError::EventDelivery("event pipeline is closed".into()))?;
        let permit = tokio::time::timeout(enqueue_timeout, sender.reserve())
            .await
            .map_err(|_| {
                RuntimeError::EventDelivery("terminal event enqueue deadline exceeded".into())
            })?
            .map_err(|_| RuntimeError::EventDelivery("event pipeline worker stopped".into()))?;
        let sequence = self
            .next_producer_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| RuntimeError::Internal("producer event sequence exhausted".into()))?;
        event.producer_sequence = sequence;
        permit.send(QueuedRuntimeEvent {
            event,
            _ordinary_slot: None,
        });
        #[cfg(test)]
        self.terminal_enqueued.notify_one();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn terminal_enqueue_observer(&self) -> Arc<Notify> {
        self.terminal_enqueued.clone()
    }

    /// Flush all accepted events before the session actor terminates.
    pub async fn close(&mut self) -> Result<()> {
        self.sender.take();
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker
            .await
            .map_err(|_| RuntimeError::EventDelivery("event worker panicked".into()))?
    }
}

async fn run_event_worker(
    mut receiver: mpsc::Receiver<QueuedRuntimeEvent>,
    sink: Arc<dyn EventSink>,
    batch_size: usize,
    retry: EventRetryPolicy,
) -> Result<()> {
    while let Some(first) = receiver.recv().await {
        let mut batch = Vec::with_capacity(batch_size);
        batch.push(first.into_event());
        while batch.len() < batch_size {
            match receiver.try_recv() {
                Ok(event) => batch.push(event.into_event()),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        let mut backoff = retry.initial_backoff;
        let mut attempt = 1_usize;
        loop {
            match sink.publish(&batch).await {
                Ok(()) => break,
                Err(error)
                    if error.kind == EventSinkErrorKind::Transient
                        && attempt < retry.max_attempts =>
                {
                    tracing::warn!(error = %error, retry_ms = backoff.as_millis(), "event ingest retry");
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(retry.max_backoff);
                    attempt += 1;
                }
                Err(error) if error.kind == EventSinkErrorKind::Transient => {
                    return Err(RuntimeError::EventDelivery(format!(
                        "runtime event ingest exhausted {} attempts: {}",
                        retry.max_attempts, error.message
                    )));
                }
                Err(error) => return Err(RuntimeError::EventDelivery(error.message)),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn bounded_pipeline_flushes_on_close() {
        let sink = Arc::new(MemoryEventSink::default());
        let mut pipeline = EventPipeline::spawn(sink.clone(), 4, 2);
        pipeline
            .try_publish(PendingRuntimeEvent::new(
                EventType::SessionReady,
                "correlation".into(),
                None,
                EventSource::Runtime,
                EventPrivacy::Internal,
                BTreeMap::new(),
            ))
            .expect("enqueue");
        pipeline.close().await.expect("flush");
        assert_eq!(sink.events().expect("events").len(), 1);
    }

    #[tokio::test]
    async fn otherwise_identical_events_receive_distinct_producer_sequences() {
        let sink = Arc::new(MemoryEventSink::default());
        let mut pipeline = EventPipeline::spawn(sink.clone(), 4, 2);
        let first = ready_event();
        let second = first.clone();
        pipeline.try_publish(first).expect("first");
        pipeline.try_publish(second).expect("second");
        pipeline.close().await.expect("flush");
        let events = sink.events().expect("events");
        assert_eq!(events[0].producer_sequence, 1);
        assert_eq!(events[1].producer_sequence, 2);
        assert_eq!(events[0].occurred_at, events[1].occurred_at);
    }

    struct GateSink {
        calls: AtomicUsize,
        events: Mutex<Vec<PendingRuntimeEvent>>,
        first_publish_started: Notify,
        release_first_publish: Notify,
        published: Notify,
    }

    impl Default for GateSink {
        fn default() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                events: Mutex::new(Vec::new()),
                first_publish_started: Notify::new(),
                release_first_publish: Notify::new(),
                published: Notify::new(),
            }
        }
    }

    #[async_trait]
    impl EventSink for GateSink {
        async fn publish(
            &self,
            events: &[PendingRuntimeEvent],
        ) -> std::result::Result<(), EventSinkError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_publish_started.notify_one();
                self.release_first_publish.notified().await;
            }
            self.events
                .lock()
                .map_err(|_| EventSinkError {
                    kind: EventSinkErrorKind::Permanent,
                    message: "gate sink unavailable".into(),
                })?
                .extend_from_slice(events);
            self.published.notify_waiters();
            Ok(())
        }
    }

    #[tokio::test]
    async fn full_spool_does_not_burn_sequence_before_terminal_event() {
        let sink = Arc::new(GateSink::default());
        let mut pipeline = EventPipeline::spawn(sink.clone(), 1, 1);

        pipeline.try_publish(ready_event()).expect("first event");
        sink.first_publish_started.notified().await;
        pipeline
            .try_publish(ready_event())
            .expect("queued event fills spool");
        assert!(matches!(
            pipeline.try_publish(ready_event()),
            Err(RuntimeError::EventSpoolFull)
        ));

        sink.release_first_publish.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let published = sink.published.notified();
                if sink.events.lock().expect("events").len() >= 2 {
                    break;
                }
                published.await;
            }
        })
        .await
        .expect("spool drained");

        pipeline
            .try_publish(PendingRuntimeEvent::new(
                EventType::SessionFailed,
                "correlation".into(),
                None,
                EventSource::Runtime,
                EventPrivacy::Internal,
                BTreeMap::new(),
            ))
            .expect("terminal event after drain");
        pipeline.close().await.expect("flush");

        let events = sink.events.lock().expect("events");
        assert_eq!(events.len(), 3);
        assert_eq!(
            events
                .iter()
                .map(|event| event.producer_sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(
            events.last().map(|event| event.event_type),
            Some(EventType::SessionFailed)
        );
    }

    struct FlakySink {
        failures_remaining: AtomicUsize,
        published: AtomicUsize,
    }

    #[async_trait]
    impl EventSink for FlakySink {
        async fn publish(
            &self,
            events: &[PendingRuntimeEvent],
        ) -> std::result::Result<(), EventSinkError> {
            if self
                .failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(EventSinkError {
                    kind: EventSinkErrorKind::Transient,
                    message: "temporary failure".into(),
                });
            }
            self.published.fetch_add(events.len(), Ordering::SeqCst);
            Ok(())
        }
    }

    fn ready_event() -> PendingRuntimeEvent {
        PendingRuntimeEvent::new(
            EventType::SessionReady,
            "correlation".into(),
            None,
            EventSource::Runtime,
            EventPrivacy::Internal,
            BTreeMap::new(),
        )
    }

    #[tokio::test]
    async fn transient_delivery_recovers_within_budget() {
        let sink = Arc::new(FlakySink {
            failures_remaining: AtomicUsize::new(2),
            published: AtomicUsize::new(0),
        });
        let mut pipeline = EventPipeline::spawn_with_retry(
            sink.clone(),
            4,
            2,
            EventRetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(2),
            },
        );
        pipeline.try_publish(ready_event()).expect("enqueue");
        pipeline.close().await.expect("eventual delivery");
        assert_eq!(sink.published.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn transient_delivery_exhaustion_is_observable() {
        let sink = Arc::new(FlakySink {
            failures_remaining: AtomicUsize::new(usize::MAX),
            published: AtomicUsize::new(0),
        });
        let mut pipeline = EventPipeline::spawn_with_retry(
            sink,
            4,
            2,
            EventRetryPolicy {
                max_attempts: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            },
        );
        pipeline.try_publish(ready_event()).expect("enqueue");
        let error = pipeline.close().await.expect_err("retry budget exhausted");
        assert_eq!(error.code(), "event_delivery_error");
    }
}

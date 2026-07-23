use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use serde_json::Value;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    Result, RuntimeError,
    domain::TenantContext,
    event::{EventPipeline, EventPrivacy, EventSource, EventType, PendingRuntimeEvent},
    protocol::{
        AudioChunkFrame, AudioChunkHeader, AudioEncoding, ClientMessage, PlayoutAckHistory,
        RealtimeEnvelope, RealtimeOutput, ServerMessage,
    },
    provider::{
        AudioInput, ConversationMessage, ProviderError, ProviderErrorKind, ProviderSet,
        ReasoningEvent, ReasoningRequest, SynthesisRequest, ToolInvocation,
    },
};

const TERMINAL_EVENT_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(1);

/// Per-session budgets. Deployment limits may only reduce the shard-wide ceilings.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub control_capacity: usize,
    pub media_capacity: usize,
    pub output_capacity: usize,
    pub max_audio_frame_bytes: usize,
    pub max_buffered_audio_bytes: usize,
    pub max_session_duration: Duration,
    pub start_timeout: Duration,
    pub max_history_messages: usize,
    pub max_history_bytes: usize,
    pub max_provider_text_delta_bytes: usize,
    pub max_response_text_bytes: usize,
    pub max_response_audio_bytes: usize,
    pub provider_event_timeout: Duration,
    pub sample_rate_hz: u32,
    pub voice_id: String,
    pub instructions: String,
    pub required_capabilities: Vec<String>,
    pub playout_history_capacity: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            control_capacity: 64,
            media_capacity: 128,
            output_capacity: 128,
            max_audio_frame_bytes: 64 * 1024,
            max_buffered_audio_bytes: 2 * 1024 * 1024,
            max_session_duration: Duration::from_secs(3_600),
            start_timeout: Duration::from_secs(30),
            max_history_messages: 100,
            max_history_bytes: 1024 * 1024,
            max_provider_text_delta_bytes: 16_000,
            max_response_text_bytes: 256 * 1024,
            max_response_audio_bytes: 16 * 1024 * 1024,
            provider_event_timeout: Duration::from_secs(15),
            sample_rate_hz: 16_000,
            voice_id: "scripted".into(),
            instructions: "Respond helpfully and concisely.".into(),
            required_capabilities: Vec::new(),
            playout_history_capacity: 64,
        }
    }
}

impl SessionConfig {
    pub fn validate(&self) -> Result<()> {
        if self.control_capacity == 0
            || self.media_capacity == 0
            || self.output_capacity == 0
            || self.max_audio_frame_bytes == 0
            || self.max_buffered_audio_bytes < self.max_audio_frame_bytes
            || self.max_history_messages < 2
            || self.max_history_bytes < 16 * 1024
            || self.max_provider_text_delta_bytes == 0
            || self.max_provider_text_delta_bytes > 16_000
            || self.max_response_text_bytes < self.max_provider_text_delta_bytes
            || self.max_response_audio_bytes < self.max_audio_frame_bytes
            || self.provider_event_timeout.is_zero()
            || self.playout_history_capacity == 0
            || !(8_000..=48_000).contains(&self.sample_rate_hz)
            || self.max_session_duration.is_zero()
            || self.start_timeout.is_zero()
        {
            return Err(RuntimeError::InvalidRequest(
                "invalid session resource budget".into(),
            ));
        }
        Ok(())
    }
}

/// Observable actor lifecycle inside a warm runtime shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPhase {
    Ready,
    Active,
    Draining,
    Completed,
    Canceled,
    Failed,
}

/// Sanitized actor outcome consumed by the realtime transport close policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionTerminalOutcome {
    Pending,
    Flushed,
    Failed,
}

enum ControlCommand {
    Client(ClientMessage),
    SttUpdate {
        input_epoch: u64,
        turn_id: String,
        result: std::result::Result<crate::provider::TranscriptSegment, ProviderError>,
    },
    SttEnded {
        input_epoch: u64,
        saw_final: bool,
    },
    Pipeline {
        response_id: String,
        epoch: u64,
        event: PipelineEvent,
    },
    Complete {
        reason: String,
    },
    Cancel {
        reason: String,
    },
}

enum MediaCommand {
    Audio(Bytes),
    OrderedControl(ClientMessage),
}

enum PipelineEvent {
    ReasoningDelta(String),
    ToolStarted {
        call_id: String,
        name: String,
        input: Value,
    },
    ToolCompleted {
        call_id: String,
        name: String,
        output: Value,
        cached: bool,
    },
    ToolFailed {
        call_id: String,
        name: String,
        code: String,
    },
    ReasoningCompleted(String),
    TtsStarted,
    Audio {
        sequence: u64,
        bytes: Bytes,
    },
    TtsCompleted,
    Completed,
    Failed(ProviderError),
}

struct CurrentResponse {
    id: String,
    epoch: u64,
    cancel: CancellationToken,
    task: JoinHandle<()>,
    pending_assistant: Option<String>,
    pending_tool: Option<PendingToolCall>,
}

struct PendingToolCall {
    call_id: String,
    name: String,
    input: Value,
}

/// Cloneable, non-blocking ingress handle for a bounded session actor.
#[derive(Clone)]
pub struct SessionHandle {
    context: TenantContext,
    control: mpsc::Sender<ControlCommand>,
    media: mpsc::Sender<MediaCommand>,
    max_audio_frame_bytes: usize,
    force_cancel: CancellationToken,
    media_overrun: CancellationToken,
    finished: watch::Receiver<bool>,
    finished_flag: Arc<AtomicBool>,
    terminal_outcome: watch::Receiver<SessionTerminalOutcome>,
}

impl SessionHandle {
    #[must_use]
    pub const fn context(&self) -> &TenantContext {
        &self.context
    }

    /// Priority-lane client control. Saturation is explicit; controls are never shed.
    pub fn try_control(&self, message: ClientMessage) -> Result<()> {
        message.validate(&self.context.session_id, self.context.runtime_generation)?;
        if matches!(
            message,
            ClientMessage::SessionStart { .. }
                | ClientMessage::InputText { .. }
                | ClientMessage::InputCommit { .. }
        ) {
            return self
                .media
                .try_send(MediaCommand::OrderedControl(message))
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => {
                        self.media_overrun.cancel();
                        RuntimeError::MailboxFull { lane: "media" }
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        RuntimeError::InvalidState("session actor has stopped".into())
                    }
                });
        }
        self.control
            .try_send(ControlCommand::Client(message))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RuntimeError::MailboxFull { lane: "control" },
                mpsc::error::TrySendError::Closed(_) => {
                    RuntimeError::InvalidState("session actor has stopped".into())
                }
            })
    }

    /// Media-lane PCM16LE audio. Saturation terminates the utterance/session fail-closed.
    pub fn try_audio(&self, audio: Bytes) -> Result<()> {
        if audio.is_empty()
            || audio.len() > self.max_audio_frame_bytes
            || !audio.len().is_multiple_of(2)
        {
            return Err(RuntimeError::Protocol(
                "binary audio frame must be nonempty, bounded PCM16LE".into(),
            ));
        }
        self.media
            .try_send(MediaCommand::Audio(audio))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    self.media_overrun.cancel();
                    RuntimeError::MailboxFull { lane: "media" }
                }
                mpsc::error::TrySendError::Closed(_) => {
                    RuntimeError::InvalidState("session actor has stopped".into())
                }
            })
    }

    /// Ask the actor to drain normally.
    pub fn end(&self, reason: impl Into<String>) -> Result<()> {
        self.control
            .try_send(ControlCommand::Complete {
                reason: reason.into(),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RuntimeError::MailboxFull { lane: "control" },
                mpsc::error::TrySendError::Closed(_) => {
                    RuntimeError::InvalidState("session actor has stopped".into())
                }
            })
    }

    /// Cancel the actor from a trusted operator or transport lifecycle path.
    pub fn cancel(&self, reason: impl Into<String>) -> Result<()> {
        self.control
            .try_send(ControlCommand::Cancel {
                reason: reason.into(),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RuntimeError::MailboxFull { lane: "control" },
                mpsc::error::TrySendError::Closed(_) => {
                    RuntimeError::InvalidState("session actor has stopped".into())
                }
            })
    }

    /// Force cancellation after a graceful shutdown deadline expires.
    pub fn force_cancel(&self) {
        self.force_cancel.cancel();
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.finished_flag.load(Ordering::Acquire)
    }

    pub async fn wait_finished(&mut self) {
        while !*self.finished.borrow() {
            if self.finished.changed().await.is_err() {
                break;
            }
        }
    }

    /// Wait until the actor confirms whether its terminal event was durably flushed.
    pub async fn wait_terminal_outcome(&mut self) -> SessionTerminalOutcome {
        loop {
            let outcome = *self.terminal_outcome.borrow_and_update();
            if outcome != SessionTerminalOutcome::Pending {
                return outcome;
            }
            if self.terminal_outcome.changed().await.is_err() {
                return SessionTerminalOutcome::Failed;
            }
        }
    }
}

/// Result of spawning an actor, consumed by the shard supervisor.
pub struct SpawnedSession {
    pub handle: SessionHandle,
    pub output: mpsc::Receiver<RealtimeOutput>,
    pub task: JoinHandle<Result<()>>,
}

struct CompletionGuard {
    flag: Arc<AtomicBool>,
    sender: watch::Sender<bool>,
    terminal_outcome: watch::Sender<SessionTerminalOutcome>,
    outcome_set: bool,
}

impl CompletionGuard {
    fn finish(&mut self, outcome: SessionTerminalOutcome) {
        self.terminal_outcome.send_replace(outcome);
        self.outcome_set = true;
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if !self.outcome_set {
            self.terminal_outcome
                .send_replace(SessionTerminalOutcome::Failed);
        }
        self.flag.store(true, Ordering::Release);
        let _result = self.sender.send(true);
    }
}

/// Single-writer session state machine with priority and media mailboxes.
pub struct SessionActor {
    config: SessionConfig,
    context: TenantContext,
    providers: ProviderSet,
    events: EventPipeline,
    phase: SessionPhase,
    control_rx: mpsc::Receiver<ControlCommand>,
    control_tx: mpsc::Sender<ControlCommand>,
    media_rx: mpsc::Receiver<MediaCommand>,
    output_tx: mpsc::Sender<RealtimeOutput>,
    force_cancel: CancellationToken,
    media_overrun: CancellationToken,
    current_response: Option<CurrentResponse>,
    response_epoch: u64,
    input_epoch: u64,
    input_cancel: Option<CancellationToken>,
    input_task: Option<JoinHandle<()>>,
    audio_buffer: BytesMut,
    speech_in_progress: bool,
    history: VecDeque<ConversationMessage>,
    history_bytes: usize,
    playout: PlayoutAckHistory,
    message_sequence: u64,
    recent_message_ids: VecDeque<String>,
    recent_message_set: HashSet<String>,
    terminal_failure_code: Option<String>,
    terminal_error: Option<RuntimeError>,
}

impl SessionActor {
    /// Validate, allocate bounded lanes, and spawn one session task.
    pub fn spawn(
        config: SessionConfig,
        context: TenantContext,
        providers: ProviderSet,
        events: EventPipeline,
    ) -> Result<SpawnedSession> {
        config.validate()?;
        context.validate()?;
        providers.ensure_capabilities(&config.required_capabilities)?;
        let (control_tx, control_rx) = mpsc::channel(config.control_capacity);
        let (media_tx, media_rx) = mpsc::channel(config.media_capacity);
        let (output_tx, output) = mpsc::channel(config.output_capacity);
        let force_cancel = CancellationToken::new();
        let media_overrun = CancellationToken::new();
        let (finished_tx, finished) = watch::channel(false);
        let (terminal_outcome_tx, terminal_outcome) =
            watch::channel(SessionTerminalOutcome::Pending);
        let finished_flag = Arc::new(AtomicBool::new(false));

        let actor = Self {
            playout: PlayoutAckHistory::new(config.playout_history_capacity),
            config: config.clone(),
            context: context.clone(),
            providers,
            events,
            phase: SessionPhase::Ready,
            control_rx,
            control_tx: control_tx.clone(),
            media_rx,
            output_tx,
            force_cancel: force_cancel.clone(),
            media_overrun: media_overrun.clone(),
            current_response: None,
            response_epoch: 0,
            input_epoch: 0,
            input_cancel: None,
            input_task: None,
            audio_buffer: BytesMut::new(),
            speech_in_progress: false,
            history: VecDeque::with_capacity(config.max_history_messages),
            history_bytes: 0,
            message_sequence: 0,
            recent_message_ids: VecDeque::with_capacity(1_024),
            recent_message_set: HashSet::with_capacity(1_024),
            terminal_failure_code: None,
            terminal_error: None,
        };
        let completion = CompletionGuard {
            flag: finished_flag.clone(),
            sender: finished_tx,
            terminal_outcome: terminal_outcome_tx,
            outcome_set: false,
        };
        let task = tokio::spawn(async move {
            let mut completion = completion;
            let result = actor.run().await;
            completion.finish(if result.is_ok() {
                SessionTerminalOutcome::Flushed
            } else {
                SessionTerminalOutcome::Failed
            });
            result
        });
        Ok(SpawnedSession {
            handle: SessionHandle {
                context,
                control: control_tx,
                media: media_tx,
                max_audio_frame_bytes: config.max_audio_frame_bytes,
                force_cancel,
                media_overrun,
                finished,
                finished_flag,
                terminal_outcome,
            },
            output,
            task,
        })
    }

    async fn run(mut self) -> Result<()> {
        let run_result = self.run_loop().await;
        let terminal_result = match &run_result {
            Err(error) => self.record_unexpected_failure(error).await,
            Ok(()) => Ok(()),
        };
        self.cancel_children();
        let event_result = self.events.close().await;
        match run_result {
            Err(error) => {
                if let Err(terminal_error) = terminal_result {
                    tracing::error!(
                        error_code = terminal_error.code(),
                        "failed to enqueue terminal session failure"
                    );
                }
                if let Err(flush_error) = event_result {
                    tracing::error!(
                        error_code = flush_error.code(),
                        "failed to flush events after actor failure"
                    );
                }
                Err(error)
            }
            Ok(()) => event_result,
        }
    }

    async fn run_loop(&mut self) -> Result<()> {
        self.emit_event(
            EventType::SessionReady,
            EventSource::Runtime,
            EventPrivacy::Internal,
            map([(
                "runtimeGeneration",
                Value::from(self.context.runtime_generation),
            )]),
        )?;
        let capabilities = self
            .providers
            .capabilities()
            .into_iter()
            .map(|capability| capability.as_str().to_owned())
            .collect();
        let envelope = self.next_server_envelope();
        self.emit_server(ServerMessage::SessionReady {
            envelope,
            capabilities,
        })?;

        let deadline = tokio::time::sleep(self.config.start_timeout);
        tokio::pin!(deadline);
        let mut active_deadline_started = false;
        loop {
            tokio::select! {
                () = self.force_cancel.cancelled() => {
                    self.fail_session("runtime_shutdown_forced", "runtime forced session shutdown")?;
                    break;
                }
                () = self.media_overrun.cancelled() => {
                    self.emit_event(
                        EventType::AudioOverrun,
                        EventSource::Runtime,
                        EventPrivacy::Internal,
                        map([("outcome", Value::String("session_failed".into()))]),
                    )?;
                    self.audio_buffer.clear();
                    self.fail_session("audio_overrun", "realtime audio mailbox overflowed")?;
                    break;
                }
                () = &mut deadline => {
                    if self.phase == SessionPhase::Ready {
                        self.fail_session(
                            "session_start_timeout",
                            "client did not start the attached session before its deadline",
                        )?;
                    } else {
                        self.complete("session_deadline").await?;
                    }
                    break;
                }
                command = self.control_rx.recv() => {
                    let Some(command) = command else {
                        self.fail_session("control_lane_closed", "control mailbox closed unexpectedly")?;
                        break;
                    };
                    if !self.handle_control(command).await? {
                        break;
                    }
                }
                media = self.media_rx.recv() => {
                    let Some(media) = media else {
                        self.fail_session("media_lane_closed", "media mailbox closed unexpectedly")?;
                        break;
                    };
                    let keep_running = match media {
                        MediaCommand::Audio(audio) => {
                            self.handle_audio(audio).await?;
                            self.phase != SessionPhase::Failed
                        }
                        MediaCommand::OrderedControl(message) => self.handle_client(message).await?,
                    };
                    if !keep_running {
                        break;
                    }
                }
            }
            if !active_deadline_started && self.phase == SessionPhase::Active {
                deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + self.config.max_session_duration);
                active_deadline_started = true;
            }
        }
        self.terminal_error.take().map_or(Ok(()), Err)
    }

    async fn handle_control(&mut self, command: ControlCommand) -> Result<bool> {
        match command {
            ControlCommand::Client(message) => self.handle_client(message).await,
            ControlCommand::SttUpdate {
                input_epoch,
                turn_id,
                result,
            } => {
                self.handle_stt_update(input_epoch, turn_id, result).await?;
                Ok(self.phase != SessionPhase::Failed)
            }
            ControlCommand::SttEnded {
                input_epoch,
                saw_final,
            } => {
                if input_epoch == self.input_epoch && !saw_final {
                    self.fail_session(
                        "stt_ended_without_final",
                        "STT ended without a final transcript",
                    )?;
                    return Ok(false);
                }
                Ok(true)
            }
            ControlCommand::Pipeline {
                response_id,
                epoch,
                event,
            } => {
                self.handle_pipeline(response_id, epoch, event).await?;
                Ok(self.phase != SessionPhase::Failed)
            }
            ControlCommand::Complete { reason } => {
                self.complete(&reason).await?;
                Ok(false)
            }
            ControlCommand::Cancel { reason } => {
                self.cancel_session(&reason).await?;
                Ok(false)
            }
        }
    }

    async fn handle_client(&mut self, message: ClientMessage) -> Result<bool> {
        message.validate(&self.context.session_id, self.context.runtime_generation)?;
        let message_id = message.envelope().message_id.clone();
        if self.is_duplicate_message(&message_id) {
            return Ok(true);
        }
        match message {
            ClientMessage::SessionStart { .. } => {
                if self.phase != SessionPhase::Ready {
                    self.send_error(RuntimeError::InvalidState(
                        "session.start requires ready state".into(),
                    ))?;
                    return Ok(true);
                }
                self.phase = SessionPhase::Active;
                self.emit_event(
                    EventType::SessionStarted,
                    EventSource::Runtime,
                    EventPrivacy::Internal,
                    map([(
                        "runtimeGeneration",
                        Value::from(self.context.runtime_generation),
                    )]),
                )?;
                let envelope = self.next_server_envelope();
                self.emit_server(ServerMessage::SessionStarted { envelope })?;
            }
            ClientMessage::InputText { text, .. } => {
                if !self.require_active()? {
                    return Ok(true);
                }
                self.audio_buffer.clear();
                self.speech_in_progress = false;
                self.begin_input();
                self.interrupt_current("text_barge_in")?;
                let turn_id = format!("turn-{}", self.input_epoch);
                self.emit_transcript(&turn_id, &text, true)?;
                self.emit_event(
                    EventType::SpeechFinal,
                    EventSource::Provider,
                    EventPrivacy::Pii,
                    map([
                        ("turnId", Value::String(turn_id)),
                        ("textBytes", Value::from(text.len() as u64)),
                    ]),
                )?;
                self.start_response(text)?;
            }
            ClientMessage::InputCommit { .. } => {
                if !self.require_active()? {
                    return Ok(true);
                }
                if self.audio_buffer.is_empty() {
                    self.send_error(RuntimeError::Protocol(
                        "input.commit requires buffered audio".into(),
                    ))?;
                    return Ok(true);
                }
                self.commit_audio();
            }
            ClientMessage::ResponseCancel { response_id, .. } => {
                if self
                    .current_response
                    .as_ref()
                    .is_some_and(|current| current.id == response_id)
                {
                    self.interrupt_current("client_cancel")?;
                }
            }
            ClientMessage::PlayoutAck {
                response_id,
                played_through_ms,
                ..
            } => {
                self.playout.record(&response_id, played_through_ms)?;
            }
            ClientMessage::SessionEnd { .. } => {
                self.complete("client_request").await?;
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn handle_audio(&mut self, audio: Bytes) -> Result<()> {
        if !self.require_active()? {
            return Ok(());
        }
        if !self.speech_in_progress {
            self.begin_input();
            self.speech_in_progress = true;
            self.interrupt_current("audio_barge_in")?;
            self.emit_event(
                EventType::SpeechStarted,
                EventSource::Runtime,
                EventPrivacy::Internal,
                map([("bufferedBytes", Value::from(0))]),
            )?;
        }
        if self.audio_buffer.len().saturating_add(audio.len())
            > self.config.max_buffered_audio_bytes
        {
            self.fail_session(
                "audio_buffer_overflow",
                "committed audio exceeded its session budget",
            )?;
            return Ok(());
        }
        self.audio_buffer.extend_from_slice(&audio);
        Ok(())
    }

    fn commit_audio(&mut self) {
        self.speech_in_progress = false;
        let audio = self.audio_buffer.split().freeze();
        let epoch = self.input_epoch;
        let turn_id = format!("turn-{epoch}");
        let cancel = self
            .input_cancel
            .as_ref()
            .map_or_else(CancellationToken::new, CancellationToken::clone);
        let provider = self.providers.speech_to_text.clone();
        let control = self.control_tx.clone();
        let sample_rate_hz = self.config.sample_rate_hz;
        let provider_event_timeout = self.config.provider_event_timeout;
        let task = tokio::spawn(async move {
            let mut stream = provider.transcribe(
                AudioInput {
                    bytes: audio,
                    sample_rate_hz,
                },
                cancel.clone(),
            );
            let mut saw_final = false;
            loop {
                let next = tokio::select! {
                    () = cancel.cancelled() => return,
                    result = tokio::time::timeout(provider_event_timeout, stream.next()) => result,
                };
                let result = match next {
                    Ok(Some(result)) => result,
                    Ok(None) => break,
                    Err(_) => {
                        let _sent = send_internal(
                            &control,
                            &cancel,
                            ControlCommand::SttUpdate {
                                input_epoch: epoch,
                                turn_id: turn_id.clone(),
                                result: Err(ProviderError {
                                    stage: "stt",
                                    kind: ProviderErrorKind::Permanent,
                                    message: "provider event deadline exceeded".into(),
                                }),
                            },
                        )
                        .await;
                        return;
                    }
                };
                let is_final = result.as_ref().is_ok_and(|segment| segment.is_final);
                if !send_internal(
                    &control,
                    &cancel,
                    ControlCommand::SttUpdate {
                        input_epoch: epoch,
                        turn_id: turn_id.clone(),
                        result,
                    },
                )
                .await
                {
                    return;
                }
                if is_final {
                    saw_final = true;
                    // A final transcript is terminal for this committed input. Dropping the
                    // stream here prevents a non-conforming provider from producing multiple
                    // user turns (and therefore multiple responses) for one commit.
                    break;
                }
            }
            let _sent = send_internal(
                &control,
                &cancel,
                ControlCommand::SttEnded {
                    input_epoch: epoch,
                    saw_final,
                },
            )
            .await;
        });
        self.input_task = Some(task);
    }

    async fn handle_stt_update(
        &mut self,
        input_epoch: u64,
        turn_id: String,
        result: std::result::Result<crate::provider::TranscriptSegment, ProviderError>,
    ) -> Result<()> {
        if input_epoch != self.input_epoch {
            return Ok(());
        }
        match result {
            Ok(segment) => {
                if segment.text.is_empty()
                    || segment.text.len() > self.config.max_provider_text_delta_bytes
                {
                    self.fail_session(
                        "stt_output_invalid",
                        "STT provider returned an invalid transcript segment",
                    )?;
                    return Ok(());
                }
                self.emit_transcript(&turn_id, &segment.text, segment.is_final)?;
                self.emit_event(
                    if segment.is_final {
                        EventType::SpeechFinal
                    } else {
                        EventType::SpeechPartial
                    },
                    EventSource::Provider,
                    EventPrivacy::Pii,
                    map([
                        ("turnId", Value::String(turn_id)),
                        ("textBytes", Value::from(segment.text.len() as u64)),
                    ]),
                )?;
                if segment.is_final {
                    self.start_response(segment.text)?;
                }
            }
            Err(error) if error.kind == ProviderErrorKind::Cancelled => {}
            Err(error) => {
                self.fail_session("stt_provider_failed", &error.message)?;
            }
        }
        Ok(())
    }

    fn start_response(&mut self, input: String) -> Result<()> {
        self.interrupt_current("superseded_response")?;
        let prior_history: Vec<ConversationMessage> = self.history.iter().cloned().collect();
        self.push_history(ConversationMessage::User(input.clone()))?;
        self.response_epoch = self.response_epoch.saturating_add(1);
        let epoch = self.response_epoch;
        let response_id = format!("response-{}-{epoch}", self.context.session_id);
        let cancel = self.force_cancel.child_token();
        self.emit_event(
            EventType::ReasoningStarted,
            EventSource::Provider,
            EventPrivacy::Internal,
            map([
                ("responseId", Value::String(response_id.clone())),
                ("epoch", Value::from(epoch)),
            ]),
        )?;

        let mut messages = prior_history;
        messages.push(ConversationMessage::User(input));
        let request = ReasoningRequest {
            instructions: self.config.instructions.clone(),
            messages,
        };
        let task = spawn_response_pipeline(
            self.control_tx.clone(),
            self.providers.clone(),
            request,
            self.config.voice_id.clone(),
            self.config.sample_rate_hz,
            self.context.session_id.clone(),
            response_id.clone(),
            epoch,
            cancel.clone(),
            PipelineLimits {
                event_timeout: self.config.provider_event_timeout,
                max_text_delta_bytes: self.config.max_provider_text_delta_bytes,
                max_response_text_bytes: self.config.max_response_text_bytes,
                max_audio_chunk_bytes: self.config.max_audio_frame_bytes,
                max_response_audio_bytes: self.config.max_response_audio_bytes,
            },
        );
        self.current_response = Some(CurrentResponse {
            id: response_id,
            epoch,
            cancel,
            task,
            pending_assistant: None,
            pending_tool: None,
        });
        Ok(())
    }

    async fn handle_pipeline(
        &mut self,
        response_id: String,
        epoch: u64,
        event: PipelineEvent,
    ) -> Result<()> {
        let is_current = self
            .current_response
            .as_ref()
            .is_some_and(|current| current.id == response_id && current.epoch == epoch);
        if !is_current {
            return Ok(());
        }
        match event {
            PipelineEvent::ReasoningDelta(text) => {
                let envelope = self.next_server_envelope();
                self.emit_server(ServerMessage::ResponseDelta {
                    envelope,
                    response_id: response_id.clone(),
                    epoch,
                    text: text.clone(),
                })?;
                self.emit_event(
                    EventType::ReasoningDelta,
                    EventSource::Provider,
                    EventPrivacy::Pii,
                    map([
                        ("responseId", Value::String(response_id)),
                        ("epoch", Value::from(epoch)),
                        ("deltaBytes", Value::from(text.len() as u64)),
                    ]),
                )?;
            }
            PipelineEvent::ToolStarted {
                call_id,
                name,
                input,
            } => {
                let current = self.current_response.as_mut().ok_or_else(|| {
                    RuntimeError::Internal("tool response state disappeared".into())
                })?;
                if current.pending_tool.is_some() {
                    return Err(RuntimeError::Provider {
                        stage: "tool",
                        message: "provider started overlapping tool calls".into(),
                    });
                }
                current.pending_tool = Some(PendingToolCall {
                    call_id: call_id.clone(),
                    name: name.clone(),
                    input,
                });
                self.emit_event(
                    EventType::ToolStarted,
                    EventSource::Tool,
                    EventPrivacy::Sensitive,
                    map([
                        ("callId", Value::String(call_id)),
                        ("toolName", Value::String(name)),
                    ]),
                )?;
            }
            PipelineEvent::ToolCompleted {
                call_id,
                name,
                output,
                cached,
            } => {
                let pending = self
                    .current_response
                    .as_mut()
                    .and_then(|current| current.pending_tool.take())
                    .ok_or_else(|| RuntimeError::Provider {
                        stage: "tool",
                        message: "tool completed without a matching call".into(),
                    })?;
                if pending.call_id != call_id || pending.name != name {
                    return Err(RuntimeError::Provider {
                        stage: "tool",
                        message: "tool completion did not match its call".into(),
                    });
                }
                self.push_tool_history_pair(pending, output)?;
                self.emit_event(
                    EventType::ToolCompleted,
                    EventSource::Tool,
                    EventPrivacy::Sensitive,
                    map([
                        ("callId", Value::String(call_id)),
                        ("toolName", Value::String(name)),
                        ("cached", Value::Bool(cached)),
                    ]),
                )?;
            }
            PipelineEvent::ToolFailed {
                call_id,
                name,
                code,
            } => {
                let pending = self
                    .current_response
                    .as_mut()
                    .and_then(|current| current.pending_tool.take())
                    .ok_or_else(|| RuntimeError::Provider {
                        stage: "tool",
                        message: "tool failed without a matching call".into(),
                    })?;
                if pending.call_id != call_id || pending.name != name {
                    return Err(RuntimeError::Provider {
                        stage: "tool",
                        message: "tool failure did not match its call".into(),
                    });
                }
                self.emit_event(
                    EventType::ToolFailed,
                    EventSource::Tool,
                    EventPrivacy::Sensitive,
                    map([
                        ("callId", Value::String(call_id)),
                        ("toolName", Value::String(name)),
                        ("code", Value::String(code)),
                    ]),
                )?;
            }
            PipelineEvent::ReasoningCompleted(text) => {
                if let Some(current) = self.current_response.as_mut() {
                    current.pending_assistant = Some(text.clone());
                }
                self.emit_event(
                    EventType::ReasoningCompleted,
                    EventSource::Provider,
                    EventPrivacy::Pii,
                    map([
                        ("responseId", Value::String(response_id)),
                        ("epoch", Value::from(epoch)),
                        ("textBytes", Value::from(text.len() as u64)),
                    ]),
                )?;
            }
            PipelineEvent::TtsStarted => {
                self.emit_event(
                    EventType::TtsStarted,
                    EventSource::Provider,
                    EventPrivacy::Internal,
                    map([
                        ("responseId", Value::String(response_id)),
                        ("epoch", Value::from(epoch)),
                    ]),
                )?;
            }
            PipelineEvent::Audio { sequence, bytes } => {
                if sequence == 0 {
                    self.emit_event(
                        EventType::TtsFirstAudio,
                        EventSource::Provider,
                        EventPrivacy::Internal,
                        map([
                            ("responseId", Value::String(response_id.clone())),
                            ("epoch", Value::from(epoch)),
                        ]),
                    )?;
                }
                let envelope = self.next_server_envelope();
                self.emit_output(RealtimeOutput::Audio(AudioChunkFrame {
                    header: AudioChunkHeader {
                        message_type: "audio.chunk".into(),
                        envelope,
                        response_id,
                        epoch,
                        sequence,
                        encoding: AudioEncoding::Pcm16Le,
                        sample_rate_hz: self.config.sample_rate_hz,
                        channels: 1,
                    },
                    audio: bytes,
                }))?;
            }
            PipelineEvent::TtsCompleted => {
                self.emit_event(
                    EventType::TtsCompleted,
                    EventSource::Provider,
                    EventPrivacy::Internal,
                    map([
                        ("responseId", Value::String(response_id)),
                        ("epoch", Value::from(epoch)),
                    ]),
                )?;
            }
            PipelineEvent::Completed => {
                let current = self.current_response.take().ok_or_else(|| {
                    RuntimeError::Internal("completed response state disappeared".into())
                })?;
                if current.pending_tool.is_some() {
                    return Err(RuntimeError::Provider {
                        stage: "tool",
                        message: "response completed with an unfinished tool call".into(),
                    });
                }
                let assistant =
                    current
                        .pending_assistant
                        .ok_or_else(|| RuntimeError::Provider {
                            stage: "reasoner",
                            message: "response completed without retained assistant text".into(),
                        })?;
                self.push_history(ConversationMessage::Assistant(assistant))?;
                let envelope = self.next_server_envelope();
                self.emit_server(ServerMessage::ResponseCompleted {
                    envelope,
                    response_id,
                    epoch,
                    interrupted: false,
                })?;
            }
            PipelineEvent::Failed(error) => {
                self.fail_session("provider_pipeline_failed", &error.message)?;
            }
        }
        Ok(())
    }

    fn interrupt_current(&mut self, reason: &str) -> Result<()> {
        let Some(current) = self.current_response.take() else {
            return Ok(());
        };
        current.cancel.cancel();
        current.task.abort();
        let played_through_ms = self.playout.played_through_ms(&current.id);
        let envelope = self.next_server_envelope();
        self.emit_server(ServerMessage::ResponseCompleted {
            envelope,
            response_id: current.id.clone(),
            epoch: current.epoch,
            interrupted: true,
        })?;
        let mut payload = map([
            ("responseId", Value::String(current.id)),
            ("epoch", Value::from(current.epoch)),
            ("reasonCode", Value::String(reason.into())),
        ]);
        if let Some(milliseconds) = played_through_ms
            && let Some(number) = serde_json::Number::from_f64(milliseconds)
        {
            payload.insert("playedThroughMs".into(), Value::Number(number));
        }
        self.emit_event(
            EventType::SessionInterrupted,
            EventSource::Runtime,
            EventPrivacy::Internal,
            payload,
        )
    }

    async fn complete(&mut self, reason: &str) -> Result<()> {
        if matches!(
            self.phase,
            SessionPhase::Completed | SessionPhase::Canceled | SessionPhase::Failed
        ) {
            return Ok(());
        }
        let reason = terminal_reason_code(reason);
        self.phase = SessionPhase::Draining;
        self.interrupt_current(reason)?;
        if let Some(cancel) = self.input_cancel.take() {
            cancel.cancel();
        }
        self.phase = SessionPhase::Completed;
        self.emit_terminal_event(
            EventType::SessionCompleted,
            map([("reasonCode", Value::String(reason.into()))]),
        )
        .await?;
        Ok(())
    }

    async fn cancel_session(&mut self, reason: &str) -> Result<()> {
        if matches!(
            self.phase,
            SessionPhase::Completed | SessionPhase::Canceled | SessionPhase::Failed
        ) {
            return Ok(());
        }
        let reason = terminal_reason_code(reason);
        self.phase = SessionPhase::Draining;
        self.interrupt_current(reason)?;
        if let Some(cancel) = self.input_cancel.take() {
            cancel.cancel();
        }
        self.phase = SessionPhase::Canceled;
        self.emit_terminal_event(
            EventType::SessionCanceled,
            map([("reasonCode", Value::String(reason.into()))]),
        )
        .await?;
        Ok(())
    }

    fn fail_session(&mut self, code: &str, message: &str) -> Result<()> {
        if matches!(
            self.phase,
            SessionPhase::Completed | SessionPhase::Canceled | SessionPhase::Failed
        ) {
            return Ok(());
        }
        self.cancel_children();
        self.phase = SessionPhase::Failed;
        if self.terminal_failure_code.is_none() {
            self.terminal_failure_code = Some(code.into());
        }
        let failure = RuntimeError::Provider {
            stage: "session",
            message: message.into(),
        };
        self.send_error(RuntimeError::Provider {
            stage: "session",
            message: message.into(),
        })?;
        self.terminal_error = Some(failure);
        Ok(())
    }

    async fn record_unexpected_failure(&mut self, error: &RuntimeError) -> Result<()> {
        self.cancel_children();
        self.phase = SessionPhase::Failed;
        let code = self
            .terminal_failure_code
            .take()
            .unwrap_or_else(|| error.code().into());
        self.emit_terminal_event(
            EventType::SessionFailed,
            map([("code", Value::String(code))]),
        )
        .await
    }

    fn cancel_children(&mut self) {
        if let Some(cancel) = self.input_cancel.take() {
            cancel.cancel();
        }
        if let Some(task) = self.input_task.take() {
            task.abort();
        }
        if let Some(current) = self.current_response.take() {
            current.cancel.cancel();
            current.task.abort();
        }
    }

    fn begin_input(&mut self) {
        if let Some(cancel) = self.input_cancel.take() {
            cancel.cancel();
        }
        if let Some(task) = self.input_task.take() {
            task.abort();
        }
        self.input_epoch = self.input_epoch.saturating_add(1);
        self.input_cancel = Some(self.force_cancel.child_token());
    }

    fn require_active(&mut self) -> Result<bool> {
        if self.phase == SessionPhase::Active {
            Ok(true)
        } else {
            self.send_error(RuntimeError::InvalidState(
                "input requires an active session".into(),
            ))?;
            Ok(false)
        }
    }

    fn emit_transcript(&mut self, turn_id: &str, text: &str, is_final: bool) -> Result<()> {
        let envelope = self.next_server_envelope();
        self.emit_server(ServerMessage::TranscriptDelta {
            envelope,
            turn_id: turn_id.into(),
            text: text.into(),
            is_final,
        })
    }

    fn emit_event(
        &self,
        event_type: EventType,
        source: EventSource,
        privacy: EventPrivacy,
        payload: BTreeMap<String, Value>,
    ) -> Result<()> {
        self.events.try_publish(PendingRuntimeEvent::new(
            event_type,
            self.context.correlation_id.clone(),
            None,
            source,
            privacy,
            payload,
        ))
    }

    async fn emit_terminal_event(
        &self,
        event_type: EventType,
        payload: BTreeMap<String, Value>,
    ) -> Result<()> {
        self.events
            .publish_terminal(
                PendingRuntimeEvent::new(
                    event_type,
                    self.context.correlation_id.clone(),
                    None,
                    EventSource::Runtime,
                    EventPrivacy::Internal,
                    payload,
                ),
                TERMINAL_EVENT_ENQUEUE_TIMEOUT,
            )
            .await
    }

    fn emit_server(&self, message: ServerMessage) -> Result<()> {
        self.emit_output(RealtimeOutput::Control(message))
    }

    fn emit_output(&self, output: RealtimeOutput) -> Result<()> {
        self.output_tx
            .try_send(output)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RuntimeError::OutputBackpressure,
                mpsc::error::TrySendError::Closed(_) => {
                    RuntimeError::InvalidState("realtime transport disconnected".into())
                }
            })
    }

    fn send_error(&mut self, error: RuntimeError) -> Result<()> {
        let details = error
            .details()
            .map(|details| details.into_iter().collect::<BTreeMap<_, _>>());
        let envelope = self.next_server_envelope();
        self.emit_server(ServerMessage::Error {
            envelope,
            code: error.code().into(),
            message: error.public_message().into_owned(),
            details,
        })
    }

    fn next_server_envelope(&mut self) -> RealtimeEnvelope {
        self.message_sequence = self.message_sequence.saturating_add(1);
        RealtimeEnvelope::server(
            &self.context.session_id,
            self.context.runtime_generation,
            format!("server-{}", self.message_sequence),
        )
    }

    fn is_duplicate_message(&mut self, message_id: &str) -> bool {
        if self.recent_message_set.contains(message_id) {
            return true;
        }
        while self.recent_message_ids.len() >= 1_024 {
            if let Some(expired) = self.recent_message_ids.pop_front() {
                self.recent_message_set.remove(&expired);
            }
        }
        self.recent_message_ids.push_back(message_id.into());
        self.recent_message_set.insert(message_id.into());
        false
    }

    fn push_history(&mut self, message: ConversationMessage) -> Result<()> {
        self.push_history_group(vec![message])
    }

    fn push_tool_history_pair(&mut self, call: PendingToolCall, output: Value) -> Result<()> {
        self.push_history_group(vec![
            ConversationMessage::AssistantToolCall {
                call_id: call.call_id.clone(),
                name: call.name.clone(),
                input: call.input,
            },
            ConversationMessage::Tool {
                call_id: call.call_id,
                name: call.name,
                output,
            },
        ])
    }

    fn push_history_group(&mut self, messages: Vec<ConversationMessage>) -> Result<()> {
        let encoded_bytes = messages
            .iter()
            .map(serde_json::to_vec)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let group_bytes = encoded_bytes.iter().map(Vec::len).sum::<usize>();
        if messages.is_empty()
            || messages.len() > self.config.max_history_messages
            || group_bytes > self.config.max_history_bytes
        {
            return Err(RuntimeError::Provider {
                stage: "history",
                message: "one provider history group exceeds its bound".into(),
            });
        }
        while self.history.len().saturating_add(messages.len()) > self.config.max_history_messages
            || self.history_bytes.saturating_add(group_bytes) > self.config.max_history_bytes
        {
            self.evict_oldest_history_group()?;
        }
        for (message, bytes) in messages.into_iter().zip(encoded_bytes) {
            self.history_bytes = self.history_bytes.saturating_add(bytes.len());
            self.history.push_back(message);
        }
        Ok(())
    }

    fn evict_oldest_history_group(&mut self) -> Result<()> {
        let remove = match (self.history.front(), self.history.get(1)) {
            (
                Some(ConversationMessage::AssistantToolCall {
                    call_id: expected_id,
                    name: expected_name,
                    ..
                }),
                Some(ConversationMessage::Tool { call_id, name, .. }),
            ) if expected_id == call_id && expected_name == name => 2,
            (
                Some(
                    ConversationMessage::AssistantToolCall { .. }
                    | ConversationMessage::Tool { .. },
                ),
                _,
            ) => {
                return Err(RuntimeError::Internal(
                    "conversation history contains an unmatched tool message".into(),
                ));
            }
            (Some(_), _) => 1,
            (None, _) => {
                return Err(RuntimeError::Internal(
                    "conversation history accounting is inconsistent".into(),
                ));
            }
        };
        for _ in 0..remove {
            let expired = self.history.pop_front().ok_or_else(|| {
                RuntimeError::Internal("conversation history accounting is inconsistent".into())
            })?;
            self.history_bytes = self
                .history_bytes
                .saturating_sub(serde_json::to_vec(&expired)?.len());
        }
        Ok(())
    }
}

fn terminal_reason_code(reason: &str) -> &'static str {
    match reason {
        "client_request" => "client_request",
        "control_plane_cancel" => "control_plane_cancel",
        "runtime_draining" => "runtime_draining",
        "runtime_shutdown_forced" => "runtime_shutdown_forced",
        "session_deadline" => "session_deadline",
        "simulation_complete" => "simulation_complete",
        "transport_closed" => "transport_closed",
        _ => "internal_request",
    }
}

#[derive(Debug, Clone, Copy)]
struct PipelineLimits {
    event_timeout: Duration,
    max_text_delta_bytes: usize,
    max_response_text_bytes: usize,
    max_audio_chunk_bytes: usize,
    max_response_audio_bytes: usize,
}

#[allow(clippy::too_many_arguments)]
fn spawn_response_pipeline(
    control: mpsc::Sender<ControlCommand>,
    providers: ProviderSet,
    request: ReasoningRequest,
    voice_id: String,
    sample_rate_hz: u32,
    session_id: String,
    response_id: String,
    epoch: u64,
    cancel: CancellationToken,
    limits: PipelineLimits,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        const MAX_REASONING_ROUNDS: usize = 5;
        const MAX_TOOL_CALLS: usize = 16;
        let mut request = request;
        let mut response_text: Option<String> = None;
        let mut total_tool_calls = 0_usize;

        'rounds: for round in 0..MAX_REASONING_ROUNDS {
            let mut reason_stream = providers.reasoner.reason(request.clone(), cancel.clone());
            let mut round_text = String::new();
            let mut round_calls = Vec::new();
            let mut completed = false;
            loop {
                let next = tokio::select! {
                    () = cancel.cancelled() => return,
                    result = tokio::time::timeout(limits.event_timeout, reason_stream.next()) => result,
                };
                let result = match next {
                    Ok(Some(result)) => result,
                    Ok(None) => break,
                    Err(_) => {
                        let _sent = send_pipeline(
                            &control,
                            &cancel,
                            &response_id,
                            epoch,
                            pipeline_failure("reasoner", "provider event deadline exceeded"),
                        )
                        .await;
                        return;
                    }
                };
                match result {
                    Ok(ReasoningEvent::Delta(delta)) => {
                        if delta.is_empty()
                            || delta.len() > limits.max_text_delta_bytes
                            || round_text.len().saturating_add(delta.len())
                                > limits.max_response_text_bytes
                        {
                            let _sent = send_pipeline(
                                &control,
                                &cancel,
                                &response_id,
                                epoch,
                                pipeline_failure(
                                    "reasoner",
                                    "provider text output exceeded its byte bound",
                                ),
                            )
                            .await;
                            return;
                        }
                        round_text.push_str(&delta);
                        if !send_pipeline(
                            &control,
                            &cancel,
                            &response_id,
                            epoch,
                            PipelineEvent::ReasoningDelta(delta),
                        )
                        .await
                        {
                            return;
                        }
                    }
                    Ok(ReasoningEvent::ToolCall(call)) => {
                        if call.call_id.is_empty()
                            || call.call_id.len() > 160
                            || call.name.is_empty()
                            || call.name.len() > 64
                            || !call.input.is_object()
                        {
                            let _sent = send_pipeline(
                                &control,
                                &cancel,
                                &response_id,
                                epoch,
                                pipeline_failure("tool", "provider returned an invalid tool call"),
                            )
                            .await;
                            return;
                        }
                        round_calls.push(call);
                    }
                    Ok(ReasoningEvent::Completed) => {
                        completed = true;
                        break;
                    }
                    Err(error) if error.kind == ProviderErrorKind::Cancelled => return,
                    Err(error) => {
                        let _sent = send_pipeline(
                            &control,
                            &cancel,
                            &response_id,
                            epoch,
                            PipelineEvent::Failed(error),
                        )
                        .await;
                        return;
                    }
                }
            }
            if !completed {
                let _sent = send_pipeline(
                    &control,
                    &cancel,
                    &response_id,
                    epoch,
                    pipeline_failure("reasoner", "reasoner ended without completion"),
                )
                .await;
                return;
            }
            if round_calls.is_empty() {
                if round_text.trim().is_empty() {
                    let _sent = send_pipeline(
                        &control,
                        &cancel,
                        &response_id,
                        epoch,
                        pipeline_failure("reasoner", "reasoner completed without text"),
                    )
                    .await;
                    return;
                }
                response_text = Some(round_text);
                break 'rounds;
            }
            total_tool_calls = total_tool_calls.saturating_add(round_calls.len());
            if total_tool_calls > MAX_TOOL_CALLS || round + 1 == MAX_REASONING_ROUNDS {
                let _sent = send_pipeline(
                    &control,
                    &cancel,
                    &response_id,
                    epoch,
                    pipeline_failure("tool", "tool reasoning budget was exhausted"),
                )
                .await;
                return;
            }
            // Content accompanying a tool call is deliberately not spoken or committed as an
            // assistant answer. The next round receives the exact call/result transcript.
            for call in round_calls {
                let input = call.input.clone();
                if !send_pipeline(
                    &control,
                    &cancel,
                    &response_id,
                    epoch,
                    PipelineEvent::ToolStarted {
                        call_id: call.call_id.clone(),
                        name: call.name.clone(),
                        input: input.clone(),
                    },
                )
                .await
                {
                    return;
                }
                let invocation = ToolInvocation {
                    call_id: call.call_id.clone(),
                    tool_name: call.name.clone(),
                    input: input.clone(),
                    idempotency_key: format!("{session_id}:{response_id}:{}", call.call_id),
                };
                match providers.tools.execute(invocation, cancel.clone()).await {
                    Ok(output) => {
                        let value = output.value;
                        if !send_pipeline(
                            &control,
                            &cancel,
                            &response_id,
                            epoch,
                            PipelineEvent::ToolCompleted {
                                call_id: call.call_id.clone(),
                                name: call.name.clone(),
                                output: value.clone(),
                                cached: output.cached,
                            },
                        )
                        .await
                        {
                            return;
                        }
                        request
                            .messages
                            .push(ConversationMessage::AssistantToolCall {
                                call_id: call.call_id.clone(),
                                name: call.name.clone(),
                                input,
                            });
                        request.messages.push(ConversationMessage::Tool {
                            call_id: call.call_id,
                            name: call.name,
                            output: value,
                        });
                    }
                    Err(error) => {
                        let _sent = send_pipeline(
                            &control,
                            &cancel,
                            &response_id,
                            epoch,
                            PipelineEvent::ToolFailed {
                                call_id: call.call_id,
                                name: call.name,
                                code: tool_error_code(&error).into(),
                            },
                        )
                        .await;
                        let _sent = send_pipeline(
                            &control,
                            &cancel,
                            &response_id,
                            epoch,
                            PipelineEvent::Failed(ProviderError {
                                stage: "tool",
                                kind: ProviderErrorKind::Permanent,
                                message: error.to_string(),
                            }),
                        )
                        .await;
                        return;
                    }
                }
            }
        }

        let Some(response_text) = response_text else {
            let _sent = send_pipeline(
                &control,
                &cancel,
                &response_id,
                epoch,
                PipelineEvent::Failed(ProviderError {
                    stage: "reasoner",
                    kind: ProviderErrorKind::Permanent,
                    message: "reasoner exhausted its round budget".into(),
                }),
            )
            .await;
            return;
        };
        if !send_pipeline(
            &control,
            &cancel,
            &response_id,
            epoch,
            PipelineEvent::ReasoningCompleted(response_text.clone()),
        )
        .await
            || !send_pipeline(
                &control,
                &cancel,
                &response_id,
                epoch,
                PipelineEvent::TtsStarted,
            )
            .await
        {
            return;
        }

        let mut tts_stream = providers.text_to_speech.synthesize(
            SynthesisRequest {
                text: response_text,
                voice_id,
                sample_rate_hz,
            },
            cancel.clone(),
        );
        let mut sequence = 0_u64;
        let mut audio_bytes = 0_usize;
        loop {
            let next = tokio::select! {
                () = cancel.cancelled() => return,
                result = tokio::time::timeout(limits.event_timeout, tts_stream.next()) => result,
            };
            let result = match next {
                Ok(Some(result)) => result,
                Ok(None) => break,
                Err(_) => {
                    let _sent = send_pipeline(
                        &control,
                        &cancel,
                        &response_id,
                        epoch,
                        pipeline_failure("tts", "provider event deadline exceeded"),
                    )
                    .await;
                    return;
                }
            };
            match result {
                Ok(audio) => {
                    audio_bytes = audio_bytes.saturating_add(audio.bytes.len());
                    if audio.bytes.is_empty()
                        || audio.bytes.len() > limits.max_audio_chunk_bytes
                        || !audio.bytes.len().is_multiple_of(2)
                        || audio_bytes > limits.max_response_audio_bytes
                    {
                        let _sent = send_pipeline(
                            &control,
                            &cancel,
                            &response_id,
                            epoch,
                            pipeline_failure("tts", "provider audio output exceeded its bound"),
                        )
                        .await;
                        return;
                    }
                    if !send_pipeline(
                        &control,
                        &cancel,
                        &response_id,
                        epoch,
                        PipelineEvent::Audio {
                            sequence,
                            bytes: audio.bytes,
                        },
                    )
                    .await
                    {
                        return;
                    }
                    sequence = sequence.saturating_add(1);
                }
                Err(error) if error.kind == ProviderErrorKind::Cancelled => return,
                Err(error) => {
                    let _sent = send_pipeline(
                        &control,
                        &cancel,
                        &response_id,
                        epoch,
                        PipelineEvent::Failed(error),
                    )
                    .await;
                    return;
                }
            }
        }
        if audio_bytes == 0 {
            let _sent = send_pipeline(
                &control,
                &cancel,
                &response_id,
                epoch,
                pipeline_failure("tts", "provider completed without audio"),
            )
            .await;
            return;
        }
        if send_pipeline(
            &control,
            &cancel,
            &response_id,
            epoch,
            PipelineEvent::TtsCompleted,
        )
        .await
        {
            let _sent = send_pipeline(
                &control,
                &cancel,
                &response_id,
                epoch,
                PipelineEvent::Completed,
            )
            .await;
        }
    })
}

fn pipeline_failure(stage: &'static str, message: &'static str) -> PipelineEvent {
    PipelineEvent::Failed(ProviderError {
        stage,
        kind: ProviderErrorKind::Permanent,
        message: message.into(),
    })
}

async fn send_pipeline(
    control: &mpsc::Sender<ControlCommand>,
    cancel: &CancellationToken,
    response_id: &str,
    epoch: u64,
    event: PipelineEvent,
) -> bool {
    send_internal(
        control,
        cancel,
        ControlCommand::Pipeline {
            response_id: response_id.into(),
            epoch,
            event,
        },
    )
    .await
}

async fn send_internal(
    control: &mpsc::Sender<ControlCommand>,
    cancel: &CancellationToken,
    command: ControlCommand,
) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => false,
        result = control.send(command) => result.is_ok(),
    }
}

fn map<const N: usize>(entries: [(&str, Value); N]) -> BTreeMap<String, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (key.into(), value))
        .collect()
}

const fn tool_error_code(error: &crate::tool::ToolError) -> &'static str {
    match error {
        crate::tool::ToolError::Undeclared => "tool_undeclared",
        crate::tool::ToolError::MissingIdempotencyKey => "idempotency_key_missing",
        crate::tool::ToolError::InvalidInput(_) => "tool_input_invalid",
        crate::tool::ToolError::AlreadyInFlight => "tool_already_in_flight",
        crate::tool::ToolError::CommitOnceUncertain(_) => "commit_once_uncertain",
        crate::tool::ToolError::Timeout => "tool_timeout",
        crate::tool::ToolError::Cancelled => "tool_cancelled",
        crate::tool::ToolError::Provider(_) => "tool_provider_failed",
        crate::tool::ToolError::LedgerUnavailable => "tool_ledger_unavailable",
        crate::tool::ToolError::LedgerCapacity => "tool_ledger_capacity",
        crate::tool::ToolError::OutputTooLarge => "tool_output_too_large",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::{
        domain::{ToolDefinition, ToolExecution, ToolSideEffect},
        event::{
            EventSink, EventSinkError, EventSinkErrorKind, EventType, MemoryEventSink,
            PendingRuntimeEvent,
        },
        protocol::{PROTOCOL_VERSION, RealtimeOutput},
        provider::{
            ProviderCapability, ProviderStream, Reasoner, SpeechToText, SynthesizedAudio,
            TextToSpeech, ToolExecutor, ToolInvocation, ToolOutput, TranscriptSegment,
        },
        tool::ToolCoordinator,
    };

    struct EmptyTts;

    struct BlockingFirstEventSink {
        calls: std::sync::atomic::AtomicUsize,
        events: Mutex<Vec<PendingRuntimeEvent>>,
        first_publish_started: tokio::sync::Notify,
        release_first_publish: tokio::sync::Notify,
    }

    impl Default for BlockingFirstEventSink {
        fn default() -> Self {
            Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                events: Mutex::new(Vec::new()),
                first_publish_started: tokio::sync::Notify::new(),
                release_first_publish: tokio::sync::Notify::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl EventSink for BlockingFirstEventSink {
        async fn publish(
            &self,
            events: &[PendingRuntimeEvent],
        ) -> std::result::Result<(), EventSinkError> {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                self.first_publish_started.notify_one();
                self.release_first_publish.notified().await;
            }
            self.events
                .lock()
                .map_err(|_| EventSinkError {
                    kind: EventSinkErrorKind::Permanent,
                    message: "blocking test sink unavailable".into(),
                })?
                .extend_from_slice(events);
            Ok(())
        }
    }

    struct DoubleFinalStt;

    impl SpeechToText for DoubleFinalStt {
        fn capabilities(&self) -> BTreeSet<ProviderCapability> {
            BTreeSet::from([ProviderCapability::StreamingStt])
        }

        fn transcribe(
            &self,
            _input: AudioInput,
            _cancel: CancellationToken,
        ) -> ProviderStream<TranscriptSegment> {
            Box::pin(futures_util::stream::iter([
                Ok(TranscriptSegment {
                    text: "first final".into(),
                    is_final: true,
                }),
                Ok(TranscriptSegment {
                    text: "invalid second final".into(),
                    is_final: true,
                }),
            ]))
        }
    }

    #[derive(Default)]
    struct RecordingReasoner {
        requests: Mutex<Vec<ReasoningRequest>>,
    }

    #[derive(Default)]
    struct ToolThenAnswerReasoner {
        requests: Mutex<Vec<ReasoningRequest>>,
    }

    impl Reasoner for ToolThenAnswerReasoner {
        fn capabilities(&self) -> BTreeSet<ProviderCapability> {
            BTreeSet::from([ProviderCapability::StreamingReasoning])
        }

        fn reason(
            &self,
            request: ReasoningRequest,
            _cancel: CancellationToken,
        ) -> ProviderStream<ReasoningEvent> {
            let requests_tool = request
                .messages
                .iter()
                .rev()
                .find_map(|message| match message {
                    ConversationMessage::User(value) => Some(value == "first"),
                    _ => None,
                })
                == Some(true);
            self.requests.lock().expect("requests").push(request);
            if requests_tool {
                Box::pin(futures_util::stream::iter([
                    Ok(ReasoningEvent::ToolCall(
                        crate::provider::RequestedToolCall {
                            call_id: "call_blocking_1".into(),
                            name: "lookup".into(),
                            input: serde_json::json!({"id": 1}),
                        },
                    )),
                    Ok(ReasoningEvent::Completed),
                ]))
            } else {
                Box::pin(futures_util::stream::iter([
                    Ok(ReasoningEvent::Delta("answer".into())),
                    Ok(ReasoningEvent::Completed),
                ]))
            }
        }
    }

    #[derive(Default)]
    struct BlockingToolExecutor {
        started: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl ToolExecutor for BlockingToolExecutor {
        async fn execute(
            &self,
            _invocation: ToolInvocation,
            cancel: CancellationToken,
        ) -> std::result::Result<ToolOutput, ProviderError> {
            self.started.notify_one();
            cancel.cancelled().await;
            Err(ProviderError::cancelled("tool"))
        }
    }

    impl Reasoner for RecordingReasoner {
        fn capabilities(&self) -> BTreeSet<ProviderCapability> {
            BTreeSet::from([ProviderCapability::StreamingReasoning])
        }

        fn reason(
            &self,
            request: ReasoningRequest,
            _cancel: CancellationToken,
        ) -> ProviderStream<ReasoningEvent> {
            self.requests.lock().expect("requests").push(request);
            Box::pin(futures_util::stream::iter([
                Ok(ReasoningEvent::Delta("answer".into())),
                Ok(ReasoningEvent::Completed),
            ]))
        }
    }

    impl TextToSpeech for EmptyTts {
        fn capabilities(&self) -> BTreeSet<ProviderCapability> {
            BTreeSet::from([ProviderCapability::StreamingTts])
        }

        fn synthesize(
            &self,
            _request: SynthesisRequest,
            _cancel: CancellationToken,
        ) -> ProviderStream<SynthesizedAudio> {
            Box::pin(futures_util::stream::empty())
        }
    }

    fn context() -> TenantContext {
        TenantContext {
            organization_id: "org_12345678".into(),
            project_id: "prj_12345678".into(),
            deployment_id: "dep_12345678".into(),
            session_id: "ses_12345678".into(),
            runtime_generation: 4,
            correlation_id: "test-correlation".into(),
        }
    }

    fn envelope(message_id: &str) -> RealtimeEnvelope {
        RealtimeEnvelope {
            protocol_version: PROTOCOL_VERSION,
            session_id: context().session_id,
            message_id: message_id.into(),
            runtime_generation: 4,
        }
    }

    async fn ready(spawned: &mut SpawnedSession) {
        let message = spawned.output.recv().await.expect("session.ready");
        assert!(matches!(
            message,
            RealtimeOutput::Control(ServerMessage::SessionReady { .. })
        ));
        spawned
            .handle
            .try_control(ClientMessage::SessionStart {
                envelope: envelope("start"),
            })
            .expect("start");
        let started = spawned.output.recv().await.expect("session.started");
        assert!(matches!(
            started,
            RealtimeOutput::Control(ServerMessage::SessionStarted { .. })
        ));
    }

    #[tokio::test]
    async fn media_commit_preserves_binary_frame_order() {
        let sink = Arc::new(MemoryEventSink::default());
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            EventPipeline::spawn(sink, 64, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        let pcm = Bytes::from(vec![0_u8; 640]);
        spawned.handle.try_audio(pcm).expect("audio");
        spawned
            .handle
            .try_control(ClientMessage::InputCommit {
                envelope: envelope("commit"),
            })
            .expect("commit barrier");

        let mut final_transcript = false;
        let mut audio = false;
        while let Some(output) = spawned.output.recv().await {
            match output {
                RealtimeOutput::Control(ServerMessage::TranscriptDelta {
                    text,
                    is_final: true,
                    ..
                }) => {
                    final_transcript = true;
                    assert_eq!(text, "audio input (640 bytes)");
                }
                RealtimeOutput::Audio(frame) => {
                    audio = true;
                    assert_eq!(frame.header.encoding, AudioEncoding::Pcm16Le);
                    assert_eq!(frame.audio.len() % 2, 0);
                }
                RealtimeOutput::Control(ServerMessage::ResponseCompleted {
                    interrupted: false,
                    ..
                }) => break,
                _ => {}
            }
        }
        assert!(final_transcript && audio);
        spawned.handle.end("test_complete").expect("end");
        spawned.task.await.expect("join").expect("actor");
    }

    #[tokio::test]
    async fn saturated_ordinary_spool_preserves_one_terminal_failure_and_releases_actor() {
        let sink = Arc::new(BlockingFirstEventSink::default());
        let pipeline = EventPipeline::spawn(sink.clone(), 1, 1);
        let terminal_enqueued = pipeline.terminal_enqueue_observer();
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            pipeline,
        )
        .expect("actor");
        assert!(matches!(
            spawned.output.recv().await,
            Some(RealtimeOutput::Control(ServerMessage::SessionReady { .. }))
        ));
        tokio::time::timeout(
            Duration::from_secs(1),
            sink.first_publish_started.notified(),
        )
        .await
        .expect("first event delivery blocked");
        spawned
            .handle
            .try_control(ClientMessage::SessionStart {
                envelope: envelope("start-saturated-events"),
            })
            .expect("start");
        assert!(matches!(
            spawned.output.recv().await,
            Some(RealtimeOutput::Control(
                ServerMessage::SessionStarted { .. }
            ))
        ));

        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("saturate-events"),
                text: "trigger ordinary event overflow".into(),
            })
            .expect("input");
        tokio::time::timeout(Duration::from_secs(1), terminal_enqueued.notified())
            .await
            .expect("terminal event reserved while sink blocked");
        assert!(!spawned.handle.is_finished());

        sink.release_first_publish.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(1), spawned.task)
            .await
            .expect("actor released after sink")
            .expect("join");
        assert!(matches!(result, Err(RuntimeError::EventSpoolFull)));
        assert!(spawned.handle.is_finished());

        let events = sink.events.lock().expect("events");
        assert_eq!(
            events
                .iter()
                .map(|event| event.producer_sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == EventType::SessionFailed)
                .count(),
            1
        );
        assert_eq!(
            events.last().map(|event| event.event_type),
            Some(EventType::SessionFailed)
        );
    }

    #[tokio::test]
    async fn first_final_transcript_terminates_the_stt_stream() {
        let recorder = Arc::new(RecordingReasoner::default());
        let mut providers = ProviderSet::scripted(Vec::new(), Duration::ZERO);
        providers.speech_to_text = Arc::new(DoubleFinalStt);
        providers.reasoner = recorder.clone();
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            providers,
            EventPipeline::spawn(Arc::new(MemoryEventSink::default()), 64, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        spawned
            .handle
            .try_audio(Bytes::from_static(&[1, 0]))
            .expect("audio");
        spawned
            .handle
            .try_control(ClientMessage::InputCommit {
                envelope: envelope("commit"),
            })
            .expect("commit");

        let mut final_transcripts = Vec::new();
        while let Some(output) = spawned.output.recv().await {
            match output {
                RealtimeOutput::Control(ServerMessage::TranscriptDelta {
                    text,
                    is_final: true,
                    ..
                }) => final_transcripts.push(text),
                RealtimeOutput::Control(ServerMessage::ResponseCompleted {
                    interrupted: false,
                    ..
                }) => break,
                _ => {}
            }
        }

        assert_eq!(final_transcripts, ["first final"]);
        {
            let requests = recorder.requests.lock().expect("requests");
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].messages,
                vec![ConversationMessage::User("first final".into())]
            );
        }
        spawned.handle.end("test_complete").expect("end");
        spawned.task.await.expect("join").expect("actor");
    }

    #[tokio::test]
    async fn audio_overflow_is_terminal_and_emitted_once() {
        let sink = Arc::new(MemoryEventSink::default());
        let config = SessionConfig {
            max_audio_frame_bytes: 4,
            max_buffered_audio_bytes: 4,
            ..SessionConfig::default()
        };
        let mut spawned = SessionActor::spawn(
            config,
            context(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            EventPipeline::spawn(sink.clone(), 64, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        spawned
            .handle
            .try_audio(Bytes::from_static(&[0, 0, 0, 0]))
            .expect("first audio");
        spawned
            .handle
            .try_audio(Bytes::from_static(&[1, 0, 1, 0]))
            .expect("second audio");
        let result = tokio::time::timeout(Duration::from_secs(1), spawned.task)
            .await
            .expect("actor terminated")
            .expect("join");
        assert!(result.is_err());
        let failed = sink
            .events()
            .expect("events")
            .into_iter()
            .filter(|event| event.event_type == EventType::SessionFailed)
            .count();
        assert_eq!(failed, 1);
    }

    #[tokio::test]
    async fn unexpected_output_backpressure_records_failure() {
        let sink = Arc::new(MemoryEventSink::default());
        let config = SessionConfig {
            output_capacity: 1,
            ..SessionConfig::default()
        };
        let spawned = SessionActor::spawn(
            config,
            context(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            EventPipeline::spawn(sink.clone(), 64, 16),
        )
        .expect("actor");
        // Keep session.ready queued so transcript output deterministically backpressures.
        spawned
            .handle
            .try_control(ClientMessage::SessionStart {
                envelope: envelope("start"),
            })
            .expect("start");
        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("input"),
                text: "sensitive-input".into(),
            })
            .expect("input");
        let result = tokio::time::timeout(Duration::from_secs(1), spawned.task)
            .await
            .expect("actor terminated")
            .expect("join");
        assert!(matches!(result, Err(RuntimeError::OutputBackpressure)));
        assert!(
            sink.events()
                .expect("events")
                .iter()
                .any(|event| event.event_type == EventType::SessionFailed)
        );
    }

    #[tokio::test]
    async fn semantic_events_exclude_private_content() {
        let sink = Arc::new(MemoryEventSink::default());
        let tool = ToolDefinition {
            name: "lookup".into(),
            description: "lookup".into(),
            input_schema: BTreeMap::new(),
            timeout_ms: 1_000,
            side_effect: ToolSideEffect::None,
            execution: ToolExecution::Local,
        };
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            ProviderSet::scripted(vec![tool], Duration::ZERO),
            EventPipeline::spawn(sink.clone(), 128, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        let sentinel = "PRIVATE_TRANSCRIPT_AND_TOOL_RESULT_92";
        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("private-input"),
                text: format!("/tool lookup {{\"value\":\"{sentinel}\"}}"),
            })
            .expect("input");
        while let Some(output) = spawned.output.recv().await {
            if matches!(
                output,
                RealtimeOutput::Control(ServerMessage::ResponseCompleted {
                    interrupted: false,
                    ..
                })
            ) {
                break;
            }
        }
        spawned.handle.end("test_complete").expect("end");
        spawned.task.await.expect("join").expect("actor");
        let serialized = serde_json::to_string(&sink.events().expect("events")).expect("JSON");
        assert!(!serialized.contains(sentinel));
        assert!(!serialized.contains("Scripted response"));
        assert!(!serialized.contains("\"input\""));
        assert!(!serialized.contains("\"output\""));
    }

    #[tokio::test]
    async fn caller_session_end_completes_once_without_exposing_reason() {
        let sink = Arc::new(MemoryEventSink::default());
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            EventPipeline::spawn(sink.clone(), 64, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        let sentinel = "CALLER_PRIVATE_END_REASON_771";
        spawned
            .handle
            .try_control(ClientMessage::SessionEnd {
                envelope: envelope("end"),
                reason: sentinel.into(),
            })
            .expect("end");
        spawned.task.await.expect("join").expect("actor");
        let events = sink.events().expect("events");
        let serialized = serde_json::to_string(&events).expect("JSON");
        assert!(!serialized.contains(sentinel));
        assert!(serialized.contains("client_request"));
        assert!(!serialized.contains("\"reason\""));
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == EventType::SessionCompleted)
                .count(),
            1
        );
        assert!(
            !events
                .iter()
                .any(|event| event.event_type == EventType::SessionCanceled)
        );
        assert_eq!(
            events.last().map(|event| event.event_type),
            Some(EventType::SessionCompleted)
        );
        assert_eq!(
            events
                .last()
                .and_then(|event| event.payload.get("reasonCode")),
            Some(&serde_json::json!("client_request"))
        );
    }

    #[tokio::test]
    async fn provider_event_deadline_is_terminal() {
        let sink = Arc::new(MemoryEventSink::default());
        let config = SessionConfig {
            provider_event_timeout: Duration::from_millis(5),
            ..SessionConfig::default()
        };
        let mut spawned = SessionActor::spawn(
            config,
            context(),
            ProviderSet::scripted(Vec::new(), Duration::from_millis(50)),
            EventPipeline::spawn(sink.clone(), 64, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("input"),
                text: "hello".into(),
            })
            .expect("input");
        let result = tokio::time::timeout(Duration::from_secs(1), spawned.task)
            .await
            .expect("deadline")
            .expect("join");
        assert!(result.is_err());
        assert!(
            sink.events()
                .expect("events")
                .iter()
                .any(|event| event.event_type == EventType::SessionFailed)
        );
    }

    #[tokio::test]
    async fn media_mailbox_overrun_fails_closed_before_partial_commit() {
        let sink = Arc::new(MemoryEventSink::default());
        let config = SessionConfig {
            media_capacity: 1,
            ..SessionConfig::default()
        };
        let spawned = SessionActor::spawn(
            config,
            context(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            EventPipeline::spawn(sink.clone(), 64, 16),
        )
        .expect("actor");
        spawned
            .handle
            .try_control(ClientMessage::SessionStart {
                envelope: envelope("start"),
            })
            .expect("ordered start");
        let overrun = spawned
            .handle
            .try_audio(Bytes::from_static(&[0, 0]))
            .expect_err("mailbox must be full before actor is scheduled");
        assert!(matches!(
            overrun,
            RuntimeError::MailboxFull { lane: "media" }
        ));
        let result = tokio::time::timeout(Duration::from_secs(1), spawned.task)
            .await
            .expect("actor terminated")
            .expect("join");
        assert!(result.is_err());
        let events = sink.events().expect("events");
        assert!(
            events
                .iter()
                .any(|event| event.event_type == EventType::AudioOverrun)
        );
        assert!(
            !events
                .iter()
                .any(|event| event.event_type == EventType::SpeechFinal)
        );
    }

    #[tokio::test]
    async fn rejects_odd_length_pcm16_frame() {
        let spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            EventPipeline::spawn(Arc::new(MemoryEventSink::default()), 64, 16),
        )
        .expect("actor");
        assert!(matches!(
            spawned.handle.try_audio(Bytes::from_static(&[0])),
            Err(RuntimeError::Protocol(_))
        ));
        spawned.handle.end("internal_request").expect("end");
        spawned.task.await.expect("join").expect("actor");
    }

    #[tokio::test(start_paused = true)]
    async fn attached_session_without_start_fails_on_attach_deadline() {
        let sink = Arc::new(MemoryEventSink::default());
        let config = SessionConfig {
            start_timeout: Duration::from_secs(5),
            ..SessionConfig::default()
        };
        let mut spawned = SessionActor::spawn(
            config,
            context(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            EventPipeline::spawn(sink.clone(), 64, 16),
        )
        .expect("actor");
        assert!(matches!(
            spawned.output.recv().await,
            Some(RealtimeOutput::Control(ServerMessage::SessionReady { .. }))
        ));
        tokio::time::advance(Duration::from_secs(6)).await;
        let result = spawned.task.await.expect("join");
        assert!(result.is_err());
        let events = sink.events().expect("events");
        assert!(events.iter().any(|event| {
            event.event_type == EventType::SessionFailed
                && event.payload.get("code") == Some(&serde_json::json!("session_start_timeout"))
        }));
        assert!(
            !events
                .iter()
                .any(|event| event.event_type == EventType::SessionCompleted)
        );
    }

    #[tokio::test]
    async fn empty_tts_stream_fails_instead_of_reporting_completion() {
        let sink = Arc::new(MemoryEventSink::default());
        let mut providers = ProviderSet::scripted(Vec::new(), Duration::ZERO);
        providers.text_to_speech = Arc::new(EmptyTts);
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            providers,
            EventPipeline::spawn(sink.clone(), 64, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("input"),
                text: "hello".into(),
            })
            .expect("input");
        let result = tokio::time::timeout(Duration::from_secs(1), spawned.task)
            .await
            .expect("terminal")
            .expect("join");
        assert!(result.is_err());
        let events = sink.events().expect("events");
        assert!(
            events
                .iter()
                .any(|event| event.event_type == EventType::SessionFailed)
        );
        assert!(
            !events
                .iter()
                .any(|event| event.event_type == EventType::TtsCompleted)
        );
    }

    #[tokio::test]
    async fn first_audio_frame_cancels_prior_stt_epoch() {
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            ProviderSet::scripted(Vec::new(), Duration::from_millis(50)),
            EventPipeline::spawn(Arc::new(MemoryEventSink::default()), 64, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        spawned
            .handle
            .try_audio(Bytes::from_static(&[1, 0]))
            .expect("first audio");
        spawned
            .handle
            .try_control(ClientMessage::InputCommit {
                envelope: envelope("first-commit"),
            })
            .expect("first commit");
        tokio::time::sleep(Duration::from_millis(5)).await;
        spawned
            .handle
            .try_audio(Bytes::from_static(&[2, 0]))
            .expect("barge audio");
        tokio::time::sleep(Duration::from_millis(80)).await;
        while let Ok(output) = spawned.output.try_recv() {
            assert!(!matches!(
                output,
                RealtimeOutput::Control(ServerMessage::TranscriptDelta { is_final: true, .. })
            ));
        }
        spawned.handle.end("internal_request").expect("end");
        spawned.task.await.expect("join").expect("actor");
    }

    #[tokio::test]
    async fn text_input_discards_partial_audio_before_next_utterance() {
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            EventPipeline::spawn(Arc::new(MemoryEventSink::default()), 128, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        spawned
            .handle
            .try_audio(Bytes::from_static(&[1, 0]))
            .expect("partial audio");
        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("text"),
                text: "typed turn".into(),
            })
            .expect("text");
        spawned
            .handle
            .try_audio(Bytes::from_static(&[2, 0]))
            .expect("next audio");
        spawned
            .handle
            .try_control(ClientMessage::InputCommit {
                envelope: envelope("commit"),
            })
            .expect("commit");
        let mut saw_new_audio_transcript = false;
        while let Some(output) = spawned.output.recv().await {
            if let RealtimeOutput::Control(ServerMessage::TranscriptDelta {
                text,
                is_final: true,
                ..
            }) = output
            {
                assert_ne!(text, "audio input (4 bytes)");
                if text == "audio input (2 bytes)" {
                    saw_new_audio_transcript = true;
                    break;
                }
            }
        }
        assert!(saw_new_audio_transcript);
        spawned.handle.end("internal_request").expect("end");
        spawned.task.await.expect("join").expect("actor");
    }

    #[tokio::test]
    async fn reasoning_request_history_excludes_the_current_input() {
        let recorder = Arc::new(RecordingReasoner::default());
        let mut providers = ProviderSet::scripted(Vec::new(), Duration::ZERO);
        providers.reasoner = recorder.clone();
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            providers,
            EventPipeline::spawn(Arc::new(MemoryEventSink::default()), 128, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;

        for (index, text) in ["first", "second"].into_iter().enumerate() {
            spawned
                .handle
                .try_control(ClientMessage::InputText {
                    envelope: envelope(&format!("input-{index}")),
                    text: text.into(),
                })
                .expect("input");
            loop {
                if matches!(
                    spawned.output.recv().await,
                    Some(RealtimeOutput::Control(ServerMessage::ResponseCompleted {
                        interrupted: false,
                        ..
                    }))
                ) {
                    break;
                }
            }
        }

        spawned.handle.end("internal_request").expect("end");
        spawned.task.await.expect("join").expect("actor");
        let requests = recorder.requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].messages,
            vec![ConversationMessage::User("first".into())]
        );
        assert_eq!(
            requests[1].messages,
            vec![
                ConversationMessage::User("first".into()),
                ConversationMessage::Assistant("answer".into()),
                ConversationMessage::User("second".into())
            ]
        );
    }

    #[tokio::test]
    async fn interrupted_unplayed_assistant_is_not_committed_to_history() {
        let recorder = Arc::new(RecordingReasoner::default());
        let mut providers = ProviderSet::scripted(Vec::new(), Duration::from_millis(100));
        providers.reasoner = recorder.clone();
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            providers,
            EventPipeline::spawn(Arc::new(MemoryEventSink::default()), 128, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("first"),
                text: "first".into(),
            })
            .expect("first input");
        loop {
            if matches!(
                spawned.output.recv().await,
                Some(RealtimeOutput::Control(ServerMessage::ResponseDelta {
                    epoch: 1,
                    ..
                }))
            ) {
                break;
            }
        }
        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("second"),
                text: "second".into(),
            })
            .expect("barge input");
        loop {
            if matches!(
                spawned.output.recv().await,
                Some(RealtimeOutput::Control(ServerMessage::ResponseDelta {
                    epoch: 2,
                    ..
                }))
            ) {
                break;
            }
        }
        spawned.handle.end("internal_request").expect("end");
        spawned.task.await.expect("join").expect("actor");
        let requests = recorder.requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1].messages,
            vec![
                ConversationMessage::User("first".into()),
                ConversationMessage::User("second".into())
            ]
        );
    }

    #[tokio::test]
    async fn barge_in_during_tool_execution_never_leaves_an_unmatched_tool_call() {
        let reasoner = Arc::new(ToolThenAnswerReasoner::default());
        let executor = Arc::new(BlockingToolExecutor::default());
        let tool = ToolDefinition {
            name: "lookup".into(),
            description: "Lookup a record".into(),
            input_schema: BTreeMap::from([("type".into(), Value::String("object".into()))]),
            timeout_ms: 10_000,
            side_effect: ToolSideEffect::None,
            execution: ToolExecution::Local,
        };
        let sink = Arc::new(MemoryEventSink::default());
        let mut providers = ProviderSet::scripted(vec![tool.clone()], Duration::ZERO);
        providers.reasoner = reasoner.clone();
        providers.tools = Arc::new(ToolCoordinator::with_concurrency(
            vec![tool],
            executor.clone(),
            1,
        ));
        let mut spawned = SessionActor::spawn(
            SessionConfig::default(),
            context(),
            providers,
            EventPipeline::spawn(sink.clone(), 128, 16),
        )
        .expect("actor");
        ready(&mut spawned).await;
        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("first-tool-turn"),
                text: "first".into(),
            })
            .expect("first input");
        tokio::time::timeout(Duration::from_secs(1), executor.started.notified())
            .await
            .expect("tool started");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if sink
                    .events()
                    .expect("events")
                    .iter()
                    .any(|event| event.event_type == EventType::ToolStarted)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("tool-start event committed");

        spawned
            .handle
            .try_control(ClientMessage::InputText {
                envelope: envelope("barge-during-tool"),
                text: "second".into(),
            })
            .expect("barge input");
        loop {
            if matches!(
                spawned.output.recv().await,
                Some(RealtimeOutput::Control(ServerMessage::ResponseCompleted {
                    epoch: 2,
                    interrupted: false,
                    ..
                }))
            ) {
                break;
            }
        }
        spawned.handle.end("internal_request").expect("end");
        spawned.task.await.expect("join").expect("actor");

        let requests = reasoner.requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1].messages,
            vec![
                ConversationMessage::User("first".into()),
                ConversationMessage::User("second".into()),
            ]
        );
        assert!(!requests[1].messages.iter().any(|message| matches!(
            message,
            ConversationMessage::AssistantToolCall { .. } | ConversationMessage::Tool { .. }
        )));
    }
}

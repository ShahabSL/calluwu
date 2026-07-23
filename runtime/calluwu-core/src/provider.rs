use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    RuntimeError,
    domain::ToolDefinition,
    manifest::AgentManifest,
    protocol::{ClientMessage, RealtimeOutput},
    tool::ToolCoordinator,
};

mod gateway;
pub use gateway::{GatewayProviderResolver, RuntimeServiceAccess};

/// Resolves immutable deployment provider references during trusted admission.
pub trait DeploymentProviderResolver: Send + Sync {
    fn resolve(
        &self,
        manifest: &AgentManifest,
        access: Option<&RuntimeServiceAccess>,
    ) -> crate::Result<ProviderSet>;
}

/// Resolver for the deterministic provider installed in foundation/local shards.
#[derive(Debug, Default)]
pub struct ScriptedProviderResolver;

impl DeploymentProviderResolver for ScriptedProviderResolver {
    fn resolve(
        &self,
        manifest: &AgentManifest,
        _access: Option<&RuntimeServiceAccess>,
    ) -> crate::Result<ProviderSet> {
        let references = [
            &manifest.definition.providers.speech_to_text,
            &manifest.definition.providers.reasoning,
            &manifest.definition.providers.text_to_speech,
        ];
        if references.iter().any(|reference| {
            reference.provider != "scripted"
                || reference.model != "scripted-v1"
                || !reference.settings.is_empty()
        }) {
            return Err(RuntimeError::InvalidRequest(
                "cloud scripted providers require provider=scripted, model=scripted-v1, and empty settings"
                    .into(),
            ));
        }
        if !manifest.definition.tools.is_empty() {
            return Err(RuntimeError::InvalidRequest(
                "scripted cloud shards do not execute deployed customer tools".into(),
            ));
        }
        let providers = ProviderSet::scripted_with_tool_concurrency(
            manifest.definition.tools.clone(),
            Duration::ZERO,
            manifest.definition.limits.max_concurrent_tools,
        );
        providers.ensure_capabilities(&manifest.required_capabilities)?;
        Ok(providers)
    }
}

/// Explicit local-only resolver used by `serve --agent-manifest`.
#[derive(Debug, Default)]
pub struct LocalScriptedProviderResolver;

impl DeploymentProviderResolver for LocalScriptedProviderResolver {
    fn resolve(
        &self,
        manifest: &AgentManifest,
        _access: Option<&RuntimeServiceAccess>,
    ) -> crate::Result<ProviderSet> {
        if !manifest.definition.tools.is_empty() {
            return Err(RuntimeError::InvalidRequest(
                "local scripted execution does not run deployment tools without an explicit adapter"
                    .into(),
            ));
        }
        let providers = ProviderSet::scripted_with_tool_concurrency(
            manifest.definition.tools.clone(),
            Duration::ZERO,
            manifest.definition.limits.max_concurrent_tools,
        );
        providers.ensure_capabilities(&manifest.required_capabilities)?;
        Ok(providers)
    }
}

/// A cancellable stream returned by native provider adapters.
pub type ProviderStream<T> =
    Pin<Box<dyn Stream<Item = std::result::Result<T, ProviderError>> + Send + 'static>>;

/// Whether a provider failure can safely be retried by its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorKind {
    Transient,
    Permanent,
    Cancelled,
}

/// Sanitized provider failure that never includes credentials or request bodies.
#[derive(Debug, Clone, Error)]
#[error("{stage}: {message}")]
pub struct ProviderError {
    pub stage: &'static str,
    pub kind: ProviderErrorKind,
    pub message: String,
}

impl ProviderError {
    #[must_use]
    pub fn cancelled(stage: &'static str) -> Self {
        Self {
            stage,
            kind: ProviderErrorKind::Cancelled,
            message: "operation cancelled".into(),
        }
    }

    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self.kind, ProviderErrorKind::Transient)
    }
}

impl From<ProviderError> for RuntimeError {
    fn from(error: ProviderError) -> Self {
        Self::Provider {
            stage: error.stage,
            message: error.message,
        }
    }
}

/// Explicit provider features negotiated before session admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderCapability {
    BatchStt,
    StreamingStt,
    StreamingReasoning,
    StreamingTts,
    ToolExecution,
    RealtimeSpeech,
}

impl ProviderCapability {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BatchStt => "batch-stt",
            Self::StreamingStt => "streaming-stt",
            Self::StreamingReasoning => "streaming-reasoning",
            Self::StreamingTts => "streaming-tts",
            Self::ToolExecution => "tool-execution",
            Self::RealtimeSpeech => "realtime-speech",
        }
    }
}

/// Transport frame after protocol decoding.
#[derive(Debug)]
pub enum TransportInput {
    Control(ClientMessage),
    Audio(Bytes),
    Ping(Bytes),
    Closed,
}

/// Runtime transport boundary. WebRTC and telephony are bridged into this contract by media edge.
#[async_trait]
pub trait Transport: Send {
    async fn receive(&mut self) -> std::result::Result<TransportInput, ProviderError>;
    async fn send(&mut self, message: &RealtimeOutput) -> std::result::Result<(), ProviderError>;
    async fn pong(&mut self, payload: Bytes) -> std::result::Result<(), ProviderError>;
    async fn close(&mut self, code: u16, reason: &str) -> std::result::Result<(), ProviderError>;
}

/// Audio committed by the caller for transcription.
#[derive(Debug, Clone)]
pub struct AudioInput {
    pub bytes: Bytes,
    pub sample_rate_hz: u32,
}

/// A partial or final transcription update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptSegment {
    pub text: String,
    pub is_final: bool,
}

/// Speech-to-text provider. The negotiated capability distinguishes complete-utterance
/// transcription from an adapter wired to receive audio before input.commit.
pub trait SpeechToText: Send + Sync {
    fn capabilities(&self) -> BTreeSet<ProviderCapability>;
    fn transcribe(
        &self,
        input: AudioInput,
        cancel: CancellationToken,
    ) -> ProviderStream<TranscriptSegment>;
}

/// One message retained in bounded conversation history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "role", content = "content")]
pub enum ConversationMessage {
    User(String),
    Assistant(String),
    AssistantToolCall {
        call_id: String,
        name: String,
        input: Value,
    },
    Tool {
        call_id: String,
        name: String,
        output: Value,
    },
}

/// Input to a provider-neutral reasoning turn.
#[derive(Debug, Clone)]
pub struct ReasoningRequest {
    pub instructions: String,
    pub messages: Vec<ConversationMessage>,
}

/// A provider-requested tool invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct RequestedToolCall {
    pub call_id: String,
    pub name: String,
    pub input: Value,
}

/// Streaming output from a reasoner.
#[derive(Debug, Clone, PartialEq)]
pub enum ReasoningEvent {
    Delta(String),
    ToolCall(RequestedToolCall),
    Completed,
}

/// Native streaming language/reasoning provider.
pub trait Reasoner: Send + Sync {
    fn capabilities(&self) -> BTreeSet<ProviderCapability>;
    fn reason(
        &self,
        request: ReasoningRequest,
        cancel: CancellationToken,
    ) -> ProviderStream<ReasoningEvent>;
}

/// Text synthesis input.
#[derive(Debug, Clone)]
pub struct SynthesisRequest {
    pub text: String,
    pub voice_id: String,
    pub sample_rate_hz: u32,
}

/// Encoded audio returned by a TTS adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynthesizedAudio {
    pub bytes: Bytes,
}

/// Native streaming text-to-speech provider.
pub trait TextToSpeech: Send + Sync {
    fn capabilities(&self) -> BTreeSet<ProviderCapability>;
    fn synthesize(
        &self,
        request: SynthesisRequest,
        cancel: CancellationToken,
    ) -> ProviderStream<SynthesizedAudio>;
}

/// Input to a future speech-to-speech model while preserving Calluwu lifecycle.
#[derive(Debug, Clone)]
pub struct RealtimeSpeechRequest {
    pub audio: Bytes,
    pub instructions: String,
    pub history: Vec<ConversationMessage>,
}

/// Provider-neutral event from a realtime speech model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RealtimeSpeechEvent {
    Transcript(String),
    Text(String),
    Audio(Bytes),
    Completed,
}

/// Optional speech-to-speech capability. It cannot bypass tools, fencing, or events.
pub trait RealtimeSpeech: Send + Sync {
    fn capabilities(&self) -> BTreeSet<ProviderCapability>;
    fn converse(
        &self,
        request: RealtimeSpeechRequest,
        cancel: CancellationToken,
    ) -> ProviderStream<RealtimeSpeechEvent>;
}

/// Invocation sent to an external or local tool executor.
#[derive(Debug, Clone)]
pub struct ToolInvocation {
    pub call_id: String,
    pub tool_name: String,
    pub input: Value,
    pub idempotency_key: String,
}

/// Successful tool result.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolOutput {
    pub value: Value,
}

/// Tool adapter boundary. Side-effect policy is enforced by [`ToolCoordinator`].
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
    ) -> std::result::Result<ToolOutput, ProviderError>;
}

/// Fully injected set of providers for one immutable deployment.
#[derive(Clone)]
pub struct ProviderSet {
    pub speech_to_text: Arc<dyn SpeechToText>,
    pub reasoner: Arc<dyn Reasoner>,
    pub text_to_speech: Arc<dyn TextToSpeech>,
    pub tools: Arc<ToolCoordinator>,
    pub realtime_speech: Option<Arc<dyn RealtimeSpeech>>,
    tool_execution_enabled: bool,
}

impl ProviderSet {
    /// Deterministic, secret-free providers for tests and local execution.
    #[must_use]
    pub fn scripted(tools: Vec<ToolDefinition>, delay: Duration) -> Self {
        Self::scripted_with_tool_concurrency(tools, delay, 4)
    }

    /// Deterministic providers with a deployment-specific tool concurrency budget.
    #[must_use]
    pub fn scripted_with_tool_concurrency(
        tools: Vec<ToolDefinition>,
        delay: Duration,
        max_concurrent_tools: usize,
    ) -> Self {
        let tool_execution_enabled = !tools.is_empty();
        let scripted = Arc::new(ScriptedProvider::new(delay));
        let tool_executor = Arc::new(ScriptedToolExecutor);
        Self {
            speech_to_text: scripted.clone(),
            reasoner: scripted.clone(),
            text_to_speech: scripted.clone(),
            tools: Arc::new(ToolCoordinator::with_concurrency(
                tools,
                tool_executor,
                max_concurrent_tools,
            )),
            realtime_speech: Some(scripted),
            tool_execution_enabled,
        }
    }

    /// Union of capabilities exposed to admission and `session.ready`.
    #[must_use]
    pub fn capabilities(&self) -> BTreeSet<ProviderCapability> {
        let mut result = self.speech_to_text.capabilities();
        result.extend(self.reasoner.capabilities());
        result.extend(self.text_to_speech.capabilities());
        if self.tool_execution_enabled {
            result.insert(ProviderCapability::ToolExecution);
        }
        if let Some(provider) = &self.realtime_speech {
            result.extend(provider.capabilities());
        }
        result
    }

    /// Fail admission rather than discovering a missing capability mid-call.
    pub fn ensure_capabilities(&self, required: &[String]) -> crate::Result<()> {
        let available: BTreeSet<_> = self
            .capabilities()
            .into_iter()
            .map(ProviderCapability::as_str)
            .collect();
        let missing: Vec<_> = required
            .iter()
            .filter(|capability| !available.contains(capability.as_str()))
            .cloned()
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(RuntimeError::InvalidRequest(
                format!(
                    "deployment requires unavailable capabilities: {}",
                    missing.join(", ")
                )
                .into(),
            ))
        }
    }
}

/// One deterministic provider implements every streaming speech/model interface.
#[derive(Debug)]
pub struct ScriptedProvider {
    delay: Duration,
}

impl ScriptedProvider {
    #[must_use]
    pub const fn new(delay: Duration) -> Self {
        Self { delay }
    }

    fn text_for_audio(input: &AudioInput) -> String {
        std::str::from_utf8(&input.bytes)
            .ok()
            .map(str::trim)
            .filter(|text| {
                !text.is_empty()
                    && text
                        .chars()
                        .all(|character| !character.is_control() || character.is_whitespace())
            })
            .map_or_else(
                || format!("audio input ({} bytes)", input.bytes.len()),
                str::to_owned,
            )
    }
}

impl SpeechToText for ScriptedProvider {
    fn capabilities(&self) -> BTreeSet<ProviderCapability> {
        // The session engine supplies a complete utterance only after input.commit. Partial
        // scripted transcript events do not make this an end-to-end streaming STT adapter.
        BTreeSet::from([ProviderCapability::BatchStt])
    }

    fn transcribe(
        &self,
        input: AudioInput,
        cancel: CancellationToken,
    ) -> ProviderStream<TranscriptSegment> {
        let delay = self.delay;
        let text = Self::text_for_audio(&input);
        Box::pin(stream! {
            let partial = text.split_whitespace().next().unwrap_or_default().to_owned();
            if !partial.is_empty() {
                tokio::select! {
                    () = cancel.cancelled() => {
                        yield Err(ProviderError::cancelled("stt"));
                        return;
                    }
                    () = tokio::time::sleep(delay) => {
                        yield Ok(TranscriptSegment { text: partial, is_final: false });
                    }
                }
            }
            tokio::select! {
                () = cancel.cancelled() => yield Err(ProviderError::cancelled("stt")),
                () = tokio::time::sleep(delay) => {
                    yield Ok(TranscriptSegment { text, is_final: true });
                }
            }
        })
    }
}

impl Reasoner for ScriptedProvider {
    fn capabilities(&self) -> BTreeSet<ProviderCapability> {
        BTreeSet::from([ProviderCapability::StreamingReasoning])
    }

    fn reason(
        &self,
        request: ReasoningRequest,
        cancel: CancellationToken,
    ) -> ProviderStream<ReasoningEvent> {
        let delay = self.delay;
        let input = request
            .messages
            .iter()
            .rev()
            .find_map(|message| match message {
                ConversationMessage::User(value) => Some(value.as_str()),
                _ => None,
            })
            .unwrap_or_default();
        let tool_call = if request
            .messages
            .iter()
            .any(|message| matches!(message, ConversationMessage::Tool { .. }))
        {
            None
        } else {
            parse_scripted_tool_call(input)
        };
        let response = if let Some(ConversationMessage::Tool { name, output, .. }) = request
            .messages
            .iter()
            .rev()
            .find(|message| matches!(message, ConversationMessage::Tool { .. }))
        {
            format!("Scripted response after {name}: {output}")
        } else {
            format!("Scripted response: {}", input.trim())
        };
        Box::pin(stream! {
            if let Some(call) = tool_call {
                yield Ok(ReasoningEvent::ToolCall(call));
            } else {
                for (index, word) in response.split_whitespace().enumerate() {
                    tokio::select! {
                        () = cancel.cancelled() => {
                            yield Err(ProviderError::cancelled("reasoner"));
                            return;
                        }
                        () = tokio::time::sleep(delay) => {
                            let suffix = if index == 0 { "" } else { " " };
                            yield Ok(ReasoningEvent::Delta(format!("{suffix}{word}")));
                        }
                    }
                }
            }
            if cancel.is_cancelled() {
                yield Err(ProviderError::cancelled("reasoner"));
            } else {
                yield Ok(ReasoningEvent::Completed);
            }
        })
    }
}

impl TextToSpeech for ScriptedProvider {
    fn capabilities(&self) -> BTreeSet<ProviderCapability> {
        BTreeSet::from([ProviderCapability::StreamingTts])
    }

    fn synthesize(
        &self,
        request: SynthesisRequest,
        cancel: CancellationToken,
    ) -> ProviderStream<SynthesizedAudio> {
        let delay = self.delay;
        let chunks: Vec<_> = request
            .text
            .split_whitespace()
            .map(|word| scripted_pcm_chunk(word, request.sample_rate_hz))
            .collect();
        Box::pin(stream! {
            for bytes in chunks {
                tokio::select! {
                    () = cancel.cancelled() => {
                        yield Err(ProviderError::cancelled("tts"));
                        return;
                    }
                    () = tokio::time::sleep(delay) => {
                        yield Ok(SynthesizedAudio { bytes });
                    }
                }
            }
        })
    }
}

fn scripted_pcm_chunk(word: &str, sample_rate_hz: u32) -> Bytes {
    let amplitude = word
        .bytes()
        .fold(0_i16, |accumulator, byte| {
            accumulator.wrapping_add(i16::from(byte))
        })
        .clamp(256, 4_096);
    let samples = (sample_rate_hz / 100).max(1);
    let mut pcm = Vec::with_capacity(samples as usize * 2);
    for index in 0..samples {
        let sample = if index % 2 == 0 {
            amplitude
        } else {
            -amplitude
        };
        pcm.extend_from_slice(&sample.to_le_bytes());
    }
    Bytes::from(pcm)
}

impl RealtimeSpeech for ScriptedProvider {
    fn capabilities(&self) -> BTreeSet<ProviderCapability> {
        BTreeSet::from([ProviderCapability::RealtimeSpeech])
    }

    fn converse(
        &self,
        request: RealtimeSpeechRequest,
        cancel: CancellationToken,
    ) -> ProviderStream<RealtimeSpeechEvent> {
        let delay = self.delay;
        let transcript = std::str::from_utf8(&request.audio)
            .unwrap_or("audio input")
            .trim()
            .to_owned();
        let response = format!("Scripted response: {transcript}");
        Box::pin(stream! {
            for event in [
                RealtimeSpeechEvent::Transcript(transcript),
                RealtimeSpeechEvent::Text(response.clone()),
                RealtimeSpeechEvent::Audio(Bytes::from(response)),
                RealtimeSpeechEvent::Completed,
            ] {
                tokio::select! {
                    () = cancel.cancelled() => {
                        yield Err(ProviderError::cancelled("realtime_speech"));
                        return;
                    }
                    () = tokio::time::sleep(delay) => yield Ok(event),
                }
            }
        })
    }
}

fn parse_scripted_tool_call(input: &str) -> Option<RequestedToolCall> {
    let rest = input.trim().strip_prefix("/tool ")?;
    let (name, json) = rest.split_once(' ').unwrap_or((rest, "{}"));
    let input = serde_json::from_str(json).unwrap_or_else(|_| {
        Value::Object(serde_json::Map::from_iter([(
            "value".into(),
            Value::String(json.into()),
        )]))
    });
    Some(RequestedToolCall {
        call_id: "tool_scripted_1".into(),
        name: name.into(),
        input,
    })
}

/// Secret-free deterministic tool adapter.
#[derive(Debug)]
pub struct ScriptedToolExecutor;

#[async_trait]
impl ToolExecutor for ScriptedToolExecutor {
    async fn execute(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
    ) -> std::result::Result<ToolOutput, ProviderError> {
        if cancel.is_cancelled() {
            return Err(ProviderError::cancelled("tool"));
        }
        Ok(ToolOutput {
            value: Value::Object(serde_json::Map::from_iter([
                ("ok".into(), Value::Bool(true)),
                ("tool".into(), Value::String(invocation.tool_name)),
                ("input".into(), invocation.input),
            ])),
        })
    }
}

/// Utility for event payload construction without exposing hash-order variation.
#[must_use]
pub fn value_map(entries: impl IntoIterator<Item = (String, Value)>) -> BTreeMap<String, Value> {
    entries.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ToolExecution, ToolSideEffect};

    #[test]
    fn tool_capability_requires_an_enabled_declared_adapter() {
        let without_tools = ProviderSet::scripted(Vec::new(), Duration::ZERO);
        assert!(
            !without_tools
                .capabilities()
                .contains(&ProviderCapability::ToolExecution)
        );

        let with_explicit_test_tool = ProviderSet::scripted(
            vec![ToolDefinition {
                name: "lookup".into(),
                description: "Lookup".into(),
                input_schema: BTreeMap::new(),
                timeout_ms: 1_000,
                side_effect: ToolSideEffect::None,
                execution: ToolExecution::Local,
            }],
            Duration::ZERO,
        );
        assert!(
            with_explicit_test_tool
                .capabilities()
                .contains(&ProviderCapability::ToolExecution)
        );
    }

    #[test]
    fn post_commit_scripted_stt_does_not_claim_end_to_end_streaming() {
        let providers = ProviderSet::scripted(Vec::new(), Duration::ZERO);
        assert!(
            providers
                .capabilities()
                .contains(&ProviderCapability::BatchStt)
        );
        assert!(
            !providers
                .capabilities()
                .contains(&ProviderCapability::StreamingStt)
        );
        assert!(
            providers
                .ensure_capabilities(&["streaming-stt".into()])
                .is_err()
        );
    }
}

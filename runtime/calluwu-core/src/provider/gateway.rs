use std::{collections::BTreeSet, sync::Arc, time::Duration};

use async_stream::stream;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::BytesMut;
use futures_util::StreamExt;
use reqwest::{Client, Response, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    AudioInput, ConversationMessage, DeploymentProviderResolver, ProviderCapability, ProviderError,
    ProviderErrorKind, ProviderSet, ProviderStream, Reasoner, ReasoningEvent, ReasoningRequest,
    RequestedToolCall, SpeechToText, SynthesisRequest, SynthesizedAudio, TextToSpeech,
    ToolExecutor, ToolInvocation, ToolOutput, TranscriptSegment,
};
use crate::{
    RuntimeError,
    domain::{TenantContext, ToolDefinition},
    manifest::{AgentManifest, ProviderReference},
    tool::ToolCoordinator,
};

const MAX_GATEWAY_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_GATEWAY_EVENT_BYTES: usize = 512 * 1024;
const MAX_TTS_CHUNK_BYTES: usize = 32 * 1024;
const MAX_TTS_TEXT_CHARACTERS: usize = 500;
const TTS_STREAM_MAGIC: &[u8; 4] = b"CWT1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-session, generation-fenced access to Calluwu's private runtime service endpoints.
#[derive(Debug, Clone)]
pub struct RuntimeServiceAccess {
    base_url: Url,
    credential: String,
    context: TenantContext,
}

impl RuntimeServiceAccess {
    pub fn from_ingest(
        ingest_url: &str,
        credential: String,
        context: TenantContext,
    ) -> crate::Result<Self> {
        let mut base_url = Url::parse(ingest_url)
            .map_err(|_| RuntimeError::InvalidRequest("runtime service URL is invalid".into()))?;
        let host = base_url.host_str();
        let loopback_http =
            base_url.scheme() == "http" && matches!(host, Some("127.0.0.1" | "localhost" | "::1"));
        if (base_url.scheme() != "https" && !loopback_http) || host.is_none() {
            return Err(RuntimeError::InvalidRequest(
                "runtime service URL must use HTTPS".into(),
            ));
        }
        base_url.set_path("/");
        base_url.set_query(None);
        base_url.set_fragment(None);
        if !(32..=128).contains(&credential.len())
            || !credential
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(RuntimeError::InvalidRequest(
                "runtime service credential is invalid".into(),
            ));
        }
        context.validate()?;
        Ok(Self {
            base_url,
            credential,
            context,
        })
    }
}

/// Production resolver for the Cloudflare-hosted provider gateway.
#[derive(Debug, Default)]
pub struct GatewayProviderResolver;

impl DeploymentProviderResolver for GatewayProviderResolver {
    fn resolve(
        &self,
        manifest: &AgentManifest,
        access: Option<&RuntimeServiceAccess>,
    ) -> crate::Result<ProviderSet> {
        let references = [
            &manifest.definition.providers.speech_to_text,
            &manifest.definition.providers.reasoning,
            &manifest.definition.providers.text_to_speech,
        ];
        if references.iter().all(|reference| {
            reference.provider == "scripted"
                && reference.model == "scripted-v1"
                && reference.settings.is_empty()
        }) {
            if !manifest.definition.tools.is_empty() {
                return Err(RuntimeError::InvalidRequest(
                    "scripted cloud deployments cannot execute tools".into(),
                ));
            }
            let providers = ProviderSet::scripted_with_tool_concurrency(
                Vec::new(),
                Duration::ZERO,
                manifest.definition.limits.max_concurrent_tools,
            );
            providers.ensure_capabilities(&manifest.required_capabilities)?;
            return Ok(providers);
        }

        validate_cloudflare_references(manifest)?;
        let access = access.ok_or_else(|| {
            RuntimeError::InvalidRequest("provider gateway access is required".into())
        })?;
        let gateway = GatewayClient::new(access.clone())?;
        let tools = manifest.definition.tools.clone();
        let tool_execution_enabled = !tools.is_empty();
        let providers = ProviderSet {
            speech_to_text: Arc::new(GatewaySpeechToText {
                gateway: gateway.clone(),
                reference: manifest.definition.providers.speech_to_text.clone(),
                language: manifest.definition.voice.language.clone(),
            }),
            reasoner: Arc::new(GatewayReasoner {
                gateway: gateway.clone(),
                reference: manifest.definition.providers.reasoning.clone(),
                tools: tools.clone(),
            }),
            text_to_speech: Arc::new(GatewayTextToSpeech {
                gateway: gateway.clone(),
                reference: manifest.definition.providers.text_to_speech.clone(),
            }),
            tools: Arc::new(ToolCoordinator::with_concurrency(
                tools,
                Arc::new(GatewayToolExecutor { gateway }),
                manifest.definition.limits.max_concurrent_tools,
            )),
            realtime_speech: None,
            tool_execution_enabled,
        };
        providers.ensure_capabilities(&manifest.required_capabilities)?;
        Ok(providers)
    }
}

fn validate_cloudflare_references(manifest: &AgentManifest) -> crate::Result<()> {
    let stt = &manifest.definition.providers.speech_to_text;
    let reason = &manifest.definition.providers.reasoning;
    let tts = &manifest.definition.providers.text_to_speech;
    if stt.provider != "cloudflare" || stt.model != "@cf/deepgram/nova-3" {
        return Err(RuntimeError::InvalidRequest(
            "speechToText must use cloudflare/@cf/deepgram/nova-3".into(),
        ));
    }
    if reason.provider != "cloudflare" || reason.model != "@cf/openai/gpt-oss-20b" {
        return Err(RuntimeError::InvalidRequest(
            "reasoning must use cloudflare/@cf/openai/gpt-oss-20b".into(),
        ));
    }
    if tts.provider != "cloudflare"
        || !matches!(
            tts.model.as_str(),
            "@cf/deepgram/aura-2-en" | "@cf/deepgram/aura-2-es"
        )
    {
        return Err(RuntimeError::InvalidRequest(
            "textToSpeech must use a supported Cloudflare Aura 2 model".into(),
        ));
    }
    validate_stt_settings(stt)?;
    validate_reasoning_settings(reason)?;
    validate_tts_settings(tts, &manifest.definition.voice.id)?;
    if !matches!(
        manifest.definition.voice.language.as_str(),
        "en" | "en-US"
            | "en-AU"
            | "en-GB"
            | "en-IN"
            | "en-NZ"
            | "es"
            | "es-419"
            | "fr"
            | "fr-CA"
            | "de"
            | "de-CH"
            | "hi"
            | "ru"
            | "pt"
            | "pt-BR"
            | "pt-PT"
            | "ja"
            | "it"
            | "nl"
            | "multi"
    ) {
        return Err(RuntimeError::InvalidRequest(
            "voice.language is not supported by Cloudflare Nova-3".into(),
        ));
    }
    if !matches!(
        manifest.definition.voice.sample_rate_hz,
        8_000 | 16_000 | 24_000 | 32_000 | 48_000
    ) {
        return Err(RuntimeError::InvalidRequest(
            "Cloudflare linear16 audio requires 8000, 16000, 24000, 32000, or 48000 Hz".into(),
        ));
    }
    Ok(())
}

fn reject_unknown_settings(reference: &ProviderReference, allowed: &[&str]) -> crate::Result<()> {
    reference
        .settings
        .keys()
        .find(|key| !allowed.contains(&key.as_str()))
        .map_or(Ok(()), |key| {
            Err(RuntimeError::InvalidRequest(
                format!("unsupported {} setting {key}", reference.provider).into(),
            ))
        })
}

fn validate_stt_settings(reference: &ProviderReference) -> crate::Result<()> {
    reject_unknown_settings(reference, &["detectLanguage", "keyterm", "mipOptOut"])?;
    for key in ["detectLanguage", "mipOptOut"] {
        if reference
            .settings
            .get(key)
            .is_some_and(|value| !value.is_boolean())
        {
            return Err(RuntimeError::InvalidRequest(
                format!("Cloudflare STT setting {key} must be boolean").into(),
            ));
        }
    }
    if let Some(value) = reference.settings.get("keyterm") {
        let Some(value) = value.as_str() else {
            return Err(RuntimeError::InvalidRequest(
                "Cloudflare STT keyterm must be a string".into(),
            ));
        };
        if value.is_empty() || value.len() > 2_000 {
            return Err(RuntimeError::InvalidRequest(
                "Cloudflare STT keyterm must contain 1 to 2000 UTF-8 bytes".into(),
            ));
        }
    }
    Ok(())
}

fn validate_reasoning_settings(reference: &ProviderReference) -> crate::Result<()> {
    reject_unknown_settings(reference, &["maxTokens", "temperature", "topP"])?;
    if let Some(value) = reference.settings.get("maxTokens")
        && !value
            .as_f64()
            .is_some_and(|value| value.fract() == 0.0 && (1.0..=4_096.0).contains(&value))
    {
        return Err(RuntimeError::InvalidRequest(
            "Cloudflare reasoning maxTokens must be an integer from 1 through 4096".into(),
        ));
    }
    if let Some(value) = reference.settings.get("temperature")
        && !value
            .as_f64()
            .is_some_and(|value| (0.0..=5.0).contains(&value))
    {
        return Err(RuntimeError::InvalidRequest(
            "Cloudflare reasoning temperature must be between 0 and 5".into(),
        ));
    }
    if let Some(value) = reference.settings.get("topP")
        && !value
            .as_f64()
            .is_some_and(|value| (0.001..=1.0).contains(&value))
    {
        return Err(RuntimeError::InvalidRequest(
            "Cloudflare reasoning topP must be between 0.001 and 1".into(),
        ));
    }
    Ok(())
}

fn validate_tts_settings(reference: &ProviderReference, voice_id: &str) -> crate::Result<()> {
    reject_unknown_settings(reference, &["speaker"])?;
    let speaker = match reference.settings.get("speaker") {
        Some(value) => value.as_str().ok_or_else(|| {
            RuntimeError::InvalidRequest("Cloudflare TTS speaker must be a string".into())
        })?,
        None => voice_id,
    };
    let supported = match reference.model.as_str() {
        "@cf/deepgram/aura-2-en" => matches!(
            speaker,
            "amalthea"
                | "andromeda"
                | "apollo"
                | "arcas"
                | "aries"
                | "asteria"
                | "athena"
                | "atlas"
                | "aurora"
                | "callista"
                | "cora"
                | "cordelia"
                | "delia"
                | "draco"
                | "electra"
                | "harmonia"
                | "helena"
                | "hera"
                | "hermes"
                | "hyperion"
                | "iris"
                | "janus"
                | "juno"
                | "jupiter"
                | "luna"
                | "mars"
                | "minerva"
                | "neptune"
                | "odysseus"
                | "ophelia"
                | "orion"
                | "orpheus"
                | "pandora"
                | "phoebe"
                | "pluto"
                | "saturn"
                | "thalia"
                | "theia"
                | "vesta"
                | "zeus"
        ),
        "@cf/deepgram/aura-2-es" => matches!(
            speaker,
            "sirio"
                | "nestor"
                | "carina"
                | "celeste"
                | "alvaro"
                | "diana"
                | "aquila"
                | "selena"
                | "estrella"
                | "javier"
        ),
        _ => false,
    };
    if !supported {
        return Err(RuntimeError::InvalidRequest(
            "voice.id or TTS speaker is not supported by the selected Aura-2 model".into(),
        ));
    }
    Ok(())
}

#[derive(Clone)]
struct GatewayClient {
    client: Client,
    access: RuntimeServiceAccess,
}

impl GatewayClient {
    fn new(access: RuntimeServiceAccess) -> crate::Result<Self> {
        let client = Client::builder()
            .https_only(access.base_url.scheme() == "https")
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| RuntimeError::Internal("provider HTTP client could not start".into()))?;
        Ok(Self { client, access })
    }

    async fn post<T: Serialize, R: DeserializeOwned>(
        &self,
        operation: &'static str,
        request: &T,
        cancel: CancellationToken,
    ) -> std::result::Result<R, ProviderError> {
        let path = format!(
            "/v1/runtime/sessions/{}/providers/{operation}",
            self.access.context.session_id
        );
        let url = self
            .access
            .base_url
            .join(&path)
            .map_err(|_| permanent(operation))?;
        let future = self
            .client
            .post(url)
            .bearer_auth(&self.access.credential)
            .header(
                "x-calluwu-runtime-generation",
                self.access.context.runtime_generation,
            )
            .header(
                "x-calluwu-organization-id",
                &self.access.context.organization_id,
            )
            .header("x-calluwu-project-id", &self.access.context.project_id)
            .header(
                "x-calluwu-deployment-id",
                &self.access.context.deployment_id,
            )
            .json(request)
            .send();
        let response = tokio::select! {
            () = cancel.cancelled() => return Err(ProviderError::cancelled(operation)),
            result = future => result.map_err(|_| transient(operation))?,
        };
        let status = response.status();
        if !status.is_success() {
            // Dropping the response cancels an untrusted or accidentally unbounded error body.
            // Error classification depends only on status; no provider payload is surfaced.
            drop(response);
            return Err(
                if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                    transient(operation)
                } else {
                    permanent(operation)
                },
            );
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_GATEWAY_RESPONSE_BYTES as u64)
        {
            return Err(permanent(operation));
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = tokio::select! {
            () = cancel.cancelled() => return Err(ProviderError::cancelled(operation)),
            chunk = stream.next() => chunk,
        } {
            let chunk = chunk.map_err(|_| transient(operation))?;
            if bytes.len().saturating_add(chunk.len()) > MAX_GATEWAY_RESPONSE_BYTES {
                return Err(permanent(operation));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| permanent(operation))
    }

    async fn post_stream<T: Serialize>(
        &self,
        operation: &'static str,
        request: &T,
        cancel: CancellationToken,
        expected_content_type: &'static str,
    ) -> std::result::Result<Response, ProviderError> {
        let path = format!(
            "/v1/runtime/sessions/{}/providers/{operation}",
            self.access.context.session_id
        );
        let url = self
            .access
            .base_url
            .join(&path)
            .map_err(|_| permanent(operation))?;
        let future = self
            .client
            .post(url)
            .bearer_auth(&self.access.credential)
            .header(
                "x-calluwu-runtime-generation",
                self.access.context.runtime_generation,
            )
            .header(
                "x-calluwu-organization-id",
                &self.access.context.organization_id,
            )
            .header("x-calluwu-project-id", &self.access.context.project_id)
            .header(
                "x-calluwu-deployment-id",
                &self.access.context.deployment_id,
            )
            .json(request)
            .send();
        let response = tokio::select! {
            () = cancel.cancelled() => return Err(ProviderError::cancelled(operation)),
            result = future => result.map_err(|_| transient(operation))?,
        };
        let status = response.status();
        if !status.is_success() {
            drop(response);
            return Err(
                if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                    transient(operation)
                } else {
                    permanent(operation)
                },
            );
        }
        if response
            .content_length()
            .is_some_and(|length| length == 0 || length > MAX_GATEWAY_RESPONSE_BYTES as u64)
        {
            return Err(permanent(operation));
        }
        let media_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if media_type != Some(expected_content_type) {
            return Err(permanent(operation));
        }
        Ok(response)
    }

    fn base<'a>(&self, reference: &'a ProviderReference) -> GatewayRequestBase<'a> {
        GatewayRequestBase {
            runtime_generation: self.access.context.runtime_generation,
            invocation_id: format!("inv_{}", Uuid::now_v7()),
            provider: &reference.provider,
            model: &reference.model,
            settings: &reference.settings,
        }
    }
}

fn transient(stage: &'static str) -> ProviderError {
    ProviderError {
        stage,
        kind: ProviderErrorKind::Transient,
        message: "provider gateway is temporarily unavailable".into(),
    }
}

fn permanent(stage: &'static str) -> ProviderError {
    ProviderError {
        stage,
        kind: ProviderErrorKind::Permanent,
        message: "provider gateway rejected the invocation".into(),
    }
}

fn stable_invocation_id(seed: &str) -> String {
    format!(
        "inv_{}",
        URL_SAFE_NO_PAD.encode(Sha256::digest(seed.as_bytes()))
    )
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GatewayRequestBase<'a> {
    runtime_generation: u64,
    invocation_id: String,
    provider: &'a str,
    model: &'a str,
    settings: &'a std::collections::BTreeMap<String, Value>,
}

#[derive(Clone)]
struct GatewaySpeechToText {
    gateway: GatewayClient,
    reference: ProviderReference,
    language: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SttRequest<'a> {
    #[serde(flatten)]
    base: GatewayRequestBase<'a>,
    audio_base64: String,
    sample_rate_hz: u32,
    language: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SttResponse {
    invocation_id: String,
    text: String,
    confidence: Option<f64>,
    #[serde(default)]
    words: Vec<Value>,
}

impl SpeechToText for GatewaySpeechToText {
    fn capabilities(&self) -> BTreeSet<ProviderCapability> {
        BTreeSet::from([ProviderCapability::BatchStt])
    }

    fn transcribe(
        &self,
        input: AudioInput,
        cancel: CancellationToken,
    ) -> ProviderStream<TranscriptSegment> {
        let provider = self.clone();
        Box::pin(stream! {
            let request = SttRequest {
                base: provider.gateway.base(&provider.reference),
                audio_base64: URL_SAFE_NO_PAD.encode(input.bytes),
                sample_rate_hz: input.sample_rate_hz,
                language: &provider.language,
            };
            let expected_invocation_id = request.base.invocation_id.clone();
            match provider.gateway.post::<_, SttResponse>("stt", &request, cancel).await {
                Ok(response) if response.invocation_id == expected_invocation_id => {
                    let _metadata = (response.confidence, response.words);
                    yield Ok(TranscriptSegment { text: response.text, is_final: true });
                }
                Ok(_) => yield Err(permanent("stt")),
                Err(error) => yield Err(error),
            }
        })
    }
}

#[derive(Clone)]
struct GatewayReasoner {
    gateway: GatewayClient,
    reference: ProviderReference,
    tools: Vec<ToolDefinition>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReasonRequest<'a> {
    #[serde(flatten)]
    base: GatewayRequestBase<'a>,
    instructions: &'a str,
    messages: Vec<Value>,
    tools: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GatewayReasonMeta {
    invocation_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GatewayReasonDelta {
    text: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GatewayToolCall {
    call_id: String,
    name: String,
    input: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GatewayReasonCompleted {
    finish_reason: String,
    usage: GatewayReasonUsage,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GatewayReasonUsage {
    prompt_tokens: u64,
    completion_tokens: u64,
}

fn sse_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    for index in 0..buffer.len().saturating_sub(1) {
        if buffer.get(index..index + 2) == Some(b"\n\n") {
            return Some((index, 2));
        }
        if buffer.get(index..index.saturating_add(4)) == Some(b"\r\n\r\n") {
            return Some((index, 4));
        }
    }
    None
}

fn take_sse_event(
    buffer: &mut Vec<u8>,
) -> std::result::Result<Option<(String, Value)>, ProviderError> {
    let Some((boundary, delimiter_bytes)) = sse_boundary(buffer) else {
        return Ok(None);
    };
    let consumed: Vec<u8> = buffer.drain(..boundary + delimiter_bytes).collect();
    let block = std::str::from_utf8(&consumed[..boundary]).map_err(|_| permanent("reason"))?;
    let mut event = None;
    let mut data = Vec::new();
    for line in block.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if let Some(value) = line.strip_prefix("event:") {
            if event.is_some() {
                return Err(permanent("reason"));
            }
            event = Some(value.strip_prefix(' ').unwrap_or(value).to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value));
        } else if !line.is_empty() && !line.starts_with(':') {
            return Err(permanent("reason"));
        }
    }
    let event = event.ok_or_else(|| permanent("reason"))?;
    if data.is_empty() {
        return Err(permanent("reason"));
    }
    let value = serde_json::from_str(&data.join("\n")).map_err(|_| permanent("reason"))?;
    Ok(Some((event, value)))
}

impl Reasoner for GatewayReasoner {
    fn capabilities(&self) -> BTreeSet<ProviderCapability> {
        BTreeSet::from([ProviderCapability::StreamingReasoning])
    }

    fn reason(
        &self,
        request: ReasoningRequest,
        cancel: CancellationToken,
    ) -> ProviderStream<ReasoningEvent> {
        let provider = self.clone();
        Box::pin(stream! {
            let messages = request.messages.iter().map(message_value).collect();
            let tools = provider.tools.iter().map(|tool| serde_json::json!({
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema,
            })).collect();
            let invocation = ReasonRequest {
                base: provider.gateway.base(&provider.reference),
                instructions: &request.instructions,
                messages,
                tools,
            };
            let expected_invocation_id = invocation.base.invocation_id.clone();
            let response = match provider.gateway.post_stream(
                "reason",
                &invocation,
                cancel.clone(),
                "text/event-stream",
            ).await {
                Ok(response) => response,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };
            let mut body = response.bytes_stream();
            let mut buffer = Vec::new();
            let mut total_bytes = 0_usize;
            let mut authenticated = false;
            while let Some(chunk) = tokio::select! {
                () = cancel.cancelled() => {
                    yield Err(ProviderError::cancelled("reason"));
                    return;
                }
                chunk = body.next() => chunk,
            } {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        yield Err(transient("reason"));
                        return;
                    }
                };
                total_bytes = total_bytes.saturating_add(chunk.len());
                if total_bytes > MAX_GATEWAY_RESPONSE_BYTES {
                    yield Err(permanent("reason"));
                    return;
                }
                buffer.extend_from_slice(&chunk);
                loop {
                    let event = match take_sse_event(&mut buffer) {
                        Ok(Some(event)) => event,
                        Ok(None) => break,
                        Err(error) => {
                            yield Err(error);
                            return;
                        }
                    };
                    match event.0.as_str() {
                        "meta" if !authenticated => {
                            let metadata = match serde_json::from_value::<GatewayReasonMeta>(event.1) {
                                Ok(metadata) => metadata,
                                Err(_) => {
                                    yield Err(permanent("reason"));
                                    return;
                                }
                            };
                            if metadata.invocation_id != expected_invocation_id {
                                yield Err(permanent("reason"));
                                return;
                            }
                            authenticated = true;
                        }
                        "delta" if authenticated => {
                            let delta = match serde_json::from_value::<GatewayReasonDelta>(event.1) {
                                Ok(delta) if !delta.text.is_empty() => delta,
                                _ => {
                                    yield Err(permanent("reason"));
                                    return;
                                }
                            };
                            yield Ok(ReasoningEvent::Delta(delta.text));
                        }
                        "tool_call" if authenticated => {
                            let call = match serde_json::from_value::<GatewayToolCall>(event.1) {
                                Ok(call) if call.input.is_object() => call,
                                _ => {
                                    yield Err(permanent("reason"));
                                    return;
                                }
                            };
                            yield Ok(ReasoningEvent::ToolCall(RequestedToolCall {
                                call_id: call.call_id,
                                name: call.name,
                                input: call.input,
                            }));
                        }
                        "completed" if authenticated => {
                            let completed = match serde_json::from_value::<GatewayReasonCompleted>(event.1) {
                                Ok(completed)
                                    if !completed.finish_reason.is_empty()
                                        && completed.usage.prompt_tokens > 0
                                        && completed.usage.completion_tokens > 0 => completed,
                                _ => {
                                    yield Err(permanent("reason"));
                                    return;
                                }
                            };
                            let _usage = completed.usage;
                            yield Ok(ReasoningEvent::Completed);
                            return;
                        }
                        _ => {
                            yield Err(permanent("reason"));
                            return;
                        }
                    }
                }
                if buffer.len() > MAX_GATEWAY_EVENT_BYTES {
                    yield Err(permanent("reason"));
                    return;
                }
            }
            yield Err(permanent("reason"));
        })
    }
}

fn message_value(message: &ConversationMessage) -> Value {
    match message {
        ConversationMessage::User(content) => {
            serde_json::json!({"kind": "user", "content": content})
        }
        ConversationMessage::Assistant(content) => {
            serde_json::json!({"kind": "assistant", "content": content})
        }
        ConversationMessage::AssistantToolCall {
            call_id,
            name,
            input,
        } => serde_json::json!({
            "kind": "assistant_tool_call",
            "callId": call_id,
            "name": name,
            "input": input,
        }),
        ConversationMessage::Tool {
            call_id,
            name,
            output,
        } => serde_json::json!({
            "kind": "tool",
            "callId": call_id,
            "name": name,
            "output": output,
        }),
    }
}

#[derive(Clone)]
struct GatewayTextToSpeech {
    gateway: GatewayClient,
    reference: ProviderReference,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TtsRequest<'a> {
    #[serde(flatten)]
    base: GatewayRequestBase<'a>,
    text: &'a str,
    voice_id: &'a str,
    sample_rate_hz: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TtsStreamHeader {
    invocation_id: String,
    encoding: String,
    sample_rate_hz: u32,
    channels: u8,
    characters: usize,
}

fn split_tts_text(text: &str) -> Vec<String> {
    let mut remaining = text.trim();
    let mut segments = Vec::new();
    while !remaining.is_empty() {
        let Some(maximum_end) = remaining
            .char_indices()
            .nth(MAX_TTS_TEXT_CHARACTERS)
            .map(|(index, _)| index)
        else {
            segments.push(remaining.to_owned());
            break;
        };
        let prefix = &remaining[..maximum_end];
        let sentence_end = prefix
            .char_indices()
            .rev()
            .find(|(_, character)| matches!(character, '.' | '!' | '?' | ';' | ':' | '\n'))
            .map(|(index, character)| index + character.len_utf8());
        let whitespace = prefix
            .char_indices()
            .rev()
            .find(|(_, character)| character.is_whitespace())
            .map(|(index, _)| index);
        let split_at = sentence_end
            .filter(|index| prefix[..*index].chars().count() >= MAX_TTS_TEXT_CHARACTERS / 2)
            .or(whitespace)
            .filter(|index| *index > 0)
            .unwrap_or(maximum_end);
        let segment = remaining[..split_at].trim();
        if !segment.is_empty() {
            segments.push(segment.to_owned());
        }
        remaining = remaining[split_at..].trim_start();
    }
    segments
}

impl TextToSpeech for GatewayTextToSpeech {
    fn capabilities(&self) -> BTreeSet<ProviderCapability> {
        BTreeSet::from([ProviderCapability::StreamingTts])
    }

    fn synthesize(
        &self,
        request: SynthesisRequest,
        cancel: CancellationToken,
    ) -> ProviderStream<SynthesizedAudio> {
        let provider = self.clone();
        Box::pin(stream! {
            for segment in split_tts_text(&request.text) {
                if cancel.is_cancelled() {
                    yield Err(ProviderError::cancelled("tts"));
                    return;
                }
                let invocation = TtsRequest {
                    base: provider.gateway.base(&provider.reference),
                    text: &segment,
                    voice_id: &request.voice_id,
                    sample_rate_hz: request.sample_rate_hz,
                };
                let expected_invocation_id = invocation.base.invocation_id.clone();
                let response = match provider.gateway.post_stream(
                    "tts",
                    &invocation,
                    cancel.clone(),
                    "application/vnd.calluwu.pcm16-stream",
                ).await {
                    Ok(response) => response,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                let mut body = response.bytes_stream();
                let mut undecoded = BytesMut::new();
                let mut audio = BytesMut::new();
                let mut metadata_received = false;
                let mut response_bytes = 0_usize;
                let mut segment_audio_bytes = 0_usize;
                let expected_characters = segment.chars().count();
                while let Some(chunk) = tokio::select! {
                    () = cancel.cancelled() => {
                        yield Err(ProviderError::cancelled("tts"));
                        return;
                    }
                    chunk = body.next() => chunk,
                } {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            yield Err(transient("tts"));
                            return;
                        }
                    };
                    response_bytes = response_bytes.saturating_add(chunk.len());
                    if response_bytes > MAX_GATEWAY_RESPONSE_BYTES {
                        yield Err(permanent("tts"));
                        return;
                    }
                    if metadata_received {
                        segment_audio_bytes = segment_audio_bytes.saturating_add(chunk.len());
                        audio.extend_from_slice(&chunk);
                    } else {
                        undecoded.extend_from_slice(&chunk);
                        if undecoded.len() >= 8 {
                            if &undecoded[..4] != TTS_STREAM_MAGIC {
                                yield Err(permanent("tts"));
                                return;
                            }
                            let metadata_bytes = u32::from_be_bytes([
                                undecoded[4], undecoded[5], undecoded[6], undecoded[7],
                            ]) as usize;
                            if metadata_bytes == 0 || metadata_bytes > 1_024 {
                                yield Err(permanent("tts"));
                                return;
                            }
                            let header_bytes = 8_usize.saturating_add(metadata_bytes);
                            if undecoded.len() >= header_bytes {
                                let header = undecoded.split_to(header_bytes);
                                let metadata = match serde_json::from_slice::<TtsStreamHeader>(&header[8..]) {
                                    Ok(metadata) => metadata,
                                    Err(_) => {
                                        yield Err(permanent("tts"));
                                        return;
                                    }
                                };
                                if metadata.invocation_id != expected_invocation_id
                                    || metadata.encoding != "pcm16le"
                                    || metadata.sample_rate_hz != request.sample_rate_hz
                                    || metadata.channels != 1
                                    || metadata.characters != expected_characters
                                {
                                    yield Err(permanent("tts"));
                                    return;
                                }
                                metadata_received = true;
                                segment_audio_bytes =
                                    segment_audio_bytes.saturating_add(undecoded.len());
                                audio.extend_from_slice(&undecoded.split());
                            }
                        }
                        if !metadata_received && undecoded.len() > 1_032 {
                            yield Err(permanent("tts"));
                            return;
                        }
                    }
                    while audio.len() >= MAX_TTS_CHUNK_BYTES {
                        yield Ok(SynthesizedAudio {
                            bytes: audio.split_to(MAX_TTS_CHUNK_BYTES).freeze(),
                        });
                    }
                }
                if !metadata_received
                    || segment_audio_bytes == 0
                    || !segment_audio_bytes.is_multiple_of(2)
                    || !audio.len().is_multiple_of(2)
                {
                    yield Err(permanent("tts"));
                    return;
                }
                if !audio.is_empty() {
                    yield Ok(SynthesizedAudio { bytes: audio.freeze() });
                }
            }
        })
    }
}

#[derive(Clone)]
struct GatewayToolExecutor {
    gateway: GatewayClient,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ToolRequest<'a> {
    runtime_generation: u64,
    invocation_id: String,
    call_id: &'a str,
    tool_name: &'a str,
    input: &'a Value,
    idempotency_key: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ToolResponse {
    value: Value,
}

#[async_trait]
impl ToolExecutor for GatewayToolExecutor {
    async fn execute(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
    ) -> std::result::Result<ToolOutput, ProviderError> {
        let request = ToolRequest {
            runtime_generation: self.gateway.access.context.runtime_generation,
            invocation_id: stable_invocation_id(&invocation.idempotency_key),
            call_id: &invocation.call_id,
            tool_name: &invocation.tool_name,
            input: &invocation.input,
            idempotency_key: &invocation.idempotency_key,
        };
        let response: ToolResponse = self.gateway.post("tools/execute", &request, cancel).await?;
        Ok(ToolOutput {
            value: response.value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn rejects_plaintext_runtime_service_urls() {
        let result = RuntimeServiceAccess::from_ingest(
            "http://control.test/v1/runtime/sessions/ses_test0001/events",
            "a".repeat(32),
            TenantContext {
                organization_id: "org_test0001".into(),
                project_id: "prj_test0001".into(),
                deployment_id: "dep_test0001".into(),
                session_id: "ses_test0001".into(),
                runtime_generation: 1,
                correlation_id: "test".into(),
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn validates_exact_cloudflare_provider_set() {
        let mut settings: BTreeMap<String, Value> = BTreeMap::new();
        // Match JavaScript/Zod integer semantics even when JSON encoded the number as 500.0.
        settings.insert("maxTokens".into(), Value::from(500.0));
        let manifest_json = serde_json::json!({
            "contractVersion": crate::manifest::CONTRACT_VERSION,
            "definition": {
                "name": "voice-agent",
                "instructions": "Be helpful",
                "providers": {
                    "speechToText": {"provider":"cloudflare","model":"@cf/deepgram/nova-3","settings":{}},
                    "reasoning": {"provider":"cloudflare","model":"@cf/openai/gpt-oss-20b","settings":settings},
                    "textToSpeech": {"provider":"cloudflare","model":"@cf/deepgram/aura-2-en","settings":{"speaker":"luna"}}
                },
                "voice": {"id":"luna","language":"en-US","sampleRateHz":16000},
                "tools": [],
                "limits": {"maxSessionSeconds":60,"maxConcurrentTools":2,"maxHistoryMessages":20},
                "metadata": {}
            },
            "requiredCapabilities":["batch-stt","streaming-reasoning","streaming-tts"],
            "artifact":{"sha256":"a".repeat(64),"sizeBytes":0,"format":"javascript-esm"}
        });
        let manifest = AgentManifest::parse_json(&manifest_json.to_string()).expect("manifest");
        validate_cloudflare_references(&manifest).expect("supported providers");
    }

    #[test]
    fn rejects_invalid_cloudflare_settings_before_provider_io() {
        let manifest_json = serde_json::json!({
            "contractVersion": crate::manifest::CONTRACT_VERSION,
            "definition": {
                "name": "voice-agent",
                "instructions": "Be helpful",
                "providers": {
                    "speechToText": {"provider":"cloudflare","model":"@cf/deepgram/nova-3","settings":{"detectLanguage":"yes"}},
                    "reasoning": {"provider":"cloudflare","model":"@cf/openai/gpt-oss-20b","settings":{"maxTokens":0}},
                    "textToSpeech": {"provider":"cloudflare","model":"@cf/deepgram/aura-2-en","settings":{"speaker":"unknown"}}
                },
                "voice": {"id":"unknown","language":"en-US","sampleRateHz":16000},
                "tools": [],
                "limits": {"maxSessionSeconds":60,"maxConcurrentTools":2,"maxHistoryMessages":20},
                "metadata": {}
            },
            "requiredCapabilities":["batch-stt","streaming-reasoning","streaming-tts"],
            "artifact":{"sha256":"a".repeat(64),"sizeBytes":0,"format":"javascript-esm"}
        });
        let manifest = AgentManifest::parse_json(&manifest_json.to_string()).expect("manifest");
        assert!(validate_cloudflare_references(&manifest).is_err());
    }

    #[test]
    fn parses_fragmentable_strict_sse_events() {
        let mut buffer = b"event: delta\r\ndata: {\"text\":\"hello\"}\r\n\r\nevent: completed\n\
data: {\"finishReason\":\"stop\",\"usage\":{\"promptTokens\":2,\"completionTokens\":1}}\n\n"
            .to_vec();
        let first = take_sse_event(&mut buffer)
            .expect("valid event")
            .expect("first event");
        assert_eq!(first.0, "delta");
        assert_eq!(first.1["text"], "hello");
        let second = take_sse_event(&mut buffer)
            .expect("valid event")
            .expect("second event");
        assert_eq!(second.0, "completed");
        assert!(buffer.is_empty());
    }

    #[test]
    fn chunks_tts_at_unicode_safe_natural_boundaries() {
        let text = format!("{}。 {}", "hello ".repeat(100), "🧡".repeat(550));
        let segments = split_tts_text(&text);
        assert!(segments.len() >= 3);
        assert!(
            segments
                .iter()
                .all(|segment| !segment.is_empty() && segment.chars().count() <= 500)
        );
        assert_eq!(
            segments.join(" ").split_whitespace().collect::<String>(),
            text.split_whitespace().collect::<String>()
        );
    }
}

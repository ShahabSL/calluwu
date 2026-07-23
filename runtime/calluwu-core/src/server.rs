use std::{net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header::HeaderName},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::{
    catch_panic::CatchPanicLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};
use uuid::Uuid;

use crate::{
    Result, RuntimeError,
    domain::TenantContext,
    event::{EventSink, HttpEventSink},
    manifest::AgentManifest,
    protocol::{RealtimeEnvelope, RealtimeOutput, ServerMessage},
    provider::{ProviderError, ProviderErrorKind, RuntimeServiceAccess, Transport, TransportInput},
    session::SessionTerminalOutcome,
    supervisor::{PreparationStatus, SessionLease, SessionPreparation, ShardSupervisor},
};

pub const HEADER_SESSION_ID: &str = "x-calluwu-session-id";
pub const HEADER_RUNTIME_GENERATION: &str = "x-calluwu-runtime-generation";
pub const HEADER_INGEST_URL: &str = "x-calluwu-runtime-ingest-url";
pub const HEADER_INGEST_TOKEN: &str = "x-calluwu-runtime-ingest-token";
pub const HEADER_ORGANIZATION_ID: &str = "x-calluwu-organization-id";
pub const HEADER_PROJECT_ID: &str = "x-calluwu-project-id";
pub const HEADER_DEPLOYMENT_ID: &str = "x-calluwu-deployment-id";
const WEBSOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Immutable metadata exposed to allocators and operators.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildInfo {
    pub service: &'static str,
    pub version: &'static str,
    pub source_revision: &'static str,
    pub source_digest: &'static str,
    pub build_timestamp: &'static str,
    pub build_toolchain: &'static str,
    pub build_profile: &'static str,
    pub builder_image: &'static str,
    pub runtime_base_image: &'static str,
    pub protocol_version: u16,
    pub boot_id: String,
}

impl Default for BuildInfo {
    fn default() -> Self {
        Self {
            service: "calluwu-runtime",
            version: env!("CARGO_PKG_VERSION"),
            source_revision: option_env!("CALLUWU_SOURCE_REVISION")
                .unwrap_or("development-unversioned"),
            source_digest: option_env!("CALLUWU_SOURCE_DIGEST")
                .unwrap_or("development-unversioned"),
            build_timestamp: option_env!("CALLUWU_BUILD_TIMESTAMP")
                .unwrap_or("development-unversioned"),
            build_toolchain: option_env!("CALLUWU_BUILD_TOOLCHAIN")
                .unwrap_or(concat!("rust ", env!("CARGO_PKG_RUST_VERSION"))),
            build_profile: option_env!("CALLUWU_BUILD_PROFILE")
                .unwrap_or("development-unversioned"),
            builder_image: option_env!("CALLUWU_BUILDER_IMAGE").unwrap_or("development-host"),
            runtime_base_image: option_env!("CALLUWU_RUNTIME_BASE_IMAGE")
                .unwrap_or("development-host"),
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            boot_id: String::new(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    supervisor: Arc<ShardSupervisor>,
    build: BuildInfo,
    max_message_bytes: usize,
}

/// Construct the container's internal HTTP application.
pub fn router(
    supervisor: Arc<ShardSupervisor>,
    mut build: BuildInfo,
    max_message_bytes: usize,
) -> Router {
    let request_id_header = HeaderName::from_static("x-request-id");
    build.boot_id = supervisor.boot_id().to_owned();
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/healthz", get(readiness))
        .route("/load", get(load))
        .route("/build", get(build_info))
        .route("/v1/sessions/admit", axum::routing::post(prepare_session))
        .route("/v1/sessions/{session_id}/cancel", post(cancel_session))
        .route("/v1/realtime", any(realtime))
        .with_state(AppState {
            supervisor,
            build,
            max_message_bytes,
        })
        .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
        .layer(SetRequestIdLayer::new(request_id_header, MakeRequestUuid))
        .layer(TraceLayer::new_for_http())
        .layer(CatchPanicLayer::new())
        .layer(DefaultBodyLimit::max(1024 * 1024))
}

/// Serve until cancellation, reject new sessions, and drain actor state.
pub async fn serve(
    listener: TcpListener,
    supervisor: Arc<ShardSupervisor>,
    shutdown: CancellationToken,
) -> Result<()> {
    let app = router(supervisor.clone(), BuildInfo::default(), 64 * 1024);
    let drain_supervisor = supervisor.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            drain_supervisor.begin_draining();
            match drain_supervisor.drain().await {
                Ok(report) => tracing::info!(
                    graceful_sessions = report.graceful,
                    forced_sessions = report.forced,
                    aborted_sessions = report.aborted,
                    "runtime shard drained"
                ),
                Err(error) => {
                    tracing::error!(error_code = error.code(), "runtime shard drain failed")
                }
            }
        })
        .await
        .map_err(|error| RuntimeError::Io(std::io::Error::other(error)))
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadinessResponse {
    status: &'static str,
    boot_id: String,
    load: crate::supervisor::LoadSnapshot,
}

async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "live" })
}

async fn readiness(State(state): State<AppState>) -> Response {
    let load = match state.supervisor.load() {
        Ok(load) => load,
        Err(error) => return http_error(error),
    };
    let ready = load.accepting && load.available_sessions > 0;
    let status = if ready { "ready" } else { "not_ready" };
    let response = Json(ReadinessResponse {
        status,
        boot_id: load.boot_id.clone(),
        load,
    });
    if ready {
        (StatusCode::OK, response).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, response).into_response()
    }
}

async fn load(State(state): State<AppState>) -> Response {
    match state.supervisor.load() {
        Ok(load) => Json(load).into_response(),
        Err(error) => http_error(error),
    }
}

async fn build_info(State(state): State<AppState>) -> Json<BuildInfo> {
    Json(state.build)
}

async fn realtime(
    State(state): State<AppState>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Response {
    let admission = match admission_from_headers(&headers) {
        Ok(admission) => admission,
        Err(error) => return http_error(error),
    };
    let attachment_fingerprint = match attachment_fingerprint_from_headers(&headers) {
        Ok(fingerprint) => fingerprint,
        Err(error) => return http_error(error),
    };
    if let Err(error) = state
        .supervisor
        .validate_prepared_attachment(&admission.context, attachment_fingerprint)
    {
        return http_error(error);
    }
    let max_message_bytes = state.max_message_bytes;
    let supervisor = state.supervisor.clone();
    let context = admission.context;
    websocket
        .protocols(["calluwu.v1"])
        .max_frame_size(max_message_bytes)
        .max_message_size(max_message_bytes)
        .on_upgrade(move |socket| async move {
            match supervisor.attach_prepared(context.clone(), attachment_fingerprint) {
                Ok(lease) => {
                    run_websocket(socket, lease, supervisor.clone(), attachment_fingerprint).await
                }
                Err(error) => {
                    let mut transport = AxumWebSocketTransport { socket };
                    if send_transport_error(
                        &mut transport,
                        &context,
                        &error,
                        WEBSOCKET_WRITE_TIMEOUT,
                    )
                    .await
                    .is_ok()
                    {
                        let _closed = close_transport(
                            &mut transport,
                            1008,
                            "session attachment rejected",
                            WEBSOCKET_WRITE_TIMEOUT,
                        )
                        .await;
                    }
                }
            }
        })
        .into_response()
}

async fn cancel_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let admission = match admission_from_headers(&headers) {
        Ok(admission) => admission,
        Err(error) => return http_error(error),
    };
    if admission.context.session_id != session_id {
        return http_error(RuntimeError::InvalidRequest(
            "session path does not match trusted session header".into(),
        ));
    }
    let attachment_fingerprint = match attachment_fingerprint_from_headers(&headers) {
        Ok(fingerprint) => fingerprint,
        Err(error) => return http_error(error),
    };
    match state
        .supervisor
        .cancel_session(&admission.context, attachment_fingerprint)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => http_error(error),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PrepareSessionRequest {
    organization_id: String,
    project_id: String,
    deployment_id: String,
    session_id: String,
    runtime_generation: u64,
    manifest: AgentManifest,
    runtime_ingest_url: String,
    runtime_ingest_token: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PrepareSessionResponse {
    session_id: String,
    runtime_generation: u64,
    boot_id: String,
}

async fn prepare_session(
    State(state): State<AppState>,
    Json(request): Json<PrepareSessionRequest>,
) -> Response {
    let context = TenantContext {
        organization_id: request.organization_id,
        project_id: request.project_id,
        deployment_id: request.deployment_id,
        session_id: request.session_id,
        runtime_generation: request.runtime_generation,
        correlation_id: format!("req_{}", Uuid::now_v7()),
    };
    if let Err(error) = context.validate() {
        return http_error(error);
    }
    if let Err(error) = request.manifest.validate() {
        return http_error(error);
    }
    let event_sink: Arc<dyn EventSink> = match HttpEventSink::new(
        &request.runtime_ingest_url,
        request.runtime_ingest_token.clone(),
        context.runtime_generation,
    ) {
        Ok(sink) => Arc::new(sink),
        Err(error) => return http_error(error),
    };
    let attachment_fingerprint =
        attachment_fingerprint(&request.runtime_ingest_url, &request.runtime_ingest_token);
    let fingerprint =
        match preparation_fingerprint(&context, &request.manifest, attachment_fingerprint) {
            Ok(fingerprint) => fingerprint,
            Err(error) => return http_error(error),
        };
    let needs_runtime_services = !request.manifest.definition.tools.is_empty()
        || [
            &request.manifest.definition.providers.speech_to_text,
            &request.manifest.definition.providers.reasoning,
            &request.manifest.definition.providers.text_to_speech,
        ]
        .iter()
        .any(|reference| reference.provider != "scripted");
    let runtime_service_access = if needs_runtime_services {
        match RuntimeServiceAccess::from_ingest(
            &request.runtime_ingest_url,
            request.runtime_ingest_token.clone(),
            context.clone(),
        ) {
            Ok(access) => Some(access),
            Err(error) => return http_error(error),
        }
    } else {
        None
    };
    let response = PrepareSessionResponse {
        session_id: context.session_id.clone(),
        runtime_generation: context.runtime_generation,
        boot_id: state.supervisor.boot_id().to_owned(),
    };
    match state.supervisor.prepare(SessionPreparation {
        context,
        manifest: request.manifest,
        event_sink,
        runtime_service_access,
        fingerprint,
        attachment_fingerprint,
    }) {
        Ok(PreparationStatus::Created) => (StatusCode::CREATED, Json(response)).into_response(),
        Ok(PreparationStatus::Existing) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => http_error(error),
    }
}

fn hash_parts(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

/// Bind an attachment to the exact opaque event-ingest capability issued by control plane.
#[must_use]
pub fn attachment_fingerprint(url: &str, token: &str) -> [u8; 32] {
    hash_parts(&[url.as_bytes(), token.as_bytes()])
}

/// Content-address the complete trusted preparation for idempotent admission.
pub fn preparation_fingerprint(
    context: &TenantContext,
    manifest: &AgentManifest,
    attachment_fingerprint: [u8; 32],
) -> Result<[u8; 32]> {
    let manifest_json = serde_json::to_vec(manifest)?;
    let generation = context.runtime_generation.to_be_bytes();
    Ok(hash_parts(&[
        context.organization_id.as_bytes(),
        context.project_id.as_bytes(),
        context.deployment_id.as_bytes(),
        context.session_id.as_bytes(),
        &generation,
        &manifest_json,
        &attachment_fingerprint,
    ]))
}

struct Admission {
    context: TenantContext,
}

fn admission_from_headers(headers: &HeaderMap) -> Result<Admission> {
    let session_id = required_header(headers, HEADER_SESSION_ID)?;
    let organization_id = required_header(headers, HEADER_ORGANIZATION_ID)?;
    let project_id = required_header(headers, HEADER_PROJECT_ID)?;
    let deployment_id = required_header(headers, HEADER_DEPLOYMENT_ID)?;
    let runtime_generation = required_header(headers, HEADER_RUNTIME_GENERATION)?
        .parse::<u64>()
        .map_err(|_| RuntimeError::InvalidRequest("runtime generation header is invalid".into()))?;
    let context = TenantContext {
        organization_id,
        project_id,
        deployment_id,
        session_id,
        runtime_generation,
        correlation_id: format!("req_{}", Uuid::now_v7()),
    };
    context.validate()?;
    Ok(Admission { context })
}

fn attachment_fingerprint_from_headers(headers: &HeaderMap) -> Result<[u8; 32]> {
    let url = required_header(headers, HEADER_INGEST_URL)?;
    let token = required_header(headers, HEADER_INGEST_TOKEN)?;
    Ok(attachment_fingerprint(&url, &token))
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String> {
    optional_header(headers, name)?.ok_or_else(|| {
        RuntimeError::InvalidRequest(format!("missing trusted proxy header {name}").into())
    })
}

fn optional_header(headers: &HeaderMap, name: &'static str) -> Result<Option<String>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| RuntimeError::InvalidRequest("proxy header is not UTF-8".into()))
        })
        .transpose()
}

async fn run_websocket(
    socket: WebSocket,
    lease: SessionLease,
    supervisor: Arc<ShardSupervisor>,
    attachment_fingerprint: [u8; 32],
) {
    let mut transport = AxumWebSocketTransport { socket };
    run_transport(
        &mut transport,
        lease,
        supervisor,
        attachment_fingerprint,
        WEBSOCKET_WRITE_TIMEOUT,
    )
    .await;
}

async fn run_transport(
    transport: &mut dyn Transport,
    mut lease: SessionLease,
    supervisor: Arc<ShardSupervisor>,
    attachment_fingerprint: [u8; 32],
    write_timeout: Duration,
) {
    let context = lease.handle.context().clone();
    loop {
        tokio::select! {
            output = lease.output.recv() => {
                let Some(output) = output else {
                    let outcome = lease.handle.wait_terminal_outcome().await;
                    let (code, reason) = if outcome == SessionTerminalOutcome::Flushed {
                        (1000, "session terminal persisted")
                    } else {
                        (1011, "session terminal persistence failed")
                    };
                    let _closed = close_transport(
                        transport,
                        code,
                        reason,
                        write_timeout,
                    ).await;
                    break;
                };
                if let Err(error) = send_transport_output(transport, &output, write_timeout).await {
                    tracing::info!(
                        session_id = %context.session_id,
                        error_kind = ?error.kind,
                        "realtime transport send ended"
                    );
                    break;
                }
            }
            input = transport.receive() => {
                match input {
                    Ok(TransportInput::Control(message)) => {
                        if let Err(error) = lease.handle.try_control(message) {
                            if send_transport_error(transport, &context, &error, write_timeout).await.is_err() {
                                break;
                            }
                            if matches!(error, RuntimeError::MailboxFull { .. }) {
                                let _closed = close_transport(
                                    transport,
                                    1013,
                                    "runtime overloaded",
                                    write_timeout,
                                ).await;
                                break;
                            }
                        }
                    }
                    Ok(TransportInput::Audio(audio)) => {
                        if let Err(error) = lease.handle.try_audio(audio) {
                            if send_transport_error(transport, &context, &error, write_timeout).await.is_err() {
                                break;
                            }
                            if matches!(error, RuntimeError::MailboxFull { .. }) {
                                let _closed = close_transport(
                                    transport,
                                    1013,
                                    "runtime overloaded",
                                    write_timeout,
                                ).await;
                                break;
                            }
                        }
                    }
                    Ok(TransportInput::Ping(payload)) => {
                        if pong_transport(transport, payload, write_timeout).await.is_err() {
                            break;
                        }
                    }
                    Ok(TransportInput::Closed) => break,
                    Err(error) => {
                        tracing::info!(
                            session_id = %context.session_id,
                            error_kind = ?error.kind,
                            "realtime transport receive ended"
                        );
                        break;
                    }
                }
            }
        }
    }
    if let Err(error) = supervisor
        .disconnect_session(&context, attachment_fingerprint)
        .await
    {
        tracing::error!(
            session_id = %context.session_id,
            error_code = error.code(),
            "failed to reap disconnected realtime session"
        );
    }
}

async fn send_transport_error(
    transport: &mut dyn Transport,
    context: &TenantContext,
    error: &RuntimeError,
    write_timeout: Duration,
) -> std::result::Result<(), ProviderError> {
    let details = error.details().map(|map| {
        map.into_iter()
            .collect::<std::collections::BTreeMap<_, _>>()
    });
    send_transport_output(
        transport,
        &RealtimeOutput::Control(ServerMessage::Error {
            envelope: RealtimeEnvelope::server(
                &context.session_id,
                context.runtime_generation,
                format!("transport-{}", Uuid::now_v7()),
            ),
            code: error.code().into(),
            message: error.public_message().into_owned(),
            details,
        }),
        write_timeout,
    )
    .await
}

fn transport_write_timeout(operation: &'static str) -> ProviderError {
    ProviderError {
        stage: "transport",
        kind: ProviderErrorKind::Permanent,
        message: format!("WebSocket {operation} deadline exceeded"),
    }
}

async fn send_transport_output(
    transport: &mut dyn Transport,
    output: &RealtimeOutput,
    write_timeout: Duration,
) -> std::result::Result<(), ProviderError> {
    tokio::time::timeout(write_timeout, transport.send(output))
        .await
        .map_err(|_| transport_write_timeout("send"))?
}

async fn pong_transport(
    transport: &mut dyn Transport,
    payload: Bytes,
    write_timeout: Duration,
) -> std::result::Result<(), ProviderError> {
    tokio::time::timeout(write_timeout, transport.pong(payload))
        .await
        .map_err(|_| transport_write_timeout("pong"))?
}

async fn close_transport(
    transport: &mut dyn Transport,
    code: u16,
    reason: &str,
    write_timeout: Duration,
) -> std::result::Result<(), ProviderError> {
    tokio::time::timeout(write_timeout, transport.close(code, reason))
        .await
        .map_err(|_| transport_write_timeout("close"))?
}

struct AxumWebSocketTransport {
    socket: WebSocket,
}

#[async_trait]
impl Transport for AxumWebSocketTransport {
    async fn receive(&mut self) -> std::result::Result<TransportInput, ProviderError> {
        loop {
            let message = self
                .socket
                .recv()
                .await
                .transpose()
                .map_err(|_| ProviderError {
                    stage: "transport",
                    kind: ProviderErrorKind::Permanent,
                    message: "WebSocket receive failed".into(),
                })?;
            match message {
                Some(Message::Text(text)) => {
                    let message =
                        serde_json::from_str(text.as_str()).map_err(|_| ProviderError {
                            stage: "transport",
                            kind: ProviderErrorKind::Permanent,
                            message: "WebSocket control JSON is invalid".into(),
                        })?;
                    return Ok(TransportInput::Control(message));
                }
                Some(Message::Binary(audio)) => return Ok(TransportInput::Audio(audio)),
                Some(Message::Ping(payload)) => return Ok(TransportInput::Ping(payload)),
                Some(Message::Pong(_)) => continue,
                Some(Message::Close(_)) | None => return Ok(TransportInput::Closed),
            }
        }
    }

    async fn send(&mut self, message: &RealtimeOutput) -> std::result::Result<(), ProviderError> {
        let frame = match message {
            RealtimeOutput::Control(message) => {
                let json = serde_json::to_string(message).map_err(|_| ProviderError {
                    stage: "transport",
                    kind: ProviderErrorKind::Permanent,
                    message: "WebSocket response serialization failed".into(),
                })?;
                Message::Text(json.into())
            }
            RealtimeOutput::Audio(audio) => {
                Message::Binary(audio.encode().map_err(|_| ProviderError {
                    stage: "transport",
                    kind: ProviderErrorKind::Permanent,
                    message: "WebSocket audio framing failed".into(),
                })?)
            }
        };
        self.socket.send(frame).await.map_err(|_| ProviderError {
            stage: "transport",
            kind: ProviderErrorKind::Permanent,
            message: "WebSocket send failed".into(),
        })
    }

    async fn pong(&mut self, payload: Bytes) -> std::result::Result<(), ProviderError> {
        self.socket
            .send(Message::Pong(payload))
            .await
            .map_err(|_| ProviderError {
                stage: "transport",
                kind: ProviderErrorKind::Permanent,
                message: "WebSocket pong failed".into(),
            })
    }

    async fn close(&mut self, code: u16, reason: &str) -> std::result::Result<(), ProviderError> {
        self.socket
            .send(Message::Close(Some(CloseFrame {
                code,
                reason: reason.to_owned().into(),
            })))
            .await
            .map_err(|_| ProviderError {
                stage: "transport",
                kind: ProviderErrorKind::Permanent,
                message: "WebSocket close failed".into(),
            })
    }
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: HttpError,
}

#[derive(Serialize)]
struct HttpError {
    code: &'static str,
    message: String,
    request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Value>,
}

fn http_error(error: RuntimeError) -> Response {
    let status = match &error {
        RuntimeError::AtCapacity | RuntimeError::Draining => StatusCode::SERVICE_UNAVAILABLE,
        RuntimeError::SessionConflict | RuntimeError::GenerationMismatch { .. } => {
            StatusCode::CONFLICT
        }
        RuntimeError::InvalidState(_) => StatusCode::CONFLICT,
        RuntimeError::InvalidRequest(_) | RuntimeError::Protocol(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let details = error.details().map(Value::Object);
    (
        status,
        Json(ErrorEnvelope {
            error: HttpError {
                code: error.code(),
                message: error.public_message().into_owned(),
                request_id: format!("req_{}", Uuid::now_v7()),
                details,
            },
        }),
    )
        .into_response()
}

/// Convenience used by CLI output and integration diagnostics.
#[must_use]
pub fn socket_address(listener: &TcpListener) -> Option<SocketAddr> {
    listener.local_addr().ok()
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;

    use super::*;
    use crate::{
        event::MemoryEventSink,
        manifest::AgentManifest,
        provider::ProviderSet,
        supervisor::{SessionPreparation, ShardConfig},
    };

    struct BlockingWriteTransport {
        send_attempts: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Transport for BlockingWriteTransport {
        async fn receive(&mut self) -> std::result::Result<TransportInput, ProviderError> {
            pending().await
        }

        async fn send(
            &mut self,
            _message: &RealtimeOutput,
        ) -> std::result::Result<(), ProviderError> {
            self.send_attempts.fetch_add(1, Ordering::SeqCst);
            pending().await
        }

        async fn pong(&mut self, _payload: Bytes) -> std::result::Result<(), ProviderError> {
            pending().await
        }

        async fn close(
            &mut self,
            _code: u16,
            _reason: &str,
        ) -> std::result::Result<(), ProviderError> {
            pending().await
        }
    }

    fn manifest(provider: &str) -> Value {
        serde_json::json!({
            "contractVersion": crate::manifest::CONTRACT_VERSION,
            "definition": {
                "name": "test-agent",
                "instructions": "Help the caller.",
                "providers": {
                    "speechToText": { "provider": provider, "model": "scripted-v1" },
                    "reasoning": { "provider": provider, "model": "scripted-v1" },
                    "textToSpeech": { "provider": provider, "model": "scripted-v1" }
                },
                "voice": { "id": "test", "sampleRateHz": 16000 }
            },
            "requiredCapabilities": [
                "batch-stt", "streaming-reasoning", "streaming-tts"
            ],
            "artifact": {
                "sha256": "0".repeat(64),
                "sizeBytes": 1,
                "format": "javascript-esm"
            }
        })
    }

    fn admission_body(provider: &str) -> Value {
        serde_json::json!({
            "organizationId": "org_12345678",
            "projectId": "prj_12345678",
            "deploymentId": "dep_12345678",
            "sessionId": "ses_12345678",
            "runtimeGeneration": 1,
            "manifest": manifest(provider),
            "runtimeIngestUrl": "http://127.0.0.1:9999/v1/runtime/sessions/ses_12345678/events",
            "runtimeIngestToken": "test-token"
        })
    }

    fn cancel_request(runtime_generation: u64) -> Request<Body> {
        Request::post("/v1/sessions/ses_12345678/cancel")
            .header(HEADER_SESSION_ID, "ses_12345678")
            .header(HEADER_RUNTIME_GENERATION, runtime_generation)
            .header(HEADER_ORGANIZATION_ID, "org_12345678")
            .header(HEADER_PROJECT_ID, "prj_12345678")
            .header(HEADER_DEPLOYMENT_ID, "dep_12345678")
            .header(
                HEADER_INGEST_URL,
                "http://127.0.0.1:9999/v1/runtime/sessions/ses_12345678/events",
            )
            .header(HEADER_INGEST_TOKEN, "test-token")
            .body(Body::empty())
            .expect("cancel request")
    }

    #[tokio::test]
    async fn health_and_build_endpoints_are_operational() {
        let supervisor = ShardSupervisor::new(
            ShardConfig::default(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let app = router(supervisor, BuildInfo::default(), 64 * 1024);
        for path in [
            "/health/live",
            "/health/ready",
            "/healthz",
            "/load",
            "/build",
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).expect("request"))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK, "{path}");
        }

        let response = app
            .oneshot(Request::get("/build").body(Body::empty()).expect("request"))
            .await
            .expect("response");
        let build: Value = serde_json::from_slice(
            &to_bytes(response.into_body(), 16 * 1024)
                .await
                .expect("build body"),
        )
        .expect("build JSON");
        for field in [
            "sourceRevision",
            "sourceDigest",
            "buildTimestamp",
            "buildToolchain",
            "buildProfile",
            "builderImage",
            "runtimeBaseImage",
        ] {
            assert!(
                build[field].as_str().is_some_and(|value| !value.is_empty()),
                "missing build provenance field {field}"
            );
        }
    }

    #[tokio::test]
    async fn blocked_transport_write_times_out_and_releases_session_capacity() {
        let supervisor = ShardSupervisor::new(
            ShardConfig::default(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let context = TenantContext {
            organization_id: "org_12345678".into(),
            project_id: "prj_12345678".into(),
            deployment_id: "dep_12345678".into(),
            session_id: "ses_blocked_transport".into(),
            runtime_generation: 1,
            correlation_id: "blocked-transport".into(),
        };
        let attachment_fingerprint = [4; 32];
        let sink = Arc::new(MemoryEventSink::default());
        supervisor
            .prepare(SessionPreparation {
                context: context.clone(),
                manifest: AgentManifest::parse_json(&manifest("scripted").to_string())
                    .expect("manifest"),
                event_sink: sink,
                runtime_service_access: None,
                fingerprint: [3; 32],
                attachment_fingerprint,
            })
            .expect("prepare");
        let lease = supervisor
            .attach_prepared(context, attachment_fingerprint)
            .expect("attach");
        assert_eq!(supervisor.load().expect("load").active_sessions, 1);

        let send_attempts = Arc::new(AtomicUsize::new(0));
        let mut transport = BlockingWriteTransport {
            send_attempts: send_attempts.clone(),
        };
        tokio::time::timeout(
            Duration::from_secs(1),
            run_transport(
                &mut transport,
                lease,
                supervisor.clone(),
                attachment_fingerprint,
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("transport loop deadline");

        assert_eq!(send_attempts.load(Ordering::SeqCst), 1);
        let load = supervisor.load().expect("load after disconnect");
        assert_eq!(load.active_sessions, 0);
        assert_eq!(load.available_sessions, load.max_sessions);
    }

    #[tokio::test]
    async fn trusted_pre_admission_is_idempotent() {
        let supervisor = ShardSupervisor::new(
            ShardConfig::default(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let boot_id = supervisor.boot_id().to_owned();
        let app = router(supervisor, BuildInfo::default(), 64 * 1024);
        let body = serde_json::to_vec(&admission_body("scripted")).expect("body");
        for expected in [StatusCode::CREATED, StatusCode::OK] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/v1/sessions/admit")
                        .header("content-type", "application/json")
                        .body(Body::from(body.clone()))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), expected);
            let body: Value = serde_json::from_slice(
                &to_bytes(response.into_body(), 16 * 1024)
                    .await
                    .expect("response body"),
            )
            .expect("response JSON");
            assert_eq!(body["sessionId"], "ses_12345678");
            assert_eq!(body["runtimeGeneration"], 1);
            assert_eq!(body["bootId"], boot_id);
        }
    }

    #[tokio::test]
    async fn trusted_cancel_is_generation_fenced_and_idempotent() {
        let supervisor = ShardSupervisor::new(
            ShardConfig::default(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let app = router(supervisor, BuildInfo::default(), 64 * 1024);
        let body = serde_json::to_vec(&admission_body("scripted")).expect("body");
        let prepared = app
            .clone()
            .oneshot(
                Request::post("/v1/sessions/admit")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(prepared.status(), StatusCode::CREATED);

        let stale = app
            .clone()
            .oneshot(cancel_request(2))
            .await
            .expect("stale response");
        assert_eq!(stale.status(), StatusCode::CONFLICT);
        for _ in 0..2 {
            let canceled = app
                .clone()
                .oneshot(cancel_request(1))
                .await
                .expect("cancel response");
            assert_eq!(canceled.status(), StatusCode::NO_CONTENT);
        }
    }

    #[tokio::test]
    async fn cloud_admission_rejects_unavailable_provider() {
        let supervisor = ShardSupervisor::new(
            ShardConfig::default(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let app = router(supervisor, BuildInfo::default(), 64 * 1024);
        let response = app
            .oneshot(
                Request::post("/v1/sessions/admit")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&admission_body("unavailable-vendor")).expect("body"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cloud_admission_rejects_all_customer_tools() {
        let supervisor = ShardSupervisor::new(
            ShardConfig::default(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let app = router(supervisor, BuildInfo::default(), 64 * 1024);
        let mut body = admission_body("scripted");
        body["manifest"]["definition"]["tools"] = serde_json::json!([{
            "name": "lookup",
            "description": "Lookup",
            "inputSchema": {"type": "object"},
            "execution": {"kind": "local"}
        }]);
        body["manifest"]["requiredCapabilities"] = serde_json::json!([
            "batch-stt",
            "streaming-reasoning",
            "streaming-tts",
            "tool-execution"
        ]);
        let response = app
            .oneshot(
                Request::post("/v1/sessions/admit")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).expect("body")))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cloud_scripted_provider_rejects_unrecognized_model_or_settings() {
        let supervisor = ShardSupervisor::new(
            ShardConfig::default(),
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
        )
        .expect("supervisor");
        let app = router(supervisor, BuildInfo::default(), 64 * 1024);
        let mut wrong_model = admission_body("scripted");
        wrong_model["manifest"]["definition"]["providers"]["reasoning"]["model"] =
            serde_json::json!("ignored-model");
        let mut settings = admission_body("scripted");
        settings["manifest"]["definition"]["providers"]["reasoning"]["settings"] =
            serde_json::json!({"secretLike": "must-not-be-ignored"});
        for body in [wrong_model, settings] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/v1/sessions/admit")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&body).expect("body")))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }
}

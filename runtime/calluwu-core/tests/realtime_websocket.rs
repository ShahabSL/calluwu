use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use calluwu_core::{
    domain::TenantContext,
    event::{
        EventRetryPolicy, EventSink, EventSinkError, EventSinkErrorKind, EventType,
        MemoryEventSink, PendingRuntimeEvent,
    },
    manifest::{AgentManifest, CONTRACT_VERSION},
    protocol::{AudioChunkFrame, ClientMessage, PROTOCOL_VERSION, RealtimeEnvelope, ServerMessage},
    provider::ProviderSet,
    server,
    supervisor::{PreparationStatus, SessionPreparation, ShardConfig, ShardSupervisor},
};
use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};
use tokio_util::sync::CancellationToken;

fn manifest() -> AgentManifest {
    AgentManifest::parse_json(
        &serde_json::json!({
            "contractVersion": CONTRACT_VERSION,
            "definition": {
                "name": "audio-agent",
                "instructions": "Respond to the caller.",
                "providers": {
                    "speechToText": { "provider": "scripted", "model": "scripted-v1" },
                    "reasoning": { "provider": "scripted", "model": "scripted-v1" },
                    "textToSpeech": { "provider": "scripted", "model": "scripted-v1" }
                },
                "voice": { "id": "test", "sampleRateHz": 16000 }
            },
            "requiredCapabilities": [
                "batch-stt", "streaming-reasoning", "streaming-tts"
            ],
            "artifact": {
                "sha256": "0".repeat(64), "sizeBytes": 1, "format": "javascript-esm"
            }
        })
        .to_string(),
    )
    .expect("manifest")
}

fn attachment_fingerprint(url: &str, token: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in [url.as_bytes(), token.as_bytes()] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn envelope(session_id: &str, message_id: &str) -> RealtimeEnvelope {
    RealtimeEnvelope {
        protocol_version: PROTOCOL_VERSION,
        session_id: session_id.into(),
        message_id: message_id.into(),
        runtime_generation: 7,
    }
}

#[derive(Default)]
struct RejectTerminalEventSink {
    attempts: Mutex<Vec<PendingRuntimeEvent>>,
}

#[async_trait]
impl EventSink for RejectTerminalEventSink {
    async fn publish(
        &self,
        events: &[PendingRuntimeEvent],
    ) -> std::result::Result<(), EventSinkError> {
        self.attempts
            .lock()
            .map_err(|_| EventSinkError {
                kind: EventSinkErrorKind::Permanent,
                message: "test event sink unavailable".into(),
            })?
            .extend_from_slice(events);
        if events.iter().any(|event| {
            matches!(
                event.event_type,
                EventType::SessionCompleted | EventType::SessionCanceled | EventType::SessionFailed
            )
        }) {
            return Err(EventSinkError {
                kind: EventSinkErrorKind::Transient,
                message: "injected terminal persistence failure".into(),
            });
        }
        Ok(())
    }
}

#[tokio::test]
async fn binary_pcm_ingress_streams_versioned_binary_audio_egress() {
    let supervisor = ShardSupervisor::new(
        ShardConfig::default(),
        ProviderSet::scripted(Vec::new(), Duration::ZERO),
    )
    .expect("supervisor");
    let context = TenantContext {
        organization_id: "org_12345678".into(),
        project_id: "prj_12345678".into(),
        deployment_id: "dep_12345678".into(),
        session_id: "ses_12345678".into(),
        runtime_generation: 7,
        correlation_id: "integration".into(),
    };
    let ingest_url = "http://127.0.0.1:9999/v1/runtime/sessions/ses_12345678/events";
    let ingest_token = "integration-ingest-token";
    let attachment_fingerprint = attachment_fingerprint(ingest_url, ingest_token);
    let event_sink = Arc::new(MemoryEventSink::default());
    assert_eq!(
        supervisor
            .prepare(SessionPreparation {
                context: context.clone(),
                manifest: manifest(),
                event_sink: event_sink.clone(),
                runtime_service_access: None,
                fingerprint: [7; 32],
                attachment_fingerprint,
            })
            .expect("prepare"),
        PreparationStatus::Created
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server_task = tokio::spawn(server::serve(listener, supervisor, server_shutdown));

    let mut request = format!("ws://{address}/v1/realtime")
        .into_client_request()
        .expect("WebSocket request");
    for (name, value) in [
        (server::HEADER_SESSION_ID, context.session_id.as_str()),
        (server::HEADER_RUNTIME_GENERATION, "7"),
        (server::HEADER_INGEST_URL, ingest_url),
        (server::HEADER_INGEST_TOKEN, ingest_token),
        (
            server::HEADER_ORGANIZATION_ID,
            context.organization_id.as_str(),
        ),
        (server::HEADER_PROJECT_ID, context.project_id.as_str()),
        (server::HEADER_DEPLOYMENT_ID, context.deployment_id.as_str()),
        ("sec-websocket-protocol", "calluwu.v1"),
    ] {
        request
            .headers_mut()
            .insert(name, HeaderValue::from_str(value).expect("header"));
    }
    let (mut websocket, response) = connect_async(request).await.expect("upgrade");
    assert_eq!(
        response
            .headers()
            .get("sec-websocket-protocol")
            .expect("selected protocol"),
        "calluwu.v1"
    );

    let ready = websocket.next().await.expect("ready frame").expect("ready");
    assert!(matches!(
        ready,
        Message::Text(text)
            if matches!(serde_json::from_str::<ServerMessage>(&text), Ok(ServerMessage::SessionReady { .. }))
    ));

    let mut duplicate_request = format!("ws://{address}/v1/realtime")
        .into_client_request()
        .expect("duplicate WebSocket request");
    for (name, value) in [
        (server::HEADER_SESSION_ID, context.session_id.as_str()),
        (server::HEADER_RUNTIME_GENERATION, "7"),
        (server::HEADER_INGEST_URL, ingest_url),
        (server::HEADER_INGEST_TOKEN, ingest_token),
        (
            server::HEADER_ORGANIZATION_ID,
            context.organization_id.as_str(),
        ),
        (server::HEADER_PROJECT_ID, context.project_id.as_str()),
        (server::HEADER_DEPLOYMENT_ID, context.deployment_id.as_str()),
        ("sec-websocket-protocol", "calluwu.v1"),
    ] {
        duplicate_request
            .headers_mut()
            .insert(name, HeaderValue::from_str(value).expect("header"));
    }
    let duplicate = connect_async(duplicate_request)
        .await
        .expect_err("second transport must be rejected");
    assert!(matches!(
        duplicate,
        tokio_tungstenite::tungstenite::Error::Http(response)
            if response.status() == tokio_tungstenite::tungstenite::http::StatusCode::CONFLICT
    ));
    websocket
        .send(Message::Text(
            serde_json::to_string(&serde_json::json!({
                "type": "session.start",
                "protocolVersion": 1,
                "sessionId": context.session_id,
                "messageId": "start",
                "runtimeGeneration": 7
            }))
            .expect("start JSON")
            .into(),
        ))
        .await
        .expect("start");

    let pcm: Vec<u8> = (0_i16..320)
        .flat_map(|sample| sample.to_le_bytes())
        .collect();
    websocket
        .send(Message::Binary(pcm.into()))
        .await
        .expect("PCM frame");
    let commit = ClientMessage::InputCommit {
        envelope: envelope(&context.session_id, "commit"),
    };
    // ClientMessage is intentionally deserialize-only; emit its stable shared shape.
    let _contract_marker = commit;
    websocket
        .send(Message::Text(
            serde_json::json!({
                "type": "input.commit",
                "protocolVersion": 1,
                "sessionId": context.session_id,
                "messageId": "commit",
                "runtimeGeneration": 7
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("commit");

    let mut final_transcript = false;
    let mut binary_audio = false;
    let mut completed = false;
    let mut started = false;
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(2), websocket.next())
        .await
        .expect("runtime response timeout")
    {
        match frame.expect("WebSocket frame") {
            Message::Text(text) => {
                match serde_json::from_str::<ServerMessage>(&text).expect("server control") {
                    ServerMessage::SessionStarted { .. } => started = true,
                    ServerMessage::TranscriptDelta {
                        text,
                        is_final: true,
                        ..
                    } => {
                        final_transcript = true;
                        assert_eq!(text, "audio input (640 bytes)");
                    }
                    ServerMessage::ResponseCompleted {
                        interrupted: false, ..
                    } => {
                        completed = true;
                        break;
                    }
                    _ => {}
                }
            }
            Message::Binary(binary) => {
                let audio = AudioChunkFrame::decode(binary).expect("CWU1 frame");
                assert_eq!(audio.header.message_type, "audio.chunk");
                assert_eq!(audio.header.envelope.runtime_generation, 7);
                assert_eq!(audio.header.sample_rate_hz, 16_000);
                assert_eq!(audio.header.channels, 1);
                assert_eq!(audio.audio.len() % 2, 0);
                binary_audio = true;
            }
            _ => {}
        }
    }
    assert!(started && final_transcript && binary_audio && completed);

    websocket
        .send(Message::Text(
            serde_json::json!({
                "type": "input.text",
                "protocolVersion": 1,
                "sessionId": context.session_id,
                "messageId": "text-input",
                "runtimeGeneration": 7,
                "text": "typed turn"
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("text input");
    let mut text_transcript = false;
    let mut text_audio = false;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), websocket.next())
            .await
            .expect("text response timeout")
            .expect("text response frame")
            .expect("WebSocket frame");
        match frame {
            Message::Text(text) => {
                match serde_json::from_str::<ServerMessage>(&text).expect("server control") {
                    ServerMessage::TranscriptDelta {
                        text,
                        is_final: true,
                        ..
                    } => {
                        assert_eq!(text, "typed turn");
                        text_transcript = true;
                    }
                    ServerMessage::ResponseCompleted {
                        epoch: 2,
                        interrupted: false,
                        ..
                    } => break,
                    ServerMessage::Error { code, message, .. } => {
                        panic!("unexpected runtime error {code}: {message}")
                    }
                    _ => {}
                }
            }
            Message::Binary(binary) => {
                AudioChunkFrame::decode(binary).expect("text CWU1 frame");
                text_audio = true;
            }
            _ => {}
        }
    }
    assert!(text_transcript && text_audio);
    websocket
        .send(Message::Text(
            serde_json::json!({
                "type": "session.end",
                "protocolVersion": 1,
                "sessionId": context.session_id,
                "messageId": "graceful-end",
                "runtimeGeneration": 7,
                "reason": "caller requested a normal hangup"
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("session.end");
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), websocket.next())
            .await
            .expect("clean close timeout")
            .expect("clean close frame")
            .expect("WebSocket close");
        if let Message::Close(Some(close)) = frame {
            assert_eq!(u16::from(close.code), 1000);
            break;
        }
    }

    let events = event_sink.events().expect("semantic events");
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
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server shutdown")
        .expect("server join")
        .expect("server result");
}

#[tokio::test]
async fn exhausted_terminal_delivery_closes_1011_and_fallback_releases_capacity() {
    let supervisor = ShardSupervisor::new(
        ShardConfig {
            max_sessions: 1,
            event_batch_size: 1,
            event_retry: EventRetryPolicy {
                max_attempts: 2,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(1),
            },
            ..ShardConfig::default()
        },
        ProviderSet::scripted(Vec::new(), Duration::ZERO),
    )
    .expect("supervisor");
    let context = TenantContext {
        organization_id: "org_12345678".into(),
        project_id: "prj_12345678".into(),
        deployment_id: "dep_12345678".into(),
        session_id: "ses_terminal_failure".into(),
        runtime_generation: 7,
        correlation_id: "terminal-failure-integration".into(),
    };
    let ingest_url = "http://127.0.0.1:9999/v1/runtime/sessions/ses_terminal_failure/events";
    let ingest_token = "terminal-failure-ingest-token";
    let attachment_fingerprint = attachment_fingerprint(ingest_url, ingest_token);
    let event_sink = Arc::new(RejectTerminalEventSink::default());
    supervisor
        .prepare(SessionPreparation {
            context: context.clone(),
            manifest: manifest(),
            event_sink: event_sink.clone(),
            runtime_service_access: None,
            fingerprint: [9; 32],
            attachment_fingerprint,
        })
        .expect("prepare");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let shutdown = CancellationToken::new();
    let server_task = tokio::spawn(server::serve(
        listener,
        supervisor.clone(),
        shutdown.clone(),
    ));
    let mut request = format!("ws://{address}/v1/realtime")
        .into_client_request()
        .expect("WebSocket request");
    for (name, value) in [
        (server::HEADER_SESSION_ID, context.session_id.as_str()),
        (server::HEADER_RUNTIME_GENERATION, "7"),
        (server::HEADER_INGEST_URL, ingest_url),
        (server::HEADER_INGEST_TOKEN, ingest_token),
        (
            server::HEADER_ORGANIZATION_ID,
            context.organization_id.as_str(),
        ),
        (server::HEADER_PROJECT_ID, context.project_id.as_str()),
        (server::HEADER_DEPLOYMENT_ID, context.deployment_id.as_str()),
        ("sec-websocket-protocol", "calluwu.v1"),
    ] {
        request
            .headers_mut()
            .insert(name, HeaderValue::from_str(value).expect("header"));
    }
    let (mut websocket, _) = connect_async(request).await.expect("upgrade");
    assert!(matches!(
        websocket.next().await.expect("ready frame").expect("ready"),
        Message::Text(text)
            if matches!(serde_json::from_str::<ServerMessage>(&text), Ok(ServerMessage::SessionReady { .. }))
    ));
    websocket
        .send(Message::Text(
            serde_json::json!({
                "type": "session.start",
                "protocolVersion": 1,
                "sessionId": context.session_id,
                "messageId": "failure-start",
                "runtimeGeneration": 7
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("start");
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), websocket.next())
            .await
            .expect("start timeout")
            .expect("start frame")
            .expect("WebSocket frame");
        if matches!(
            frame,
            Message::Text(ref text)
                if matches!(serde_json::from_str::<ServerMessage>(text), Ok(ServerMessage::SessionStarted { .. }))
        ) {
            break;
        }
    }

    let private_reason = "PRIVATE_TERMINAL_FAILURE_REASON_983";
    websocket
        .send(Message::Text(
            serde_json::json!({
                "type": "session.end",
                "protocolVersion": 1,
                "sessionId": context.session_id,
                "messageId": "failure-end",
                "runtimeGeneration": 7,
                "reason": private_reason
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("session.end");
    let (close_code, close_reason) = loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), websocket.next())
            .await
            .expect("failure close timeout")
            .expect("failure close frame")
            .expect("WebSocket close");
        if let Message::Close(Some(close)) = frame {
            break (u16::from(close.code), close.reason.to_string());
        }
    };
    assert_eq!(close_code, 1011);
    assert_eq!(close_reason, "session terminal persistence failed");
    assert!(!close_reason.contains(private_reason));

    supervisor
        .cancel_session(&context, attachment_fingerprint)
        .await
        .expect("idempotent fallback cancel");
    let load = supervisor.load().expect("load after fallback");
    assert_eq!(load.active_sessions, 0);
    assert_eq!(load.available_sessions, load.max_sessions);

    {
        let attempts = event_sink.attempts.lock().expect("event attempts");
        let terminal_attempts: Vec<_> = attempts
            .iter()
            .filter(|event| event.event_type == EventType::SessionCompleted)
            .collect();
        assert_eq!(terminal_attempts.len(), 2);
        assert!(
            terminal_attempts
                .windows(2)
                .all(|pair| pair[0].producer_sequence == pair[1].producer_sequence)
        );
        assert!(
            !serde_json::to_string(&*attempts)
                .expect("event JSON")
                .contains(private_reason)
        );
    }

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server shutdown")
        .expect("server join")
        .expect("server result");
}

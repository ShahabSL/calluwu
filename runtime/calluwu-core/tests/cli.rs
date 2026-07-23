use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    process::{Command, Output},
    thread::{self, JoinHandle},
    time::Duration,
};

use calluwu_core::manifest::CONTRACT_VERSION;
use tempfile::NamedTempFile;

fn manifest(required_capabilities: &[&str]) -> String {
    serde_json::json!({
        "contractVersion": CONTRACT_VERSION,
        "definition": {
            "name": "local-agent",
            "instructions": "Help the caller.",
            "providers": {
                "speechToText": { "provider": "project-default", "model": "default" },
                "reasoning": { "provider": "project-default", "model": "default" },
                "textToSpeech": { "provider": "project-default", "model": "default" }
            },
            "voice": { "id": "default", "sampleRateHz": 16000 }
        },
        "requiredCapabilities": required_capabilities,
        "artifact": {
            "sha256": "0".repeat(64), "sizeBytes": 1, "format": "javascript-esm"
        }
    })
    .to_string()
}

fn runtime() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_calluwu-runtime"))
}

fn run_probe(address: SocketAddr, timeout_ms: u64) -> Output {
    Command::new(runtime())
        .args([
            "probe",
            "--address",
            &address.to_string(),
            "--timeout-ms",
            &timeout_ms.to_string(),
        ])
        .output()
        .expect("runtime probe process")
}

fn read_probe_request(stream: &mut TcpStream, address: SocketAddr) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("request read timeout");
    let mut request = Vec::with_capacity(256);
    let mut chunk = [0_u8; 256];
    while !request.ends_with(b"\r\n\r\n") {
        let count = stream.read(&mut chunk).expect("probe request");
        assert_ne!(count, 0, "probe closed before completing its request");
        request.extend_from_slice(&chunk[..count]);
        assert!(request.len() <= 4096, "probe request was not bounded");
    }
    let expected = format!(
        "GET /healthz HTTP/1.1\r\nHost: {address}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    assert_eq!(request, expected.as_bytes());
}

fn spawn_response_server(response: Vec<u8>) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("probe test listener");
    let address = listener.local_addr().expect("probe test address");
    let server = thread::spawn(move || {
        let (mut stream, _peer) = listener.accept().expect("probe connection");
        read_probe_request(&mut stream, address);
        stream.write_all(&response).expect("probe response");
    });
    (address, server)
}

fn readiness_body(status: &str, accepting: bool, available_sessions: usize) -> String {
    serde_json::json!({
        "status": status,
        "bootId": "boot-cli-test",
        "load": {
            "bootId": "boot-cli-test",
            "accepting": accepting,
            "draining": !accepting,
            "activeSessions": 0,
            "preparedSessions": 0,
            "maxSessions": 4,
            "availableSessions": available_sessions,
            "utilization": 0.0
        }
    })
    .to_string()
}

fn http_response(status: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

#[test]
fn simulate_emits_ndjson_and_private_safe_events() {
    let events = NamedTempFile::new().expect("events file");
    let sentinel = "CLI_PRIVATE_SENTINEL_441";
    let output = Command::new(runtime())
        .args([
            "simulate",
            "--agent-manifest",
            &manifest(&["batch-stt", "streaming-reasoning", "streaming-tts"]),
            "--input",
            sentinel,
            "--events",
            events.path().to_str().expect("UTF-8 path"),
        ])
        .output()
        .expect("runtime process");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("NDJSON UTF-8");
    let messages: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).expect("protocol NDJSON"))
        .collect();
    assert_eq!(
        messages.first().and_then(|value| value["type"].as_str()),
        Some("session.ready")
    );
    assert!(
        messages
            .iter()
            .any(|message| message["type"] == "audio.chunk" && message["audioBase64"].is_string())
    );
    assert!(messages.iter().any(|message| {
        message["type"] == "response.completed" && message["interrupted"] == false
    }));
    let semantic_events = std::fs::read_to_string(events.path()).expect("semantic events");
    assert!(!semantic_events.contains(sentinel));
    assert!(!semantic_events.contains("Scripted response"));
    let semantic_events: Vec<serde_json::Value> = semantic_events
        .lines()
        .map(|line| serde_json::from_str(line).expect("semantic event NDJSON"))
        .collect();
    assert_eq!(
        semantic_events
            .iter()
            .filter(|event| event["type"] == "session.completed")
            .count(),
        1
    );
    assert!(
        !semantic_events
            .iter()
            .any(|event| event["type"] == "session.canceled")
    );
}

#[test]
fn simulate_fails_nonzero_for_missing_capability() {
    let output = Command::new(runtime())
        .args([
            "simulate",
            "--agent-manifest",
            &manifest(&["unsupported-capability"]),
            "--input",
            "hello",
        ])
        .output()
        .expect("runtime process");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requiredCapabilities"));
}

#[test]
fn simulate_never_fabricates_external_tool_success() {
    let mut value: serde_json::Value = serde_json::from_str(&manifest(&[
        "batch-stt",
        "streaming-reasoning",
        "streaming-tts",
    ]))
    .expect("manifest JSON");
    value["definition"]["tools"] = serde_json::json!([{
        "name": "lookup",
        "description": "Lookup",
        "inputSchema": {"type": "object"},
        "execution": {"kind": "https", "url": "https://example.com/tool"}
    }]);
    value["requiredCapabilities"] = serde_json::json!([
        "batch-stt",
        "streaming-reasoning",
        "streaming-tts",
        "tool-execution"
    ]);
    let output = Command::new(runtime())
        .args([
            "simulate",
            "--agent-manifest",
            &value.to_string(),
            "--input",
            "/tool lookup {}",
        ])
        .output()
        .expect("runtime process");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does not run deployment tools"));
}

#[test]
fn serve_exposes_local_manifest_mode() {
    let output = Command::new(runtime())
        .args(["serve", "--help"])
        .output()
        .expect("runtime process");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("help UTF-8");
    assert!(help.contains("--agent-manifest"));
    assert!(help.contains("deterministic local realtime session"));
}

#[test]
fn probe_accepts_only_the_loopback_ready_contract() {
    let body = readiness_body("ready", true, 4);
    let (address, server) = spawn_response_server(http_response("200 OK", &body));
    let output = run_probe(address, 500);
    server.join().expect("probe server");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());

    let output = run_probe("192.0.2.1:8080".parse().expect("non-loopback address"), 500);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("loopback"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn probe_rejects_non_ready_and_malformed_responses() {
    let body = readiness_body("not_ready", false, 0);
    let (address, server) = spawn_response_server(http_response("503 Service Unavailable", &body));
    let output = run_probe(address, 500);
    server.join().expect("non-ready probe server");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("HTTP 503"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let body = readiness_body("ready", true, 4);
    let malformed = format!(
        "HTTP/1.1 200 OK\ncontent-type: application/json\ncontent-length: {}\n\n{body}",
        body.len()
    )
    .into_bytes();
    let (address, server) = spawn_response_server(malformed);
    let output = run_probe(address, 500);
    server.join().expect("malformed probe server");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("malformed HTTP"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn probe_fails_closed_on_timeout_and_connection_refusal() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("stalling listener");
    let address = listener.local_addr().expect("stalling listener address");
    let server = thread::spawn(move || {
        let (mut stream, _peer) = listener.accept().expect("probe connection");
        read_probe_request(&mut stream, address);
        let mut byte = [0_u8; 1];
        assert_eq!(
            stream.read(&mut byte).expect("probe disconnect"),
            0,
            "probe sent unexpected bytes after its request"
        );
    });
    let output = run_probe(address, 50);
    server.join().expect("stalling probe server");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("timed out"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let listener = TcpListener::bind("127.0.0.1:0").expect("refusal address reservation");
    let address = listener.local_addr().expect("refusal address");
    drop(listener);
    let output = run_probe(address, 500);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("failed to connect"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn probe_rejects_oversized_responses_without_unbounded_reads() {
    let body = "x".repeat(20 * 1024);
    let (address, server) = spawn_response_server(http_response("200 OK", &body));
    let output = run_probe(address, 500);
    server.join().expect("oversized probe server");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("exceeded 16384 bytes"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

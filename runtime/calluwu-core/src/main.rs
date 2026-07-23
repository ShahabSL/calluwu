use std::{
    io::{self, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use calluwu_core::{
    domain::TenantContext,
    event::{MemoryEventSink, TracingEventSink},
    manifest::AgentManifest,
    protocol::{ClientMessage, PROTOCOL_VERSION, RealtimeEnvelope, RealtimeOutput, ServerMessage},
    provider::{
        DeploymentProviderResolver, GatewayProviderResolver, LocalScriptedProviderResolver,
        ProviderSet,
    },
    server,
    session::SessionConfig,
    supervisor::{SessionPreparation, ShardConfig, ShardSupervisor},
};
use clap::{Parser, Subcommand};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

mod probe;

#[derive(Debug, Parser)]
#[command(
    name = "calluwu-runtime",
    version,
    about = "Calluwu realtime voice-agent runtime",
    subcommand_required = true,
    arg_required_else_help = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a warm, multiplexed HTTP/WebSocket runtime shard.
    Serve {
        /// Address exposed inside the runtime container.
        #[arg(long, default_value = "127.0.0.1:8080", env = "CALLUWU_RUNTIME_BIND")]
        bind: SocketAddr,
        /// Maximum simultaneous session actors.
        #[arg(long, default_value_t = 128, env = "CALLUWU_MAX_SESSIONS")]
        max_sessions: usize,
        /// Graceful actor drain deadline in milliseconds.
        #[arg(long, default_value_t = 10_000, env = "CALLUWU_SHUTDOWN_GRACE_MS")]
        shutdown_grace_ms: u64,
        /// Optional local AgentManifest JSON/path. Enables one deterministic local realtime session.
        #[arg(long)]
        agent_manifest: Option<String>,
    },
    /// Execute one deterministic text turn without external provider credentials.
    Simulate {
        /// Inline AgentManifest JSON, or a path to a JSON manifest.
        #[arg(long)]
        agent_manifest: String,
        /// Caller text for the deterministic scripted provider.
        #[arg(long)]
        input: String,
        /// Optional NDJSON destination for pending semantic runtime events.
        #[arg(long)]
        events: Option<PathBuf>,
    },
    /// Verify that a loopback runtime shard is ready to admit sessions.
    Probe {
        /// Loopback address of the runtime shard.
        #[arg(
            long,
            default_value = "127.0.0.1:8080",
            env = "CALLUWU_RUNTIME_PROBE_ADDRESS"
        )]
        address: SocketAddr,
        /// End-to-end connect, request, and response deadline in milliseconds.
        #[arg(
            long,
            default_value_t = 1_500,
            env = "CALLUWU_RUNTIME_PROBE_TIMEOUT_MS",
            value_parser = clap::value_parser!(u64).range(50..=10_000)
        )]
        timeout_ms: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    initialize_tracing()?;
    match Cli::parse().command {
        Command::Serve {
            bind,
            max_sessions,
            shutdown_grace_ms,
            agent_manifest,
        } => {
            serve(
                bind,
                max_sessions,
                shutdown_grace_ms,
                agent_manifest.as_deref(),
            )
            .await
        }
        Command::Simulate {
            agent_manifest,
            input,
            events,
        } => simulate(&agent_manifest, &input, events.as_deref()).await,
        Command::Probe {
            address,
            timeout_ms,
        } => probe::run(address, Duration::from_millis(timeout_ms)).await,
    }
}

fn initialize_tracing() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("calluwu_core=info,calluwu_runtime=info,tower_http=info")
    });
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(true)
        .with_span_list(true)
        .with_writer(io::stderr)
        .try_init()
        .map_err(|error| anyhow!("failed to initialize tracing: {error}"))
}

async fn serve(
    bind: SocketAddr,
    max_sessions: usize,
    shutdown_grace_ms: u64,
    agent_manifest: Option<&str>,
) -> anyhow::Result<()> {
    let shard_config = ShardConfig {
        max_sessions,
        shutdown_grace: Duration::from_millis(shutdown_grace_ms),
        ..ShardConfig::default()
    };
    let supervisor = if agent_manifest.is_some() {
        ShardSupervisor::new_with_resolver(
            shard_config,
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            Arc::new(LocalScriptedProviderResolver),
        )
    } else {
        ShardSupervisor::new_with_resolver(
            shard_config,
            ProviderSet::scripted(Vec::new(), Duration::ZERO),
            Arc::new(GatewayProviderResolver),
        )
    }
    .context("runtime shard configuration is invalid")?;
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind runtime shard to {bind}"))?;
    let local_address = listener
        .local_addr()
        .context("failed to read bound address")?;
    if let Some(manifest_argument) = agent_manifest {
        prepare_local_realtime(&supervisor, local_address, manifest_argument).await?;
    }
    tracing::info!(bind = %local_address, max_sessions, "runtime shard listening");

    let shutdown = CancellationToken::new();
    let signal_token = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        signal_token.cancel();
    });
    server::serve(listener, supervisor, shutdown)
        .await
        .context("runtime server failed")
}

async fn prepare_local_realtime(
    supervisor: &Arc<ShardSupervisor>,
    local_address: SocketAddr,
    manifest_argument: &str,
) -> anyhow::Result<()> {
    let manifest = load_manifest(manifest_argument).await?;
    let context = TenantContext {
        organization_id: "org_local0001".into(),
        project_id: "prj_local0001".into(),
        deployment_id: "dep_local0001".into(),
        session_id: "ses_localserve".into(),
        runtime_generation: 1,
        correlation_id: "local-serve".into(),
    };
    let ingest_url = "local://semantic-events";
    let ingest_token = format!("local_{}", uuid::Uuid::now_v7());
    let attachment_fingerprint = server::attachment_fingerprint(ingest_url, &ingest_token);
    let fingerprint = server::preparation_fingerprint(&context, &manifest, attachment_fingerprint)?;
    supervisor
        .prepare(SessionPreparation {
            context: context.clone(),
            manifest,
            event_sink: Arc::new(TracingEventSink),
            runtime_service_access: None,
            fingerprint,
            attachment_fingerprint,
        })
        .context("local realtime session could not be prepared")?;

    let ready = serde_json::json!({
        "type": "runtime.local.ready",
        "realtimeUrl": format!("ws://{local_address}/v1/realtime"),
        "sessionId": context.session_id,
        "runtimeGeneration": context.runtime_generation,
        "headers": {
            "X-Calluwu-Session-Id": context.session_id,
            "X-Calluwu-Runtime-Generation": context.runtime_generation.to_string(),
            "X-Calluwu-Runtime-Ingest-Url": ingest_url,
            "X-Calluwu-Runtime-Ingest-Token": ingest_token,
            "X-Calluwu-Organization-Id": context.organization_id,
            "X-Calluwu-Project-Id": context.project_id,
            "X-Calluwu-Deployment-Id": context.deployment_id
        }
    });
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &ready).context("failed to write local connection info")?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(error) => {
                tracing::error!(%error, "failed to register SIGTERM handler");
                let _result = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(%error, "Ctrl-C signal handler failed");
                }
            }
            _signal = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "Ctrl-C signal handler failed");
        }
    }
}

async fn simulate(
    manifest_argument: &str,
    input: &str,
    event_path: Option<&Path>,
) -> anyhow::Result<()> {
    if input.is_empty() || input.len() > 16_000 {
        return Err(anyhow!("--input must contain 1 to 16,000 bytes"));
    }
    let manifest = load_manifest(manifest_argument).await?;
    let providers = LocalScriptedProviderResolver
        .resolve(&manifest, None)
        .context("local simulation cannot execute this deployment")?;
    providers
        .ensure_capabilities(&manifest.required_capabilities)
        .context("agent provider requirements cannot be satisfied")?;

    let session_config = SessionConfig {
        max_session_duration: Duration::from_secs(manifest.definition.limits.max_session_seconds),
        max_history_messages: manifest.definition.limits.max_history_messages,
        sample_rate_hz: manifest.definition.voice.sample_rate_hz,
        voice_id: manifest.definition.voice.id.clone(),
        instructions: manifest.definition.instructions.clone(),
        required_capabilities: manifest.required_capabilities.clone(),
        ..SessionConfig::default()
    };
    let supervisor = ShardSupervisor::new(
        ShardConfig {
            max_sessions: 1,
            session: session_config.clone(),
            ..ShardConfig::default()
        },
        providers.clone(),
    )?;
    let event_sink = Arc::new(MemoryEventSink::default());
    let context = TenantContext {
        organization_id: "org_local0001".into(),
        project_id: "prj_local0001".into(),
        deployment_id: "dep_local0001".into(),
        session_id: "ses_local0001".into(),
        runtime_generation: 1,
        correlation_id: "simulation".into(),
    };
    let mut lease = supervisor.admit_with(
        context.clone(),
        session_config,
        providers,
        event_sink.clone(),
    )?;
    let mut output = io::BufWriter::new(io::stdout().lock());
    let ready = receive_output(&mut lease).await?;
    write_ndjson(&mut output, &ready)?;

    lease.handle.try_control(ClientMessage::SessionStart {
        envelope: client_envelope(&context, "simulation-start"),
    })?;
    lease.handle.try_control(ClientMessage::InputText {
        envelope: client_envelope(&context, "simulation-input"),
        text: input.to_owned(),
    })?;

    loop {
        let message = receive_output(&mut lease).await?;
        let completed = matches!(
            &message,
            RealtimeOutput::Control(ServerMessage::ResponseCompleted {
                interrupted: false,
                ..
            })
        );
        write_ndjson(&mut output, &message)?;
        if completed {
            break;
        }
    }
    output
        .flush()
        .context("failed to flush simulation NDJSON")?;
    lease.handle.end("simulation_complete")?;
    let mut handle = lease.handle;
    tokio::time::timeout(Duration::from_secs(2), handle.wait_finished())
        .await
        .context("simulation session did not drain")?;

    if let Some(path) = event_path {
        let mut ndjson = String::new();
        for event in event_sink.events()? {
            ndjson.push_str(&serde_json::to_string(&event)?);
            ndjson.push('\n');
        }
        tokio::fs::write(path, ndjson)
            .await
            .with_context(|| format!("failed to write semantic events to {}", path.display()))?;
    }
    Ok(())
}

async fn load_manifest(manifest_argument: &str) -> anyhow::Result<AgentManifest> {
    let manifest_json = if manifest_argument.trim_start().starts_with('{') {
        manifest_argument.to_owned()
    } else {
        tokio::fs::read_to_string(manifest_argument)
            .await
            .with_context(|| format!("failed to read agent manifest {manifest_argument}"))?
    };
    AgentManifest::parse_json(&manifest_json).context("agent manifest failed contract validation")
}

async fn receive_output(
    lease: &mut calluwu_core::supervisor::SessionLease,
) -> anyhow::Result<RealtimeOutput> {
    tokio::time::timeout(Duration::from_secs(2), lease.output.recv())
        .await
        .context("simulation timed out waiting for runtime output")?
        .ok_or_else(|| anyhow!("simulation session ended before response completion"))
}

fn client_envelope(context: &TenantContext, message_id: &str) -> RealtimeEnvelope {
    RealtimeEnvelope {
        protocol_version: PROTOCOL_VERSION,
        session_id: context.session_id.clone(),
        message_id: message_id.into(),
        runtime_generation: context.runtime_generation,
    }
}

fn write_ndjson(writer: &mut impl Write, output: &RealtimeOutput) -> anyhow::Result<()> {
    match output {
        RealtimeOutput::Control(message) => {
            serde_json::to_writer(&mut *writer, message)
                .context("failed to serialize runtime message")?;
        }
        RealtimeOutput::Audio(frame) => {
            let mut value =
                serde_json::to_value(&frame.header).context("failed to serialize audio header")?;
            value["audioBase64"] = serde_json::Value::String(BASE64.encode(&frame.audio));
            serde_json::to_writer(&mut *writer, &value)
                .context("failed to serialize local audio message")?;
        }
    }
    writer
        .write_all(b"\n")
        .context("failed to write simulation NDJSON")
}

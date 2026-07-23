use std::{hint::black_box, sync::Arc, time::Duration};

use calluwu_core::{
    domain::TenantContext,
    event::{EventPipeline, MemoryEventSink},
    protocol::{ClientMessage, PROTOCOL_VERSION, RealtimeEnvelope, RealtimeOutput},
    provider::ProviderSet,
    session::{SessionActor, SessionConfig},
};
use criterion::{Criterion, criterion_group, criterion_main};

async fn scripted_text_to_first_audio() {
    let context = TenantContext {
        organization_id: "org_benchmark".into(),
        project_id: "prj_benchmark".into(),
        deployment_id: "dep_benchmark".into(),
        session_id: "ses_benchmark".into(),
        runtime_generation: 1,
        correlation_id: "benchmark".into(),
    };
    let events = EventPipeline::spawn(Arc::new(MemoryEventSink::default()), 64, 16);
    let mut spawned = SessionActor::spawn(
        SessionConfig::default(),
        context.clone(),
        ProviderSet::scripted(Vec::new(), Duration::ZERO),
        events,
    )
    .expect("benchmark actor");
    let _ready = spawned.output.recv().await.expect("session.ready");
    spawned
        .handle
        .try_control(ClientMessage::SessionStart {
            envelope: envelope(&context, "benchmark-start"),
        })
        .expect("session start");
    spawned
        .handle
        .try_control(ClientMessage::InputText {
            envelope: envelope(&context, "benchmark-input"),
            text: black_box("What is the status of order 42?").into(),
        })
        .expect("text input");
    while let Some(message) = spawned.output.recv().await {
        if let RealtimeOutput::Audio(frame) = message {
            black_box(frame.audio);
            break;
        }
    }
    spawned.handle.end("benchmark_complete").expect("end");
    spawned
        .task
        .await
        .expect("actor join")
        .expect("actor result");
}

fn envelope(context: &TenantContext, message_id: &str) -> RealtimeEnvelope {
    RealtimeEnvelope {
        protocol_version: PROTOCOL_VERSION,
        session_id: context.session_id.clone(),
        message_id: message_id.into(),
        runtime_generation: context.runtime_generation,
    }
}

fn latency_benchmark(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("Tokio runtime");
    criterion.benchmark_group("warm_session").bench_function(
        "scripted_text_to_first_audio",
        |bencher| {
            bencher
                .to_async(&runtime)
                .iter(scripted_text_to_first_audio);
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(Duration::from_secs(5));
    targets = latency_benchmark
}
criterion_main!(benches);

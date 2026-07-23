use std::collections::{BTreeMap, HashMap, VecDeque};

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Result, RuntimeError, domain::is_resource_id};

/// Current realtime protocol version shared with `@calluwu/types`.
pub const PROTOCOL_VERSION: u16 = 1;
pub const AUDIO_FRAME_MAGIC: &[u8; 4] = b"CWU1";
pub const MAX_AUDIO_HEADER_BYTES: usize = 4 * 1024;
pub const MAX_AUDIO_PAYLOAD_BYTES: usize = 64 * 1024;

/// Fields present on every realtime JSON message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RealtimeEnvelope {
    pub protocol_version: u16,
    pub session_id: String,
    pub message_id: String,
    pub runtime_generation: u64,
}

impl RealtimeEnvelope {
    /// Create a server-side envelope for an admitted session.
    #[must_use]
    pub fn server(session_id: &str, runtime_generation: u64, message_id: String) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            session_id: session_id.to_owned(),
            message_id,
            runtime_generation,
        }
    }

    /// Fence a client message to exactly one runtime generation.
    pub fn validate(&self, session_id: &str, runtime_generation: u64) -> Result<()> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(RuntimeError::Protocol(
                format!("unsupported protocolVersion {}", self.protocol_version).into(),
            ));
        }
        if self.session_id != session_id {
            return Err(RuntimeError::Protocol(
                "sessionId does not match connection".into(),
            ));
        }
        if self.runtime_generation != runtime_generation {
            return Err(RuntimeError::GenerationMismatch {
                expected: runtime_generation,
                received: self.runtime_generation,
            });
        }
        if !is_resource_id(&self.session_id) {
            return Err(RuntimeError::Protocol("sessionId is invalid".into()));
        }
        if self.message_id.is_empty() || self.message_id.len() > 160 {
            return Err(RuntimeError::Protocol(
                "messageId must contain 1 to 160 bytes".into(),
            ));
        }
        Ok(())
    }
}

/// JSON control message sent from a realtime client.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ClientMessage {
    #[serde(rename = "session.start")]
    SessionStart {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
    },
    #[serde(rename = "input.text")]
    InputText {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
        text: String,
    },
    #[serde(rename = "input.commit")]
    InputCommit {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
    },
    #[serde(rename = "response.cancel")]
    ResponseCancel {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
        #[serde(rename = "responseId")]
        response_id: String,
    },
    #[serde(rename = "playout.ack")]
    PlayoutAck {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
        #[serde(rename = "responseId")]
        response_id: String,
        #[serde(rename = "playedThroughMs")]
        played_through_ms: f64,
    },
    #[serde(rename = "session.end")]
    SessionEnd {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
        reason: String,
    },
}

impl ClientMessage {
    /// Access common protocol fields.
    #[must_use]
    pub const fn envelope(&self) -> &RealtimeEnvelope {
        match self {
            Self::SessionStart { envelope }
            | Self::InputText { envelope, .. }
            | Self::InputCommit { envelope }
            | Self::ResponseCancel { envelope, .. }
            | Self::PlayoutAck { envelope, .. }
            | Self::SessionEnd { envelope, .. } => envelope,
        }
    }

    /// Validate the shared envelope and message-specific size/range limits.
    pub fn validate(&self, session_id: &str, runtime_generation: u64) -> Result<()> {
        self.envelope().validate(session_id, runtime_generation)?;
        match self {
            Self::InputText { text, .. } if text.is_empty() || text.len() > 16_000 => Err(
                RuntimeError::Protocol("input.text must contain 1 to 16,000 bytes".into()),
            ),
            Self::ResponseCancel { response_id, .. } | Self::PlayoutAck { response_id, .. }
                if response_id.is_empty() || response_id.len() > 160 =>
            {
                Err(RuntimeError::Protocol("responseId is invalid".into()))
            }
            Self::PlayoutAck {
                played_through_ms, ..
            } if !played_through_ms.is_finite() || *played_through_ms < 0.0 => Err(
                RuntimeError::Protocol("playedThroughMs must be finite and nonnegative".into()),
            ),
            Self::SessionEnd { reason, .. } if reason.len() > 500 => Err(RuntimeError::Protocol(
                "session.end reason exceeds 500 bytes".into(),
            )),
            _ => Ok(()),
        }
    }
}

/// JSON event sent by the runtime to a realtime client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "session.ready")]
    SessionReady {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
        capabilities: Vec<String>,
    },
    #[serde(rename = "session.started")]
    SessionStarted {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
    },
    #[serde(rename = "transcript.delta")]
    TranscriptDelta {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
        #[serde(rename = "turnId")]
        turn_id: String,
        text: String,
        #[serde(rename = "isFinal")]
        is_final: bool,
    },
    #[serde(rename = "response.delta")]
    ResponseDelta {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
        #[serde(rename = "responseId")]
        response_id: String,
        epoch: u64,
        text: String,
    },
    #[serde(rename = "response.completed")]
    ResponseCompleted {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
        #[serde(rename = "responseId")]
        response_id: String,
        epoch: u64,
        interrupted: bool,
    },
    #[serde(rename = "error")]
    Error {
        #[serde(flatten)]
        envelope: RealtimeEnvelope,
        code: String,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<BTreeMap<String, Value>>,
    },
}

/// Encoding metadata for a binary realtime audio frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AudioEncoding {
    #[serde(rename = "pcm16le")]
    Pcm16Le,
}

/// JSON header embedded in the versioned binary audio frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AudioChunkHeader {
    #[serde(rename = "type")]
    pub message_type: String,
    #[serde(flatten)]
    pub envelope: RealtimeEnvelope,
    pub response_id: String,
    pub epoch: u64,
    pub sequence: u64,
    pub encoding: AudioEncoding,
    pub sample_rate_hz: u32,
    pub channels: u8,
}

/// One compact, ordered WebSocket audio frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioChunkFrame {
    pub header: AudioChunkHeader,
    pub audio: Bytes,
}

impl AudioChunkFrame {
    /// Encode `CWU1 | header_len:u32be | header_json | raw_pcm`.
    pub fn encode(&self) -> Result<Bytes> {
        self.validate()?;
        let header = serde_json::to_vec(&self.header)?;
        if header.len() > MAX_AUDIO_HEADER_BYTES {
            return Err(RuntimeError::Protocol(
                "audio frame header exceeds 4 KiB".into(),
            ));
        }
        let header_len = u32::try_from(header.len())
            .map_err(|_| RuntimeError::Protocol("audio frame header is too large".into()))?;
        let mut frame = BytesMut::with_capacity(8 + header.len() + self.audio.len());
        frame.extend_from_slice(AUDIO_FRAME_MAGIC);
        frame.put_u32(header_len);
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&self.audio);
        Ok(frame.freeze())
    }

    /// Decode and validate a complete versioned binary frame.
    pub fn decode(frame: Bytes) -> Result<Self> {
        if frame.len() < 8 || &frame[..4] != AUDIO_FRAME_MAGIC {
            return Err(RuntimeError::Protocol("invalid audio frame magic".into()));
        }
        let header_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
        if header_len == 0 || header_len > MAX_AUDIO_HEADER_BYTES || frame.len() < 8 + header_len {
            return Err(RuntimeError::Protocol(
                "invalid audio frame header length".into(),
            ));
        }
        let header: AudioChunkHeader = serde_json::from_slice(&frame[8..8 + header_len])?;
        let decoded = Self {
            header,
            audio: frame.slice(8 + header_len..),
        };
        decoded.validate()?;
        Ok(decoded)
    }

    fn validate(&self) -> Result<()> {
        if self.header.message_type != "audio.chunk"
            || self.header.channels != 1
            || !(8_000..=48_000).contains(&self.header.sample_rate_hz)
            || self.header.response_id.is_empty()
            || self.header.response_id.len() > 160
            || self.audio.is_empty()
            || self.audio.len() > MAX_AUDIO_PAYLOAD_BYTES
            || !self.audio.len().is_multiple_of(2)
        {
            return Err(RuntimeError::Protocol(
                "invalid audio frame metadata or PCM payload".into(),
            ));
        }
        self.header.envelope.validate(
            &self.header.envelope.session_id,
            self.header.envelope.runtime_generation,
        )
    }
}

/// Actor output keeps raw audio out of JSON/base64 on the realtime fast path.
#[derive(Debug, Clone, PartialEq)]
pub enum RealtimeOutput {
    Control(ServerMessage),
    Audio(AudioChunkFrame),
}

/// Recent playout acknowledgements used to make interruption/recovery decisions.
#[derive(Debug, Clone)]
pub struct PlayoutAckHistory {
    capacity: usize,
    order: VecDeque<String>,
    played_through: HashMap<String, f64>,
}

impl PlayoutAckHistory {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "playout history capacity must be positive");
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            played_through: HashMap::with_capacity(capacity),
        }
    }

    /// Record only forward progress. Stale/out-of-order ACKs are harmless.
    pub fn record(&mut self, response_id: &str, played_through_ms: f64) -> Result<bool> {
        if response_id.is_empty()
            || response_id.len() > 160
            || !played_through_ms.is_finite()
            || played_through_ms < 0.0
        {
            return Err(RuntimeError::Protocol(
                "invalid playout acknowledgement".into(),
            ));
        }
        if let Some(current) = self.played_through.get_mut(response_id) {
            if played_through_ms <= *current {
                return Ok(false);
            }
            *current = played_through_ms;
            return Ok(true);
        }
        while self.order.len() >= self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.played_through.remove(&expired);
            }
        }
        self.order.push_back(response_id.to_owned());
        self.played_through
            .insert(response_id.to_owned(), played_through_ms);
        Ok(true)
    }

    #[must_use]
    pub fn played_through_ms(&self, response_id: &str) -> Option<f64> {
        self.played_through.get(response_id).copied()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.order.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn matches_typescript_wire_shape() {
        let json = serde_json::json!({
            "type": "playout.ack",
            "protocolVersion": 1,
            "sessionId": "ses_12345678",
            "messageId": "msg-1",
            "runtimeGeneration": 7,
            "responseId": "response-1",
            "playedThroughMs": 42.5
        });
        let message: ClientMessage = serde_json::from_value(json).expect("protocol message");
        message.validate("ses_12345678", 7).expect("valid message");
    }

    #[test]
    fn rejects_shared_unknown_realtime_field_fixture() {
        let fixture: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packages/types/test/fixtures/unicode-boundaries.json"
        )))
        .expect("shared boundary fixture");
        let unknown = &fixture["unknownWireField"];
        let name = unknown["name"].as_str().expect("unknown field name");
        let value = unknown["value"].clone();
        let mut message = serde_json::json!({
            "type": "session.start",
            "protocolVersion": PROTOCOL_VERSION,
            "sessionId": "ses_12345678",
            "messageId": "msg-unknown-field",
            "runtimeGeneration": 7
        });
        message
            .as_object_mut()
            .expect("realtime message object")
            .insert(name.to_owned(), value);

        assert!(serde_json::from_value::<ClientMessage>(message).is_err());
    }

    #[test]
    fn binary_audio_frame_round_trips() {
        let frame = AudioChunkFrame {
            header: AudioChunkHeader {
                message_type: "audio.chunk".into(),
                envelope: RealtimeEnvelope::server("ses_12345678", 3, "server-1".into()),
                response_id: "response-1".into(),
                epoch: 9,
                sequence: 2,
                encoding: AudioEncoding::Pcm16Le,
                sample_rate_hz: 16_000,
                channels: 1,
            },
            audio: Bytes::from_static(&[0, 0, 1, 0]),
        };
        let encoded = frame.encode().expect("encode");
        assert_eq!(&encoded[..4], b"CWU1");
        assert_eq!(AudioChunkFrame::decode(encoded).expect("decode"), frame);
    }

    #[test]
    fn matches_shared_utf8_realtime_boundary_fixtures() {
        let fixture: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packages/types/test/fixtures/unicode-boundaries.json"
        )))
        .expect("shared boundary fixture");
        let repeated = |name: &str, repeat: &str| {
            let boundary = &fixture[name];
            boundary["scalar"]
                .as_str()
                .expect("scalar")
                .repeat(boundary[repeat].as_u64().expect("repeat") as usize)
        };

        let valid_message_id = repeated("realtimeMessageId", "validRepeat");
        let invalid_message_id = repeated("realtimeMessageId", "invalidRepeat");
        let valid_text = repeated("realtimeInputText", "validRepeat");
        let invalid_text = repeated("realtimeInputText", "invalidRepeat");
        let valid_reason = repeated("realtimeSessionEndReason", "validRepeat");
        let invalid_reason = repeated("realtimeSessionEndReason", "invalidRepeat");

        let base = |message_id: String| RealtimeEnvelope {
            protocol_version: PROTOCOL_VERSION,
            session_id: "ses_12345678".into(),
            message_id,
            runtime_generation: 7,
        };
        ClientMessage::SessionStart {
            envelope: base(valid_message_id),
        }
        .validate("ses_12345678", 7)
        .expect("160-byte message ID");
        assert!(
            ClientMessage::SessionStart {
                envelope: base(invalid_message_id)
            }
            .validate("ses_12345678", 7)
            .is_err()
        );
        ClientMessage::InputText {
            envelope: base("input-valid".into()),
            text: valid_text,
        }
        .validate("ses_12345678", 7)
        .expect("16,000-byte input");
        assert!(
            ClientMessage::InputText {
                envelope: base("input-invalid".into()),
                text: invalid_text,
            }
            .validate("ses_12345678", 7)
            .is_err()
        );
        ClientMessage::SessionEnd {
            envelope: base("end-valid".into()),
            reason: valid_reason,
        }
        .validate("ses_12345678", 7)
        .expect("500-byte reason");
        assert!(
            ClientMessage::SessionEnd {
                envelope: base("end-invalid".into()),
                reason: invalid_reason,
            }
            .validate("ses_12345678", 7)
            .is_err()
        );

        let invalid_response_id = repeated("realtimeMessageId", "invalidRepeat");
        let audio = AudioChunkFrame {
            header: AudioChunkHeader {
                message_type: "audio.chunk".into(),
                envelope: base("audio".into()),
                response_id: invalid_response_id,
                epoch: 1,
                sequence: 0,
                encoding: AudioEncoding::Pcm16Le,
                sample_rate_hz: 16_000,
                channels: 1,
            },
            audio: Bytes::from_static(&[0, 0]),
        };
        assert!(audio.encode().is_err());
    }

    proptest! {
        #[test]
        fn playout_ack_is_monotonic(values in prop::collection::vec(0_u32..1_000_000, 1..200)) {
            let mut history = PlayoutAckHistory::new(8);
            for value in &values {
                history.record("response", f64::from(*value)).expect("valid ack");
            }
            let expected = values.iter().copied().max().map_or(0.0, f64::from);
            prop_assert_eq!(history.played_through_ms("response"), Some(expected));
        }

        #[test]
        fn history_never_exceeds_bound(ids in prop::collection::vec("[a-z]{1,20}", 1..100)) {
            let mut history = PlayoutAckHistory::new(5);
            for id in ids {
                history.record(&id, 1.0).expect("valid ack");
                prop_assert!(history.len() <= 5);
            }
        }
    }
}

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    RuntimeError,
    domain::{ToolDefinition, ToolExecution, is_slug},
    tool::validate_input_schema_contract,
};

/// Shared SDK/runtime contract version.
pub const CONTRACT_VERSION: &str = "2026-07-19";

/// A model/provider selection from an immutable agent definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReference {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub settings: BTreeMap<String, Value>,
}

/// Provider selections for the cascade execution path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentProviders {
    pub speech_to_text: ProviderReference,
    pub reasoning: ProviderReference,
    pub text_to_speech: ProviderReference,
}

/// Synthesized voice configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VoiceDefinition {
    pub id: String,
    #[serde(default = "default_language")]
    pub language: String,
    #[serde(default = "default_sample_rate")]
    pub sample_rate_hz: u32,
}

fn default_language() -> String {
    "en-US".into()
}

const fn default_sample_rate() -> u32 {
    16_000
}

/// Session resource budgets carried by a deployment snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentLimits {
    #[serde(default = "default_session_seconds")]
    pub max_session_seconds: u64,
    #[serde(default = "default_concurrent_tools")]
    pub max_concurrent_tools: usize,
    #[serde(default = "default_history_messages")]
    pub max_history_messages: usize,
}

impl Default for AgentLimits {
    fn default() -> Self {
        Self {
            max_session_seconds: default_session_seconds(),
            max_concurrent_tools: default_concurrent_tools(),
            max_history_messages: default_history_messages(),
        }
    }
}

const fn default_session_seconds() -> u64 {
    3_600
}

const fn default_concurrent_tools() -> usize {
    4
}

const fn default_history_messages() -> usize {
    100
}

/// Provider-neutral definition authored through `@calluwu/sdk`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDefinition {
    pub name: String,
    pub instructions: String,
    pub providers: AgentProviders,
    pub voice: VoiceDefinition,
    #[serde(default)]
    pub tools: Vec<ToolDefinition>,
    #[serde(default)]
    pub limits: AgentLimits,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

/// Content-addressed deployment artifact metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactReference {
    pub sha256: String,
    pub size_bytes: u64,
    pub format: ArtifactFormat,
}

/// Supported artifact encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactFormat {
    JavascriptEsm,
}

/// Immutable SDK-to-runtime deployment contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentManifest {
    pub contract_version: String,
    pub definition: AgentDefinition,
    pub required_capabilities: Vec<String>,
    pub artifact: ArtifactReference,
}

impl AgentManifest {
    /// Parse and strictly validate a manifest before starting provider work.
    pub fn parse_json(input: &str) -> crate::Result<Self> {
        let manifest: Self = serde_json::from_str(input)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Enforce the same important boundaries as the TypeScript Zod contract.
    pub fn validate(&self) -> crate::Result<()> {
        if self.contract_version != CONTRACT_VERSION {
            return Err(RuntimeError::InvalidRequest(
                format!(
                    "unsupported contractVersion {}; expected {CONTRACT_VERSION}",
                    self.contract_version
                )
                .into(),
            ));
        }
        if !is_slug(&self.definition.name) {
            return Err(RuntimeError::InvalidRequest(
                "agent name must be a valid slug".into(),
            ));
        }
        if self.definition.instructions.is_empty() || self.definition.instructions.len() > 64_000 {
            return Err(RuntimeError::InvalidRequest(
                "instructions must contain 1 to 64,000 bytes".into(),
            ));
        }
        for reference in [
            &self.definition.providers.speech_to_text,
            &self.definition.providers.reasoning,
            &self.definition.providers.text_to_speech,
        ] {
            if !is_slug(&reference.provider)
                || reference.model.is_empty()
                || reference.model.len() > 160
            {
                return Err(RuntimeError::InvalidRequest(
                    "provider references must contain a valid provider slug and model".into(),
                ));
            }
        }
        if self.definition.voice.id.is_empty() || self.definition.voice.id.len() > 160 {
            return Err(RuntimeError::InvalidRequest("voice.id is invalid".into()));
        }
        if !(2..=35).contains(&self.definition.voice.language.len()) {
            return Err(RuntimeError::InvalidRequest(
                "voice.language must contain 2 to 35 UTF-8 bytes".into(),
            ));
        }
        if !(8_000..=48_000).contains(&self.definition.voice.sample_rate_hz) {
            return Err(RuntimeError::InvalidRequest(
                "voice.sampleRateHz must be between 8000 and 48000".into(),
            ));
        }
        let limits = self.definition.limits;
        if !(10..=14_400).contains(&limits.max_session_seconds)
            || !(1..=16).contains(&limits.max_concurrent_tools)
            || !(2..=1_000).contains(&limits.max_history_messages)
        {
            return Err(RuntimeError::InvalidRequest(
                "agent session limits are outside supported bounds".into(),
            ));
        }
        if self.definition.tools.len() > 64 || self.required_capabilities.len() > 128 {
            return Err(RuntimeError::InvalidRequest(
                "manifest declares too many tools or capabilities".into(),
            ));
        }
        let mut capabilities = std::collections::BTreeSet::new();
        for capability in &self.required_capabilities {
            if !is_slug(capability) {
                return Err(RuntimeError::InvalidRequest(
                    "requiredCapabilities must contain valid slugs".into(),
                ));
            }
            if !capabilities.insert(capability.as_str()) {
                return Err(RuntimeError::InvalidRequest(
                    "requiredCapabilities must not contain duplicates".into(),
                ));
            }
        }
        for required in ["batch-stt", "streaming-reasoning", "streaming-tts"] {
            if !capabilities.contains(required) {
                return Err(RuntimeError::InvalidRequest(
                    format!("requiredCapabilities is missing {required}").into(),
                ));
            }
        }
        let has_tools = !self.definition.tools.is_empty();
        if has_tools != capabilities.contains("tool-execution") {
            return Err(RuntimeError::InvalidRequest(
                "tool-execution capability must exactly match whether tools are declared".into(),
            ));
        }
        let allowed = [
            "batch-stt",
            "streaming-stt",
            "streaming-reasoning",
            "streaming-tts",
            "tool-execution",
            "realtime-speech",
        ];
        if let Some(unsupported) = capabilities
            .iter()
            .find(|capability| !allowed.contains(capability))
        {
            return Err(RuntimeError::InvalidRequest(
                format!("unsupported required capability {unsupported}").into(),
            ));
        }
        let mut tool_names = std::collections::BTreeSet::new();
        for tool in &self.definition.tools {
            if !tool_names.insert(tool.name.as_str()) {
                return Err(RuntimeError::InvalidRequest(
                    format!("tool name {} is declared more than once", tool.name).into(),
                ));
            }
            validate_tool(tool)?;
        }
        if self.artifact.sha256.len() != 64
            || !self
                .artifact
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || self.artifact.size_bytes > 10 * 1024 * 1024
        {
            return Err(RuntimeError::InvalidRequest(
                "artifact digest or size is invalid".into(),
            ));
        }
        Ok(())
    }
}

fn validate_tool(tool: &ToolDefinition) -> crate::Result<()> {
    let valid_name = (1..=64).contains(&tool.name.len())
        && tool.name.bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_alphabetic()
            } else {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
            }
        });
    if !valid_name
        || tool.description.is_empty()
        || tool.description.len() > 2_000
        || !(100..=30_000).contains(&tool.timeout_ms)
    {
        return Err(RuntimeError::InvalidRequest(
            format!("tool {} has an invalid contract", tool.name).into(),
        ));
    }
    validate_input_schema_contract(&tool.input_schema)?;
    match &tool.execution {
        ToolExecution::Builtin { integration } if !is_slug(integration) => Err(
            RuntimeError::InvalidRequest("builtin integration must be a slug".into()),
        ),
        ToolExecution::Https { url, secret_ref } => {
            let parsed = reqwest::Url::parse(url)
                .map_err(|_| RuntimeError::InvalidRequest("tool HTTPS URL is invalid".into()))?;
            if parsed.scheme() != "https" {
                return Err(RuntimeError::InvalidRequest(
                    "remote tools require HTTPS".into(),
                ));
            }
            if secret_ref
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 128)
            {
                return Err(RuntimeError::InvalidRequest(
                    "tool secretRef is invalid".into(),
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_json() -> String {
        serde_json::json!({
            "contractVersion": CONTRACT_VERSION,
            "definition": {
                "name": "support-agent",
                "instructions": "Help the caller.",
                "providers": {
                    "speechToText": { "provider": "scripted", "model": "test" },
                    "reasoning": { "provider": "scripted", "model": "test" },
                    "textToSpeech": { "provider": "scripted", "model": "test" }
                },
                "voice": { "id": "test", "language": "en-US", "sampleRateHz": 16000 }
            },
            "requiredCapabilities": ["batch-stt", "streaming-reasoning", "streaming-tts"],
            "artifact": {
                "sha256": "0".repeat(64), "sizeBytes": 1, "format": "javascript-esm"
            }
        })
        .to_string()
    }

    #[test]
    fn parses_sdk_contract() {
        let manifest = AgentManifest::parse_json(&manifest_json()).expect("valid manifest");
        assert_eq!(manifest.definition.limits, AgentLimits::default());
    }

    #[test]
    fn rejects_non_https_tool() {
        let mut value: Value = serde_json::from_str(&manifest_json()).expect("JSON fixture");
        value["definition"]["tools"] = serde_json::json!([{
            "name":"lookup", "description":"Lookup", "inputSchema":{},
            "sideEffect":"none", "timeoutMs":1000,
            "execution":{"kind":"https", "url":"http://example.test"}
        }]);
        value["requiredCapabilities"] = serde_json::json!([
            "batch-stt",
            "streaming-reasoning",
            "streaming-tts",
            "tool-execution"
        ]);
        let error = AgentManifest::parse_json(&value.to_string()).expect_err("HTTP rejected");
        assert_eq!(error.code(), "invalid_request");
    }

    #[test]
    fn rejects_missing_or_duplicate_capabilities() {
        let mut missing: Value = serde_json::from_str(&manifest_json()).expect("JSON fixture");
        missing["requiredCapabilities"] = serde_json::json!(["batch-stt"]);
        assert!(AgentManifest::parse_json(&missing.to_string()).is_err());

        let mut duplicate: Value = serde_json::from_str(&manifest_json()).expect("JSON fixture");
        duplicate["requiredCapabilities"] = serde_json::json!([
            "batch-stt",
            "batch-stt",
            "streaming-reasoning",
            "streaming-tts"
        ]);
        assert!(AgentManifest::parse_json(&duplicate.to_string()).is_err());
    }

    #[test]
    fn rejects_unknown_manifest_fields() {
        let mut value: Value = serde_json::from_str(&manifest_json()).expect("JSON fixture");
        value["unexpected"] = Value::Bool(true);
        assert!(AgentManifest::parse_json(&value.to_string()).is_err());
    }

    #[test]
    fn required_capabilities_cannot_be_omitted() {
        let mut value: Value = serde_json::from_str(&manifest_json()).expect("JSON fixture");
        value
            .as_object_mut()
            .expect("object")
            .remove("requiredCapabilities");
        assert!(AgentManifest::parse_json(&value.to_string()).is_err());
    }

    #[test]
    fn rejects_duplicate_tool_names() {
        let mut value: Value = serde_json::from_str(&manifest_json()).expect("JSON fixture");
        let tool = serde_json::json!({
            "name":"lookup", "description":"Lookup", "inputSchema":{"type":"object"},
            "sideEffect":"none", "timeoutMs":1000, "execution":{"kind":"local"}
        });
        value["definition"]["tools"] = serde_json::json!([tool.clone(), tool]);
        value["requiredCapabilities"] = serde_json::json!([
            "batch-stt",
            "streaming-reasoning",
            "streaming-tts",
            "tool-execution"
        ]);
        assert!(AgentManifest::parse_json(&value.to_string()).is_err());
    }

    #[test]
    fn matches_shared_utf8_provider_model_boundary_fixture() {
        let fixture: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packages/types/test/fixtures/unicode-boundaries.json"
        )))
        .expect("shared boundary fixture");
        let boundary = &fixture["providerModel"];
        let scalar = boundary["scalar"].as_str().expect("scalar");
        let valid = scalar.repeat(boundary["validRepeat"].as_u64().expect("valid repeat") as usize);
        let invalid =
            scalar.repeat(boundary["invalidRepeat"].as_u64().expect("invalid repeat") as usize);

        let mut valid_manifest: Value =
            serde_json::from_str(&manifest_json()).expect("JSON fixture");
        valid_manifest["definition"]["providers"]["speechToText"]["model"] = Value::String(valid);
        AgentManifest::parse_json(&valid_manifest.to_string()).expect("160 UTF-8 bytes");

        valid_manifest["definition"]["providers"]["speechToText"]["model"] = Value::String(invalid);
        assert!(AgentManifest::parse_json(&valid_manifest.to_string()).is_err());
    }
}

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Whether executing a tool can change external state.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSideEffect {
    /// Pure/read-only work. A failed invocation may be retried.
    #[default]
    None,
    /// Repeating an invocation with the same key is safe.
    Idempotent,
    /// The runtime must never automatically repeat an uncertain invocation.
    CommitOnce,
}

/// Location at which a declared tool is executed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolExecution {
    Builtin {
        integration: String,
    },
    Https {
        url: String,
        #[serde(rename = "secretRef", skip_serializing_if = "Option::is_none")]
        secret_ref: Option<String>,
    },
    Local,
}

/// Deployment-time tool contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: BTreeMap<String, Value>,
    #[serde(default = "default_tool_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub side_effect: ToolSideEffect,
    pub execution: ToolExecution,
}

const fn default_tool_timeout_ms() -> u64 {
    10_000
}

/// Tenant hierarchy carried on every admitted runtime session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantContext {
    pub organization_id: String,
    pub project_id: String,
    pub deployment_id: String,
    pub session_id: String,
    pub runtime_generation: u64,
    pub correlation_id: String,
}

impl TenantContext {
    /// Validate IDs at the trusted proxy/runtime boundary.
    pub fn validate(&self) -> crate::Result<()> {
        for (name, value) in [
            ("organizationId", self.organization_id.as_str()),
            ("projectId", self.project_id.as_str()),
            ("deploymentId", self.deployment_id.as_str()),
            ("sessionId", self.session_id.as_str()),
        ] {
            if !is_resource_id(value) {
                return Err(crate::RuntimeError::InvalidRequest(
                    format!("{name} is not a Calluwu resource ID").into(),
                ));
            }
        }
        if self.correlation_id.is_empty() || self.correlation_id.len() > 160 {
            return Err(crate::RuntimeError::InvalidRequest(
                "correlationId must contain 1 to 160 bytes".into(),
            ));
        }
        Ok(())
    }
}

/// Match the cross-language resource ID contract without a regex dependency.
#[must_use]
pub fn is_resource_id(value: &str) -> bool {
    if !(8..=80).contains(&value.len()) {
        return false;
    }
    let Some((prefix, suffix)) = value.split_once('_') else {
        return false;
    };
    !prefix.is_empty()
        && prefix.bytes().all(|byte| byte.is_ascii_lowercase())
        && !suffix.is_empty()
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// Match the SDK's lower-case, hyphen-separated slug contract.
#[must_use]
pub fn is_slug(value: &str) -> bool {
    if !(2..=63).contains(&value.len()) {
        return false;
    }
    value.split('-').all(|segment| {
        !segment.is_empty()
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    })
}

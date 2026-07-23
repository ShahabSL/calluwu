use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::Value;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{ToolDefinition, ToolSideEffect},
    provider::{ProviderErrorKind, ToolExecutor, ToolInvocation, ToolOutput},
};

/// Result after side-effect and idempotency enforcement.
#[derive(Debug, Clone, PartialEq)]
pub struct CoordinatedToolOutput {
    pub value: Value,
    pub cached: bool,
    pub side_effect: ToolSideEffect,
}

/// Stable tool coordination failure.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ToolError {
    #[error("tool is not declared by this deployment")]
    Undeclared,
    #[error("tool invocation requires an idempotency key")]
    MissingIdempotencyKey,
    #[error("tool invocation input does not match its declared schema: {0}")]
    InvalidInput(String),
    #[error("the same tool invocation is already in flight")]
    AlreadyInFlight,
    #[error("commit-once invocation previously ended with an uncertain result: {0}")]
    CommitOnceUncertain(String),
    #[error("tool invocation timed out")]
    Timeout,
    #[error("tool invocation was cancelled")]
    Cancelled,
    #[error("tool provider failed: {0}")]
    Provider(String),
    #[error("tool idempotency ledger is unavailable")]
    LedgerUnavailable,
    #[error("tool idempotency ledger reached its session bound")]
    LedgerCapacity,
    #[error("tool provider output exceeds the session byte bound")]
    OutputTooLarge,
}

#[derive(Debug, Clone)]
enum LedgerEntry {
    InFlight,
    Completed(Value),
    CommitOnceUncertain(String),
}

/// Owns tool declaration lookup, concurrency, deadlines, and idempotency fencing.
pub struct ToolCoordinator {
    definitions: HashMap<String, ToolDefinition>,
    executor: Arc<dyn ToolExecutor>,
    ledger: Mutex<HashMap<String, LedgerEntry>>,
    permits: Arc<Semaphore>,
    max_ledger_entries: usize,
}

const MAX_TOOL_VALUE_BYTES: usize = 64 * 1024;
const MAX_TOOL_LEDGER_ENTRIES: usize = 1_024;
const MAX_SCHEMA_DEPTH: usize = 8;

impl ToolCoordinator {
    #[must_use]
    pub fn new(definitions: Vec<ToolDefinition>, executor: Arc<dyn ToolExecutor>) -> Self {
        Self::with_concurrency(definitions, executor, 4)
    }

    #[must_use]
    pub fn with_concurrency(
        definitions: Vec<ToolDefinition>,
        executor: Arc<dyn ToolExecutor>,
        max_concurrent: usize,
    ) -> Self {
        assert!(max_concurrent > 0, "tool concurrency must be positive");
        Self {
            definitions: definitions
                .into_iter()
                .map(|definition| (definition.name.clone(), definition))
                .collect(),
            executor,
            ledger: Mutex::new(HashMap::new()),
            permits: Arc::new(Semaphore::new(max_concurrent)),
            max_ledger_entries: MAX_TOOL_LEDGER_ENTRIES,
        }
    }

    /// Execute exactly according to the deployment's declared side-effect class.
    pub async fn execute(
        &self,
        invocation: ToolInvocation,
        cancel: CancellationToken,
    ) -> std::result::Result<CoordinatedToolOutput, ToolError> {
        let definition = self
            .definitions
            .get(&invocation.tool_name)
            .ok_or(ToolError::Undeclared)?;
        if invocation.idempotency_key.is_empty() || invocation.idempotency_key.len() > 200 {
            return Err(ToolError::MissingIdempotencyKey);
        }
        if invocation.call_id.is_empty()
            || invocation.call_id.len() > 160
            || invocation.tool_name.len() > 64
            || serialized_size(&invocation.input)? > MAX_TOOL_VALUE_BYTES
        {
            return Err(ToolError::InvalidInput(
                "tool call identifiers or input exceed supported bounds".into(),
            ));
        }
        validate_tool_input(&definition.input_schema, &invocation.input)?;

        let permit = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(ToolError::Cancelled),
            result = self.permits.clone().acquire_owned() => {
                result.map_err(|_| ToolError::Provider("tool coordinator is shutting down".into()))?
            }
        };

        let ledger_key = format!("{}:{}", invocation.tool_name, invocation.idempotency_key);
        {
            let mut ledger = self
                .ledger
                .lock()
                .map_err(|_| ToolError::LedgerUnavailable)?;
            match ledger.get(&ledger_key) {
                Some(LedgerEntry::InFlight) => return Err(ToolError::AlreadyInFlight),
                Some(LedgerEntry::Completed(value)) => {
                    return Ok(CoordinatedToolOutput {
                        value: value.clone(),
                        cached: true,
                        side_effect: definition.side_effect,
                    });
                }
                Some(LedgerEntry::CommitOnceUncertain(message)) => {
                    return Err(ToolError::CommitOnceUncertain(message.clone()));
                }
                None => {
                    if ledger.len() >= self.max_ledger_entries {
                        return Err(ToolError::LedgerCapacity);
                    }
                    ledger.insert(ledger_key.clone(), LedgerEntry::InFlight);
                }
            }
        }

        let timeout = Duration::from_millis(definition.timeout_ms);
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(ToolError::Cancelled),
            result = tokio::time::timeout(
                timeout,
                self.executor.execute(invocation, cancel.clone()),
            ) => match result {
                Ok(Ok(ToolOutput { value })) => {
                    if serialized_size(&value)? > MAX_TOOL_VALUE_BYTES {
                        Err(ToolError::OutputTooLarge)
                    } else {
                        Ok(value)
                    }
                },
                Ok(Err(error)) if error.kind == ProviderErrorKind::Cancelled => {
                    Err(ToolError::Cancelled)
                }
                Ok(Err(error)) => Err(ToolError::Provider(error.message)),
                Err(_) => Err(ToolError::Timeout),
            }
        };
        drop(permit);

        let mut ledger = self
            .ledger
            .lock()
            .map_err(|_| ToolError::LedgerUnavailable)?;
        match result {
            Ok(value) => {
                ledger.insert(ledger_key, LedgerEntry::Completed(value.clone()));
                Ok(CoordinatedToolOutput {
                    value,
                    cached: false,
                    side_effect: definition.side_effect,
                })
            }
            Err(error) => {
                if definition.side_effect == ToolSideEffect::CommitOnce {
                    ledger.insert(
                        ledger_key,
                        LedgerEntry::CommitOnceUncertain(error.to_string()),
                    );
                } else {
                    ledger.remove(&ledger_key);
                }
                Err(error)
            }
        }
    }
}

fn serialized_size(value: &Value) -> std::result::Result<usize, ToolError> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| ToolError::InvalidInput("tool value is not serializable".into()))
}

/// Validate the deliberately small JSON-Schema subset supported by tool execution.
/// Unsupported keywords fail deployment validation instead of being silently ignored.
pub(crate) fn validate_input_schema_contract(
    schema: &BTreeMap<String, Value>,
) -> crate::Result<()> {
    if serde_json::to_vec(schema)?.len() > MAX_TOOL_VALUE_BYTES {
        return Err(crate::RuntimeError::InvalidRequest(
            "tool inputSchema exceeds 64 KiB".into(),
        ));
    }
    validate_schema_node(schema, 0).map_err(|message| {
        crate::RuntimeError::InvalidRequest(
            format!("unsupported tool inputSchema: {message}").into(),
        )
    })
}

fn validate_schema_node(
    schema: &BTreeMap<String, Value>,
    depth: usize,
) -> std::result::Result<(), String> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err("schema nesting exceeds 8 levels".into());
    }
    if schema.is_empty() {
        return Ok(());
    }
    const ALLOWED: &[&str] = &[
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "enum",
        "description",
        "minLength",
        "maxLength",
        "minimum",
        "maximum",
    ];
    if let Some(keyword) = schema.keys().find(|key| !ALLOWED.contains(&key.as_str())) {
        return Err(format!("keyword {keyword} is not supported"));
    }
    if let Some(value) = schema.get("type") {
        let Some(kind) = value.as_str() else {
            return Err("type must be a string".into());
        };
        if !matches!(
            kind,
            "object" | "array" | "string" | "number" | "integer" | "boolean" | "null"
        ) {
            return Err(format!("type {kind} is not supported"));
        }
    }
    let properties = match schema.get("properties") {
        Some(Value::Object(properties)) => Some(properties),
        Some(_) => return Err("properties must be an object".into()),
        None => None,
    };
    if let Some(properties) = properties {
        if properties.len() > 128 {
            return Err("properties exceeds 128 entries".into());
        }
        for (name, child) in properties {
            if name.is_empty() || name.len() > 160 {
                return Err("property name is invalid".into());
            }
            let Value::Object(child) = child else {
                return Err(format!("property {name} schema must be an object"));
            };
            validate_schema_node(&child.clone().into_iter().collect(), depth + 1)?;
        }
    }
    if let Some(required) = schema.get("required") {
        let Value::Array(required) = required else {
            return Err("required must be an array".into());
        };
        let mut unique = HashSet::new();
        for name in required {
            let Some(name) = name.as_str() else {
                return Err("required entries must be strings".into());
            };
            if !unique.insert(name) {
                return Err("required entries must be unique".into());
            }
            if properties.is_none_or(|properties| !properties.contains_key(name)) {
                return Err(format!("required property {name} is not declared"));
            }
        }
    }
    if schema
        .get("additionalProperties")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err("additionalProperties must be boolean".into());
    }
    if let Some(items) = schema.get("items") {
        let Value::Object(items) = items else {
            return Err("items must be an object schema".into());
        };
        validate_schema_node(&items.clone().into_iter().collect(), depth + 1)?;
    }
    if let Some(value) = schema.get("enum")
        && !matches!(value, Value::Array(values) if !values.is_empty() && values.len() <= 128)
    {
        return Err("enum must contain 1 to 128 values".into());
    }
    if schema
        .get("description")
        .is_some_and(|value| value.as_str().is_none_or(|text| text.len() > 2_000))
    {
        return Err("description must be a string of at most 2,000 bytes".into());
    }
    for key in ["minLength", "maxLength"] {
        if schema
            .get(key)
            .is_some_and(|value| value.as_u64().is_none())
        {
            return Err(format!("{key} must be a nonnegative integer"));
        }
    }
    for key in ["minimum", "maximum"] {
        if schema
            .get(key)
            .is_some_and(|value| value.as_f64().is_none())
        {
            return Err(format!("{key} must be a finite number"));
        }
    }
    Ok(())
}

fn validate_tool_input(
    schema: &BTreeMap<String, Value>,
    input: &Value,
) -> std::result::Result<(), ToolError> {
    validate_instance(schema, input, 0).map_err(ToolError::InvalidInput)
}

fn validate_instance(
    schema: &BTreeMap<String, Value>,
    input: &Value,
    depth: usize,
) -> std::result::Result<(), String> {
    if schema.is_empty() {
        return Ok(());
    }
    if depth > MAX_SCHEMA_DEPTH {
        return Err("input nesting exceeds schema bound".into());
    }
    if let Some(Value::Array(values)) = schema.get("enum")
        && !values.contains(input)
    {
        return Err("value is not in the declared enum".into());
    }
    if let Some(Value::String(kind)) = schema.get("type") {
        let matches = match kind.as_str() {
            "object" => input.is_object(),
            "array" => input.is_array(),
            "string" => input.is_string(),
            "number" => input.is_number(),
            "integer" => input.as_i64().is_some() || input.as_u64().is_some(),
            "boolean" => input.is_boolean(),
            "null" => input.is_null(),
            _ => false,
        };
        if !matches {
            return Err(format!("expected {kind}"));
        }
    }
    if let Some(object) = input.as_object() {
        if let Some(Value::Array(required)) = schema.get("required") {
            for name in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(name) {
                    return Err(format!("missing required property {name}"));
                }
            }
        }
        let properties = schema.get("properties").and_then(Value::as_object);
        if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
            for name in object.keys() {
                if properties.is_none_or(|properties| !properties.contains_key(name)) {
                    return Err(format!("unexpected property {name}"));
                }
            }
        }
        if let Some(properties) = properties {
            for (name, value) in object {
                if let Some(Value::Object(child)) = properties.get(name) {
                    validate_instance(&child.clone().into_iter().collect(), value, depth + 1)?;
                }
            }
        }
    }
    if let Some(array) = input.as_array()
        && let Some(Value::Object(items)) = schema.get("items")
    {
        let items: BTreeMap<_, _> = items.clone().into_iter().collect();
        for value in array {
            validate_instance(&items, value, depth + 1)?;
        }
    }
    if let Some(text) = input.as_str() {
        let length = text.chars().count() as u64;
        if schema
            .get("minLength")
            .and_then(Value::as_u64)
            .is_some_and(|minimum| length < minimum)
            || schema
                .get("maxLength")
                .and_then(Value::as_u64)
                .is_some_and(|maximum| length > maximum)
        {
            return Err("string length is outside declared bounds".into());
        }
    }
    if let Some(number) = input.as_f64()
        && (schema
            .get("minimum")
            .and_then(Value::as_f64)
            .is_some_and(|minimum| number < minimum)
            || schema
                .get("maximum")
                .and_then(Value::as_f64)
                .is_some_and(|maximum| number > maximum))
    {
        return Err("number is outside declared bounds".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::{
        domain::ToolExecution,
        provider::{ProviderError, ToolInvocation},
    };

    struct CountingExecutor(AtomicUsize);

    #[async_trait]
    impl ToolExecutor for CountingExecutor {
        async fn execute(
            &self,
            invocation: ToolInvocation,
            _cancel: CancellationToken,
        ) -> std::result::Result<ToolOutput, ProviderError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(ToolOutput {
                value: invocation.input,
            })
        }
    }

    fn definition(side_effect: ToolSideEffect) -> ToolDefinition {
        ToolDefinition {
            name: "lookup".into(),
            description: "Lookup a record".into(),
            input_schema: Default::default(),
            timeout_ms: 1_000,
            side_effect,
            execution: ToolExecution::Local,
        }
    }

    #[tokio::test]
    async fn deduplicates_successful_commit_once_call() {
        let executor = Arc::new(CountingExecutor(AtomicUsize::new(0)));
        let coordinator = ToolCoordinator::new(
            vec![definition(ToolSideEffect::CommitOnce)],
            executor.clone(),
        );
        let invocation = ToolInvocation {
            call_id: "call-1".into(),
            tool_name: "lookup".into(),
            input: serde_json::json!({"id": 1}),
            idempotency_key: "session:response:call-1".into(),
        };
        let first = coordinator
            .execute(invocation.clone(), CancellationToken::new())
            .await
            .expect("first call");
        let duplicate = coordinator
            .execute(invocation, CancellationToken::new())
            .await
            .expect("cached call");
        assert!(!first.cached);
        assert!(duplicate.cached);
        assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn validates_declared_input_schema_before_execution() {
        let executor = Arc::new(CountingExecutor(AtomicUsize::new(0)));
        let mut declared = definition(ToolSideEffect::None);
        declared.input_schema = BTreeMap::from([
            ("type".into(), Value::String("object".into())),
            (
                "properties".into(),
                serde_json::json!({"id":{"type":"integer"}}),
            ),
            ("required".into(), serde_json::json!(["id"])),
            ("additionalProperties".into(), Value::Bool(false)),
        ]);
        validate_input_schema_contract(&declared.input_schema).expect("supported schema");
        let coordinator = ToolCoordinator::new(vec![declared], executor.clone());
        let error = coordinator
            .execute(
                ToolInvocation {
                    call_id: "call-1".into(),
                    tool_name: "lookup".into(),
                    input: serde_json::json!({"unexpected": true}),
                    idempotency_key: "key-1".into(),
                },
                CancellationToken::new(),
            )
            .await
            .expect_err("schema mismatch");
        assert!(matches!(error, ToolError::InvalidInput(_)));
        assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ledger_fails_closed_at_its_session_bound() {
        let executor = Arc::new(CountingExecutor(AtomicUsize::new(0)));
        let coordinator = ToolCoordinator::new(
            vec![definition(ToolSideEffect::Idempotent)],
            executor.clone(),
        );
        for index in 0..MAX_TOOL_LEDGER_ENTRIES {
            coordinator
                .execute(
                    ToolInvocation {
                        call_id: format!("call-{index}"),
                        tool_name: "lookup".into(),
                        input: serde_json::json!({"id": index}),
                        idempotency_key: format!("key-{index}"),
                    },
                    CancellationToken::new(),
                )
                .await
                .expect("bounded entry");
        }
        let error = coordinator
            .execute(
                ToolInvocation {
                    call_id: "overflow".into(),
                    tool_name: "lookup".into(),
                    input: Value::Null,
                    idempotency_key: "overflow".into(),
                },
                CancellationToken::new(),
            )
            .await
            .expect_err("ledger capacity");
        assert_eq!(error, ToolError::LedgerCapacity);
        assert_eq!(executor.0.load(Ordering::SeqCst), MAX_TOOL_LEDGER_ENTRIES);
    }
}

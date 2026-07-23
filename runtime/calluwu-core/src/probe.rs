use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, anyhow, bail};
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const MAX_HEADER_BYTES: usize = 8 * 1024;
const MAX_RESPONSE_BYTES: usize = 16 * 1024;
pub(crate) const MIN_TIMEOUT: Duration = Duration::from_millis(50);
pub(crate) const MAX_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadinessContract {
    status: String,
    boot_id: String,
    load: ReadinessLoad,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadinessLoad {
    boot_id: String,
    accepting: bool,
    draining: bool,
    available_sessions: usize,
}

pub(crate) async fn run(address: SocketAddr, deadline: Duration) -> anyhow::Result<()> {
    if !address.ip().is_loopback() {
        bail!("probe address must use an IPv4 or IPv6 loopback address");
    }
    if !(MIN_TIMEOUT..=MAX_TIMEOUT).contains(&deadline) {
        bail!(
            "probe timeout must be between {} and {} milliseconds",
            MIN_TIMEOUT.as_millis(),
            MAX_TIMEOUT.as_millis()
        );
    }

    tokio::time::timeout(deadline, exchange(address))
        .await
        .map_err(|_| {
            anyhow!(
                "runtime readiness probe timed out after {} milliseconds",
                deadline.as_millis()
            )
        })?
}

async fn exchange(address: SocketAddr) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(address)
        .await
        .with_context(|| format!("failed to connect to runtime at {address}"))?;
    let request = format!(
        "GET /healthz HTTP/1.1\r\nHost: {address}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .context("failed to send runtime readiness request")?;
    stream
        .flush()
        .await
        .context("failed to flush runtime readiness request")?;

    let mut response = Vec::with_capacity(1024);
    stream
        .take((MAX_RESPONSE_BYTES + 1) as u64)
        .read_to_end(&mut response)
        .await
        .context("failed to read runtime readiness response")?;
    if response.len() > MAX_RESPONSE_BYTES {
        bail!("runtime readiness response exceeded {MAX_RESPONSE_BYTES} bytes");
    }
    validate_response(&response)
}

fn validate_response(response: &[u8]) -> anyhow::Result<()> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| anyhow!("runtime readiness response has malformed HTTP headers"))?;
    if header_end > MAX_HEADER_BYTES {
        bail!("runtime readiness response headers exceeded {MAX_HEADER_BYTES} bytes");
    }

    let header = std::str::from_utf8(&response[..header_end])
        .context("runtime readiness response headers are not valid UTF-8")?;
    let mut lines = header.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| anyhow!("runtime readiness response is missing an HTTP status line"))?;
    let mut status_parts = status_line.splitn(3, ' ');
    let version = status_parts.next().unwrap_or_default();
    let status = status_parts.next().unwrap_or_default();
    let reason = status_parts.next().unwrap_or_default();
    if version != "HTTP/1.1"
        || status.len() != 3
        || !status.bytes().all(|byte| byte.is_ascii_digit())
        || reason.is_empty()
    {
        bail!("runtime readiness response has a malformed HTTP/1.1 status line");
    }
    if status != "200" {
        bail!("runtime readiness endpoint returned HTTP {status}");
    }

    let mut content_length = None;
    let mut content_type = None;
    for line in lines {
        if line.is_empty() || line.starts_with([' ', '\t']) {
            bail!("runtime readiness response has a malformed HTTP header");
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| anyhow!("runtime readiness response has a malformed HTTP header"))?;
        if !is_header_name(name) {
            bail!("runtime readiness response has an invalid HTTP header name");
        }
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                bail!("runtime readiness response has duplicate content-length headers");
            }
            content_length = Some(
                value
                    .parse::<usize>()
                    .context("runtime readiness response has an invalid content-length")?,
            );
        } else if name.eq_ignore_ascii_case("content-type") {
            if content_type.is_some() {
                bail!("runtime readiness response has duplicate content-type headers");
            }
            content_type = Some(value);
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            bail!("runtime readiness response cannot use transfer encoding");
        }
    }

    let body = &response[header_end + 4..];
    let content_length = content_length
        .ok_or_else(|| anyhow!("runtime readiness response is missing content-length"))?;
    if body.len() != content_length {
        bail!("runtime readiness response body length does not match content-length");
    }
    let content_type = content_type
        .ok_or_else(|| anyhow!("runtime readiness response is missing content-type"))?;
    if !content_type
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
    {
        bail!("runtime readiness response is not application/json");
    }

    let readiness: ReadinessContract =
        serde_json::from_slice(body).context("runtime readiness response is not valid JSON")?;
    if readiness.status != "ready"
        || readiness.boot_id.is_empty()
        || readiness.boot_id != readiness.load.boot_id
        || !readiness.load.accepting
        || readiness.load.draining
        || readiness.load.available_sessions == 0
    {
        bail!("runtime readiness response does not satisfy the ready contract");
    }
    Ok(())
}

fn is_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn ready_body() -> String {
        serde_json::json!({
            "status": "ready",
            "bootId": "boot-test",
            "load": {
                "bootId": "boot-test",
                "accepting": true,
                "draining": false,
                "activeSessions": 0,
                "preparedSessions": 0,
                "maxSessions": 128,
                "availableSessions": 128,
                "utilization": 0.0
            }
        })
        .to_string()
    }

    #[test]
    fn validates_the_ready_http_contract() {
        validate_response(&response("200 OK", &ready_body())).expect("ready response");
    }

    #[test]
    fn rejects_inconsistent_or_incomplete_ready_contracts() {
        let mut body: serde_json::Value = serde_json::from_str(&ready_body()).expect("ready JSON");
        body["load"]["bootId"] = serde_json::json!("different-boot");
        assert!(
            validate_response(&response("200 OK", &body.to_string()))
                .expect_err("mismatched boot ID")
                .to_string()
                .contains("ready contract")
        );

        let mut body: serde_json::Value = serde_json::from_str(&ready_body()).expect("ready JSON");
        body["load"]["availableSessions"] = serde_json::json!(0);
        assert!(
            validate_response(&response("200 OK", &body.to_string()))
                .expect_err("no available sessions")
                .to_string()
                .contains("ready contract")
        );
    }

    #[test]
    fn rejects_malformed_or_unsupported_http_framing() {
        let body = ready_body();
        let lf_only = format!(
            "HTTP/1.1 200 OK\ncontent-type: application/json\ncontent-length: {}\n\n{body}",
            body.len()
        );
        assert!(validate_response(lf_only.as_bytes()).is_err());

        let chunked = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n",
            body.len()
        );
        assert!(validate_response(chunked.as_bytes()).is_err());
    }
}

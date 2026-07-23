import { randomBytes } from "node:crypto";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";

const DEFAULT_TIMEOUT_MS = 5_000;
const DEFAULT_RESPONSE_LIMIT_BYTES = 16 * 1024;

/** Perform a raw WebSocket upgrade so rejection status and JSON remain observable. */
export async function replayWebSocketHandshake(
  realtime,
  { timeoutMs = DEFAULT_TIMEOUT_MS, maxResponseBytes = DEFAULT_RESPONSE_LIMIT_BYTES } = {},
) {
  const websocketUrl = new URL(realtime.url);
  if (websocketUrl.protocol !== "wss:" && websocketUrl.protocol !== "ws:") {
    throw new Error("Realtime URL did not use a WebSocket protocol");
  }
  const requestUrl = new URL(websocketUrl);
  requestUrl.protocol = websocketUrl.protocol === "wss:" ? "https:" : "http:";

  return new Promise((resolve, reject) => {
    let settled = false;
    let timeout;
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      if (timeout !== undefined) clearTimeout(timeout);
      if (error) reject(error);
      else resolve(value);
    };
    let request;
    try {
      request = (requestUrl.protocol === "https:" ? httpsRequest : httpRequest)(requestUrl, {
        method: "GET",
        headers: {
          Connection: "Upgrade",
          Upgrade: "websocket",
          "Sec-WebSocket-Key": randomBytes(16).toString("base64"),
          "Sec-WebSocket-Protocol": `calluwu.v1, calluwu-ticket.${realtime.token}`,
          "Sec-WebSocket-Version": "13",
        },
      });
    } catch (error) {
      finish(error);
      return;
    }
    request.once("upgrade", (_response, socket) => {
      socket.destroy();
      finish(new Error("A consumed realtime ticket was accepted with HTTP 101"));
    });
    request.once("response", (response) => {
      const chunks = [];
      let size = 0;
      response.on("data", (chunk) => {
        size += chunk.byteLength;
        if (size > maxResponseBytes) {
          response.destroy(new Error("Ticket replay response exceeded its byte limit"));
          return;
        }
        chunks.push(chunk);
      });
      response.once("error", (error) => finish(error));
      response.once("end", () => {
        let body;
        try {
          body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        } catch {
          finish(new Error("Ticket replay rejection did not return JSON"));
          return;
        }
        finish(undefined, { status: response.statusCode, body });
      });
    });
    request.once("error", (error) => finish(error));
    request.end();
    timeout = setTimeout(() => {
      request.destroy();
      finish(new Error(`Ticket replay handshake did not finish within ${timeoutMs} ms`));
    }, timeoutMs);
  });
}

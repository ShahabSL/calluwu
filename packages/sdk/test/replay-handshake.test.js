import { createServer } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { replayWebSocketHandshake } from "../../../scripts/replay-handshake.mjs";

let server;

afterEach(async () => {
  if (server !== undefined) {
    if (server.listening) {
      await new Promise((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }
    server = undefined;
  }
});

describe("raw WebSocket replay handshake", () => {
  it("translates ws to HTTP and preserves the ticket subprotocol for classified rejection", async () => {
    let observedProtocol;
    server = createServer((request, response) => {
      observedProtocol = request.headers["sec-websocket-protocol"];
      response.writeHead(401, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: { code: "invalid_session_ticket" } }));
    });
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("missing test port");

    await expect(
      replayWebSocketHandshake({
        url: `ws://127.0.0.1:${address.port}/v1/sessions/ses_test/realtime`,
        token: "opaque-ticket",
      }),
    ).resolves.toEqual({ status: 401, body: { error: { code: "invalid_session_ticket" } } });
    expect(observedProtocol).toBe("calluwu.v1, calluwu-ticket.opaque-ticket");
  });
});

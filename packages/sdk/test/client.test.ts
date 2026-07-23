import { afterEach, describe, expect, it, vi } from "vitest";
import { CalluwuApiError, CalluwuClient } from "../src/client.js";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("CalluwuClient", () => {
  it("rejects invalid idempotency headers before making a request", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const client = new CalluwuClient({
      apiUrl: "https://api.calluwu.example",
      apiKey: "project-key",
    });

    await expect(
      client.createProject({ slug: "calluwu", name: "Calluwu" }, "header\ninjection"),
    ).rejects.toThrow(/idempotency keys/);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("allows plaintext only for loopback development endpoints", () => {
    expect(
      () => new CalluwuClient({ apiUrl: "http://api.calluwu.example", apiKey: "key" }),
    ).toThrow(/must use HTTPS/);
    expect(
      () => new CalluwuClient({ apiUrl: "http://127.0.0.1:8787", apiKey: "key" }),
    ).not.toThrow();
  });

  it("sends bearer authentication without putting credentials in the URL", async () => {
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const request = new Request(input, init);
      expect(request.url).toBe("https://api.calluwu.example/v1/projects");
      expect(request.headers.get("authorization")).toBe("Bearer secret-key");
      return Response.json({ projects: [] });
    });
    vi.stubGlobal("fetch", fetchMock);

    const client = new CalluwuClient({
      apiUrl: "https://api.calluwu.example/",
      apiKey: "secret-key",
    });

    await expect(client.listProjects()).resolves.toEqual([]);
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it("carries a caller-owned idempotency key for project creation", async () => {
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const request = new Request(input, init);
      expect(request.headers.get("idempotency-key")).toBe("project-create-intent");
      return Response.json({
        project: {
          id: "prj_contract-test",
          organizationId: "org_contract-test",
          slug: "voice-ops",
          name: "Voice Ops",
          activeDeploymentId: null,
          createdAt: "2026-07-19T12:00:00.000Z",
          updatedAt: "2026-07-19T12:00:00.000Z",
        },
      });
    });
    vi.stubGlobal("fetch", fetchMock);
    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });

    await expect(
      client.createProject({ slug: "voice-ops", name: "Voice Ops" }, "project-create-intent"),
    ).resolves.toMatchObject({ slug: "voice-ops" });
  });

  it("carries a caller-owned idempotency key for session admission", async () => {
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const request = new Request(input, init);
      expect(request.url).toBe("https://api.calluwu.example/v1/sessions");
      expect(request.headers.get("idempotency-key")).toBe("session-intent-01");
      return Response.json({
        session: {
          id: "ses_contract-test",
          organizationId: "org_contract-test",
          projectId: "prj_contract-test",
          deploymentId: "dep_12345678",
          status: "ready",
          runtimeShardId: "runtime-000",
          runtimeGeneration: 1,
          createdAt: "2026-07-19T12:00:00.000Z",
          startedAt: null,
          endedAt: null,
        },
        realtime: {
          url: "wss://api.calluwu.example/v1/sessions/ses_contract-test/realtime",
          token: "a-long-session-ticket",
          expiresAt: "2026-07-19T12:01:00.000Z",
          sampleRateHz: 16_000,
        },
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });
    await client.createSession({ deploymentId: "dep_12345678", metadata: {} }, "session-intent-01");

    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it("preserves the stable API error contract", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json(
          {
            error: {
              code: "forbidden",
              message: "The principal cannot access this resource",
              requestId: "req_01TEST",
            },
          },
          { status: 403 },
        ),
      ),
    );

    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });
    const error = await client.listProjects().catch((reason: unknown) => reason);

    expect(error).toBeInstanceOf(CalluwuApiError);
    expect(error).toMatchObject({ status: 403, code: "forbidden", requestId: "req_01TEST" });
  });

  it("does not trust malformed error payloads", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("not-json", { status: 502 })),
    );

    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });

    await expect(client.listProjects()).rejects.toMatchObject({
      status: 502,
      code: "invalid_response",
      message: "Calluwu API returned malformed JSON",
    });
  });

  it("rejects oversized responses before buffering them", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () => new Response("{}", { headers: { "Content-Length": String(3 * 1024 * 1024) } }),
      ),
    );
    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });

    await expect(client.listProjects()).rejects.toMatchObject({ code: "response_too_large" });
  });

  it("activates deployments idempotently and cancels admitted sessions", async () => {
    const now = "2026-07-19T12:00:00.000Z";
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const request = new Request(input, init);
      if (request.url.endsWith("/activate")) {
        expect(request.method).toBe("POST");
        expect(request.headers.get("idempotency-key")).toBe("activation-intent");
        return Response.json({
          deployment: {
            id: "dep_12345678",
            organizationId: "org_12345678",
            projectId: "prj_12345678",
            agentId: "agt_12345678",
            versionId: "ver_12345678",
            status: "ready",
            environment: "production",
            createdAt: now,
            activatedAt: now,
          },
        });
      }
      expect(request.url).toBe("https://api.calluwu.example/v1/sessions/ses_12345678/cancel");
      expect(request.method).toBe("POST");
      return Response.json({
        session: {
          id: "ses_12345678",
          organizationId: "org_12345678",
          projectId: "prj_12345678",
          deploymentId: "dep_12345678",
          status: "canceled",
          runtimeShardId: "runtime-0",
          runtimeGeneration: 1,
          createdAt: now,
          startedAt: null,
          endedAt: now,
        },
      });
    });
    vi.stubGlobal("fetch", fetchMock);
    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });

    await expect(
      client.activateDeployment("prj_12345678", "dep_12345678", "activation-intent"),
    ).resolves.toMatchObject({ id: "dep_12345678", activatedAt: now });
    await expect(client.cancelSession("ses_12345678")).resolves.toMatchObject({
      status: "canceled",
    });
  });

  it("auto-pages ordered events and reports an explicit history bound", async () => {
    const event = (sequence: number) => ({
      id: `evt_page-${sequence}`,
      type: "reasoning.delta",
      version: 1,
      organizationId: "org_12345678",
      projectId: "prj_12345678",
      deploymentId: "dep_12345678",
      sessionId: "ses_12345678",
      sequence,
      correlationId: "turn-1",
      occurredAt: `2026-07-19T12:00:0${sequence}.000Z`,
      source: "provider",
      privacy: "internal",
      payload: { responseId: "response-1" },
    });
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(input instanceof Request ? input.url : input);
      if (url.searchParams.get("after") === null) {
        expect(url.searchParams.get("limit")).toBe("3");
        return Response.json({ events: [event(0), event(1)], nextAfter: 1, hasMore: true });
      }
      expect(url.searchParams.get("after")).toBe("1");
      expect(url.searchParams.get("limit")).toBe("1");
      return Response.json({ events: [event(2)], nextAfter: 2, hasMore: true });
    });
    vi.stubGlobal("fetch", fetchMock);
    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });

    await expect(client.listSessionEvents("ses_12345678", { maxEvents: 3 })).resolves.toEqual({
      events: [event(0), event(1), event(2)],
      truncated: true,
      nextAfter: 2,
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("exposes loss-safe two-phase key rotation metadata", async () => {
    const now = "2026-07-19T12:00:00.000Z";
    const expiresAt = new Date(Date.now() + 30 * 86_400_000).toISOString();
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const request = new Request(input, init);
        expect(request.headers.get("idempotency-key")).toBe("rotate-previous-key");
        await expect(request.json()).resolves.toEqual({ expiresAt });
        return Response.json({
          apiKey: {
            id: "key_replacement",
            organizationId: "org_12345678",
            projectId: null,
            name: "operator",
            scopes: ["api-keys:read", "api-keys:write"],
            status: "active",
            createdAt: now,
            expiresAt,
            lastUsedAt: null,
            revokedAt: null,
            rotatedFromKeyId: "key_previous",
          },
          credentialAlreadyIssued: false,
          credential: "key_replacement.this-is-a-long-random-secret-value",
          rotation: {
            previousKeyId: "key_previous",
            requiresPreviousKeyRevocation: true,
          },
        });
      }),
    );
    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });

    await expect(
      client.rotateApiKey("key_previous", { expiresAt }, "rotate-previous-key"),
    ).resolves.toMatchObject({
      apiKey: { id: "key_replacement", status: "active" },
      credentialAlreadyIssued: false,
      rotation: { previousKeyId: "key_previous", requiresPreviousKeyRevocation: true },
    });
  });

  it("returns replay metadata without inventing an API-key credential", async () => {
    const now = new Date().toISOString();
    const expiresAt = new Date(Date.now() + 30 * 86_400_000).toISOString();
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({
          apiKey: {
            id: "key_replayed",
            organizationId: "org_12345678",
            projectId: null,
            name: "operator",
            scopes: ["api-keys:read"],
            status: "active",
            createdAt: now,
            expiresAt,
            lastUsedAt: null,
            revokedAt: null,
            rotatedFromKeyId: null,
          },
          credentialAlreadyIssued: true,
        }),
      ),
    );
    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });

    const result = await client.createApiKey(
      { name: "operator", scopes: ["api-keys:read"], expiresAt },
      "create-operator",
    );

    expect(result).toMatchObject({ credentialAlreadyIssued: true, apiKey: { id: "key_replayed" } });
    expect("credential" in result).toBe(false);
  });

  it("rejects managed API-key expirations outside the five-minute to 90-day window", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const client = new CalluwuClient({ apiUrl: "https://api.calluwu.example", apiKey: "key" });

    await expect(
      client.createApiKey(
        {
          name: "too-short",
          scopes: ["api-keys:read"],
          expiresAt: new Date(Date.now() + 60_000).toISOString(),
        },
        "too-short",
      ),
    ).rejects.toThrow(/between five minutes and 90 days/);
    await expect(
      client.rotateApiKey(
        "key_previous",
        { expiresAt: new Date(Date.now() + 91 * 86_400_000).toISOString() },
        "too-long",
      ),
    ).rejects.toThrow(/between five minutes and 90 days/);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

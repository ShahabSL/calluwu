import {
  ApiErrorSchema,
  type ApiKeyCredentialResponse,
  ApiKeyCredentialResponseSchema,
  ApiKeyListResponseSchema,
  type ApiKeyMetadata,
  ApiKeyResponseSchema,
  type ApiKeyRotationResponse,
  ApiKeyRotationResponseSchema,
  type CreateApiKeyRequest,
  CreateApiKeyRequestSchema,
  type CreateDeploymentRequest,
  CreateProjectRequestSchema,
  type CreateSessionRequest,
  type Deployment,
  DeploymentListResponseSchema,
  DeploymentResponseSchema,
  type EventEnvelope,
  type EventListResponse,
  EventListResponseSchema,
  type Project,
  ProjectListResponseSchema,
  ProjectResponseSchema,
  type RotateApiKeyRequest,
  RotateApiKeyRequestSchema,
  type Session,
  SessionListResponseSchema,
  SessionMutationResponseSchema,
  SessionResponseSchema,
} from "@calluwu/types";

const DEFAULT_TIMEOUT_MS = 12_000;
const MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
const MIN_MANAGED_API_KEY_LIFETIME_MS = 5 * 60 * 1_000;
const MAX_MANAGED_API_KEY_LIFETIME_MS = 90 * 24 * 60 * 60 * 1_000;

export class CalluwuApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly requestId?: string;

  constructor(message: string, status: number, code: string, requestId?: string) {
    super(message);
    this.name = "CalluwuApiError";
    this.status = status;
    this.code = code;
    if (requestId !== undefined) {
      this.requestId = requestId;
    }
  }
}

export type CalluwuClientOptions = {
  apiUrl: string;
  apiKey: string;
  timeoutMs?: number;
};

export type EventHistory = {
  events: EventEnvelope[];
  truncated: boolean;
  nextAfter: number | null;
};

function normalizeApiUrl(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new TypeError("Calluwu API URL must be an absolute URL");
  }
  const loopback =
    url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "[::1]";
  if (url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) {
    throw new TypeError("Calluwu API URL must use HTTPS (HTTP is allowed only on loopback)");
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new TypeError(
      "Calluwu API URL cannot contain credentials, query parameters, or a fragment",
    );
  }
  return url.toString().replace(/\/+$/, "");
}

function normalizeManagedApiKeyExpiration(expiresAt: string): string {
  const expirationMs = Date.parse(expiresAt);
  const nowMs = Date.now();
  if (
    !Number.isFinite(expirationMs) ||
    expirationMs < nowMs + MIN_MANAGED_API_KEY_LIFETIME_MS ||
    expirationMs > nowMs + MAX_MANAGED_API_KEY_LIFETIME_MS
  ) {
    throw new TypeError(
      "API key expiration must be between five minutes and 90 days in the future",
    );
  }
  return new Date(expirationMs).toISOString();
}

function normalizeIdempotencyKey(value: string): string {
  if (!/^[A-Za-z0-9._:-]{1,160}$/u.test(value)) {
    throw new TypeError(
      "Calluwu idempotency keys must contain 1 to 160 letters, digits, dots, underscores, colons, or hyphens",
    );
  }
  return value;
}

async function readBoundedJson(response: Response): Promise<unknown> {
  const declaredLength = response.headers.get("content-length");
  if (declaredLength !== null) {
    const parsed = Number(declaredLength);
    if (!Number.isSafeInteger(parsed) || parsed < 0 || parsed > MAX_RESPONSE_BYTES) {
      await response.body?.cancel();
      throw new CalluwuApiError(
        "Calluwu API response exceeded the size limit",
        502,
        "response_too_large",
      );
    }
  }
  if (response.body === null) return null;
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const result = await reader.read();
      if (result.done) break;
      total += result.value.byteLength;
      if (total > MAX_RESPONSE_BYTES) {
        await reader.cancel("response_too_large");
        throw new CalluwuApiError(
          "Calluwu API response exceeded the size limit",
          502,
          "response_too_large",
        );
      }
      chunks.push(result.value);
    }
  } finally {
    reader.releaseLock();
  }
  if (total === 0) return null;
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown;
  } catch {
    throw new CalluwuApiError("Calluwu API returned malformed JSON", 502, "invalid_response");
  }
}

function parseResponse<T>(
  schema: { safeParse(value: unknown): { success: true; data: T } | { success: false } },
  value: unknown,
): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) {
    throw new CalluwuApiError(
      "Calluwu API response did not match the platform contract",
      502,
      "invalid_response",
    );
  }
  return parsed.data;
}

export class CalluwuClient {
  readonly apiUrl: string;
  readonly apiKey: string;
  readonly timeoutMs: number;

  constructor(options: CalluwuClientOptions) {
    this.apiUrl = normalizeApiUrl(options.apiUrl);
    this.apiKey = options.apiKey;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    if (!Number.isSafeInteger(this.timeoutMs) || this.timeoutMs < 100 || this.timeoutMs > 120_000) {
      throw new TypeError("Calluwu API timeout must be between 100 and 120000 milliseconds");
    }
  }

  async listProjects(): Promise<Project[]> {
    const value = await this.request("/v1/projects");
    return parseResponse(ProjectListResponseSchema, value).projects;
  }

  async createProject(
    input: { slug: string; name: string },
    idempotencyKey: string,
  ): Promise<Project> {
    const body = CreateProjectRequestSchema.parse(input);
    const value = await this.request("/v1/projects", {
      method: "POST",
      headers: { "Idempotency-Key": normalizeIdempotencyKey(idempotencyKey) },
      body: JSON.stringify(body),
    });
    return parseResponse(ProjectResponseSchema, value).project;
  }

  async createDeployment(
    projectId: string,
    body: CreateDeploymentRequest,
    idempotencyKey: string,
  ): Promise<Deployment> {
    const value = await this.request(`/v1/projects/${encodeURIComponent(projectId)}/deployments`, {
      method: "POST",
      headers: { "Idempotency-Key": normalizeIdempotencyKey(idempotencyKey) },
      body: JSON.stringify(body),
    });
    return parseResponse(DeploymentResponseSchema, value).deployment;
  }

  async listDeployments(projectId: string): Promise<Deployment[]> {
    const value = await this.request(`/v1/projects/${encodeURIComponent(projectId)}/deployments`);
    return parseResponse(DeploymentListResponseSchema, value).deployments;
  }

  async activateDeployment(
    projectId: string,
    deploymentId: string,
    idempotencyKey: string,
  ): Promise<Deployment> {
    const value = await this.request(
      `/v1/projects/${encodeURIComponent(projectId)}/deployments/${encodeURIComponent(deploymentId)}/activate`,
      {
        method: "POST",
        headers: { "Idempotency-Key": normalizeIdempotencyKey(idempotencyKey) },
      },
    );
    return parseResponse(DeploymentResponseSchema, value).deployment;
  }

  async createSession(
    body: CreateSessionRequest,
    idempotencyKey: string,
  ): Promise<{
    session: Session;
    realtime: { url: string; token: string; expiresAt: string; sampleRateHz: number };
  }> {
    const value = await this.request("/v1/sessions", {
      method: "POST",
      headers: { "Idempotency-Key": normalizeIdempotencyKey(idempotencyKey) },
      body: JSON.stringify(body),
    });
    return parseResponse(SessionResponseSchema, value);
  }

  async listSessions(projectId: string): Promise<Session[]> {
    const value = await this.request(`/v1/projects/${encodeURIComponent(projectId)}/sessions`);
    return parseResponse(SessionListResponseSchema, value).sessions;
  }

  async cancelSession(sessionId: string): Promise<Session> {
    const value = await this.request(`/v1/sessions/${encodeURIComponent(sessionId)}/cancel`, {
      method: "POST",
    });
    return parseResponse(SessionMutationResponseSchema, value).session;
  }

  async listSessionEventsPage(
    sessionId: string,
    options: { after?: number; limit?: number } = {},
  ): Promise<EventListResponse> {
    const after = options.after ?? -1;
    const limit = options.limit ?? 25;
    if (!Number.isSafeInteger(after) || after < -1) {
      throw new TypeError("Event cursor must be an integer greater than or equal to -1");
    }
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 25) {
      throw new TypeError("Event page limit must be between 1 and 25");
    }
    const query = new URLSearchParams({ limit: limit.toString() });
    if (after >= 0) query.set("after", after.toString());
    const value = await this.request(
      `/v1/sessions/${encodeURIComponent(sessionId)}/events?${query.toString()}`,
    );
    const page = parseResponse(EventListResponseSchema, value);
    let prior = after;
    for (const event of page.events) {
      if (event.sequence <= prior) {
        throw new CalluwuApiError(
          "Calluwu API returned a non-monotonic event page",
          502,
          "invalid_response",
        );
      }
      prior = event.sequence;
    }
    return page;
  }

  async listSessionEvents(
    sessionId: string,
    options: { maxEvents?: number } = {},
  ): Promise<EventHistory> {
    const maxEvents = options.maxEvents ?? 10_000;
    if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || maxEvents > 100_000) {
      throw new TypeError("Event history limit must be between 1 and 100000");
    }
    const events: EventEnvelope[] = [];
    let after = -1;
    for (;;) {
      const page = await this.listSessionEventsPage(sessionId, {
        after,
        limit: Math.min(25, maxEvents - events.length),
      });
      events.push(...page.events);
      if (!page.hasMore) {
        return { events, truncated: false, nextAfter: page.nextAfter };
      }
      if (events.length >= maxEvents || page.nextAfter === null || page.nextAfter <= after) {
        return { events, truncated: true, nextAfter: page.nextAfter };
      }
      after = page.nextAfter;
    }
  }

  async listApiKeys(limit = 100): Promise<ApiKeyMetadata[]> {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 200) {
      throw new TypeError("API key list limit must be between 1 and 200");
    }
    const value = await this.request(`/v1/api-keys?limit=${limit.toString()}`);
    return parseResponse(ApiKeyListResponseSchema, value).apiKeys;
  }

  async getApiKey(keyId: string): Promise<ApiKeyMetadata> {
    const value = await this.request(`/v1/api-keys/${encodeURIComponent(keyId)}`);
    return parseResponse(ApiKeyResponseSchema, value).apiKey;
  }

  async createApiKey(
    input: CreateApiKeyRequest,
    idempotencyKey: string,
  ): Promise<ApiKeyCredentialResponse> {
    const parsed = CreateApiKeyRequestSchema.parse(input);
    const body = {
      ...parsed,
      expiresAt: normalizeManagedApiKeyExpiration(parsed.expiresAt),
    };
    const value = await this.request("/v1/api-keys", {
      method: "POST",
      headers: { "Idempotency-Key": normalizeIdempotencyKey(idempotencyKey) },
      body: JSON.stringify(body),
    });
    return parseResponse(ApiKeyCredentialResponseSchema, value);
  }

  async rotateApiKey(
    keyId: string,
    input: RotateApiKeyRequest,
    idempotencyKey: string,
  ): Promise<ApiKeyRotationResponse> {
    const parsed = RotateApiKeyRequestSchema.parse(input);
    const body = {
      expiresAt: normalizeManagedApiKeyExpiration(parsed.expiresAt),
    };
    const value = await this.request(`/v1/api-keys/${encodeURIComponent(keyId)}/rotate`, {
      method: "POST",
      headers: { "Idempotency-Key": normalizeIdempotencyKey(idempotencyKey) },
      body: JSON.stringify(body),
    });
    return parseResponse(ApiKeyRotationResponseSchema, value);
  }

  async revokeApiKey(keyId: string): Promise<void> {
    await this.request(
      `/v1/api-keys/${encodeURIComponent(keyId)}`,
      { method: "DELETE" },
      { allowEmpty: true },
    );
  }

  private async request(
    path: string,
    init: RequestInit = {},
    options: { allowEmpty?: boolean } = {},
  ): Promise<unknown> {
    const headers = new Headers(init.headers);
    headers.set("Authorization", `Bearer ${this.apiKey}`);
    headers.set("Accept", "application/json");
    if (init.body !== undefined) {
      headers.set("Content-Type", "application/json");
    }
    const timeout = AbortSignal.timeout(this.timeoutMs);
    const signal = init.signal ? AbortSignal.any([init.signal, timeout]) : timeout;
    let response: Response;
    let payload: unknown;
    try {
      response = await fetch(`${this.apiUrl}${path}`, { ...init, headers, signal });
      payload = await readBoundedJson(response);
    } catch (error) {
      if (error instanceof CalluwuApiError) throw error;
      const timedOut = timeout.aborted;
      throw new CalluwuApiError(
        timedOut ? "Calluwu API request timed out" : "Unable to reach the Calluwu API",
        timedOut ? 408 : 0,
        timedOut ? "request_timeout" : "network_error",
      );
    }
    if (!response.ok) {
      const error = ApiErrorSchema.safeParse(payload);
      throw new CalluwuApiError(
        error.success ? error.data.error.message : `Calluwu API returned HTTP ${response.status}`,
        response.status,
        error.success ? error.data.error.code : "api_error",
        error.success ? error.data.error.requestId : undefined,
      );
    }
    if (payload === null && !options.allowEmpty) {
      throw new CalluwuApiError("Calluwu API returned an empty response", 502, "empty_response");
    }
    return payload;
  }
}

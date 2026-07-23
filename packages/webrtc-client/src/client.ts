import {
  CalluwuWebRtcError,
  type ReconnectCause,
  type RemoteTrackEvent,
  type RemoteTrackReference,
  type WebRtcActivationResult,
  type WebRtcClientEventListener,
  type WebRtcClientEventMap,
  type WebRtcClientEventName,
  type WebRtcClientOptions,
  type WebRtcClientState,
  type WebRtcConnectionSnapshot,
  type WebRtcConnectOptions,
  type WebRtcControlPlaneGrant,
  type WebRtcOperationOptions,
  type WebRtcScheduler,
} from "./types.js";
import {
  assertTrackName,
  endpoint,
  parseNegotiationResponse,
  parsePublishResponse,
  parseSession,
  parseTracksResponse,
  type TracksResponseValue,
  trackKey,
  validateActivationResult,
  validateGrant,
  validateRemoteTrack,
} from "./validation.js";

const MAX_RESPONSE_BYTES = 1024 * 1024;
const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_CONNECTION_TIMEOUT_MS = 12_000;
const DEFAULT_TOKEN_REFRESH_SKEW_MS = 15_000;
const DEFAULT_MAXIMUM_REMOTE_TRACKS = 64;
const DEFAULT_RECONNECT_ATTEMPTS = 3;
const DEFAULT_RECONNECT_BASE_DELAY_MS = 250;
const DEFAULT_RECONNECT_MAXIMUM_DELAY_MS = 4_000;
const DEFAULT_DISCONNECTED_GRACE_MS = 2_000;
const MAX_TIMER_DELAY_MS = 2_147_000_000;

interface ActivePublication {
  readonly mid: string;
  readonly trackName: string;
}

interface ActiveSubscription {
  readonly source: RemoteTrackReference;
  readonly mid: string;
  track: MediaStreamTrack | null;
}

interface CombinedSignal {
  readonly signal: AbortSignal;
  cleanup(): void;
}

function browserScheduler(): WebRtcScheduler {
  return {
    setTimeout: (callback, delayMs) => globalThis.setTimeout(callback, delayMs),
    clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
  };
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  const result = value ?? fallback;
  if (!Number.isInteger(result) || result <= 0) {
    throw new CalluwuWebRtcError("invalid_configuration", `${name} must be a positive integer`);
  }
  return result;
}

function nonNegativeInteger(value: number | undefined, fallback: number, name: string): number {
  const result = value ?? fallback;
  if (!Number.isInteger(result) || result < 0) {
    throw new CalluwuWebRtcError("invalid_configuration", `${name} must be a non-negative integer`);
  }
  return result;
}

function aborted(cause?: unknown): CalluwuWebRtcError {
  return new CalluwuWebRtcError("operation_aborted", "The WebRTC operation was aborted", {
    ...(cause === undefined ? {} : { cause }),
  });
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw aborted(signal.reason);
}

function combineSignals(signals: readonly (AbortSignal | undefined)[]): CombinedSignal {
  const controller = new AbortController();
  const active = signals.filter((signal): signal is AbortSignal => signal !== undefined);
  const cleanups: Array<() => void> = [];
  for (const signal of active) {
    if (signal.aborted) {
      controller.abort(signal.reason);
      break;
    }
    const onAbort = () => controller.abort(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    cleanups.push(() => signal.removeEventListener("abort", onAbort));
  }
  return {
    signal: controller.signal,
    cleanup(): void {
      for (const cleanup of cleanups) cleanup();
    },
  };
}

function normalizeError(
  error: unknown,
  fallbackCode: "connection_failed" | "control_plane_request_failed" | "media_access_failed",
  fallbackMessage: string,
  signal?: AbortSignal,
): CalluwuWebRtcError {
  if (error instanceof CalluwuWebRtcError) return error;
  if (signal?.aborted === true) return aborted(signal.reason);
  return new CalluwuWebRtcError(fallbackCode, fallbackMessage, { cause: error });
}

function deduplicateTracks(tracks: readonly RemoteTrackReference[]): RemoteTrackReference[] {
  const result = new Map<string, RemoteTrackReference>();
  for (const raw of tracks) {
    const track = validateRemoteTrack(raw);
    result.set(trackKey(track), track);
  }
  return [...result.values()];
}

function assertNoSfuError(value: unknown): void {
  if (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    typeof Reflect.get(value, "errorCode") === "string"
  ) {
    throw new CalluwuWebRtcError(
      "control_plane_response_invalid",
      "Cloudflare Realtime rejected renegotiation",
    );
  }
}

function parseSessionCleanupStatus(value: unknown): "closed" | "reconcile" {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new CalluwuWebRtcError(
      "control_plane_response_invalid",
      "The media edge returned an invalid cleanup result",
    );
  }
  const status = Reflect.get(value, "status");
  const failedResources = Reflect.get(value, "failedResources");
  if (
    (status !== "closed" && status !== "reconcile") ||
    !Array.isArray(failedResources) ||
    failedResources.length > 64 ||
    failedResources.some((failure) => typeof failure !== "string" || failure.length > 256)
  ) {
    throw new CalluwuWebRtcError(
      "control_plane_response_invalid",
      "The media edge returned an invalid cleanup result",
    );
  }
  return status;
}

async function readBoundedResponse(response: Response): Promise<string> {
  if (response.body === null) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > MAX_RESPONSE_BYTES) {
        await reader.cancel();
        throw new CalluwuWebRtcError(
          "control_plane_response_invalid",
          "The media edge response was too large",
        );
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch (error) {
    throw new CalluwuWebRtcError(
      "control_plane_response_invalid",
      "The media edge returned invalid UTF-8",
      { cause: error },
    );
  }
}

export class CalluwuWebRtcClient {
  readonly #authorize: WebRtcClientOptions["authorize"];
  readonly #activate: WebRtcClientOptions["activate"];
  readonly #fetch: typeof globalThis.fetch;
  readonly #mediaDevices: Pick<MediaDevices, "getUserMedia">;
  readonly #peerConnectionFactory: (configuration: RTCConfiguration) => RTCPeerConnection;
  readonly #rtcConfiguration: RTCConfiguration;
  readonly #audioConstraints: MediaTrackConstraints;
  readonly #localTrackName: NonNullable<WebRtcClientOptions["localTrackName"]>;
  readonly #requestTimeoutMs: number;
  readonly #connectionTimeoutMs: number;
  readonly #tokenRefreshSkewMs: number;
  readonly #maximumRemoteTracks: number;
  readonly #maximumReconnectAttempts: number;
  readonly #reconnectBaseDelayMs: number;
  readonly #reconnectMaximumDelayMs: number;
  readonly #disconnectedGraceMs: number;
  readonly #scheduler: WebRtcScheduler;
  readonly #random: () => number;
  readonly #now: () => number;
  readonly #listeners = new Map<WebRtcClientEventName, Set<(event: never) => void>>();
  readonly #desiredRemoteTracks = new Map<string, RemoteTrackReference>();
  readonly #automaticRemoteTracks = new Map<string, RemoteTrackReference>();
  readonly #subscriptionsByMid = new Map<string, ActiveSubscription>();

  #state: WebRtcClientState = "idle";
  #grant: WebRtcControlPlaneGrant | null = null;
  #peer: RTCPeerConnection | null = null;
  #mediaStream: MediaStream | null = null;
  #microphoneTrack: MediaStreamTrack | null = null;
  #sfuSessionId: string | null = null;
  #publication: ActivePublication | null = null;
  #reconnectAttempt = 0;
  #epoch = 0;
  #lifecycleController: AbortController | null = null;
  #connectPromise: Promise<WebRtcConnectionSnapshot> | null = null;
  #reconnectPromise: Promise<void> | null = null;
  #negotiationTail: Promise<void> = Promise.resolve();
  #disconnectedTimer: unknown = null;
  #tokenRefreshTimer: unknown = null;

  constructor(options: WebRtcClientOptions) {
    if (typeof options.authorize !== "function") {
      throw new CalluwuWebRtcError(
        "invalid_configuration",
        "An authorization callback is required",
      );
    }
    this.#authorize = options.authorize;
    this.#activate = options.activate;
    this.#fetch =
      options.fetch ??
      ((input, init) => {
        if (typeof globalThis.fetch !== "function") {
          throw new CalluwuWebRtcError("invalid_configuration", "The Fetch API is unavailable");
        }
        return globalThis.fetch(input, init);
      });
    this.#mediaDevices =
      options.mediaDevices ??
      ({
        getUserMedia: (constraints: MediaStreamConstraints) => {
          if (globalThis.navigator?.mediaDevices === undefined) {
            throw new CalluwuWebRtcError(
              "invalid_configuration",
              "Browser media devices are unavailable",
            );
          }
          return globalThis.navigator.mediaDevices.getUserMedia(constraints);
        },
      } satisfies Pick<MediaDevices, "getUserMedia">);
    this.#peerConnectionFactory =
      options.peerConnectionFactory ??
      ((configuration) => {
        if (typeof globalThis.RTCPeerConnection !== "function") {
          throw new CalluwuWebRtcError("invalid_configuration", "RTCPeerConnection is unavailable");
        }
        return new globalThis.RTCPeerConnection(configuration);
      });
    this.#rtcConfiguration = {
      ...options.rtcConfiguration,
      bundlePolicy: options.rtcConfiguration?.bundlePolicy ?? "max-bundle",
      iceServers: options.rtcConfiguration?.iceServers ?? [
        { urls: "stun:stun.cloudflare.com:3478" },
      ],
    };
    this.#audioConstraints = {
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
      channelCount: 1,
      ...options.audioConstraints,
    };
    this.#localTrackName = options.localTrackName ?? ((participantId) => `mic_${participantId}`);
    if (typeof this.#localTrackName === "string") assertTrackName(this.#localTrackName);
    this.#requestTimeoutMs = positiveInteger(
      options.requestTimeoutMs,
      DEFAULT_REQUEST_TIMEOUT_MS,
      "requestTimeoutMs",
    );
    this.#connectionTimeoutMs = positiveInteger(
      options.connectionTimeoutMs,
      DEFAULT_CONNECTION_TIMEOUT_MS,
      "connectionTimeoutMs",
    );
    this.#tokenRefreshSkewMs = nonNegativeInteger(
      options.tokenRefreshSkewMs,
      DEFAULT_TOKEN_REFRESH_SKEW_MS,
      "tokenRefreshSkewMs",
    );
    this.#maximumRemoteTracks = positiveInteger(
      options.maximumRemoteTracks,
      DEFAULT_MAXIMUM_REMOTE_TRACKS,
      "maximumRemoteTracks",
    );
    if (this.#maximumRemoteTracks > 64) {
      throw new CalluwuWebRtcError(
        "invalid_configuration",
        "maximumRemoteTracks cannot exceed the media edge limit of 64",
      );
    }
    this.#maximumReconnectAttempts = positiveInteger(
      options.reconnect?.maximumAttempts,
      DEFAULT_RECONNECT_ATTEMPTS,
      "reconnect.maximumAttempts",
    );
    this.#reconnectBaseDelayMs = nonNegativeInteger(
      options.reconnect?.baseDelayMs,
      DEFAULT_RECONNECT_BASE_DELAY_MS,
      "reconnect.baseDelayMs",
    );
    this.#reconnectMaximumDelayMs = nonNegativeInteger(
      options.reconnect?.maximumDelayMs,
      DEFAULT_RECONNECT_MAXIMUM_DELAY_MS,
      "reconnect.maximumDelayMs",
    );
    if (this.#reconnectMaximumDelayMs < this.#reconnectBaseDelayMs) {
      throw new CalluwuWebRtcError(
        "invalid_configuration",
        "The maximum reconnect delay cannot be smaller than the base delay",
      );
    }
    this.#disconnectedGraceMs = nonNegativeInteger(
      options.reconnect?.disconnectedGraceMs,
      DEFAULT_DISCONNECTED_GRACE_MS,
      "reconnect.disconnectedGraceMs",
    );
    this.#scheduler = options.scheduler ?? browserScheduler();
    this.#random = options.random ?? Math.random;
    this.#now = options.now ?? Date.now;
  }

  get state(): WebRtcClientState {
    return this.#state;
  }

  get snapshot(): WebRtcConnectionSnapshot {
    return {
      state: this.#state,
      roomId: this.#grant?.roomId ?? null,
      participantId: this.#grant?.participantId ?? null,
      sfuSessionId: this.#sfuSessionId,
      localTrackName: this.#publication?.trackName ?? null,
      localTrackId: this.#microphoneTrack?.id ?? null,
      microphoneEnabled: this.#microphoneTrack?.enabled ?? false,
      remoteTrackCount: this.#subscriptionsByMid.size,
      reconnectAttempt: this.#reconnectAttempt,
    };
  }

  on<K extends WebRtcClientEventName>(
    eventName: K,
    listener: WebRtcClientEventListener<K>,
  ): () => void {
    const listeners = this.#listeners.get(eventName) ?? new Set<(event: never) => void>();
    listeners.add(listener as (event: never) => void);
    this.#listeners.set(eventName, listeners);
    return () => listeners.delete(listener as (event: never) => void);
  }

  connect(options: WebRtcConnectOptions = {}): Promise<WebRtcConnectionSnapshot> {
    if (this.#connectPromise !== null) return this.#connectPromise;
    if (!(["idle", "closed", "failed"] as const).includes(this.#state as never)) {
      return Promise.reject(
        new CalluwuWebRtcError(
          "invalid_state",
          `Cannot connect while the client is ${this.#state}`,
        ),
      );
    }
    const operation = this.#connect(options);
    this.#connectPromise = operation;
    void operation.then(
      () => {
        if (this.#connectPromise === operation) this.#connectPromise = null;
      },
      () => {
        if (this.#connectPromise === operation) this.#connectPromise = null;
      },
    );
    return operation;
  }

  setMicrophoneEnabled(enabled: boolean): void {
    if (typeof enabled !== "boolean") {
      throw new CalluwuWebRtcError("invalid_configuration", "Microphone state must be boolean");
    }
    const microphone = this.#microphoneTrack;
    if (
      microphone === null ||
      microphone.readyState === "ended" ||
      (this.#state !== "connected" && this.#state !== "reconnecting")
    ) {
      throw new CalluwuWebRtcError("invalid_state", "No live microphone is available");
    }
    microphone.enabled = enabled;
  }

  async subscribe(
    remoteTracks: readonly RemoteTrackReference[],
    options: WebRtcOperationOptions = {},
  ): Promise<void> {
    if (this.#state !== "connected" || this.#lifecycleController === null) {
      throw new CalluwuWebRtcError("invalid_state", "Remote tracks require an active connection");
    }
    const tracks = deduplicateTracks(remoteTracks).filter(
      (track) =>
        !this.#desiredRemoteTracks.has(trackKey(track)) &&
        !this.#automaticRemoteTracks.has(trackKey(track)),
    );
    if (tracks.length === 0) return;
    if (
      this.#desiredRemoteTracks.size + this.#automaticRemoteTracks.size + tracks.length >
      this.#maximumRemoteTracks
    ) {
      throw new CalluwuWebRtcError("invalid_configuration", "The remote track limit was exceeded");
    }
    const combined = combineSignals([this.#lifecycleController.signal, options.signal]);
    try {
      await this.#serializeNegotiation(() => this.#subscribeTracks(tracks, combined.signal));
      for (const track of tracks) this.#desiredRemoteTracks.set(trackKey(track), track);
    } finally {
      combined.cleanup();
    }
  }

  async unsubscribe(
    remoteTracks: readonly RemoteTrackReference[],
    options: WebRtcOperationOptions = {},
  ): Promise<void> {
    if (this.#state !== "connected" || this.#lifecycleController === null) {
      throw new CalluwuWebRtcError("invalid_state", "Remote tracks require an active connection");
    }
    const requested = deduplicateTracks(remoteTracks).filter((track) =>
      this.#desiredRemoteTracks.has(trackKey(track)),
    );
    if (requested.length === 0) return;
    const combined = combineSignals([this.#lifecycleController.signal, options.signal]);
    try {
      await this.#serializeNegotiation(() => this.#closeRemoteTracks(requested, combined.signal));
      const keys = new Set(requested.map(trackKey));
      for (const key of keys) this.#desiredRemoteTracks.delete(key);
      for (const [mid, subscription] of this.#subscriptionsByMid) {
        if (keys.has(trackKey(subscription.source))) {
          subscription.track?.stop();
          this.#subscriptionsByMid.delete(mid);
        }
      }
    } finally {
      combined.cleanup();
    }
  }

  async close(options: WebRtcOperationOptions = {}): Promise<void> {
    if (this.#state === "closed" || this.#state === "idle") {
      this.#setState("closed");
      return;
    }
    this.#setState("closing");
    this.#lifecycleController?.abort(aborted());
    await this.#teardownTransport(true, true, options.signal);
    this.#lifecycleController = null;
    this.#desiredRemoteTracks.clear();
    this.#automaticRemoteTracks.clear();
    this.#reconnectAttempt = 0;
    this.#setState("closed");
    this.#emit("closed", this.snapshot);
  }

  async #connect(options: WebRtcConnectOptions): Promise<WebRtcConnectionSnapshot> {
    await this.#teardownTransport(true, true);
    this.#desiredRemoteTracks.clear();
    this.#automaticRemoteTracks.clear();
    this.#reconnectAttempt = 0;
    const lifecycle = new AbortController();
    this.#lifecycleController = lifecycle;
    const combined = combineSignals([lifecycle.signal, options.signal]);
    try {
      throwIfAborted(combined.signal);
      this.#setState("authorizing");
      const grant = await this.#getGrant("connect", 0, null, combined.signal);
      const requestedTracks = deduplicateTracks([
        ...(grant.remoteTracks ?? []),
        ...(options.remoteTracks ?? []),
      ]);
      if (requestedTracks.length > this.#maximumRemoteTracks) {
        throw new CalluwuWebRtcError(
          "invalid_configuration",
          "The remote track limit was exceeded",
        );
      }
      for (const track of requestedTracks) this.#desiredRemoteTracks.set(trackKey(track), track);

      this.#setState("acquiring-media");
      const audioConstraints: MediaTrackConstraints = {
        ...this.#audioConstraints,
        ...(options.deviceId === undefined ? {} : { deviceId: { exact: options.deviceId } }),
      };
      let media: MediaStream;
      try {
        media = await this.#mediaDevices.getUserMedia({ audio: audioConstraints, video: false });
      } catch (error) {
        throw normalizeError(
          error,
          "media_access_failed",
          "Microphone access failed",
          combined.signal,
        );
      }
      if (combined.signal.aborted) {
        for (const track of media.getTracks()) track.stop();
        throw aborted(combined.signal.reason);
      }
      const audioTracks = media.getAudioTracks();
      const microphone = audioTracks[0];
      if (microphone === undefined) {
        for (const track of media.getTracks()) track.stop();
        throw new CalluwuWebRtcError("media_access_failed", "No microphone track was provided");
      }
      for (const extraTrack of audioTracks.slice(1)) extraTrack.stop();
      this.#mediaStream = media;
      this.#microphoneTrack = microphone;

      this.#setState("connecting");
      await this.#startTransport(grant, requestedTracks, combined.signal);
      this.#setState("connected");
      this.#scheduleTokenRefresh();
      return this.snapshot;
    } catch (error) {
      const normalized = normalizeError(
        error,
        "connection_failed",
        "The WebRTC connection failed",
        combined.signal,
      );
      await this.#teardownTransport(true, true);
      this.#lifecycleController = null;
      this.#desiredRemoteTracks.clear();
      this.#automaticRemoteTracks.clear();
      this.#setState(normalized.code === "operation_aborted" ? "closed" : "failed");
      if (normalized.code !== "operation_aborted") this.#emitError(normalized, false);
      throw normalized;
    } finally {
      combined.cleanup();
    }
  }

  async #getGrant(
    reason: "connect" | "reconnect",
    attempt: number,
    previousSessionId: string | null,
    signal: AbortSignal,
  ): Promise<WebRtcControlPlaneGrant> {
    throwIfAborted(signal);
    let raw: WebRtcControlPlaneGrant;
    try {
      raw = await this.#authorize({
        reason,
        attempt,
        previousSessionId: previousSessionId ?? undefined,
        signal,
      });
    } catch (error) {
      if (signal.aborted) throw aborted(signal.reason);
      throw new CalluwuWebRtcError(
        "control_plane_request_failed",
        "The control plane could not authorize media",
        { cause: error },
      );
    }
    throwIfAborted(signal);
    return validateGrant(raw, this.#now(), this.#requestTimeoutMs);
  }

  async #startTransport(
    grant: WebRtcControlPlaneGrant,
    remoteTracks: readonly RemoteTrackReference[],
    signal: AbortSignal,
  ): Promise<void> {
    throwIfAborted(signal);
    this.#grant = grant;
    const peer = this.#peerConnectionFactory(this.#rtcConfiguration);
    const epoch = ++this.#epoch;
    this.#peer = peer;
    this.#installPeerHandlers(peer, epoch);

    const sessionId = parseSession(
      await this.#request(grant, "/v1/webrtc/sessions", "POST", {}, signal),
    );
    this.#sfuSessionId = sessionId;
    const microphone = this.#microphoneTrack;
    if (microphone === null || microphone.readyState === "ended") {
      throw new CalluwuWebRtcError("media_access_failed", "The microphone track is no longer live");
    }
    const transceiver = peer.addTransceiver(microphone, { direction: "sendonly" });
    const offer = await peer.createOffer();
    await peer.setLocalDescription(offer);
    throwIfAborted(signal);
    if (transceiver.mid === null) {
      throw new CalluwuWebRtcError(
        "connection_failed",
        "The browser did not assign a media transceiver identifier",
      );
    }
    const localTrackName =
      typeof this.#localTrackName === "function"
        ? this.#localTrackName(grant.participantId, microphone)
        : this.#localTrackName;
    assertTrackName(localTrackName);
    const localDescription = peer.localDescription ?? offer;
    if (localDescription.type !== "offer" || !localDescription.sdp) {
      throw new CalluwuWebRtcError("connection_failed", "The browser did not create an SDP offer");
    }
    const answer = parsePublishResponse(
      await this.#request(
        grant,
        `/v1/webrtc/sessions/${encodeURIComponent(sessionId)}/tracks`,
        "POST",
        {
          sessionDescription: { type: "offer", sdp: localDescription.sdp },
          tracks: [{ location: "local", mid: transceiver.mid, trackName: localTrackName }],
        },
        signal,
      ),
    );
    this.#publication = { mid: transceiver.mid, trackName: localTrackName };
    await peer.setRemoteDescription(answer);
    await this.#waitUntilConnected(peer, signal);
    const activatedTracks = await this.#activateRuntimeBridge(
      grant,
      sessionId,
      localTrackName,
      signal,
    );
    this.#automaticRemoteTracks.clear();
    for (const track of activatedTracks) {
      this.#automaticRemoteTracks.set(trackKey(track), track);
    }
    const allRemoteTracks = deduplicateTracks([...remoteTracks, ...activatedTracks]);
    if (allRemoteTracks.length > this.#maximumRemoteTracks) {
      throw new CalluwuWebRtcError("invalid_configuration", "The remote track limit was exceeded");
    }
    if (allRemoteTracks.length > 0) {
      await this.#serializeNegotiation(() => this.#subscribeTracks(allRemoteTracks, signal));
    }
  }

  async #activateRuntimeBridge(
    grant: WebRtcControlPlaneGrant,
    sfuSessionId: string,
    trackName: string,
    signal: AbortSignal,
  ): Promise<readonly RemoteTrackReference[]> {
    const activate = this.#activate;
    if (activate === undefined) return [];
    throwIfAborted(signal);

    const timeoutController = new AbortController();
    let didTimeout = false;
    const timeout = this.#scheduler.setTimeout(() => {
      didTimeout = true;
      timeoutController.abort();
    }, this.#requestTimeoutMs);
    const combined = combineSignals([signal, timeoutController.signal]);
    let abortListener: (() => void) | undefined;
    try {
      const abortedOperation = new Promise<never>((_resolve, reject) => {
        abortListener = () => {
          reject(
            didTimeout
              ? new CalluwuWebRtcError("request_timeout", "Runtime-bridge activation timed out")
              : aborted(combined.signal.reason),
          );
        };
        combined.signal.addEventListener("abort", abortListener, { once: true });
      });
      let raw: WebRtcActivationResult;
      try {
        raw = await Promise.race([
          Promise.resolve().then(() =>
            activate({
              roomId: grant.roomId,
              participantId: grant.participantId,
              sfuSessionId,
              trackName,
              signal: combined.signal,
            }),
          ),
          abortedOperation,
        ]);
      } catch (error) {
        if (error instanceof CalluwuWebRtcError) throw error;
        if (signal.aborted) throw aborted(signal.reason);
        throw new CalluwuWebRtcError(
          "media_activation_failed",
          "The control plane could not activate the runtime media bridge",
          { cause: error },
        );
      }
      throwIfAborted(combined.signal);
      return validateActivationResult(raw, this.#maximumRemoteTracks).remoteTracks;
    } finally {
      if (abortListener !== undefined) {
        combined.signal.removeEventListener("abort", abortListener);
      }
      this.#scheduler.clearTimeout(timeout);
      combined.cleanup();
    }
  }

  async #subscribeTracks(
    remoteTracks: readonly RemoteTrackReference[],
    signal: AbortSignal,
  ): Promise<void> {
    const peer = this.#requirePeer();
    const grant = this.#requireGrant();
    const sessionId = this.#requireSessionId();
    const response = parseTracksResponse(
      await this.#request(
        grant,
        `/v1/webrtc/sessions/${encodeURIComponent(sessionId)}/tracks`,
        "POST",
        {
          tracks: remoteTracks.map((track) => ({ location: "remote", ...track })),
        },
        signal,
      ),
    );
    this.#registerSubscriptions(remoteTracks, response);
    try {
      await this.#applyServerNegotiation(peer, grant, sessionId, response, signal);
    } catch (error) {
      const requestedKeys = new Set(remoteTracks.map(trackKey));
      for (const [mid, subscription] of this.#subscriptionsByMid) {
        if (requestedKeys.has(trackKey(subscription.source))) this.#subscriptionsByMid.delete(mid);
      }
      throw error;
    }
  }

  #registerSubscriptions(
    requested: readonly RemoteTrackReference[],
    response: TracksResponseValue,
  ): void {
    const requestedKeys = new Set(requested.map(trackKey));
    const returnedKeys = new Set<string>();
    const subscriptions: ActiveSubscription[] = [];
    for (const track of response.tracks) {
      const key = trackKey(track);
      if (!requestedKeys.has(key)) {
        throw new CalluwuWebRtcError(
          "control_plane_response_invalid",
          "The media edge returned an unauthorized track",
        );
      }
      returnedKeys.add(key);
      subscriptions.push({
        source: { sessionId: track.sessionId, trackName: track.trackName },
        mid: track.mid,
        track: null,
      });
    }
    if ([...requestedKeys].some((key) => !returnedKeys.has(key))) {
      throw new CalluwuWebRtcError(
        "control_plane_response_invalid",
        "The media edge omitted requested track metadata",
      );
    }
    for (const subscription of subscriptions) {
      this.#subscriptionsByMid.set(subscription.mid, subscription);
    }
  }

  async #applyServerNegotiation(
    peer: RTCPeerConnection,
    grant: WebRtcControlPlaneGrant,
    sessionId: string,
    response: TracksResponseValue,
    signal: AbortSignal,
  ): Promise<void> {
    if (!response.requiresImmediateRenegotiation && response.sessionDescription === undefined)
      return;
    if (response.sessionDescription?.type !== "offer") {
      throw new CalluwuWebRtcError(
        "control_plane_response_invalid",
        "The media edge did not return the required renegotiation offer",
      );
    }
    await peer.setRemoteDescription(response.sessionDescription);
    const answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);
    throwIfAborted(signal);
    const localDescription = peer.localDescription ?? answer;
    if (localDescription.type !== "answer" || !localDescription.sdp) {
      throw new CalluwuWebRtcError(
        "connection_failed",
        "The browser did not create a renegotiation answer",
      );
    }
    const result = await this.#request(
      grant,
      `/v1/webrtc/sessions/${encodeURIComponent(sessionId)}/renegotiate`,
      "PUT",
      { sessionDescription: { type: "answer", sdp: localDescription.sdp } },
      signal,
    );
    assertNoSfuError(result);
  }

  async #closeRemoteTracks(
    remoteTracks: readonly RemoteTrackReference[],
    signal: AbortSignal,
  ): Promise<void> {
    const grant = this.#requireGrant();
    const sessionId = this.#requireSessionId();
    const response = parseNegotiationResponse(
      await this.#request(
        grant,
        `/v1/webrtc/sessions/${encodeURIComponent(sessionId)}/tracks/close`,
        "PUT",
        { tracks: remoteTracks },
        signal,
      ),
    );
    await this.#applyServerNegotiation(this.#requirePeer(), grant, sessionId, response, signal);
  }

  async #request(
    grant: WebRtcControlPlaneGrant,
    path: string,
    method: "POST" | "PUT",
    body: unknown,
    signal: AbortSignal,
  ): Promise<unknown> {
    throwIfAborted(signal);
    const timeoutController = new AbortController();
    let didTimeout = false;
    const timeout = this.#scheduler.setTimeout(() => {
      didTimeout = true;
      timeoutController.abort();
    }, this.#requestTimeoutMs);
    const combined = combineSignals([signal, timeoutController.signal]);
    try {
      let response: Response;
      try {
        response = await this.#fetch(endpoint(grant.baseUrl, path), {
          method,
          cache: "no-store",
          credentials: "omit",
          headers: {
            authorization: `Bearer ${grant.token}`,
            "content-type": "application/json",
          },
          body: JSON.stringify(body),
          redirect: "error",
          referrerPolicy: "no-referrer",
          signal: combined.signal,
        });
      } catch (error) {
        if (didTimeout) {
          throw new CalluwuWebRtcError("request_timeout", "The media edge request timed out", {
            cause: error,
          });
        }
        if (signal.aborted) throw aborted(signal.reason);
        throw new CalluwuWebRtcError(
          "control_plane_request_failed",
          "The media edge request failed",
          { cause: error },
        );
      }
      const declaredLength = Number(response.headers.get("content-length") ?? "0");
      if (Number.isFinite(declaredLength) && declaredLength > MAX_RESPONSE_BYTES) {
        await response.body?.cancel();
        throw new CalluwuWebRtcError(
          "control_plane_response_invalid",
          "The media edge response was too large",
        );
      }
      const text = await readBoundedResponse(response);
      let value: unknown;
      try {
        value = JSON.parse(text) as unknown;
      } catch (error) {
        throw new CalluwuWebRtcError(
          "control_plane_response_invalid",
          "The media edge returned invalid JSON",
          { cause: error },
        );
      }
      if (!response.ok) {
        const requestId = response.headers.get("x-request-id") ?? undefined;
        throw new CalluwuWebRtcError(
          "control_plane_request_failed",
          `The media edge rejected the request with status ${response.status.toString()}`,
          {
            status: response.status,
            ...(requestId === undefined ? {} : { requestId }),
          },
        );
      }
      return value;
    } finally {
      this.#scheduler.clearTimeout(timeout);
      combined.cleanup();
    }
  }

  async #waitUntilConnected(peer: RTCPeerConnection, signal: AbortSignal): Promise<void> {
    if (this.#peerConnected(peer)) return;
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const timeout = this.#scheduler.setTimeout(() => {
        finish(
          new CalluwuWebRtcError(
            "connection_timeout",
            "The WebRTC connection did not become ready in time",
          ),
        );
      }, this.#connectionTimeoutMs);
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        this.#scheduler.clearTimeout(timeout);
        peer.removeEventListener("connectionstatechange", onStateChange);
        peer.removeEventListener("iceconnectionstatechange", onStateChange);
        signal.removeEventListener("abort", onAbort);
        if (error === undefined) resolve();
        else reject(error);
      };
      const onAbort = () => finish(aborted(signal.reason));
      const onStateChange = () => {
        if (this.#peerConnected(peer)) finish();
        else if (peer.connectionState === "failed" || peer.iceConnectionState === "failed") {
          finish(new CalluwuWebRtcError("connection_failed", "The WebRTC handshake failed"));
        }
      };
      peer.addEventListener("connectionstatechange", onStateChange);
      peer.addEventListener("iceconnectionstatechange", onStateChange);
      signal.addEventListener("abort", onAbort, { once: true });
      onStateChange();
    });
  }

  #peerConnected(peer: RTCPeerConnection): boolean {
    return (
      peer.connectionState === "connected" ||
      peer.iceConnectionState === "connected" ||
      peer.iceConnectionState === "completed"
    );
  }

  #installPeerHandlers(peer: RTCPeerConnection, epoch: number): void {
    peer.addEventListener("track", (event) => {
      if (this.#epoch !== epoch || this.#peer !== peer) {
        (event as RTCTrackEvent).track.stop();
        return;
      }
      const trackEvent = event as RTCTrackEvent;
      const mid = trackEvent.transceiver.mid;
      const subscription = mid === null ? undefined : this.#subscriptionsByMid.get(mid);
      if (subscription === undefined) {
        trackEvent.track.stop();
        return;
      }
      subscription.track = trackEvent.track;
      const payload: RemoteTrackEvent = {
        source: subscription.source,
        mid: subscription.mid,
        track: trackEvent.track,
        streams: [...trackEvent.streams],
      };
      this.#emit("remotetrack", payload);
    });
    const onStateChange = () => {
      if (this.#epoch !== epoch || this.#peer !== peer || this.#state !== "connected") return;
      if (peer.connectionState === "failed") {
        this.#triggerReconnect("connection_failed");
      } else if (peer.iceConnectionState === "failed") {
        this.#triggerReconnect("ice_failed");
      } else if (
        peer.connectionState === "disconnected" ||
        peer.iceConnectionState === "disconnected"
      ) {
        this.#scheduleDisconnectedReconnect(peer, epoch);
      } else if (this.#peerConnected(peer)) {
        this.#clearDisconnectedTimer();
      }
    };
    peer.addEventListener("connectionstatechange", onStateChange);
    peer.addEventListener("iceconnectionstatechange", onStateChange);
  }

  #scheduleDisconnectedReconnect(peer: RTCPeerConnection, epoch: number): void {
    if (this.#disconnectedTimer !== null) return;
    this.#disconnectedTimer = this.#scheduler.setTimeout(() => {
      this.#disconnectedTimer = null;
      if (
        this.#epoch === epoch &&
        this.#peer === peer &&
        this.#state === "connected" &&
        (peer.connectionState === "disconnected" || peer.iceConnectionState === "disconnected")
      ) {
        this.#triggerReconnect("connection_disconnected");
      }
    }, this.#disconnectedGraceMs);
  }

  #triggerReconnect(cause: ReconnectCause): void {
    if (this.#state !== "connected" || this.#reconnectPromise !== null) return;
    const reconnect = this.#reconnect(cause);
    this.#reconnectPromise = reconnect;
    void reconnect.then(
      () => {
        if (this.#reconnectPromise === reconnect) this.#reconnectPromise = null;
      },
      () => {
        if (this.#reconnectPromise === reconnect) this.#reconnectPromise = null;
      },
    );
  }

  async #reconnect(cause: ReconnectCause): Promise<void> {
    const lifecycle = this.#lifecycleController;
    if (lifecycle === null || lifecycle.signal.aborted) return;
    this.#clearTimers();
    this.#setState("reconnecting");
    let previousSessionId = this.#sfuSessionId;
    let finalError: CalluwuWebRtcError | null = null;
    for (let attempt = 1; attempt <= this.#maximumReconnectAttempts; attempt += 1) {
      if (lifecycle.signal.aborted) return;
      this.#reconnectAttempt = attempt;
      const delayMs = this.#reconnectDelay(attempt);
      this.#emit("reconnecting", {
        attempt,
        cause,
        delayMs,
        previousSessionId,
      });
      try {
        await this.#delay(delayMs, lifecycle.signal);
        await this.#teardownTransport(false, true, lifecycle.signal);
        const grant = await this.#getGrant(
          "reconnect",
          attempt,
          previousSessionId,
          lifecycle.signal,
        );
        const tracks = deduplicateTracks([
          ...this.#desiredRemoteTracks.values(),
          ...(grant.remoteTracks ?? []),
        ]);
        if (tracks.length > this.#maximumRemoteTracks) {
          throw new CalluwuWebRtcError(
            "invalid_configuration",
            "The remote track limit was exceeded",
          );
        }
        for (const track of tracks) this.#desiredRemoteTracks.set(trackKey(track), track);
        await this.#startTransport(grant, tracks, lifecycle.signal);
        const sessionId = this.#requireSessionId();
        this.#setState("connected");
        this.#scheduleTokenRefresh();
        this.#emit("reconnected", {
          attempt,
          cause,
          previousSessionId,
          sessionId,
        });
        this.#reconnectAttempt = 0;
        return;
      } catch (error) {
        if (lifecycle.signal.aborted) return;
        finalError = normalizeError(
          error,
          "connection_failed",
          "A WebRTC reconnect attempt failed",
          lifecycle.signal,
        );
        this.#emitError(finalError, attempt < this.#maximumReconnectAttempts);
        previousSessionId = this.#sfuSessionId ?? previousSessionId;
        await this.#teardownTransport(false, true, lifecycle.signal);
      }
    }
    for (const track of this.#mediaStream?.getTracks() ?? []) track.stop();
    this.#mediaStream = null;
    this.#microphoneTrack = null;
    this.#lifecycleController = null;
    this.#automaticRemoteTracks.clear();
    const exhausted = new CalluwuWebRtcError(
      "reconnect_exhausted",
      "WebRTC reconnect attempts were exhausted",
      { ...(finalError === null ? {} : { cause: finalError }) },
    );
    this.#setState("failed");
    this.#emitError(exhausted, false);
  }

  #reconnectDelay(attempt: number): number {
    const exponential = Math.min(
      this.#reconnectMaximumDelayMs,
      this.#reconnectBaseDelayMs * 2 ** (attempt - 1),
    );
    const random = Math.max(0, Math.min(1, this.#random()));
    return Math.round(exponential * (0.8 + random * 0.4));
  }

  async #delay(delayMs: number, signal: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    if (delayMs === 0) return;
    await new Promise<void>((resolve, reject) => {
      const handle = this.#scheduler.setTimeout(() => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      }, delayMs);
      const onAbort = () => {
        this.#scheduler.clearTimeout(handle);
        reject(aborted(signal.reason));
      };
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  async #teardownTransport(
    stopMedia: boolean,
    closeRemote: boolean,
    signal?: AbortSignal,
  ): Promise<void> {
    this.#clearTimers();
    const peer = this.#peer;
    const grant = this.#grant;
    const sessionId = this.#sfuSessionId;
    const subscriptions = [...this.#subscriptionsByMid.values()];
    let cleanupError: CalluwuWebRtcError | null = null;
    if (closeRemote && grant !== null && sessionId !== null) {
      const cleanupSignal = signal ?? new AbortController().signal;
      try {
        const status = parseSessionCleanupStatus(
          await this.#request(
            grant,
            `/v1/webrtc/sessions/${encodeURIComponent(sessionId)}/close`,
            "POST",
            {},
            cleanupSignal,
          ),
        );
        if (status === "reconcile") {
          cleanupError = new CalluwuWebRtcError(
            "control_plane_request_failed",
            "The media edge accepted transport cleanup for reconciliation",
          );
        }
      } catch (error) {
        cleanupError = normalizeError(
          error,
          "control_plane_request_failed",
          "The media edge could not confirm transport cleanup",
          cleanupSignal,
        );
      }
    }
    // Persist server-owned cleanup while the PeerConnection is still live, then release browser
    // state. Local teardown remains final on a control-plane failure, but the error is observable.
    this.#peer = null;
    this.#grant = null;
    this.#sfuSessionId = null;
    this.#publication = null;
    this.#subscriptionsByMid.clear();
    this.#epoch += 1;
    peer?.close();
    for (const subscription of subscriptions) subscription.track?.stop();
    if (stopMedia) {
      for (const track of this.#mediaStream?.getTracks() ?? []) track.stop();
      this.#mediaStream = null;
      this.#microphoneTrack = null;
    }
    if (cleanupError !== null) this.#emitError(cleanupError, true);
  }

  #scheduleTokenRefresh(): void {
    this.#clearTokenRefreshTimer();
    const grant = this.#grant;
    if (grant === null) return;
    const expiresAt = Date.parse(grant.expiresAt);
    const delay = Math.min(
      MAX_TIMER_DELAY_MS,
      Math.max(1_000, expiresAt - this.#now() - this.#tokenRefreshSkewMs),
    );
    this.#tokenRefreshTimer = this.#scheduler.setTimeout(() => {
      this.#tokenRefreshTimer = null;
      this.#triggerReconnect("token_expiring");
    }, delay);
  }

  #serializeNegotiation<T>(operation: () => Promise<T>): Promise<T> {
    const current = this.#negotiationTail.then(operation, operation);
    this.#negotiationTail = current.then(
      () => undefined,
      () => undefined,
    );
    return current;
  }

  #requireGrant(): WebRtcControlPlaneGrant {
    if (this.#grant === null) {
      throw new CalluwuWebRtcError("invalid_state", "No media authorization is active");
    }
    return this.#grant;
  }

  #requirePeer(): RTCPeerConnection {
    if (this.#peer === null) {
      throw new CalluwuWebRtcError("invalid_state", "No peer connection is active");
    }
    return this.#peer;
  }

  #requireSessionId(): string {
    if (this.#sfuSessionId === null) {
      throw new CalluwuWebRtcError("invalid_state", "No SFU session is active");
    }
    return this.#sfuSessionId;
  }

  #clearDisconnectedTimer(): void {
    if (this.#disconnectedTimer !== null) {
      this.#scheduler.clearTimeout(this.#disconnectedTimer);
      this.#disconnectedTimer = null;
    }
  }

  #clearTokenRefreshTimer(): void {
    if (this.#tokenRefreshTimer !== null) {
      this.#scheduler.clearTimeout(this.#tokenRefreshTimer);
      this.#tokenRefreshTimer = null;
    }
  }

  #clearTimers(): void {
    this.#clearDisconnectedTimer();
    this.#clearTokenRefreshTimer();
  }

  #setState(state: WebRtcClientState): void {
    if (this.#state === state) return;
    const previous = this.#state;
    this.#state = state;
    this.#emit("statechange", { previous, current: state, snapshot: this.snapshot });
  }

  #emitError(error: CalluwuWebRtcError, recoverable: boolean): void {
    this.#emit("error", { error, recoverable });
  }

  #emit<K extends WebRtcClientEventName>(eventName: K, event: WebRtcClientEventMap[K]): void {
    const listeners = this.#listeners.get(eventName);
    if (listeners === undefined) return;
    for (const listener of [...listeners]) {
      try {
        listener(event as never);
      } catch (error) {
        const reporter = Reflect.get(globalThis, "reportError");
        if (typeof reporter === "function") Reflect.apply(reporter, globalThis, [error]);
      }
    }
  }
}

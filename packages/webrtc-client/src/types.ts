export type WebRtcClientState =
  | "idle"
  | "authorizing"
  | "acquiring-media"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "closing"
  | "closed"
  | "failed";

export type ReconnectCause =
  | "connection_failed"
  | "ice_failed"
  | "connection_disconnected"
  | "token_expiring";

export interface RemoteTrackReference {
  readonly sessionId: string;
  readonly trackName: string;
}

/**
 * Ephemeral browser grant returned by the application control plane.
 *
 * The client holds this value only in memory for the current transport. It is
 * never exposed through snapshots or events and is cleared during teardown.
 */
export interface WebRtcControlPlaneGrant {
  readonly baseUrl: string;
  readonly roomId: string;
  readonly participantId: string;
  readonly token: string;
  readonly expiresAt: string;
  readonly remoteTracks?: readonly RemoteTrackReference[];
}

export interface WebRtcAuthorizationRequest {
  readonly reason: "connect" | "reconnect";
  readonly attempt: number;
  readonly previousSessionId: string | undefined;
  readonly signal: AbortSignal;
}

export type WebRtcControlPlane = (
  request: WebRtcAuthorizationRequest,
) => Promise<WebRtcControlPlaneGrant>;

/**
 * Activates the server-side bridge after the browser microphone has been
 * published to an SFU session. The callback normally calls Calluwu's
 * `/media/runtime-bridge` control-plane endpoint and returns the runtime's
 * published audio track.
 */
export interface WebRtcActivationRequest {
  readonly roomId: string;
  readonly participantId: string;
  readonly sfuSessionId: string;
  readonly trackName: string;
  readonly signal: AbortSignal;
}

export interface WebRtcActivationResult {
  readonly remoteTracks: readonly RemoteTrackReference[];
}

export type WebRtcActivationControlPlane = (
  request: WebRtcActivationRequest,
) => Promise<WebRtcActivationResult>;

export interface WebRtcConnectionSnapshot {
  readonly state: WebRtcClientState;
  readonly roomId: string | null;
  readonly participantId: string | null;
  readonly sfuSessionId: string | null;
  readonly localTrackName: string | null;
  readonly localTrackId: string | null;
  readonly microphoneEnabled: boolean;
  readonly remoteTrackCount: number;
  readonly reconnectAttempt: number;
}

export interface RemoteTrackEvent {
  readonly source: RemoteTrackReference;
  readonly mid: string;
  readonly track: MediaStreamTrack;
  readonly streams: readonly MediaStream[];
}

export interface StateChangeEvent {
  readonly previous: WebRtcClientState;
  readonly current: WebRtcClientState;
  readonly snapshot: WebRtcConnectionSnapshot;
}

export interface ReconnectingEvent {
  readonly attempt: number;
  readonly cause: ReconnectCause;
  readonly delayMs: number;
  readonly previousSessionId: string | null;
}

export interface ReconnectedEvent {
  readonly attempt: number;
  readonly cause: ReconnectCause;
  readonly previousSessionId: string | null;
  readonly sessionId: string;
}

export interface ClientErrorEvent {
  readonly error: CalluwuWebRtcError;
  readonly recoverable: boolean;
}

export interface WebRtcClientEventMap {
  readonly statechange: StateChangeEvent;
  readonly remotetrack: RemoteTrackEvent;
  readonly reconnecting: ReconnectingEvent;
  readonly reconnected: ReconnectedEvent;
  readonly error: ClientErrorEvent;
  readonly closed: WebRtcConnectionSnapshot;
}

export type WebRtcClientEventName = keyof WebRtcClientEventMap;
export type WebRtcClientEventListener<K extends WebRtcClientEventName> = (
  event: WebRtcClientEventMap[K],
) => void;

export interface WebRtcReconnectOptions {
  readonly maximumAttempts?: number;
  readonly baseDelayMs?: number;
  readonly maximumDelayMs?: number;
  readonly disconnectedGraceMs?: number;
}

export interface WebRtcScheduler {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

export interface WebRtcClientOptions {
  readonly authorize: WebRtcControlPlane;
  readonly activate?: WebRtcActivationControlPlane;
  readonly fetch?: typeof globalThis.fetch;
  readonly mediaDevices?: Pick<MediaDevices, "getUserMedia">;
  readonly peerConnectionFactory?: (configuration: RTCConfiguration) => RTCPeerConnection;
  readonly rtcConfiguration?: RTCConfiguration;
  readonly audioConstraints?: MediaTrackConstraints;
  readonly localTrackName?: string | ((participantId: string, track: MediaStreamTrack) => string);
  readonly reconnect?: WebRtcReconnectOptions;
  readonly requestTimeoutMs?: number;
  readonly connectionTimeoutMs?: number;
  readonly tokenRefreshSkewMs?: number;
  readonly maximumRemoteTracks?: number;
  readonly scheduler?: WebRtcScheduler;
  readonly random?: () => number;
  readonly now?: () => number;
}

export interface WebRtcConnectOptions {
  readonly deviceId?: string;
  readonly remoteTracks?: readonly RemoteTrackReference[];
  readonly signal?: AbortSignal;
}

export interface WebRtcOperationOptions {
  readonly signal?: AbortSignal;
}

export type CalluwuWebRtcErrorCode =
  | "invalid_configuration"
  | "invalid_control_plane_grant"
  | "invalid_state"
  | "media_access_failed"
  | "control_plane_request_failed"
  | "control_plane_response_invalid"
  | "media_activation_failed"
  | "request_timeout"
  | "connection_timeout"
  | "connection_failed"
  | "operation_aborted"
  | "reconnect_exhausted";

export class CalluwuWebRtcError extends Error {
  readonly code: CalluwuWebRtcErrorCode;
  readonly status: number | undefined;
  readonly requestId: string | undefined;

  constructor(
    code: CalluwuWebRtcErrorCode,
    message: string,
    options: { cause?: unknown; status?: number; requestId?: string } = {},
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "CalluwuWebRtcError";
    this.code = code;
    this.status = options.status;
    this.requestId = options.requestId;
  }
}

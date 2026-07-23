import {
  CalluwuWebRtcError,
  type RemoteTrackReference,
  type WebRtcActivationResult,
  type WebRtcControlPlaneGrant,
} from "./types.js";

const SFU_ID = /^[A-Za-z0-9_-]{8,160}$/;
const TOKEN = /^[A-Za-z0-9._~-]{16,4096}$/;
const MAX_SDP_BYTES = 512 * 1024;

export interface SessionDescriptionValue {
  readonly type: "offer" | "answer";
  readonly sdp: string;
}

export interface TrackResponseValue extends RemoteTrackReference {
  readonly mid: string;
}

export interface TracksResponseValue {
  readonly requiresImmediateRenegotiation: boolean;
  readonly sessionDescription: SessionDescriptionValue | undefined;
  readonly tracks: readonly TrackResponseValue[];
}

function hasControlCharacter(value: string): boolean {
  for (const character of value) {
    const codePoint = character.codePointAt(0);
    if (codePoint !== undefined && (codePoint <= 31 || codePoint === 127)) return true;
  }
  return false;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function invalid(message: string): never {
  throw new CalluwuWebRtcError("control_plane_response_invalid", message);
}

function isTrackName(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length >= 1 &&
    value.length <= 160 &&
    !hasControlCharacter(value)
  );
}

export function assertTrackName(value: unknown): asserts value is string {
  if (!isTrackName(value)) {
    throw new CalluwuWebRtcError("invalid_configuration", "A media track name is invalid");
  }
}

function parseRemoteTrack(
  value: unknown,
  code: "invalid_configuration" | "invalid_control_plane_grant" | "control_plane_response_invalid",
): RemoteTrackReference {
  if (
    !isRecord(value) ||
    typeof value.sessionId !== "string" ||
    !SFU_ID.test(value.sessionId) ||
    !isTrackName(value.trackName)
  ) {
    throw new CalluwuWebRtcError(code, "A remote media track reference is invalid");
  }
  return { sessionId: value.sessionId, trackName: value.trackName };
}

export function validateRemoteTrack(value: unknown): RemoteTrackReference {
  return parseRemoteTrack(value, "invalid_configuration");
}

export function validateActivationResult(
  value: unknown,
  maximumRemoteTracks: number,
): WebRtcActivationResult {
  if (
    !isRecord(value) ||
    Object.keys(value).some((key) => key !== "remoteTracks") ||
    !Array.isArray(value.remoteTracks) ||
    value.remoteTracks.length < 1 ||
    value.remoteTracks.length > maximumRemoteTracks
  ) {
    throw new CalluwuWebRtcError(
      "control_plane_response_invalid",
      "The control plane returned an invalid runtime-bridge activation",
    );
  }
  return {
    remoteTracks: value.remoteTracks.map((track) =>
      parseRemoteTrack(track, "control_plane_response_invalid"),
    ),
  };
}

export function validateGrant(
  value: unknown,
  now: number,
  requestTimeoutMs: number,
): WebRtcControlPlaneGrant {
  if (
    !isRecord(value) ||
    typeof value.baseUrl !== "string" ||
    typeof value.roomId !== "string" ||
    typeof value.participantId !== "string" ||
    typeof value.token !== "string" ||
    typeof value.expiresAt !== "string" ||
    (value.remoteTracks !== undefined && !Array.isArray(value.remoteTracks))
  ) {
    throw new CalluwuWebRtcError(
      "invalid_control_plane_grant",
      "The control plane returned an invalid participant grant",
    );
  }
  let base: URL;
  try {
    base = new URL(value.baseUrl);
  } catch (cause) {
    throw new CalluwuWebRtcError("invalid_control_plane_grant", "The media edge URL is invalid", {
      cause,
    });
  }
  const localDevelopment =
    base.protocol === "http:" &&
    (base.hostname === "localhost" || base.hostname === "127.0.0.1" || base.hostname === "[::1]");
  if (
    (base.protocol !== "https:" && !localDevelopment) ||
    base.username !== "" ||
    base.password !== "" ||
    base.search !== "" ||
    base.hash !== ""
  ) {
    throw new CalluwuWebRtcError(
      "invalid_control_plane_grant",
      "The media edge URL must be credential-free HTTPS",
    );
  }
  if (
    !/^room_[A-Za-z0-9_-]{8,75}$/.test(value.roomId) ||
    !/^part_[A-Za-z0-9_-]{8,75}$/.test(value.participantId)
  ) {
    throw new CalluwuWebRtcError(
      "invalid_control_plane_grant",
      "The room or participant identity is invalid",
    );
  }
  if (!TOKEN.test(value.token)) {
    throw new CalluwuWebRtcError("invalid_control_plane_grant", "The participant grant is invalid");
  }
  const expiresAt = Date.parse(value.expiresAt);
  if (!Number.isFinite(expiresAt) || expiresAt <= now + requestTimeoutMs) {
    throw new CalluwuWebRtcError(
      "invalid_control_plane_grant",
      "The participant grant expires too soon",
    );
  }
  const remoteTracks = value.remoteTracks?.map((track) =>
    parseRemoteTrack(track, "invalid_control_plane_grant"),
  );
  return {
    baseUrl: base.toString().replace(/\/$/, ""),
    roomId: value.roomId,
    participantId: value.participantId,
    token: value.token,
    expiresAt: new Date(expiresAt).toISOString(),
    ...(remoteTracks === undefined ? {} : { remoteTracks }),
  };
}

export function parseSession(value: unknown): string {
  if (!isRecord(value) || typeof value.sessionId !== "string" || !SFU_ID.test(value.sessionId)) {
    return invalid("The media edge returned an invalid SFU session");
  }
  return value.sessionId;
}

function parseDescription(value: unknown): SessionDescriptionValue | undefined {
  if (value === undefined) return undefined;
  if (
    !isRecord(value) ||
    (value.type !== "offer" && value.type !== "answer") ||
    typeof value.sdp !== "string" ||
    value.sdp.length < 1 ||
    value.sdp.length > MAX_SDP_BYTES
  ) {
    return invalid("The media edge returned an invalid session description");
  }
  return { type: value.type, sdp: value.sdp };
}

export function parseTracksResponse(value: unknown): TracksResponseValue {
  if (!isRecord(value)) return invalid("The media edge returned an invalid track response");
  if (typeof value.errorCode === "string") {
    return invalid("Cloudflare Realtime rejected the track operation");
  }
  const rawTracks = value.tracks ?? [];
  if (!Array.isArray(rawTracks) || rawTracks.length > 64) {
    return invalid("The media edge returned an invalid track response");
  }
  const tracks = rawTracks.map((track): TrackResponseValue => {
    if (
      !isRecord(track) ||
      typeof track.mid !== "string" ||
      track.mid.length < 1 ||
      track.mid.length > 160 ||
      typeof track.sessionId !== "string" ||
      !SFU_ID.test(track.sessionId) ||
      typeof track.trackName !== "string"
    ) {
      return invalid("The media edge returned invalid track metadata");
    }
    if (typeof track.errorCode === "string") {
      return invalid("Cloudflare Realtime rejected a requested track");
    }
    if (!isTrackName(track.trackName)) {
      return invalid("The media edge returned invalid track metadata");
    }
    return { mid: track.mid, sessionId: track.sessionId, trackName: track.trackName };
  });
  return {
    requiresImmediateRenegotiation: value.requiresImmediateRenegotiation === true,
    sessionDescription: parseDescription(value.sessionDescription),
    tracks,
  };
}

export function parseNegotiationResponse(value: unknown): TracksResponseValue {
  if (!isRecord(value)) return invalid("The media edge returned an invalid track response");
  if (typeof value.errorCode === "string") {
    return invalid("Cloudflare Realtime rejected the track operation");
  }
  return {
    requiresImmediateRenegotiation: value.requiresImmediateRenegotiation === true,
    sessionDescription: parseDescription(value.sessionDescription),
    tracks: [],
  };
}

export function parsePublishResponse(value: unknown): SessionDescriptionValue {
  if (!isRecord(value)) return invalid("The media edge returned an invalid publication response");
  if (typeof value.errorCode === "string") {
    return invalid("Cloudflare Realtime rejected microphone publication");
  }
  const description = parseDescription(value.sessionDescription);
  if (description?.type !== "answer") {
    return invalid("The media edge did not return the publication answer");
  }
  return description;
}

export function endpoint(baseUrl: string, path: string): string {
  const base = new URL(baseUrl);
  const prefix = base.pathname === "/" ? "" : base.pathname.replace(/\/$/, "");
  base.pathname = `${prefix}${path}`;
  base.search = "";
  base.hash = "";
  return base.toString();
}

export function trackKey(track: RemoteTrackReference): string {
  return `${track.sessionId}\u0000${track.trackName}`;
}

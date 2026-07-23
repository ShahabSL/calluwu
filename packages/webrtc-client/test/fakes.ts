import type { RemoteTrackReference, WebRtcControlPlaneGrant } from "../src/index.js";

export class FakeMediaTrack {
  readonly kind = "audio";
  readonly id: string;
  enabled = true;
  readyState: MediaStreamTrackState = "live";
  stopCalls = 0;

  constructor(id = "microphone-track") {
    this.id = id;
  }

  stop(): void {
    this.stopCalls += 1;
    this.readyState = "ended";
  }

  asMediaStreamTrack(): MediaStreamTrack {
    return this as unknown as MediaStreamTrack;
  }
}

export class FakeMediaStream {
  readonly track: FakeMediaTrack;

  constructor(track = new FakeMediaTrack()) {
    this.track = track;
  }

  getTracks(): MediaStreamTrack[] {
    return [this.track.asMediaStreamTrack()];
  }

  getAudioTracks(): MediaStreamTrack[] {
    return this.getTracks();
  }

  asMediaStream(): MediaStream {
    return this as unknown as MediaStream;
  }
}

type Listener = EventListenerOrEventListenerObject;

export class FakePeerConnection {
  connectionState: RTCPeerConnectionState = "new";
  iceConnectionState: RTCIceConnectionState = "new";
  signalingState: RTCSignalingState = "stable";
  iceGatheringState: RTCIceGatheringState = "complete";
  localDescription: RTCSessionDescription | null = null;
  remoteDescription: RTCSessionDescription | null = null;
  currentLocalDescription: RTCSessionDescription | null = null;
  currentRemoteDescription: RTCSessionDescription | null = null;
  pendingLocalDescription: RTCSessionDescription | null = null;
  pendingRemoteDescription: RTCSessionDescription | null = null;
  readonly configuration: RTCConfiguration;
  readonly transceivers: Array<RTCRtpTransceiver & { mid: string | null }> = [];
  readonly listeners = new Map<string, Set<Listener>>();
  closed = false;
  nextRemoteMid = "remote-mid";
  nextRemoteTrack = new FakeMediaTrack("remote-audio");

  constructor(configuration: RTCConfiguration) {
    this.configuration = configuration;
  }

  addEventListener(type: string, callback: Listener | null): void {
    if (callback === null) return;
    const listeners = this.listeners.get(type) ?? new Set<Listener>();
    listeners.add(callback);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, callback: Listener | null): void {
    if (callback !== null) this.listeners.get(type)?.delete(callback);
  }

  dispatch(type: string, event: Event = { type } as Event): void {
    for (const listener of [...(this.listeners.get(type) ?? [])]) {
      if (typeof listener === "function") listener.call(this, event);
      else listener.handleEvent(event);
    }
  }

  addTransceiver(
    track: MediaStreamTrack | string,
    _init?: RTCRtpTransceiverInit,
  ): RTCRtpTransceiver {
    const mediaTrack = typeof track === "string" ? null : track;
    const transceiver = {
      mid: null,
      sender: { track: mediaTrack },
      receiver: { track: this.nextRemoteTrack.asMediaStreamTrack() },
      direction: "sendonly",
      currentDirection: null,
      stopped: false,
      setCodecPreferences: () => undefined,
      stop: () => undefined,
    } as unknown as RTCRtpTransceiver & { mid: string | null };
    this.transceivers.push(transceiver);
    return transceiver;
  }

  async createOffer(): Promise<RTCSessionDescriptionInit> {
    return { type: "offer", sdp: "v=0\r\na=calluwu-offer\r\n" };
  }

  async createAnswer(): Promise<RTCSessionDescriptionInit> {
    return { type: "answer", sdp: "v=0\r\na=calluwu-answer\r\n" };
  }

  async setLocalDescription(description?: RTCLocalSessionDescriptionInit): Promise<void> {
    if (description === undefined) return;
    this.localDescription = description as RTCSessionDescription;
    this.currentLocalDescription = this.localDescription;
    if (description.type === "offer") {
      for (const [index, transceiver] of this.transceivers.entries()) {
        transceiver.mid = index.toString();
      }
    }
    this.signalingState = description.type === "offer" ? "have-local-offer" : "stable";
  }

  async setRemoteDescription(description: RTCSessionDescriptionInit): Promise<void> {
    this.remoteDescription = description as RTCSessionDescription;
    this.currentRemoteDescription = this.remoteDescription;
    if (description.type === "answer") {
      this.signalingState = "stable";
      this.connectionState = "connected";
      this.iceConnectionState = "connected";
      this.dispatch("iceconnectionstatechange");
      this.dispatch("connectionstatechange");
      return;
    }
    this.signalingState = "have-remote-offer";
    const remoteTransceiver = {
      mid: this.nextRemoteMid,
      receiver: { track: this.nextRemoteTrack.asMediaStreamTrack() },
      sender: { track: null },
      direction: "recvonly",
      currentDirection: "recvonly",
      stopped: false,
      setCodecPreferences: () => undefined,
      stop: () => undefined,
    } as unknown as RTCRtpTransceiver;
    this.dispatch("track", {
      type: "track",
      track: this.nextRemoteTrack.asMediaStreamTrack(),
      transceiver: remoteTransceiver,
      receiver: remoteTransceiver.receiver,
      streams: [],
    } as unknown as RTCTrackEvent);
  }

  fail(): void {
    this.connectionState = "failed";
    this.iceConnectionState = "failed";
    this.dispatch("connectionstatechange");
    this.dispatch("iceconnectionstatechange");
  }

  disconnect(): void {
    this.connectionState = "disconnected";
    this.iceConnectionState = "disconnected";
    this.dispatch("connectionstatechange");
  }

  close(): void {
    this.closed = true;
    this.connectionState = "closed";
    this.iceConnectionState = "closed";
    this.signalingState = "closed";
  }

  asPeerConnection(): RTCPeerConnection {
    return this as unknown as RTCPeerConnection;
  }
}

export interface RecordedRequest {
  readonly url: URL;
  readonly method: string;
  readonly authorization: string | null;
  readonly body: unknown;
  readonly peerClosed: boolean | undefined;
}

export class MediaEdgeHarness {
  readonly requests: RecordedRequest[] = [];
  readonly peers: FakePeerConnection[];
  sessionCounter = 0;
  failSessionCreationAfter = Number.POSITIVE_INFINITY;
  failSessionCleanup = false;
  reconcileSessionCleanup = false;
  failTrackCleanup = false;

  constructor(peers: FakePeerConnection[]) {
    this.peers = peers;
  }

  readonly fetch: typeof globalThis.fetch = async (input, init) => {
    const url = new URL(
      typeof input === "string" ? input : input instanceof URL ? input : input.url,
    );
    const body = init?.body === undefined ? undefined : JSON.parse(String(init.body));
    const headers = new Headers(init?.headers);
    this.requests.push({
      url,
      method: init?.method ?? "GET",
      authorization: headers.get("authorization"),
      body,
      peerClosed: this.peers.at(-1)?.closed,
    });
    if (url.pathname.endsWith("/v1/webrtc/sessions")) {
      this.sessionCounter += 1;
      if (this.sessionCounter > this.failSessionCreationAfter) {
        return Response.json({ error: { code: "upstream_unavailable" } }, { status: 503 });
      }
      return Response.json(
        { sessionId: `session000${this.sessionCounter.toString()}` },
        { status: 201 },
      );
    }
    if (url.pathname.endsWith("/tracks") && init?.method === "POST") {
      if (body?.sessionDescription !== undefined) {
        return Response.json({
          sessionDescription: { type: "answer", sdp: "v=0\r\na=sfu-answer\r\n" },
        });
      }
      const remoteTracks = body?.tracks as RemoteTrackReference[];
      const peer = this.peers.at(-1);
      if (peer !== undefined) peer.nextRemoteMid = "remote-mid";
      return Response.json({
        requiresImmediateRenegotiation: true,
        sessionDescription: { type: "offer", sdp: "v=0\r\na=sfu-offer\r\n" },
        tracks: remoteTracks.map((track, index) => ({
          ...track,
          mid: index === 0 ? "remote-mid" : `remote-mid-${index.toString()}`,
        })),
      });
    }
    if (url.pathname.endsWith("/renegotiate")) return Response.json({});
    if (/\/v1\/webrtc\/sessions\/[^/]+\/close$/.test(url.pathname)) {
      if (this.failSessionCleanup) {
        return Response.json({ error: { code: "upstream_unavailable" } }, { status: 503 });
      }
      if (this.reconcileSessionCleanup) {
        return Response.json(
          { status: "reconcile", failedResources: ["tracks:still_open"] },
          { status: 202 },
        );
      }
      return Response.json({ status: "closed", failedResources: [] });
    }
    if (url.pathname.endsWith("/tracks/close")) {
      if (this.failTrackCleanup) {
        return Response.json({ error: { code: "upstream_unavailable" } }, { status: 503 });
      }
      return Response.json({ tracks: [] });
    }
    return Response.json({ error: { code: "not_found" } }, { status: 404 });
  };
}

export function grant(
  token: string,
  remoteTracks: readonly RemoteTrackReference[] = [],
): WebRtcControlPlaneGrant {
  return {
    baseUrl: "https://media.example.test",
    roomId: "room_abcdefgh",
    participantId: "part_abcdefgh",
    token,
    expiresAt: "2030-01-01T00:00:00.000Z",
    remoteTracks,
  };
}

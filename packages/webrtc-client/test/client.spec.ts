import { describe, expect, it, vi } from "vitest";
import {
  CalluwuWebRtcClient,
  type CalluwuWebRtcError,
  type WebRtcAuthorizationRequest,
  type WebRtcClientOptions,
} from "../src/index.js";
import { FakeMediaStream, FakePeerConnection, grant, MediaEdgeHarness } from "./fakes.js";

const TOKEN_ONE = "token.one.abcdefghijklmno";
const TOKEN_TWO = "token.two.abcdefghijklmno";
const REMOTE_TRACK = { sessionId: "remote0001", trackName: "agent-audio" } as const;

function fixture(overrides: Partial<WebRtcClientOptions> = {}): {
  client: CalluwuWebRtcClient;
  media: FakeMediaStream;
  getUserMedia: ReturnType<typeof vi.fn>;
  peers: FakePeerConnection[];
  harness: MediaEdgeHarness;
  authorizationRequests: WebRtcAuthorizationRequest[];
} {
  const peers: FakePeerConnection[] = [];
  const harness = new MediaEdgeHarness(peers);
  const media = new FakeMediaStream();
  const getUserMedia = vi.fn(async () => media.asMediaStream());
  const authorizationRequests: WebRtcAuthorizationRequest[] = [];
  const client = new CalluwuWebRtcClient({
    authorize: async (request) => {
      authorizationRequests.push(request);
      return grant(request.reason === "connect" ? TOKEN_ONE : TOKEN_TWO);
    },
    fetch: harness.fetch,
    mediaDevices: { getUserMedia },
    peerConnectionFactory: (configuration) => {
      const peer = new FakePeerConnection(configuration);
      peers.push(peer);
      return peer.asPeerConnection();
    },
    reconnect: {
      maximumAttempts: 2,
      baseDelayMs: 0,
      maximumDelayMs: 0,
      disconnectedGraceMs: 0,
    },
    random: () => 0.5,
    now: () => Date.parse("2029-01-01T00:00:00.000Z"),
    ...overrides,
  });
  return { client, media, getUserMedia, peers, harness, authorizationRequests };
}

describe("CalluwuWebRtcClient", () => {
  it("acquires the microphone only on connect and publishes a named audio track", async () => {
    const { client, getUserMedia, peers, harness } = fixture();

    expect(getUserMedia).not.toHaveBeenCalled();
    const snapshot = await client.connect({ deviceId: "preferred-microphone" });

    expect(getUserMedia).toHaveBeenCalledOnce();
    expect(getUserMedia).toHaveBeenCalledWith({
      audio: expect.objectContaining({
        deviceId: { exact: "preferred-microphone" },
        echoCancellation: true,
        noiseSuppression: true,
        channelCount: 1,
      }),
      video: false,
    });
    expect(snapshot).toMatchObject({
      state: "connected",
      sfuSessionId: "session0001",
      localTrackName: "mic_part_abcdefgh",
      localTrackId: "microphone-track",
    });
    expect(JSON.stringify(snapshot)).not.toContain(TOKEN_ONE);
    expect(peers[0]?.configuration).toMatchObject({
      bundlePolicy: "max-bundle",
      iceServers: [{ urls: "stun:stun.cloudflare.com:3478" }],
    });
    const publication = harness.requests.find(
      (request) =>
        request.url.pathname.endsWith("/tracks") &&
        typeof request.body === "object" &&
        request.body !== null &&
        "sessionDescription" in request.body,
    );
    expect(publication?.authorization).toBe(`Bearer ${TOKEN_ONE}`);
    expect(publication?.body).toMatchObject({
      sessionDescription: { type: "offer" },
      tracks: [
        {
          location: "local",
          mid: "0",
          trackName: "mic_part_abcdefgh",
        },
      ],
    });

    await client.close();
  });

  it("subscribes to a remote track and completes server-initiated renegotiation", async () => {
    const { client, harness } = fixture();
    const remoteEvents: string[] = [];
    client.on("remotetrack", (event) => remoteEvents.push(event.source.trackName));
    await client.connect();

    await client.subscribe([REMOTE_TRACK]);

    expect(remoteEvents).toEqual(["agent-audio"]);
    expect(client.snapshot.remoteTrackCount).toBe(1);
    const pull = harness.requests.find((request) => {
      if (
        !request.url.pathname.endsWith("/tracks") ||
        typeof request.body !== "object" ||
        request.body === null ||
        !("tracks" in request.body) ||
        !Array.isArray(request.body.tracks)
      ) {
        return false;
      }
      return request.body.tracks[0]?.location === "remote";
    });
    expect(pull?.body).toEqual({ tracks: [{ location: "remote", ...REMOTE_TRACK }] });
    const renegotiation = harness.requests.find((request) =>
      request.url.pathname.endsWith("/renegotiate"),
    );
    expect(renegotiation?.method).toBe("PUT");
    expect(renegotiation?.body).toMatchObject({
      sessionDescription: { type: "answer" },
    });

    await client.unsubscribe([REMOTE_TRACK]);
    expect(client.snapshot.remoteTrackCount).toBe(0);
    expect(
      harness.requests.find((request) => request.url.pathname.endsWith("/tracks/close")),
    ).toMatchObject({ method: "PUT", body: { tracks: [REMOTE_TRACK] } });
    await client.close();
  });

  it("activates the runtime bridge after publication and automatically subscribes its track", async () => {
    const activationRequests: Array<{
      participantId: string;
      sfuSessionId: string;
      trackName: string;
    }> = [];
    let harness: MediaEdgeHarness | undefined;
    const fixtureValue = fixture({
      activate: async (request) => {
        const currentHarness = harness;
        if (currentHarness === undefined) throw new Error("test harness unavailable");
        const publication = currentHarness.requests.find(
          (entry) =>
            entry.url.pathname.endsWith(`/sessions/${request.sfuSessionId}/tracks`) &&
            typeof entry.body === "object" &&
            entry.body !== null &&
            "sessionDescription" in entry.body,
        );
        expect(publication).toBeDefined();
        expect(
          currentHarness.requests.some(
            (entry) =>
              entry.url.pathname.endsWith(`/sessions/${request.sfuSessionId}/tracks`) &&
              typeof entry.body === "object" &&
              entry.body !== null &&
              "tracks" in entry.body &&
              Array.isArray(entry.body.tracks) &&
              entry.body.tracks[0]?.location === "remote",
          ),
        ).toBe(false);
        activationRequests.push({
          participantId: request.participantId,
          sfuSessionId: request.sfuSessionId,
          trackName: request.trackName,
        });
        return { remoteTracks: [REMOTE_TRACK] };
      },
    });
    harness = fixtureValue.harness;
    const remoteEvents: string[] = [];
    fixtureValue.client.on("remotetrack", ({ source }) => remoteEvents.push(source.trackName));

    const snapshot = await fixtureValue.client.connect();

    expect(activationRequests).toEqual([
      {
        participantId: "part_abcdefgh",
        sfuSessionId: "session0001",
        trackName: "mic_part_abcdefgh",
      },
    ]);
    expect(remoteEvents).toEqual(["agent-audio"]);
    expect(snapshot.remoteTrackCount).toBe(1);

    fixtureValue.client.setMicrophoneEnabled(false);
    expect(fixtureValue.media.track.enabled).toBe(false);
    expect(fixtureValue.client.snapshot.microphoneEnabled).toBe(false);
    fixtureValue.client.setMicrophoneEnabled(true);
    expect(fixtureValue.client.snapshot.microphoneEnabled).toBe(true);

    fixtureValue.peers[0]?.fail();
    await vi.waitFor(() => {
      expect(fixtureValue.client.snapshot.sfuSessionId).toBe("session0002");
      expect(fixtureValue.client.state).toBe("connected");
    });
    expect(activationRequests.map(({ sfuSessionId }) => sfuSessionId)).toEqual([
      "session0001",
      "session0002",
    ]);

    await fixtureValue.client.close();
  });

  it("fails the connection when runtime-bridge activation is malformed", async () => {
    const { client, media } = fixture({
      activate: async () => ({ remoteTracks: [] }),
    });

    await expect(client.connect()).rejects.toMatchObject({
      code: "control_plane_response_invalid",
    } satisfies Partial<CalluwuWebRtcError>);
    expect(media.track.stopCalls).toBeGreaterThan(0);
    expect(client.state).toBe("failed");
  });

  it("creates a fresh SFU session on failure without reacquiring the microphone", async () => {
    const { client, getUserMedia, peers, harness, authorizationRequests } = fixture();
    const reconnected = vi.fn();
    client.on("reconnected", reconnected);
    await client.connect({ remoteTracks: [REMOTE_TRACK] });

    peers[0]?.fail();
    await vi.waitFor(() => {
      expect(client.snapshot.sfuSessionId).toBe("session0002");
      expect(client.state).toBe("connected");
    });

    expect(getUserMedia).toHaveBeenCalledOnce();
    expect(peers).toHaveLength(2);
    expect(peers[0]?.closed).toBe(true);
    expect(
      harness.requests.find((request) =>
        request.url.pathname.endsWith("/sessions/session0001/close"),
      ),
    ).toMatchObject({ method: "POST", body: {}, peerClosed: false });
    expect(authorizationRequests.map(({ reason }) => reason)).toEqual(["connect", "reconnect"]);
    expect(reconnected).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: 1,
        previousSessionId: "session0001",
        sessionId: "session0002",
      }),
    );

    await client.close();
  });

  it("bounds reconnect attempts and stops capture after exhaustion", async () => {
    const { client, media, peers, harness } = fixture();
    const errors: string[] = [];
    client.on("error", ({ error }) => errors.push(error.code));
    await client.connect();
    harness.failSessionCreationAfter = 1;

    peers[0]?.fail();
    await vi.waitFor(() => expect(client.state).toBe("failed"));

    expect(harness.sessionCounter).toBe(3);
    expect(media.track.stopCalls).toBeGreaterThan(0);
    expect(errors.at(-1)).toBe("reconnect_exhausted");
  });

  it("persists server-owned session cleanup before discarding the in-memory grant", async () => {
    const { client, media, peers, harness } = fixture({
      authorize: async () => grant(TOKEN_ONE, [REMOTE_TRACK]),
    });
    await client.connect();

    await client.close();

    expect(client.state).toBe("closed");
    expect(client.snapshot).toMatchObject({
      roomId: null,
      participantId: null,
      sfuSessionId: null,
      localTrackName: null,
      remoteTrackCount: 0,
    });
    expect(media.track.stopCalls).toBeGreaterThan(0);
    expect(peers[0]?.closed).toBe(true);
    const close = harness.requests.filter((request) =>
      request.url.pathname.endsWith("/sessions/session0001/close"),
    );
    expect(close.at(-1)).toMatchObject({ method: "POST", body: {}, peerClosed: false });
  });

  it("finishes local teardown but reports an unconfirmed upstream cleanup", async () => {
    const { client, peers, harness } = fixture();
    const errors: Array<{ code: string; recoverable: boolean }> = [];
    client.on("error", ({ error, recoverable }) => errors.push({ code: error.code, recoverable }));
    await client.connect();
    harness.failSessionCleanup = true;

    await client.close();

    expect(client.state).toBe("closed");
    expect(peers[0]?.closed).toBe(true);
    expect(errors.at(-1)).toEqual({ code: "control_plane_request_failed", recoverable: true });
  });

  it("reports cleanup accepted for durable reconciliation before closing the peer", async () => {
    const { client, peers, harness } = fixture();
    const errors: Array<{ code: string; recoverable: boolean }> = [];
    client.on("error", ({ error, recoverable }) => errors.push({ code: error.code, recoverable }));
    await client.connect();
    harness.reconcileSessionCleanup = true;

    await client.close();

    expect(client.state).toBe("closed");
    expect(peers[0]?.closed).toBe(true);
    expect(
      harness.requests.find((request) =>
        request.url.pathname.endsWith("/sessions/session0001/close"),
      )?.peerClosed,
    ).toBe(false);
    expect(errors.at(-1)).toEqual({ code: "control_plane_request_failed", recoverable: true });
  });

  it("honors AbortSignal and stops media acquired after cancellation", async () => {
    const media = new FakeMediaStream();
    let resolveMedia: ((stream: MediaStream) => void) | undefined;
    const mediaPromise = new Promise<MediaStream>((resolve) => {
      resolveMedia = resolve;
    });
    const { client } = fixture({
      mediaDevices: { getUserMedia: vi.fn(() => mediaPromise) },
    });
    const controller = new AbortController();
    const connecting = client.connect({ signal: controller.signal });
    await vi.waitFor(() => expect(client.state).toBe("acquiring-media"));

    controller.abort();
    resolveMedia?.(media.asMediaStream());

    await expect(connecting).rejects.toMatchObject({
      code: "operation_aborted",
    } satisfies Partial<CalluwuWebRtcError>);
    expect(media.track.stopCalls).toBeGreaterThan(0);
    expect(client.state).toBe("closed");
  });

  it("rejects malformed runtime grants before requesting browser media", async () => {
    const { client, getUserMedia } = fixture({
      authorize: async () => null as never,
    });

    await expect(client.connect()).rejects.toMatchObject({
      code: "invalid_control_plane_grant",
    } satisfies Partial<CalluwuWebRtcError>);
    expect(getUserMedia).not.toHaveBeenCalled();
    expect(client.state).toBe("failed");
  });
});

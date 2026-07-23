# `@calluwu/webrtc-client`

Dependency-free, typed browser signaling for Calluwu's Cloudflare Realtime SFU media edge.

The package deliberately does not call Cloudflare with an application secret. A caller-provided control-plane callback obtains a short-lived Calluwu participant grant; the browser presents that grant only to the media edge. The grant remains in memory for the active transport and is removed during reconnect or close. It is never written to browser storage, included in snapshots, or emitted in events.

## Usage

```ts
import { CalluwuWebRtcClient } from "@calluwu/webrtc-client";

const client = new CalluwuWebRtcClient({
  authorize: async ({ reason, attempt, previousSessionId, signal }) => {
    const response = await fetch(`/api/v1/sessions/${calluwuSessionId}/media/participants`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": `browser-${calluwuSessionId}`,
      },
      body: JSON.stringify({ role: "caller", ttlSeconds: 300 }),
      signal,
    });
    if (!response.ok) throw new Error("Media authorization failed");
    return (await response.json()).media;
  },
  activate: async ({ participantId, sfuSessionId, trackName, signal }) => {
    const response = await fetch(`/api/v1/sessions/${calluwuSessionId}/media/runtime-bridge`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "idempotency-key": `bridge-${sfuSessionId}`,
      },
      body: JSON.stringify({ participantId, sfuSessionId, trackName }),
      signal,
    });
    if (!response.ok) throw new Error("Runtime media activation failed");
    const { media } = await response.json();
    return { remoteTracks: media.remoteTracks };
  },
});

client.on("remotetrack", ({ track, streams }) => {
  const audio = document.querySelector<HTMLAudioElement>("#call-audio");
  if (!audio) return;
  audio.srcObject = streams[0] ?? new MediaStream([track]);
  void audio.play(); // Keep autoplay/user-gesture policy in the product UI.
});

client.on("statechange", ({ current }) => {
  console.log(`call media is ${current}`);
});

// Call from an explicit user gesture. No microphone request occurs before this.
await client.connect({ deviceId: selectedMicrophoneDeviceId });

// Mute without closing the call or renegotiating the peer connection.
client.setMicrophoneEnabled(false);

await client.close();
```

`connect()` requests audio only (`video: false`) and enables echo cancellation, noise suppression, automatic gain control, and mono capture by default. Pass `audioConstraints` to the constructor to override those browser constraints. A per-connect `deviceId` is applied as an exact device constraint.

## Connection lifecycle

The implementation follows the [Cloudflare Realtime SFU Connection API](https://developers.cloudflare.com/realtime/sfu/https-api/):

1. Obtain a room/participant grant from the Calluwu control plane.
2. Create a Calluwu-brokered SFU session (`POST /v1/webrtc/sessions`).
3. Create one `RTCPeerConnection` with Cloudflare's STUN service and `max-bundle` by default.
4. Add the microphone as a named, send-only local track, send the browser SDP offer, and apply the SFU answer.
5. After publication, activate the server-side SFU-to-runtime bridge. The bridge returns the runtime's SFU track; the client subscribes to it automatically, applies the SFU-generated offer, creates a browser answer, and sends that answer to the media-edge renegotiation endpoint.
6. On failed or persistently disconnected ICE/peer state, ask the media edge to durably close the old session before closing the old peer locally, obtain a new grant, create a fresh SFU session and peer connection, and republish/resubscribe. The existing live microphone track is reused, so reconnection does not re-prompt for media permission.

Retries are bounded (three attempts by default) and use capped exponential backoff with jitter. A transient `disconnected` state gets a two-second recovery window; `failed` reconnects immediately through the retry loop. Participant-token expiry also initiates a fresh-session reconnect before expiration. The authorization callback should mint a fresh participant identity when the server does not support extending an existing participant's expiry.

Calluwu exposes `subscribe()` and `unsubscribe()` for additional control-plane-driven presence changes. The automatically activated runtime track is managed by the connection lifecycle and cannot be removed accidentally through `unsubscribe()`. Remote track references are still authorized server-side by the room Durable Object; client input alone cannot pull arbitrary tracks.

## Events and state

`state` and `snapshot` expose only non-secret operational state. Typed events are registered with `on(name, listener)`, which returns an unsubscribe function.

- `statechange`: lifecycle transitions and a non-secret snapshot.
- `remotetrack`: browser `MediaStreamTrack`, source session/name, MID, and streams.
- `reconnecting`: attempt, cause, delay, and previous session ID.
- `reconnected`: successful fresh-session recovery.
- `error`: a `CalluwuWebRtcError` and whether another retry remains.
- `closed`: final non-secret snapshot.

All async public operations accept `AbortSignal` where cancellation is meaningful. `close()` first submits the authenticated server-owned cleanup command while the peer is still live, then stops capture and closes the local peer. A network/control-plane failure cannot prevent local shutdown, but it is emitted as a recoverable `control_plane_request_failed` event. A `202` durable-reconciliation response is surfaced through the same recoverable event so operators can distinguish immediate provider confirmation from deferred cleanup.

## Media-edge contract

The client expects the hosted Calluwu media-edge contract:

| Operation | Method and path | Request |
| --- | --- | --- |
| Create SFU session | `POST /v1/webrtc/sessions` | `{}` |
| Publish/pull tracks | `POST /v1/webrtc/sessions/:id/tracks` | Cloudflare session description and track objects |
| Renegotiate | `PUT /v1/webrtc/sessions/:id/renegotiate` | Browser answer |
| Unsubscribe tracks | `PUT /v1/webrtc/sessions/:id/tracks/close` | Remote session/name references |
| Close SFU session | `POST /v1/webrtc/sessions/:id/close` | `{}` |

Every request uses `Authorization: Bearer <participant capability>`, JSON, and the browser's normal `Origin` header. `baseUrl` must be credential-free HTTPS; plain HTTP is accepted only for loopback development.

Cloudflare Realtime maps one SFU session to one peer connection and exposes track-close rather than a session-delete endpoint. Calluwu's session-close route is therefore the ownership boundary: the room Durable Object persists cleanup intent before provider I/O, fences concurrent attempts, closes every known publication and subscription in provider-sized batches, and retries partial failure by alarm. It returns `202` while reconciliation remains and `200` only after provider-confirmed closure. Runtime adapters have their own lifecycle and are not silently closed by the browser command. See [Sessions and Tracks](https://developers.cloudflare.com/realtime/sfu/sessions-tracks/) and [Realtime limits](https://developers.cloudflare.com/realtime/sfu/limits/).

## Testing

The unit suite injects all nondeterministic browser and network boundaries: Fetch, `getUserMedia`, `RTCPeerConnection`, time, randomness, and (when needed by consumers) the scheduler. It covers explicit media acquisition, device selection, publication SDP, subscription renegotiation, remote delivery, fresh-session reconnect, retry exhaustion, abort, session cleanup ordering, and observable cleanup failure.

```sh
pnpm --filter @calluwu/webrtc-client typecheck
pnpm --filter @calluwu/webrtc-client test
pnpm --filter @calluwu/webrtc-client build
```

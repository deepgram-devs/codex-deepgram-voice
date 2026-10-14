# Codex CLI voice, powered by Deepgram Flux

A small local shim that makes [Deepgram Flux](https://developers.deepgram.com/docs/flux/quickstart) the speech layer for Codex CLI voice mode. Press `F8` in Codex, talk through a task, and Codex starts the turn when Flux reports the end of it. Flux does two things here: it transcribes the microphone audio as you speak, and its end-of-turn detection decides when you have finished a thought, so a pause to think does not cut you off and a finished sentence does not wait on a silence timer.

No Codex fork and no patched binary. Codex lets you point its two realtime voice endpoints at another server. The shim listens on localhost, speaks Codex's realtime protocol, and runs Flux underneath.

This is an unofficial community shim, not affiliated with or endorsed by OpenAI. The two Codex settings it relies on are marked experimental by OpenAI and may change in a future Codex release. A spoken session with a real microphone and the Codex 0.162.1 TUI is recorded under `evidence/`, alongside the checks that drive the shim with a fake Codex client (see [What has been verified](#what-has-been-verified)).

```
Codex CLI (F8) --WebRTC audio--> shim on 127.0.0.1:8765 --16 kHz PCM--> Deepgram Flux
Codex CLI      <--sideband WS--- shim                    <--Update / EndOfTurn--- Flux
                live captions, then the handoff (final transcript) => Codex runs the turn
```

## Prerequisites

- **Codex CLI 0.159 or newer**, installed with `npm install -g @openai/codex` and signed in. Voice mode shipped as a stable feature in 0.159; `codex features list` should show `realtime_conversation  stable  true`. Both protocol generations work: 0.159 speaks realtime v1 (`/v1/realtime/calls`), 0.162 and later speak realtime v3 (`/v1/live`), and the shim serves both. The CLI bundled with the Codex desktop app can be older (0.142.5 has the feature off), so install the CLI itself.
- **A Deepgram API key** in the `DEEPGRAM_API_KEY` environment variable. Get one from the [Deepgram console](https://console.deepgram.com/). The shim reads it from the environment only and never writes it anywhere.
- **Python 3.11 or newer** on the host for the live microphone session. Check with `python3 --version`; the macOS system Python is often older.
- **Microphone access for your terminal app.** Codex captures the audio, not the shim, so the terminal running Codex needs the permission (macOS: System Settings, Privacy & Security, Microphone).
- **Port 8765 free** on 127.0.0.1, or pick another with `--port` (see [Troubleshooting](#troubleshooting)). The launcher script uses `nc` to check the port.
- **Docker**, only if you want to run the headless checks or the shim in a container.

The headless checks were run on macOS with Docker Desktop. Linux should work the same way; Windows is untested.

## Quick start

Three steps: start the shim, start Codex pointed at it, talk.

### 1. Start the shim

```bash
git clone https://github.com/deepgram-devs/codex-deepgram-voice.git && cd codex-deepgram-voice
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
export DEEPGRAM_API_KEY="..."
python -m shim.server -v
```

You should see:

```
Codex voice shim listening on http://127.0.0.1:8765  (calls: /v1/realtime/calls and /v1/live, sideband: /v1/realtime and /v1/live/<call_id>)
Codex config: experimental_realtime_webrtc_call_base_url = "http://127.0.0.1:8765/v1", experimental_realtime_ws_base_url = "ws://127.0.0.1:8765/v1"
```

Leave this terminal open. `Ctrl+C` stops the shim. `curl 127.0.0.1:8765/healthz` returns `{"ok": true, "sessions": 0}` while it runs. The shim exits with status 1 at startup if `DEEPGRAM_API_KEY` is not set. `-v` prints every Flux turn event; drop it later for a quieter log.

### 2. Start Codex pointed at the shim

In a second terminal, from the repo:

```bash
./run-codex-with-flux.sh
```

The script checks that the shim is listening, then runs `codex` with the two endpoint settings passed as `-c key=value` overrides, which apply to that one run and touch no config file. It sets nothing else: Codex 0.159 (realtime v1) and 0.162+ (realtime v3) both work against these two URLs. Any extra arguments go through to `codex`. If the shim is on another port, set `SHIM_PORT` (and `SHIM_HOST`) in the environment first.

To make it permanent instead, add two lines to your user config at `~/.codex/config.toml`:

```toml
experimental_realtime_webrtc_call_base_url = "http://127.0.0.1:8765/v1"
experimental_realtime_ws_base_url = "ws://127.0.0.1:8765/v1"
```

Codex ignores these two keys in a repo-level `.codex/config.toml`, so they go in the user config or on the command line.

### 3. Talk

Press `F8` (or type `/voice`), say what you want done, and stop talking. `Ctrl+X` mutes, `F8` again ends the voice session.

In the shim terminal you should see this sequence. Codex 0.162 and later (realtime v3) log the two lines marked v3; Codex 0.159 (realtime v1) logs the `quicksilver` ones:

```
call created (dialect v3, query {})                                     <- v3
call created (session type quicksilver, ...)                            <- v1
WebRTC connection state: connected
sideband WebSocket attached (dialect v3); sending session.started       <- v3
sideband WebSocket attached                                             <- v1, then session.update type=quicksilver
data channel 'oai-events' opened by Codex                               <- v3
first audio frame: 48000 Hz, stereo, 960 samples
Flux STT stream opened (eot_threshold=0.8 eot_timeout_ms=6000)
turn 0 StartOfTurn
turn 0 Update conf=0.0xx 'Create a file called...'      <- only with -v
turn 0 EndOfTurn conf=0.8xx -> Codex: Create a file called hello.txt that says hello from Flux
Codex reply for handoff_...: ...
```

In Codex you should see the live caption while you speak, then the finished transcript, then the turn running. In the log, a *call* is the WebRTC connection Codex opens, the *sideband* is the WebSocket it opens beside the call to carry events, and a *session* is the voice session both belong to; one call, one sideband, one session.

## Troubleshooting

Each row names the log line to look for in the shim terminal.

| What you see | Shim output | What to do |
| --- | --- | --- |
| The shim exits immediately | `error: DEEPGRAM_API_KEY is not set` | Export the key in that shell and start the shim again. |
| The shim exits with status 2 | `error: --host '0.0.0.0' is not a loopback address` | For a local run, use the default `127.0.0.1`. Inside a container, the image already passes `--allow-unauthenticated-remote`; publish the port as `127.0.0.1:8765:8765` (see [Privacy and keys](#privacy-and-keys)). |
| Codex shows an error right after `F8` | `Deepgram rejected the API key (HTTP 401)` | Check the key in `DEEPGRAM_API_KEY`, then restart the shim. |
| Codex shows an error mid-session | `Deepgram Flux error: ...` or `Deepgram Flux closed the stream unexpectedly` | Read the detail in the log, then press `F8` to start a new voice session. |
| The launcher refuses to start | `shim is not listening on 127.0.0.1:8765` | Start the shim first (step 1). If it runs on another port, set `SHIM_PORT`. |
| The shim cannot start on 8765 | `address already in use` | Find the other process with `lsof -i :8765`, or start the shim with `--port 8766` and run the launcher with `SHIM_PORT=8766`. |
| Codex voice works but the shim logs nothing | no `call created` line | Codex is still talking to OpenAI. Use the launcher, or check the two keys are in `~/.codex/config.toml` and not a repo `.codex/config.toml`. Confirm `codex --version` is 0.159 or newer (0.159 posts to `/v1/realtime/calls`, 0.162+ to `/v1/live`). |
| The call never connects | stuck at `WebRTC connection state: connecting` | Run the shim with `-v` and check the answer SDP carried a candidate for a local interface. A VPN can add many interfaces; Codex accepts at most 32 candidates. Try with the VPN off. |
| Connected, but no audio | no `first audio frame` line | Grant the terminal app microphone access and check Codex is not muted (`Ctrl+X` toggles). |
| Codex shows `Voice connection timed out. Retrying once after cleanup.` (observed about 15 s after `F8`), then the same again after its own retry | `WebRTC connection state: connected` but no `data channel 'oai-events' opened by Codex` line | Codex is waiting for its data channel, which opens only when the shim's SDP answer says `a=setup:passive`. This build does that; an older checkout answers `active`. Pull, restart the shim, and press `F8` again. |
| The call is dropped after 30 seconds | `no sideband after 30s; dropping the call` | Codex never opened its WebSocket to the shim. Check `experimental_realtime_ws_base_url` points at `ws://127.0.0.1:8765/v1`. |
| You spoke, but nothing reached Codex | `EndOfTurn with empty transcript` | Flux heard silence. Check the input device Codex is using and speak closer to the microphone. |
| Codex cuts you off mid-thought, or waits too long after you finish | `EndOfTurn conf=...` arrives too early or too late | Tune the turn detection (next section). |

## Tuning turn detection

Defaults are `eot_threshold=0.8` and `eot_timeout_ms=6000`. Flux's own defaults are 0.7 and 5000. A higher threshold means Flux wants more certainty that you are done before it ends the turn, which suits dictating a task with thinking pauses. Override at start:

```bash
python -m shim.server --eot-threshold 0.85 --eot-timeout-ms 8000
```

Ranges and meaning: [End-of-Turn Detection Parameters](https://developers.deepgram.com/docs/flux/configuration). `--eager-eot-threshold` turns on Flux's eager end-of-turn; the shim leaves it off because Codex has no way to take back a turn once the handoff is sent.

### Add keyterms

Flux can be biased toward words it would otherwise mishear, such as product names or identifiers. Repeat the flag for each term:

```bash
python -m shim.server --keyterm Deepgram --keyterm Codex --keyterm aiortc
```

### Disable live captions

Codex appends caption deltas and never rewrites them, while Flux can revise its last word or two as you speak, so a revised word can show up twice in the live caption. The text Codex acts on is always the exact final transcript. If you would rather see only that, start the shim with `--no-live-captions`.

### The 10-minute "does it cut me off" test

1. Start the shim with `-v` so every Flux `Update` and its `end_of_turn_confidence` prints.
2. In Codex, press `F8` and dictate five tasks. Make each one at least three sentences and pause for two to four seconds mid-sentence at least once ("Add a retry to the upload client... [pause] ...with exponential backoff, and cap it at five attempts").
3. For each task, check the shim log: the pauses should show `Update` events with confidence under the threshold, and exactly one `EndOfTurn` after you actually stop.
4. Count early cut-offs (an `EndOfTurn` while you were still mid-thought). If you get any, raise `--eot-threshold` to 0.85 or 0.9, or raise `--eot-timeout-ms` if the cut-off came after a long silent pause, and repeat.
5. Check the other direction too: after you finish a task, the turn should end within about a second and Codex should start. If turns feel slow to close, drop `--eot-timeout-ms` toward 4000.

## Switch back to OpenAI voice

Run plain `codex` instead of the launcher. If you added the two lines to `~/.codex/config.toml`, remove them. Stop the shim with `Ctrl+C` whenever you like; Codex only talks to it during a voice session.

## Run the checks in Docker

Everything in this section is headless: no microphone, no Codex login. The image build takes about three minutes the first time.

```bash
export DEEPGRAM_API_KEY="..."
docker build -t codex-flux-voice .
docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice \
  python tests/wav_to_flux.py audio/spacewalk-16k.wav --realtime --eot 0.8 --timeout-ms 6000
```

This streams the sample clip to Flux and prints every turn event. You should see `TurnInfo Update` lines with rising and falling `eot_conf` values and one `TurnInfo EndOfTurn` carrying the transcript.

The full protocol check starts the shim inside the container and drives it with a fake Codex client that does what the real CLI does (WebRTC offer, multipart call creation, sideband WebSocket, `session.update`). It ends with `RESULT: PASS` when a `conversation.handoff.requested` carrying the finished transcript came back. `--dialect v3` speaks realtime v3 instead, as Codex 0.162 does (`POST /v1/live`, sideband `/v1/live/<call_id>`, no `session.update`, `session.started` first, then `input_transcript.added`, `turn.done`, and `delegation.created`, and the sideband closed with code 1000 after `session.close`):

```bash
docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/e2e_fake_codex.py audio/spacewalk-16k.wav
docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/e2e_fake_codex.py audio/spacewalk-16k.wav --dialect v3
```

The same check can target a shim you already have running on the host, which is a good test before your first real session:

```bash
python tests/e2e_fake_codex.py audio/spacewalk-16k.wav --shim http://127.0.0.1:8765/v1
python tests/e2e_fake_codex.py audio/spacewalk-16k.wav --shim http://127.0.0.1:8765/v1 --dialect v3
```

Every check is also a `compose.yaml` service. `DEEPGRAM_API_KEY` must be exported in your shell for the ones that reach Flux. Run one at a time:

| Service | Needs a key | What it checks |
| --- | --- | --- |
| `flux-check` | yes | The sample clip against live Flux: one `EndOfTurn` |
| `e2e`, `e2e-v3` | yes | The full Codex protocol path through the shim, WebRTC and sideband, in realtime v1 (Codex 0.159) and realtime v3 (Codex 0.162) |
| `ws-check-v1`, `ws-check-v2`, `ws-check-v2-handoff`, `ws-check-v3` | yes | The plain WebSocket transport in the v1 shape, the v2 transcription shape, the v2 realtime shape with its `background_agent` handoff, and the v3 shape on `/v1/live`: `session.started` first, `input_transcript.added`, one `turn.done`, one `delegation.created` with the same text, audio sent before `session.update` ignored, and `session.close` closing the socket |
| `hardening-check` | no (needs network) | Startup refusals, cross-origin requests, orphaned calls, a bad key, late sidebands, clients that never send `session.update`, malformed call bodies and sideband frames, and bad WebSocket upgrades (list below) |
| `unit-check` | no | Offline session logic: captions, event ordering, close paths, the resampler tail, queue caps, Origin and loopback checks, message validation, the session registry, and the realtime v3 events (list below) |

What `hardening-check` covers:

- `missing-key`: the shim started without `DEEPGRAM_API_KEY` exits 1 with a message naming the variable.
- `origin`: a WebSocket with no Origin or with a `localhost`, `127.0.0.1`, or `[::1]` Origin gets 101. A WebSocket with an `evil.example`, `localhost.evil.example`, or `null` Origin gets 403. A `POST /v1/realtime/calls` with an `evil.example` Origin gets 403, and one with no Origin gets 400 because its body is not an offer.
- `orphan`: a call whose sideband never attaches leaves the registry after the expiry window.
- `bad-key`: the real shim at `-v` with a fake key sends one Codex `error` event after one Flux connect attempt, and the key appears nowhere in its output.
- `late-sideband`: a WebRTC call whose audio fails against Flux before its sideband attaches ~3 s later gets the queued `error` after `session.updated`, with one Flux connect attempt and no key in the output.
- `no-session-update`: a WebSocket client that streams audio but never sends `session.update` opens no Flux connection, is closed with code 1008 at the deadline, and leaves the registry.
- `remote-bind`: `--host 0.0.0.0` exits 2 without `--allow-unauthenticated-remote`, and with the flag the shim answers `/healthz` and logs that there is no client authentication.
- `bad-call-body`: each malformed call request below gets a 400 naming the problem, except the 413 cases, the disconnect case (see below), and the ICE-candidate-never-answers offer (see Offers), and no case logs a server error.
  - JSON shape and syntax: a JSON array, non-JSON, JSON nested too deep, a string `session`, and a non-string `sdp` each get their own reason. A JSON integer of 5000 digits, past Python's digit limit, gets "call request could not be parsed". The same integer inside a multipart `session` part gets "malformed multipart body" instead (the reason depends on the content type).
  - Encoding and charset: bad UTF-8 sent as JSON and as SDP gets "call request body is not valid text for its charset". A gzip JSON body and a gzip multipart body that do not match their Content-Encoding get "call request body does not match its Content-Encoding". So does a deflate JSON body whose bytes are not deflate at all; a deflate stream cut short is rejected by aiohttp itself and is not sent here. Non-JSON bytes under an unknown Content-Encoding get "call request is not valid JSON".
  - Multipart: a string `session` part and a non-JSON `session` part get the same 400s as their JSON counterparts. An unknown charset, an unknown Content-Transfer-Encoding, and a nested part each get their own reason. A header without a colon and a body cut short are malformed multipart bodies. A gzip multipart body truncated to 30 bytes gets either "malformed multipart body" or "call request body does not match its Content-Encoding".
  - Offers: eleven offers in five groups. Four offers aiortc's SDP parser rejects (a bad setup line, a bare media line after a full session header, `v=0` then a bare media line, garbage after `v=0`) get "SDP offer could not be parsed". Three offers whose BUNDLE group names a mid that no media line has are found by the same parser (an unknown token, a session-level `a=mid:9` before the `m=` line, a second `a=mid:9` in the audio section). Each gets "BUNDLE group names a mid that no media line has" before negotiation. An otherwise valid offer whose mid and BUNDLE group are a lone surrogate (`\ud800` in the JSON) gets "sdp is not valid text". Two minimal audio offers with an ICE candidate on port 0 and on port 70000 get "ICE candidate port out of range". An offer with an ICE candidate that never answers (`127.0.0.1 65535`) gets 201. aioice's STUN retry then fires a few seconds after the session closes and raises an AttributeError. The shim's loop exception handler logs it as one WARNING (the test asserts exactly one), so no ERROR is logged when the session closes. None of these offers logs a server error.
  - Size cap and disconnect: a chunked JSON body or multipart part over the 4 MB cap gets 413, and so do two 3 MB multipart parts that are over it together. A client that disconnects mid-body logs a warning and no server error.
  - What still gets 201: a well-formed call, a valid gzip body, and a multipart call whose `sdp` part is base64 with a Content-Transfer-Encoding header.
- `bad-ws-frame`: a sideband frame nested too deep to parse is dropped without a server error, and a `session.update` on the same socket still gets `session.updated`.
- `bad-upgrade`: a plain GET to the realtime endpoint gets 400 before any session is looked up. A live call keeps its state, so its real sideband still attaches and a second call's orphan timer still fires. An unknown call id gets the same 400. A handshake failure injected by patching `prepare` releases the session, answering 400 for a connection error, 500 with a logged traceback for a shim fault, and close code 1011 when the failure comes after the 101. The 400 and 1011 variants assert no server error. Each injected variant runs on both a standalone WebSocket and a call sideband. For realtime v3, a call posted to `/v1/live` gets `Location: /v1/live/rtc_...`, a plain GET to `/v1/live/<call_id>` gets the same 400 before any lookup, an unknown call id over a real WebSocket gets 404, a second v3 sideband gets 409, and a v3 sideband that sends nothing gets `session.started` first and is still open 3 s later, past a 1 s `session.update` deadline.

What `unit-check` covers:

- `prefix`: the caption prefix is cut at word boundaries and never indexes past either string.
- `origin`: the Origin filter passes no Origin and localhost, and rejects other hosts and `null`.
- `ordering`: Flux events that arrive before `session.update` are queued and sent in order after `session.updated`, and later events go out directly.
- `captions`: a Flux revision of a shown word appends from the stable prefix, the handoff carries the exact final transcript, an empty `EndOfTurn` sends nothing, and a repeated `StartOfTurn` does not re-send captions.
- `double-close`: two concurrent closes of the Flux client close the socket once and raise nothing.
- `immediate-close`: a close right after start, while the reader fails, or from inside the reader closes the socket exactly once without raising. So does Flux dropping the socket mid-stream, and the drop reaches the error path.
- `tail`: the samples a resampler holds back reach Flux on `input_audio_buffer.commit` and session close for the WebSocket transport, and at track end and session close for WebRTC. Audio after a commit still flows, and audio before `session.update` is ignored.
- `queue-cap`: events queued before `session.update` stop at the cap and the session closes, still closing the Flux socket.
- `loopback`: `127.0.0.1`, `127.1.2.3`, `::1`, and `localhost` count as loopback, while `0.0.0.0`, LAN addresses, hostnames, and an empty string do not.
- `bad-messages`: Codex messages with the right `type` but a string `session`, a `session.type` that is not a string, a string `item`, a non-string `audio`, a missing `audio`, or a null `output_text` raise no exception. A well-formed `session.update` still works on the same session afterwards.
- `close-paths`: closing a session still closes Flux and drops the registry entry when the peer connection refuses to close. That refusal logs one WARNING naming only the exception type, with no traceback. A close cancelled mid-drain still releases the session.
- `registry`: a call leaves no entry behind once its sideband closes: /healthz reports 0, and a late WebSocket attempt gets 404. Two sidebands opened at once for one call get one 101 and one 409. An oversized offer gets 413 before it is read.
- `v3-events`: on a realtime v3 call sideband, `session.started` is the first event and the session is ready without a `session.update`, events queued before the attach follow it in order, and the `session.update` deadline does not close it. Flux `Update` and `EndOfTurn` become `input_transcript.added`, `turn.done` (role `user`), and `delegation.created` carrying the final text; `StartOfTurn` sends nothing; a Flux error carries a top-level `message`. `delegation.context.append` and `session.context.append` are logged, `input_audio.append` reaches Flux, and `session.close` ends the sideband.
- `dtls-role`: the SDP answer says `a=setup:passive` when the offer says `actpass`, as Codex's does. Codex's voice helper opens the SCTP association for its data channel only as the DTLS client, and aiortc opens it only as the ICE controlling side, so an `active` answer leaves both waiting and Codex 0.162 reports `Voice connection timed out` after about 15 s (observed). A caller still opens a data channel against the passive answer, and an offer that fixed its own role is answered with the complementary one.

```bash
docker compose run --rm hardening-check
docker compose run --rm e2e
docker compose run --rm e2e-v3
```

### Run the shim in Docker

`docker compose up shim` starts the shim in a container, published on 127.0.0.1:8765 only. Inside the container the shim must bind `0.0.0.0` for the port mapping to work, so the image's default command passes `--allow-unauthenticated-remote`. The compose port mapping on 127.0.0.1 is what keeps it off your network. On macOS, Docker Desktop does not carry WebRTC media from Codex into a container, so the containerized shim serves the plain WebSocket transport only and the real microphone session needs the host venv from the quick start. On Linux, `docker run --network host` with the command overridden to `python -m shim.server` (the default `127.0.0.1` bind, no flag) should carry the WebRTC path as well; that has not been tested here. Do not combine host networking with the image's default command, which binds every interface.

## How it works

| Codex does | Shim answers |
| --- | --- |
| `POST /v1/realtime/calls?intent=quicksilver&architecture=avas` (v1, Codex 0.159) or `POST /v1/live` (v3, Codex 0.162), multipart `sdp` + `session` | SDP answer, `Location: /v1/realtime/calls/rtc_...` (v1) or `/v1/live/rtc_...` (v3) |
| WebRTC Opus audio track, `oai-events` data channel | Decodes audio, resamples to 16 kHz mono PCM, streams 80 ms chunks to Flux |
| v1: `GET /v1/realtime?call_id=...` then `session.update` | `session.updated` with a session id |
| v3: `GET /v1/live/<call_id>`, then nothing | `session.started` with a session id, sent as soon as the sideband attaches |
| waits for transcript events | v1: Flux `Update` becomes `conversation.input_transcript.delta`; Flux `EndOfTurn` becomes `conversation.input_transcript.turn_marked` and `conversation.handoff.requested`. v3: `input_transcript.added`, then `turn.done` and `delegation.created` |
| v1 `conversation.handoff.append`, v3 `delegation.context.append`, with its reply text | Logged (spoken replies are out of scope for this shim) |

The plain WebSocket transport (`[realtime] transport = "websocket"`, used by app-server clients rather than the CLI) also works against `ws://127.0.0.1:8765/v1`, in the v1 and v2 message shapes on `/v1/realtime` and the v3 shape on `/v1/live` (audio as `input_audio.append`).

How the shim knows what Codex expects, with citations into the Codex source, is in [DECISION.md](DECISION.md).

## What has been verified

Every check under `tests/` was run in Docker and in a host venv, with its trimmed output under `evidence/`; the host venv additionally has a Flux-defaults run of `wav_to_flux.py` (`evidence/host-wav-to-flux-defaults.txt`). All of them except `unit_check.py` connect to the live Flux API; `hardening_check.py` does so only with a deliberately bad key, so it needs network access but no key.

- `wav_to_flux.py` on the sample clip: one `EndOfTurn`, at confidence 0.820 with `eot_threshold` 0.8 and 0.778 with the Flux default of 0.7. The clip's filler pauses ("um", "uh") peaked at 0.340 and never ended the turn.
- `e2e_fake_codex.py`: the full WebRTC call, sideband WebSocket, caption deltas, `turn_marked`, and one `handoff.requested` carrying the finished transcript, ending in `RESULT: PASS`. With `--dialect v3` (host venv, `evidence/host-e2e-fake-codex-v3.txt`): `session.started` first, `input_transcript.added` captions, one `turn.done`, one `delegation.created` carrying the same transcript, and the sideband closed with code 1000 after `session.close`, ending in `RESULT: PASS`.
- `ws_transport_check.py` in the v1 shape, the v2 transcription shape, the v2 realtime shape that hands off through a `background_agent` function call, and the v3 shape on `/v1/live` (`--dialect v3`, `evidence/host-ws-transport-v3.txt` and `evidence/docker-ws-transport-v3.txt`): `session.started` first, `input_transcript.added` captions, one `turn.done` and one `delegation.created` carrying the same transcript, an `input_audio.append` sent before `session.update` ignored, and the socket closed after `session.close`.
- `hardening_check.py` (ten checks) and `unit_check.py` (fourteen checks), as listed under the table above.
- The real Codex CLI 0.162.1 TUI against the shim, driven headlessly with `/voice` and no speech (host venv, `evidence/host-codex-0.162.1-headless.txt`): the v3 call, the sideband, `data channel 'oai-events' opened by Codex`, microphone frames arriving, and the Flux stream opening, with the session staying up until the TUI was closed.
- A spoken session with a real microphone and the Codex CLI 0.162.1 TUI on macOS (host venv, `evidence/host-codex-0.162.1-mic-session.txt`, with `-v` so every Flux `Update` and its end-of-turn confidence is in the log): three utterances in 37 s, each ending in `EndOfTurn` at confidence 0.821, 0.834, and 0.805 against the 0.8 threshold and handed to Codex, the last one `Create a file called hello dot t x t that says hello from Flux.`, which Codex carried out (it wrote `hello.txt`). While words were still arriving the confidence stayed below 0.25; it climbed past 0.8 only after each sentence was complete, and in the gap before the last word of the third turn it rose to 0.70 without ending the turn. Codex's spoken-reply context (`session.context.append`) arrived after the first two turns.

Not yet recorded: the 10-minute turn-detection exercise above; the spoken session covers three short turns.

## Privacy and keys

- `DEEPGRAM_API_KEY` is read from the environment only. Nothing in this repo stores it, and Docker gets it with `-e DEEPGRAM_API_KEY`.
- Codex sends its own auth headers to the shim (it thinks it is talking to OpenAI). The shim ignores and never logs them.
- The shim is localhost-only. It binds `127.0.0.1` by default and refuses any non-loopback `--host` unless you pass `--allow-unauthenticated-remote` (exit status 2 otherwise).
- There is no client authentication, and none can be added: Codex decides which headers it sends, so the shim cannot ask it for a shared secret. Anyone who can reach the port can stream audio on your Deepgram key and read the transcripts. Exposing the shim beyond your machine is unsupported.
- Requests carrying a browser Origin other than localhost get 403. That stops a web page you visit from opening Flux sessions on your key; it is not authentication, since Codex itself sends no Origin and any non-browser client can omit one.
- In Docker the shim must bind `0.0.0.0` inside the container, so the image's default command passes `--allow-unauthenticated-remote`. The compose port mapping on 127.0.0.1 is what keeps it off your network. A bare `docker run -p 8765:8765` would publish it on every interface and the shim cannot see the mapping from inside; use `-p 127.0.0.1:8765:8765`.
- A sideband or WebSocket client that never sends `session.update` is closed after 10 seconds (close code 1008), and the events queued before it are capped at 2048 events or 1 MiB, so a misbehaving client cannot hold a session open or grow it without bound.
- `-v` turns on debug logging for the shim. It does not print your API key.

## Files

- `shim/flux_client.py`: Flux STT WebSocket client, 80 ms chunking, TurnInfo parsing
- `shim/server.py`: the Codex-facing realtime server (WebRTC, sideband, and WebSocket transport)
- `tests/wav_to_flux.py`: stream a WAV to Flux and print turn events
- `tests/e2e_fake_codex.py`: fake Codex client, full protocol check (`--dialect v1|v3`, `--shim` targets a running shim)
- `tests/ws_transport_check.py`: plain WebSocket transport check (`--dialect v1|v2|v3`, `--session-type`)
- `tests/hardening_check.py`: the hardening checks (`--only` runs a subset)
- `tests/unit_check.py`: offline session-logic checks
- `evidence/`: trimmed output of the live runs recorded for this repo
- `audio/spacewalk-16k.wav`: Deepgram's public 25.9 s sample clip ([dpgr.am/spacewalk.wav](https://dpgr.am/spacewalk.wav), a NASA spacewalk interview), resampled to 16 kHz mono with ffmpeg. NASA audio is generally not subject to copyright in the US; see [NASA's media usage guidelines](https://www.nasa.gov/nasa-brand-center/images-and-media/).
- `Dockerfile`, `compose.yaml`: container image and one-service-at-a-time run recipes
- `run-codex-with-flux.sh`: starts `codex` with the two endpoint overrides
- `DECISION.md`: how Codex voice works on the wire, cited to the Codex source, and why the shim is built the way it is

## License

[MIT](LICENSE).

# Codex CLI voice, powered by Deepgram Flux

A small local shim that makes [Deepgram Flux](https://developers.deepgram.com/docs/flux/quickstart) the speech layer for Codex CLI voice mode. You press `F8` in Codex, talk through a task, Flux transcribes it and decides when you have finished speaking, and Codex starts working on it. No Codex fork, no patched binary: two endpoint settings, passed by `./run-codex-with-flux.sh` or set in `~/.codex/config.toml`, point Codex's realtime voice endpoints at this shim, and the shim speaks Codex's realtime protocol with Flux underneath.

Unofficial community shim, not affiliated with or endorsed by OpenAI; the config keys are undocumented and may change.

```
  Codex CLI (F8)  --WebRTC audio-->  shim (localhost:8765)  --16 kHz PCM, 80 ms chunks-->  wss://api.deepgram.com/v2/listen
  Codex CLI       <--sideband WS---  shim                   <--TurnInfo Update / EndOfTurn---  Flux (flux-general-en)
                     live captions, then handoff.requested(finished transcript)  =>  Codex runs the turn
```

How the shim knows what Codex expects is written up with source citations in [DECISION.md](DECISION.md). What has been verified, and what has not, is in [What has been verified](#what-has-been-verified).

## Setup in under 5 minutes

You need Docker (or Python 3.11+), a Deepgram API key in `DEEPGRAM_API_KEY`, and Codex CLI with voice mode (`/voice` or `F8`).

### 1. Check Flux from a container (about 3 minutes the first time, mostly the image build)

```bash
git clone https://github.com/deepgram-devs/codex-deepgram-voice.git && cd codex-deepgram-voice
export DEEPGRAM_API_KEY="..."            # from your shell or 1Password, never in a file
docker build -t codex-flux-voice .
docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice \
  python tests/wav_to_flux.py audio/spacewalk-16k.wav --realtime --eot 0.8 --timeout-ms 6000
```

You should see `TurnInfo Update` lines with `eot_conf` values and one `TurnInfo EndOfTurn` with the transcript.

### 2. Run the full Codex protocol check headlessly (about 1 minute)

```bash
docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/e2e_fake_codex.py audio/spacewalk-16k.wav
```

This starts the shim inside the container and drives it with a fake Codex client that does what the real CLI does (WebRTC offer, multipart call creation, sideband WebSocket, `session.update`). It ends with `RESULT: PASS` when a `conversation.handoff.requested` carrying the finished transcript came back.

The plain WebSocket transport has its own check, in both message shapes Codex can send:

```bash
docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/ws_transport_check.py audio/spacewalk-16k.wav --dialect v1
docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/ws_transport_check.py audio/spacewalk-16k.wav --dialect v2
```

The same commands work through `compose.yaml` (`docker compose run --rm flux-check`, `e2e`, `ws-check-v1`, `ws-check-v2`, and `ws-check-v2-handoff`, which runs v2 as a realtime session and checks the `background_agent` handoff). Run one at a time.

The `hardening-check` service needs no key: `docker compose run --rm hardening-check`.

### 3. Run the shim on your machine for the real microphone

WebRTC media from Codex to a container does not work on Docker Desktop for macOS, so the real session runs the shim on the host:

```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt   # python3 must be 3.11+
DEEPGRAM_API_KEY="..." python -m shim.server            # listens on 127.0.0.1:8765
```

The shim exits with status 1 at startup if `DEEPGRAM_API_KEY` is not set. If the key is set but Deepgram rejects it, the shim only finds out when it connects to Flux on the first audio frame, so the error shows up in Codex within moments of pressing `F8`, and in the shim log.

### 4. Point Codex at the shim

You need Codex CLI 0.159 or newer; voice mode shipped as a stable feature there
(`codex features list` shows `realtime_conversation  stable  true`). The CLI
bundled with the Codex desktop app can be older: 0.142.5 has the feature off and
no voice host, so install the CLI itself with `npm install -g @openai/codex`.

Easiest, with no config changes: `./run-codex-with-flux.sh` starts `codex` with
the two endpoint keys as `-c` overrides (the keys are only refused from
project-level `.codex/config.toml`; user config and `-c` both work). Any extra
arguments go through to `codex`.

Or make it permanent in `~/.codex/config.toml`:

```toml
experimental_realtime_webrtc_call_base_url = "http://127.0.0.1:8765/v1"
experimental_realtime_ws_base_url = "ws://127.0.0.1:8765/v1"
```

### 5. Talk

Start Codex (`./run-codex-with-flux.sh`, or `codex` if you set the config lines), press `F8` (or type `/voice`), say what you want done, and stop talking. The shim log shows the Flux turn events and the line `EndOfTurn conf=... -> Codex: <your words>`; Codex shows the transcript and starts the turn. `Ctrl+X` mutes, `F8` again ends the session. To go back to OpenAI's voice backend, run plain `codex` (and remove the two config lines if you added them).

## Tuning turn detection

Defaults are `eot_threshold=0.8` and `eot_timeout_ms=6000` (Flux ships with 0.7 and 5000). Higher threshold means Flux wants more certainty that you are done before it ends the turn, which suits dictating a task with thinking pauses. Override at start:

```bash
python -m shim.server --eot-threshold 0.85 --eot-timeout-ms 8000 --keyterm Deepgram --keyterm Codex
```

Add `--no-live-captions` if you only want the finished transcript to appear in Codex. Ranges and meaning: [End-of-Turn Detection Parameters](https://developers.deepgram.com/docs/flux/configuration).

### 10-minute "does it cut me off" test

1. Start the shim with `-v` so every Flux `Update` and its `end_of_turn_confidence` prints.
2. In Codex, press `F8` and dictate five tasks. Make each one at least three sentences and pause for two to four seconds mid-sentence at least once ("Add a retry to the upload client... [pause] ...with exponential backoff, and cap it at five attempts").
3. For each task, check the shim log: the pauses should show `Update` events with confidence under the threshold, and exactly one `EndOfTurn` after you actually stop.
4. Count early cut-offs (an `EndOfTurn` while you were still mid-thought). If you get any, raise `--eot-threshold` to 0.85 or 0.9, or raise `--eot-timeout-ms` if the cut-off came after a long silent pause, and repeat.
5. Also check the other direction: after you finish a task, the turn should end within about a second, and Codex should start. If turns feel slow to close, drop `--eot-timeout-ms` toward 4000.

## What the shim implements

| Codex does | Shim answers |
| --- | --- |
| `POST /v1/realtime/calls?intent=quicksilver&architecture=avas` (multipart `sdp` + `session`) | SDP answer, `Location: /v1/realtime/calls/rtc_...` |
| WebRTC Opus audio track, `oai-events` data channel | Decodes audio, resamples to 16 kHz mono PCM, streams 80 ms chunks to Flux |
| `GET /v1/realtime?call_id=...` then `session.update` | `session.updated` with a session id |
| waits for transcript events | Flux `Update` becomes `conversation.input_transcript.delta`; Flux `EndOfTurn` becomes `conversation.input_transcript.turn_marked` and `conversation.handoff.requested` |
| `conversation.handoff.append` with its reply text | Logged (spoken replies are out of scope for this shim) |

The plain WebSocket transport (`[realtime] transport = "websocket"`, used by app-server clients rather than the CLI) also works against `ws://127.0.0.1:8765/v1`, in both the v1 and v2 message shapes. Realtime v3 (`/live`) is not implemented.

## What has been verified

Every check under `tests/` was run against the live Flux API, in Docker and in a host venv, and the trimmed output is under `evidence/` (request ids stripped):

- `wav_to_flux.py` on the spacewalk clip: one `EndOfTurn`, at confidence 0.820 with `eot_threshold` 0.8 and 0.778 with the Flux default of 0.7. The clip's filler pauses ("um", "uh") peaked at 0.340 and never ended the turn.
- `e2e_fake_codex.py`: the full WebRTC call, sideband WebSocket, caption deltas, `turn_marked`, and one `handoff.requested` carrying the finished transcript, ending in `RESULT: PASS`.
- `ws_transport_check.py` in the v1 shape, the v2 transcription shape, and the v2 realtime shape that hands off through a `background_agent` function call.
- `hardening_check.py`: missing-key exit, cross-origin 403, orphaned-call expiry, a bad-key error with no key in `-v` output, and that error reaching a WebRTC call whose sideband attaches late.

Not yet recorded here: a session with a real microphone and the Codex TUI. The headless checks drive the shim with a fake Codex client built from the same source citations, because no microphone or OpenAI login was available where they ran. When you run the real thing, the shim log should show `call created`, `WebRTC connection state: connected`, `first audio frame: 48000 Hz`, `turn 0 StartOfTurn`, then `EndOfTurn conf=0.8x -> Codex: <your words>`, then `Codex reply for handoff_...`. If the WebRTC state never reaches `connected`, run the shim with `-v` and check that the answer SDP carried a host candidate for a local interface; VPN interfaces can add candidates, and Codex accepts at most 32. The 10-minute test above is the matching gap for turn detection.

## Privacy and keys

- `DEEPGRAM_API_KEY` is read from the environment only. Nothing in this repo stores it, and Docker gets it with `-e DEEPGRAM_API_KEY`.
- Codex sends its own auth headers to the shim (it thinks it is talking to OpenAI). The shim ignores and never logs them.
- The shim binds to `127.0.0.1` by default. Do not expose it beyond your machine.
- Requests carrying a browser Origin other than localhost get 403, so a web page you visit cannot open Flux sessions on your key.
- In Docker the shim binds `0.0.0.0` inside the container, and `compose.yaml` publishes it on `127.0.0.1:8765` only, so other machines on your network cannot reach it.
- `-v` turns on debug logging for the shim. It does not print your API key.

## Files

- `shim/flux_client.py`: Flux STT WebSocket client, 80 ms chunking, TurnInfo parsing
- `shim/server.py`: the Codex-facing realtime server (WebRTC + sideband + WebSocket transport)
- `tests/wav_to_flux.py`: stream a WAV to Flux and print turn events
- `tests/e2e_fake_codex.py`: fake Codex client, full protocol check
- `tests/ws_transport_check.py`: plain WebSocket transport check (v1 and v2 shapes)
- `tests/hardening_check.py`: missing-key exit, cross-origin 403, orphaned-call expiry, bad-key error with no key in -v output, and the same error queued for a WebRTC call whose sideband attaches late
- `evidence/`: trimmed output of every live run recorded for this repo
- `audio/spacewalk-16k.wav`: Deepgram's public 26 s sample clip ([dpgr.am/spacewalk.wav](https://dpgr.am/spacewalk.wav), a NASA spacewalk interview), resampled to 16 kHz mono with ffmpeg. NASA audio is generally not subject to copyright in the US; see [NASA's media usage guidelines](https://www.nasa.gov/nasa-brand-center/images-and-media/).
- `Dockerfile`, `compose.yaml`: container image and one-service-at-a-time run recipes
- `DECISION.md`: how Codex's voice mode works on the wire, with citations into the Codex source, and why the shim is built the way it is

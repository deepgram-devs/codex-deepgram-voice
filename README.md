# Codex CLI voice, powered by Deepgram Flux

A small local shim that makes [Deepgram Flux](https://developers.deepgram.com/docs/flux/quickstart) the speech layer for Codex CLI voice mode. You press `F8` in Codex, talk through a task, Flux transcribes it and decides when you have finished speaking, and Codex starts working on it. No Codex fork, no patched binary: two lines in `~/.codex/config.toml` point Codex's realtime voice endpoints at this shim, and the shim speaks Codex's realtime protocol with Flux underneath.

```
  Codex CLI (F8)  --WebRTC audio-->  shim (localhost:8765)  --16 kHz PCM, 80 ms chunks-->  wss://api.deepgram.com/v2/listen
  Codex CLI       <--sideband WS---  shim                   <--TurnInfo Update / EndOfTurn---  Flux (flux-general-en)
                     live captions, then handoff.requested(finished transcript)  =>  Codex runs the turn
```

How the shim knows what Codex expects is written up with source citations in [DECISION.md](DECISION.md). Evidence for each "done when" item is in [DONE.md](DONE.md).

## Setup in under 5 minutes

You need Docker (or Python 3.11+), a Deepgram API key in `DEEPGRAM_API_KEY`, and Codex CLI with voice mode (`/voice` or `F8`).

### 1. Check Flux from a container (about 1 minute)

```bash
git clone <this repo> && cd codex-deepgram-voice
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

### 3. Run the shim on your machine for the real microphone

WebRTC media from Codex to a container does not work on Docker Desktop for macOS, so the real session runs the shim on the host:

```bash
python3.12 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
DEEPGRAM_API_KEY="..." python -m shim.server            # listens on 127.0.0.1:8765
```

### 4. Point Codex at the shim

Add to `~/.codex/config.toml` (these keys are refused in project-level `.codex/config.toml`):

```toml
experimental_realtime_webrtc_call_base_url = "http://127.0.0.1:8765/v1"
experimental_realtime_ws_base_url = "ws://127.0.0.1:8765/v1"
```

### 5. Talk

Start `codex`, press `F8` (or type `/voice`), say what you want done, and stop talking. The shim log shows the Flux turn events and the line `EndOfTurn conf=... -> Codex: <your words>`; Codex shows the transcript and starts the turn. `Ctrl+X` mutes, `F8` again ends the session. Remove the two config lines to go back to OpenAI's voice backend.

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
| `conversation.handoff.append` with its reply text | Logged (spoken replies are out of scope for this goal) |

The plain WebSocket transport (`[realtime] transport = "websocket"`, used by app-server clients rather than the CLI) also works against `ws://127.0.0.1:8765/v1`, in both the v1 and v2 message shapes. Realtime v3 (`/live`) is not implemented.

## Privacy and keys

- `DEEPGRAM_API_KEY` is read from the environment only. Nothing in this repo stores it, and Docker gets it with `-e DEEPGRAM_API_KEY`.
- Codex sends its own auth headers to the shim (it thinks it is talking to OpenAI). The shim ignores and never logs them.
- The shim binds to `127.0.0.1` by default. Do not expose it beyond your machine.

## Files

- `shim/flux_client.py`: Flux STT WebSocket client, 80 ms chunking, TurnInfo parsing
- `shim/server.py`: the Codex-facing realtime server (WebRTC + sideband + WebSocket transport)
- `tests/wav_to_flux.py`: stream a WAV to Flux and print turn events
- `tests/e2e_fake_codex.py`: fake Codex client, full protocol check
- `audio/spacewalk-16k.wav`: 26 s public sample (dpgr.am/spacewalk.wav) resampled to 16 kHz mono
- `Dockerfile`, `compose.yaml`: container image and one-service-at-a-time run recipes
- `DECISION.md`, `DONE.md`, `GOAL.md`, `social/BG-4.md`: spike answers, evidence, spec, social kit

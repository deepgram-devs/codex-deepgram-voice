# Codex CLI voice, powered by Deepgram Flux

A small local shim that makes [Deepgram Flux](https://developers.deepgram.com/docs/flux/quickstart) the speech layer for Codex CLI voice mode. Press `F8` in Codex, talk through a task, and Codex starts on it the moment you stop. Flux does two things here: it transcribes the microphone audio as you speak, and its end-of-turn detection decides when you have finished a thought, so a pause to think does not cut you off and a finished sentence does not wait on a silence timer.

No Codex fork and no patched binary. Codex lets you point its two realtime voice endpoints at another server. The shim listens on localhost, speaks Codex's realtime protocol, and runs Flux underneath.

This is an unofficial community shim, not affiliated with or endorsed by OpenAI. The two Codex settings it relies on are marked experimental by OpenAI and may change in a future Codex release.

```
Codex CLI (F8) --WebRTC audio--> shim on 127.0.0.1:8765 --16 kHz PCM--> Deepgram Flux
Codex CLI      <--sideband WS--- shim                    <--Update / EndOfTurn--- Flux
                live captions, then handoff.requested(final transcript) => Codex runs the turn
```

## Prerequisites

- **Codex CLI 0.159 or newer**, installed with `npm install -g @openai/codex` and signed in. Voice mode shipped as a stable feature in 0.159; `codex features list` should show `realtime_conversation  stable  true`. The CLI bundled with the Codex desktop app can be older (0.142.5 has the feature off), so install the CLI itself.
- **A Deepgram API key** in the `DEEPGRAM_API_KEY` environment variable. Get one from the [Deepgram console](https://console.deepgram.com/). The shim reads it from the environment only and never writes it anywhere.
- **Python 3.11 or newer** on the host for the live microphone session. Check with `python3 --version`; the macOS system Python is often older.
- **Microphone access for your terminal app.** Codex captures the audio, not the shim, so the terminal running Codex needs the permission (macOS: System Settings, Privacy & Security, Microphone).
- **Port 8765 free** on 127.0.0.1, or pick another with `--port` (see [Troubleshooting](#troubleshooting)). The launcher script uses `nc` to check the port.
- **Docker**, only if you want to run the headless checks or the shim in a container.

Tested on macOS with Docker Desktop. Linux should work the same way; Windows is untested.

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
Codex voice shim listening on http://127.0.0.1:8765  (calls: /v1/realtime/calls, sideband: /v1/realtime)
Codex config: experimental_realtime_webrtc_call_base_url = "http://127.0.0.1:8765/v1", experimental_realtime_ws_base_url = "ws://127.0.0.1:8765/v1"
```

Leave this terminal open. `Ctrl+C` stops the shim. `curl 127.0.0.1:8765/healthz` returns `{"ok": true, "sessions": 0}` while it runs. The shim exits with status 1 at startup if `DEEPGRAM_API_KEY` is not set. `-v` prints every Flux turn event, which is what you want the first time; drop it later for a quieter log.

### 2. Start Codex pointed at the shim

In a second terminal, from the repo:

```bash
./run-codex-with-flux.sh
```

The script checks that the shim is listening, then runs `codex` with the two endpoint settings passed as `-c key=value` overrides, which apply to that one run and touch no config file. Any extra arguments go through to `codex`. If the shim is on another port, set `SHIM_PORT` (and `SHIM_HOST`) in the environment first.

To make it permanent instead, add two lines to your user config at `~/.codex/config.toml`:

```toml
experimental_realtime_webrtc_call_base_url = "http://127.0.0.1:8765/v1"
experimental_realtime_ws_base_url = "ws://127.0.0.1:8765/v1"
```

Codex ignores these two keys in a repo-level `.codex/config.toml`, so they go in the user config or on the command line.

### 3. Talk

Press `F8` (or type `/voice`), say what you want done, and stop talking. `Ctrl+X` mutes, `F8` again ends the voice session.

In the shim terminal you should see this sequence:

```
call created (session type quicksilver, ...)
WebRTC connection state: connected
sideband WebSocket attached
first audio frame: 48000 Hz, stereo, 960 samples
Flux STT stream opened (eot_threshold=0.8 eot_timeout_ms=6000)
turn 0 StartOfTurn
turn 0 Update conf=0.0xx 'Create a file called...'      <- only with -v
turn 0 EndOfTurn conf=0.8xx -> Codex: Create a file called hello.txt that says hello from Flux
Codex reply for handoff_...: ...
```

In Codex you should see the live caption while you speak, then the finished transcript, then the turn running.

## Troubleshooting

Each row names the log line to look for in the shim terminal.

| What you see | Shim log line | What to do |
| --- | --- | --- |
| The shim exits immediately | `error: DEEPGRAM_API_KEY is not set` | Export the key in that shell and start the shim again. |
| Codex shows an error right after `F8` | `Deepgram rejected the API key (HTTP 401)` | Check the key in `DEEPGRAM_API_KEY`, then restart the shim. |
| Codex shows an error mid-session | `Deepgram Flux error: ...` or `Deepgram Flux closed the stream unexpectedly` | Read the detail in the log, then press `F8` to start a new voice session. |
| The launcher refuses to start | `shim is not listening on 127.0.0.1:8765` | Start the shim first (step 1). If it runs on another port, set `SHIM_PORT`. |
| The shim cannot start on 8765 | `address already in use` | Find the other process with `lsof -i :8765`, or start the shim with `--port 8766` and run the launcher with `SHIM_PORT=8766`. |
| Codex voice works but the shim logs nothing | no `call created` line | Codex is still talking to OpenAI. Use the launcher, or check the two keys are in `~/.codex/config.toml` and not a repo `.codex/config.toml`. Confirm `codex --version` is 0.159 or newer. |
| The call never connects | stuck at `WebRTC connection state: connecting` | Run the shim with `-v` and check the answer SDP carried a candidate for a local interface. A VPN can add many interfaces; Codex accepts at most 32 candidates. Try with the VPN off. |
| Connected, but no audio | no `first audio frame` line | Grant the terminal app microphone access and check Codex is not muted (`Ctrl+X` toggles). |
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

The full protocol check starts the shim inside the container and drives it with a fake Codex client that does what the real CLI does (WebRTC offer, multipart call creation, sideband WebSocket, `session.update`). It ends with `RESULT: PASS` when a `conversation.handoff.requested` carrying the finished transcript came back:

```bash
docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/e2e_fake_codex.py audio/spacewalk-16k.wav
```

The same check can target a shim you already have running on the host, which is a good test before your first real session:

```bash
python tests/e2e_fake_codex.py audio/spacewalk-16k.wav --shim http://127.0.0.1:8765/v1
```

Every check is also a `compose.yaml` service. `DEEPGRAM_API_KEY` must be exported in your shell for the ones that reach Flux. Run one at a time:

| Service | Needs a key | What it checks |
| --- | --- | --- |
| `flux-check` | yes | The sample clip against live Flux: one `EndOfTurn` |
| `e2e` | yes | The full Codex protocol path through the shim, WebRTC and sideband |
| `ws-check-v1`, `ws-check-v2`, `ws-check-v2-handoff` | yes | The plain WebSocket transport in the v1 shape, the v2 transcription shape, and the v2 realtime shape with its `background_agent` handoff |
| `hardening-check` | no | Missing-key exit, cross-origin 403, orphaned-call expiry, a bad key producing one `error` event with no key in the `-v` output, and that error reaching a call whose sideband attaches late |
| `unit-check` | no | Offline session logic: event ordering before `session.updated`, caption revision, empty turns, concurrent close, registry cleanup |

```bash
docker compose run --rm hardening-check
docker compose run --rm e2e
```

### Run the shim in Docker

`docker compose up shim` starts the shim in a container, published on 127.0.0.1:8765 only. On macOS, Docker Desktop does not carry WebRTC media from Codex into a container, so the containerized shim serves the plain WebSocket transport only and the real microphone session needs the host venv from the quick start. On Linux with host networking the WebRTC path should work as well; that has not been tested here.

## How it works

| Codex does | Shim answers |
| --- | --- |
| `POST /v1/realtime/calls?intent=quicksilver&architecture=avas` (multipart `sdp` + `session`) | SDP answer, `Location: /v1/realtime/calls/rtc_...` |
| WebRTC Opus audio track, `oai-events` data channel | Decodes audio, resamples to 16 kHz mono PCM, streams 80 ms chunks to Flux |
| `GET /v1/realtime?call_id=...` then `session.update` | `session.updated` with a session id |
| waits for transcript events | Flux `Update` becomes `conversation.input_transcript.delta`; Flux `EndOfTurn` becomes `conversation.input_transcript.turn_marked` and `conversation.handoff.requested` |
| `conversation.handoff.append` with its reply text | Logged (spoken replies are out of scope for this shim) |

The plain WebSocket transport (`[realtime] transport = "websocket"`, used by app-server clients rather than the CLI) also works against `ws://127.0.0.1:8765/v1`, in both the v1 and v2 message shapes. Realtime v3 (`/live`) is not implemented.

How the shim knows what Codex expects, with citations into the Codex source, is in [DECISION.md](DECISION.md).

## What has been verified

Every check under `tests/` was run against the live Flux API, in Docker and in a host venv, and the trimmed output is under `evidence/`:

- `wav_to_flux.py` on the sample clip: one `EndOfTurn`, at confidence 0.820 with `eot_threshold` 0.8 and 0.778 with the Flux default of 0.7. The clip's filler pauses ("um", "uh") peaked at 0.340 and never ended the turn.
- `e2e_fake_codex.py`: the full WebRTC call, sideband WebSocket, caption deltas, `turn_marked`, and one `handoff.requested` carrying the finished transcript, ending in `RESULT: PASS`.
- `ws_transport_check.py` in the v1 shape, the v2 transcription shape, and the v2 realtime shape that hands off through a `background_agent` function call.
- `hardening_check.py` and `unit_check.py`, as described in the table above.

Not yet recorded: a session with a real microphone and the Codex TUI. The headless checks drive the shim with a fake Codex client built from the same source citations. The log sequence in [step 3](#3-talk) is what that session should produce, and the 10-minute test above is the matching check for turn detection.

## Privacy and keys

- `DEEPGRAM_API_KEY` is read from the environment only. Nothing in this repo stores it, and Docker gets it with `-e DEEPGRAM_API_KEY`.
- Codex sends its own auth headers to the shim (it thinks it is talking to OpenAI). The shim ignores and never logs them.
- The shim binds to `127.0.0.1` by default. Do not expose it beyond your machine.
- Requests carrying a browser Origin other than localhost get 403, so a web page you visit cannot open Flux sessions on your key.
- In Docker the shim binds `0.0.0.0` inside the container, and `compose.yaml` publishes it on `127.0.0.1:8765` only, so other machines on your network cannot reach it. A bare `docker run -p 8765:8765` would publish it on every interface; use `-p 127.0.0.1:8765:8765`.
- `-v` turns on debug logging for the shim. It does not print your API key.

## Files

- `shim/flux_client.py`: Flux STT WebSocket client, 80 ms chunking, TurnInfo parsing
- `shim/server.py`: the Codex-facing realtime server (WebRTC, sideband, and WebSocket transport)
- `tests/wav_to_flux.py`: stream a WAV to Flux and print turn events
- `tests/e2e_fake_codex.py`: fake Codex client, full protocol check (`--shim` targets a running shim)
- `tests/ws_transport_check.py`: plain WebSocket transport check (`--dialect v1|v2`, `--session-type`)
- `tests/hardening_check.py`: the hardening checks (`--only` runs a subset)
- `tests/unit_check.py`: offline session-logic checks
- `evidence/`: trimmed output of the live runs recorded for this repo
- `audio/spacewalk-16k.wav`: Deepgram's public 25.9 s sample clip ([dpgr.am/spacewalk.wav](https://dpgr.am/spacewalk.wav), a NASA spacewalk interview), resampled to 16 kHz mono with ffmpeg. NASA audio is generally not subject to copyright in the US; see [NASA's media usage guidelines](https://www.nasa.gov/nasa-brand-center/images-and-media/).
- `Dockerfile`, `compose.yaml`: container image and one-service-at-a-time run recipes
- `run-codex-with-flux.sh`: starts `codex` with the two endpoint overrides
- `DECISION.md`: how Codex voice works on the wire, cited to the Codex source, and why the shim is built the way it is

## License

[MIT](LICENSE).

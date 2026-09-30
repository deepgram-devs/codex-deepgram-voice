# BG-4 social kit: Codex CLI voice on Deepgram Flux

Drafts only. Nothing here has been posted. Handle: coreylweathers. Repo link: https://github.com/deepgram-devs/codex-deepgram-voice (private until Corey flips it public; [verify] visibility before posting).

## LinkedIn draft (190 words)

I made Deepgram Flux the speech layer for Codex CLI voice mode, so you can press F8, describe a task out loud, and Codex starts on it the moment you finish talking.

Codex added voice at DevDay this week. Under the hood it opens a WebRTC call to OpenAI's realtime backend and listens on a sideband WebSocket for transcript and handoff events. Two experimental keys in config.toml let you point both endpoints somewhere else, so I wrote a small local server that speaks that protocol with Flux underneath. Codex ships the microphone audio, the shim resamples it to 16 kHz and streams 80 ms chunks to Flux, and Flux's end-of-turn model decides when you are done. That decision becomes the handoff event Codex was already waiting for.

The part I care about is the turn detection. In a 26-second test clip full of "um" and "uh" pauses, Flux held the turn open through every one of them and closed it once, at 0.91 confidence, with the complete sentence intact.

Setup is two config lines and one container. Code and the source-cited protocol notes are in the repo: https://github.com/deepgram-devs/codex-deepgram-voice

## X draft (under 280 characters)

Codex CLI voice now runs on Deepgram Flux on my machine. Press F8, talk through the task, Flux calls the end of turn, Codex gets to work. No fork, just a small local shim speaking Codex's realtime protocol with Flux underneath. Notes and code: https://github.com/deepgram-devs/codex-deepgram-voice

Character count: 243 without the URL plus a 23-character t.co link puts it at 267. [verify] with a live counter before posting.

## Optional X thread

1. Codex voice opens a WebRTC call to /v1/realtime/calls and a sideband WebSocket to /v1/realtime. Both base URLs are overridable from ~/.codex/config.toml (experimental_realtime_webrtc_call_base_url and experimental_realtime_ws_base_url). That is the whole opening.

2. The shim answers the WebRTC offer with aiortc, decodes the Opus track, resamples to 16 kHz mono, and streams 80 ms chunks to wss://api.deepgram.com/v2/listen with model=flux-general-en.

3. Flux sends TurnInfo events. Update becomes a live caption delta in Codex. EndOfTurn becomes conversation.input_transcript.turn_marked plus conversation.handoff.requested, and that handoff is what starts the Codex turn.

4. Turn detection settings: eot_threshold 0.8 and eot_timeout_ms 6000 (Flux defaults are 0.7 and 5000). Dictating a coding task has thinking pauses, so I want more certainty before the turn closes. On the test clip the filler pauses stayed under 0.25 confidence and the real end scored 0.91.

5. Build lesson: Codex appends caption deltas and never rewrites them, Flux revises its last word or two. The shim only forwards the prefix that stayed stable across two Updates and flushes the tail at EndOfTurn. The task text Codex acts on is always the exact final transcript.

6. Everything is validated in Docker with a fake Codex client that does the same WebRTC offer, multipart call, and sideband handshake the real CLI does, against live Flux. The real F8 session runs the shim on the host because WebRTC media does not reach a container on Docker Desktop for macOS.

## Demo shot list (30 to 60 seconds)

1. (0 to 5 s) Terminal, `python -m shim.server` starting, the log line that prints the two config.toml keys.
2. (5 to 12 s) Split view: `~/.codex/config.toml` with the two `experimental_realtime_*_base_url` lines highlighted.
3. (12 to 20 s) Codex CLI open, press F8, the voice footer appears. Shim log shows "call created" and "WebRTC connection state: connected".
4. (20 to 40 s) Speak a real task with a deliberate two-second pause mid-sentence ("Add a retry to the upload client... with exponential backoff, cap it at five attempts"). Shim log scrolls Update events with low eot_conf during the pause; Codex shows the live caption.
5. (40 to 48 s) Stop talking. Shim log prints "EndOfTurn conf=0.8x -> Codex: <task>". Codex shows the transcript and starts the turn (tool calls appear).
6. (48 to 60 s) Cut to the diff Codex produced, then back to the shim log line "Codex reply for handoff_...". End on the repo README.

## Assets to capture

- Screen recording of the shot list above (terminal at 120 columns, dark theme, shim log on the left, Codex on the right)
- Terminal GIF of `docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/e2e_fake_codex.py audio/spacewalk-16k.wav` ending in RESULT: PASS
- Screenshot of `~/.codex/config.toml` with the two keys
- Repo link and a link to DECISION.md for the protocol notes
- Optional: a short clip of the 10-minute cut-off test from the README, showing a long pause that did not end the turn

## Claims that need a number, with sources

- "closed it once, at 0.91 confidence": `evidence/docker-e2e-fake-codex.txt` and `evidence/host-e2e-fake-codex.txt` (EndOfTurn conf=0.910 at eot_threshold 0.8). [verify] against the final evidence files before posting.
- "filler pauses stayed under 0.25": `evidence/host-wav-to-flux-defaults.txt`, the Update lines for turn 0 (max observed 0.249 mid-speech, then 0.396 right before the end). [verify]
- "Flux defaults are 0.7 and 5000": Deepgram docs, End-of-Turn Detection Parameters (developers.deepgram.com/docs/flux/configuration).
- "Codex added voice at DevDay this week": the goal prompt says Sep 29, 2026. [verify] against OpenAI's announcement before posting.
- "Two experimental keys in config.toml": `codex-rs/config/src/config_toml.rs:431-439` in the Codex source; the keys are marked experimental by OpenAI and may change.
- The real F8 session with a microphone was not run in this build (no mic or OpenAI login in the build environment). [verify] by running the README step 5 before posting any claim about the live TUI experience.

# BG-4 done-when checklist with evidence

All runs on 2026-09-30 against the live Deepgram API (`DEEPGRAM_API_KEY` from the shell, passed to Docker with `-e DEEPGRAM_API_KEY`). Trimmed output for every run is under `evidence/`; request ids were stripped. Image: `codex-flux-voice` built from this repo's `Dockerfile` on Docker Desktop (Docker 29.8.1). Test audio: `audio/spacewalk-16k.wav`, the public dpgr.am spacewalk clip (25.9 s, one speaker, many filler pauses) resampled to 16 kHz mono PCM with ffmpeg.

## 1. Speaking a task in Codex CLI gets Codex working on it, with Flux doing the transcription

Status: protocol path verified headlessly end to end against live Flux; the microphone plus real Codex TUI run needs Corey to verify (no microphone and no OpenAI login in this build environment).

What was verified, in Docker, with a fake Codex client that performs the exact sequence the CLI performs (each step cited to the Codex source in DECISION.md, question 2):

```
$ docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/e2e_fake_codex.py audio/spacewalk-16k.wav
POST http://127.0.0.1:<port>/v1/realtime/calls?intent=quicksilver&architecture=avas -> 201, Location: /v1/realtime/calls/rtc_...
sideband connected: ws://127.0.0.1:<port>/v1/realtime?call_id=rtc_...&intent=quicksilver
session.updated id=sess_...
input_transcript.delta #1 ... (live captions)
input_transcript.turn_marked: "Yeah. As as much as, um, it's worth celebrating, uh, the first, uh, spacewalk, ..."
handoff.requested id=handoff_... input_transcript="Yeah. As as much as, ..."      <- this event starts the Codex turn
RESULT: PASS
```

Full output: `evidence/docker-e2e-fake-codex.txt` (61 caption deltas, 1 turn_marked, 1 handoff, Flux EndOfTurn at confidence 0.910 with eot_threshold 0.8, exit 0). The same check on the host venv: `evidence/host-e2e-fake-codex.txt` (63 caption deltas, 1 turn_marked, 1 handoff, Flux EndOfTurn at confidence 0.910 with eot_threshold 0.8).

The plain WebSocket transport, which app-server clients use instead of WebRTC, was checked in both message shapes: `evidence/docker-ws-transport-v1.txt` (quicksilver shape: 60 deltas, 1 `turn_marked`, 1 `handoff.requested`, PASS) and `evidence/docker-ws-transport-v2.txt` (transcription shape: 60 deltas, 1 `input_audio_transcription.completed`, `speech_started` emitted, no handoff by design, PASS); host copies in `evidence/host-ws-transport-v1.txt` and `-v2.txt`, both PASS.

Why this counts as "Codex starts working": in Codex core the fan-out task routes `conversation.handoff.requested.input_transcript` into a turn (`codex-rs/core/src/realtime_conversation.rs:1764-1772`, `1850-1870`). The shim sends exactly that event with the Flux EndOfTurn transcript. Codex's own reply path (`conversation.handoff.append`) was exercised too: the fake client sent one and the shim logged "Codex reply for handoff_...".

Needs Corey to verify (about 3 minutes, README steps 3 to 5):

1. `. .venv/bin/activate && DEEPGRAM_API_KEY=... python -m shim.server -v`
2. Add the two `experimental_realtime_*_base_url` lines to `~/.codex/config.toml` (user level, not project level).
3. Run `codex`, press `F8`, say "Create a file called hello.txt that says hello from Flux", stop talking.
4. Expect in the shim log: `call created`, `WebRTC connection state: connected`, `first audio frame: 48000 Hz`, `turn 0 StartOfTurn`, then `EndOfTurn conf=0.8x -> Codex: Create a file called hello.txt ...`, then `Codex reply for handoff_...`. Expect in Codex: the caption, then the turn running and the file created.
5. If the WebRTC state never reaches `connected`, run the shim with `-v` and check that the answer SDP carried a host candidate for a local interface; VPN utun interfaces can add candidates, the cap in `codex-voice-host` is 32.

## 2. Turn detection does not cut me off mid-thought in a 10-minute session

Status: the model behavior is verified on a 26-second clip with dense filler pauses; the 10-minute spoken session needs Corey to verify with the README procedure. Settings chosen: `eot_threshold=0.8`, `eot_timeout_ms=6000`, eager end-of-turn off (reasoning in DECISION.md, "Flux turn-detection settings").

Evidence from Docker (`evidence/docker-wav-to-flux-eot0.8.txt`):

```
$ docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/wav_to_flux.py audio/spacewalk-16k.wav --realtime --eot 0.8 --timeout-ms 6000
url: wss://api.deepgram.com/v2/listen?model=flux-general-en&encoding=linear16&sample_rate=16000&eot_threshold=0.8&eot_timeout_ms=6000
[  1.05s] TurnInfo StartOfTurn    turn=0 eot_conf=0.022 'Yeah.'
... 110 TurnInfo Update events for turn 0; highest end_of_turn_confidence during speech: 0.395 ...
[ 28.72s] TurnInfo EndOfTurn      turn=0 eot_conf=0.820 "Yeah. As as much as, um, it's worth celebrating, uh, the first, uh, spacewalk, um, with..."
EndOfTurn events: 1
exit=0
```

The speaker says "um" or "uh" nine times in the clip and pauses at each one; none of those pauses crossed the threshold, so the whole thought arrived as one turn. With Flux defaults on the host (`evidence/host-wav-to-flux-defaults.txt`, eot_threshold 0.7) the result was the same single turn at confidence 0.778, and mid-speech confidence peaked at 0.396.

What the shim cannot prove without a person: a 10-minute session with real thinking pauses of two to four seconds. The README section "10-minute does it cut me off test" gives the procedure and the knobs (`--eot-threshold`, `--eot-timeout-ms`) to turn if it does.

## 3. The README explains setup in under 5 minutes

Status: met. `README.md` has five numbered steps: build the image and run the Flux check (about 1 minute, mostly the image build), run the headless protocol check (about 1 minute), create the host venv (about 1 minute on this machine), add two config lines, press F8. Every command in it was run in this session; the Docker ones are recorded under `evidence/`. The image build from a cold cache took under three minutes here and is a one-time cost.

## 4. social/BG-4.md has the social kit, including a 60-second shot list

Status: met. `social/BG-4.md` contains a 190-word LinkedIn draft, a 243-character X draft plus a six-post thread, a six-shot list timed to 60 seconds, the assets to capture, and every number-bearing claim tagged with its evidence file or marked [verify].

## Guardrails

- No upstream Codex PR, fork, or comment; the Codex clone was read only.
- `DEEPGRAM_API_KEY` appears in no file; Docker receives it through `-e DEEPGRAM_API_KEY`.
- Notion: only BG-4's Status and Run log were touched.
- This repo has no remote yet; nothing was pushed. The draft PR waits on Corey to choose a remote.

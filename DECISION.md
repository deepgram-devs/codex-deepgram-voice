# Spike: can Deepgram Flux be the speech layer for Codex CLI voice?

Spike run on 2026-09-30 against the Codex source at `../codex` (commit `67727e7`, "Allow managed requirements to disable the Windows MXC sandbox (#49642)", 2026-09-30). Every answer cites a file and line in that checkout, a command run here, or a public doc. Third-party writeups are quoted as claims, not as source.

## Short version

- Codex CLI voice (`/voice`, F8) is a WebRTC call to `POST {base}/realtime/calls` plus a sideband WebSocket to `{base}/realtime?call_id=...`, speaking OpenAI's "quicksilver v1" realtime event set. Both base URLs are overridable from `~/.codex/config.toml`, so a local shim can stand in for OpenAI's realtime backend. That is path A, and it is what this repo builds.
- Transcription happens server side inside the realtime session. There is no local speech engine, no Wispr Flow, and no provider setting in the current source.
- The one clean text entry point besides voice is the app-server JSON-RPC `turn/start` (and `codex exec` for one-shot). Hooks cannot inject a prompt.

## Question 1: does `[realtime]` accept a custom URL or endpoint?

Yes, through two top-level keys in `config.toml` (the `[realtime]` table itself only selects the session kind):

| Key | What it overrides | Source |
| --- | --- | --- |
| `experimental_realtime_webrtc_call_base_url` | Base URL for the WebRTC call creation `POST .../realtime/calls` | `codex-rs/config/src/config_toml.rs:436-439`; applied in `codex-rs/core/src/realtime_conversation.rs:1355-1360` |
| `experimental_realtime_ws_base_url` | Base URL for the realtime WebSocket (standalone `/v1/realtime`, or the sideband that joins a WebRTC call) | `codex-rs/config/src/config_toml.rs:431-435`; applied at `realtime_conversation.rs:1340-1353` |
| `experimental_realtime_ws_model` | Model query parameter on the WebSocket URL | `config_toml.rs:440-442` |
| `[realtime] version = "v1" \| "v2" \| "v3"`, `type = "conversational" \| "transcription"`, `transport = "webrtc" \| "websocket"`, `voice` | Session selection | `config_toml.rs:443-446`, structs at `config_toml.rs:625-661` |
| `[audio] microphone`, `[audio] speaker` | Local device names | `config_toml.rs:427-429`, `665-668` |

Constraints that matter for the shim:

- Both URL overrides are on the project-local config denylist, so a project `.codex/config.toml` cannot set them. They still work from the user (`~/.codex/config.toml`), system, managed, and runtime (`-c`) config layers (`codex-rs/config/src/loader/mod.rs:84-102`).
- The TUI always starts voice over WebRTC: it creates an SDP offer in the `codex-voice-host` helper and submits `AppCommand::RealtimeConversationStart { offer_sdp }` (`codex-rs/tui/src/chatwidget/realtime.rs:414-417`, `codex-rs/tui/src/app/thread_routing.rs:946-966`). Core then forces realtime **v1** for WebRTC (`realtime_conversation.rs:1362-1366`) and rejects `type = "transcription"` for WebRTC (`validate_avas_webrtc_start`, `realtime_conversation.rs:1463-1476`). So for the CLI, `[realtime] type/version/transport` do not switch the TUI to a plain WebSocket; the shim has to accept the WebRTC call.
- The WebSocket transport (`ConversationStartTransport::Websocket`) exists for app-server clients and honors `[realtime] version` and `type` (`realtime_conversation.rs:1362-1363`, `1574-1577`), but it requires an OpenAI API key (`realtime_api_key`, `realtime_conversation.rs:1873-1896`).
- URL normalization: a base URL ending in `/v1` gets `/realtime` appended for the WebSocket (`codex-rs/codex-api/src/endpoint/realtime_websocket/methods.rs:1249-1283`); the call path is `{base}/realtime/calls` (`codex-rs/codex-api/src/endpoint/realtime_call.rs:62-64`). Plain `http://` and `ws://` are accepted (the crate's own tests use `http://127.0.0.1:...`, `methods.rs:2122`).
- OpenAI's public docs do not document any of these keys. The config reference (`https://learn.chatgpt.com/docs/config-file/config-reference`, redirected from `developers.openai.com/codex/config-reference`) mentions `experimental_realtime_ws_base_url` only as a key that project config cannot set, and `docs/config.md` in the repo has no realtime section.

Answer: yes. The endpoint is configurable, which makes path A possible.

## Question 2: what protocol and events does Codex expect from the server?

Codex speaks three wire dialects, selected by `RealtimeEventParser` (`methods.rs`, `protocol_v1.rs`, `protocol_v2.rs`, `protocol_frameless_bidi.rs`). The CLI voice path uses **v1 over WebRTC plus a sideband WebSocket**:

1. `POST {call_base}/realtime/calls?intent=quicksilver&architecture=avas` with `Content-Type: multipart/form-data; boundary=codex-realtime-call-boundary`, parts `sdp` (`application/sdp`, the offer) and `session` (JSON). Response body is the SDP answer; the call id is parsed from the `Location` header, either an `rtc_...` segment or a UUID (`realtime_call.rs:150-175`, `280-292`, query at `210-222`; the call site is `codex-rs/core/src/client.rs:675-707`).
2. The offer comes from `codex-voice-host`: one Opus track, 48 kHz, payload type 111, 20 ms frames, mono-encoded (`codex-rs/voice-host/src/audio_track.rs:19-54, 93-119`), plus a client-created data channel named `oai-events` that it ignores (`codex-rs/voice-host/src/transport.rs:113-134`). The answer may carry at most 32 ICE candidates (`transport.rs:25`, `185-186`).
3. Sideband WebSocket: `GET {ws_base}/realtime?call_id=<id>&intent=quicksilver` with headers `openai-alpha: quicksilver=v1`, `x-session-id`, Codex session headers, and auth headers (`methods.rs:1234-1246`, `realtime_conversation.rs:1899-1930`). Core sends `{"type":"session.update","session":{"type":"quicksilver","instructions":...,"audio":{...}}}` right after connecting (`methods.rs:1096-1110`, session shape `methods_v1.rs:51-79`).
4. Server events Codex parses in v1 (`protocol_v1.rs:12-90`):
   - `session.updated` with `session.id` (required to log the session; `protocol_common.rs:27-44`)
   - `conversation.input_transcript.delta` `{delta}` for live user captions
   - `conversation.input_transcript.turn_marked` `{transcript}` when the user turn is done
   - `conversation.handoff.requested` `{handoff_id, item_id, input_transcript}`: **this is what makes Codex start working**. Core wraps `input_transcript` in `<realtime_delegation>` and routes it as a turn (`realtime_conversation.rs:1764-1772`, `1850-1870`).
   - `conversation.output_audio.delta`, `conversation.output_transcript.delta`, `response.output_audio_transcript.done`, `conversation.item.added/done`, `error`
5. Client messages the server will receive (`realtime_websocket/protocol.rs:50-85`): `conversation.handoff.append {handoff_id, output_text}` carries Codex's reply text back for the voice model to speak; `input_audio_buffer.append {audio}` (base64 PCM16 at 24 kHz, `methods_common.rs:26`) is used only on the WebSocket transport; `session.close`.

The other two dialects, for completeness: **v2** is the public Realtime API shape (`session.update` with `session.type = "realtime" | "transcription"`, `input_audio_buffer.append`, `conversation.item.input_audio_transcription.delta/.completed`, handoff as a `background_agent` function call in `conversation.item.done`; `methods_v2.rs:75-160`, `protocol_v2.rs:24-80`, transcription fixture at `methods.rs:2908-3020`). **v3** ("frameless bidi", `/v1/live`) uses `input_transcript.added`, `turn.done`, `delegation.created` (`protocol_frameless_bidi.rs:15-35`). In v2 transcription mode nothing triggers a Codex turn; transcripts only land in history (`normalized_session_mode`, `methods_common.rs:28-37`; the event fan-out routes only `HandoffRequested`, `realtime_conversation.rs:1764-1772`).

## Question 3: can the push-to-talk layer choose a speech provider? Is it Wispr Flow?

No provider choice exists, and Wispr is not in the source. `grep -rIl -i wispr ~/code/codex` (all file types, excluding `node_modules`) returns nothing, and neither does `push_to_talk`, `push-to-talk`, or `dictat` in `codex-rs/` (only `Feature::InAppDictation`, a desktop-app flag at `codex-rs/features/src/lib.rs:271-274`). Voice is `/voice` (`codex-rs/tui/src/slash_command.rs:42,136`) bound to `F8` with `Ctrl+X` for mute (`codex-rs/tui/src/keymap.rs:1659-1660`); it is a hands-free realtime call, not push-to-talk.

The claim comes from a third-party knowledge base (`https://codex.danielvaughan.com/2026/04/17/codex-cli-voice-realtime-webrtc-push-to-talk/`), which says the v0.105.0 "Layer 1" push-to-talk "uses the Wispr Flow transcription engine" and enabled with `[features] voice_transcription = true`. That flag does not exist in the current source (`grep -rn voice_transcription codex-rs` is empty), so whatever shipped in February was replaced by the realtime voice path. Speech recognition now happens inside OpenAI's realtime session; the client ships Opus audio and receives transcript events. The public voice page (`https://learn.chatgpt.com/docs/features/voice`, redirected from `developers.openai.com/codex/features/voice`) names no transcription model or provider.

## Question 4: is there a hook or plugin point that can supply transcribed text?

- **Realtime sideband (used here):** `conversation.handoff.requested` with `input_transcript` starts a Codex turn; this is the supported way voice text enters Codex today (`realtime_conversation.rs:1764-1772`).
- **App-server JSON-RPC:** `turn/start` (`codex-rs/app-server-protocol/src/protocol/common.rs:1038-1042`) submits a user turn to a running thread; `thread/realtime/start` exists too (`codex-rs/app-server/src/request_processors.rs:257-259`). A standalone composer (path C) would use `codex app-server` over stdio or `codex exec "<text>"` (`docs/exec.md`).
- **Hooks:** `UserPromptSubmit` can block a prompt or add `additional_contexts`, but cannot replace or inject the prompt text (`codex-rs/hooks/src/events/user_prompt_submit.rs:36-41`). The 12 hook events are listed in `codex-rs/hooks/src/lib.rs:23-49`.
- **`Op::RealtimeConversationSpeech`:** appends *speakable* text to the running voice session, meant for TTS, not for input (`codex-rs/protocol/src/protocol.rs:615-616`).

## Path chosen: A, local shim

Path A is possible because question 1 is yes, and it is the only path that keeps the experience inside Codex CLI: press F8, talk, Codex starts. The shim (`shim/`) is a small Python server that:

1. Answers `POST /v1/realtime/calls` with an aiortc WebRTC answer and a `Location: /v1/realtime/calls/rtc_<id>` header.
2. Decodes the incoming Opus track, resamples to 16 kHz mono PCM16, and streams 80 ms (2560 byte) chunks to `wss://api.deepgram.com/v2/listen?model=flux-general-en&encoding=linear16&sample_rate=16000`.
3. Serves the sideband `GET /v1/realtime?call_id=...`, replies to `session.update` with `session.updated`, forwards Flux `Update` events as `conversation.input_transcript.delta`, and on Flux `EndOfTurn` sends `conversation.input_transcript.turn_marked` followed by `conversation.handoff.requested` with the finished transcript. Codex then runs the turn.
4. Also accepts the plain WebSocket transport (`input_audio_buffer.append` base64 PCM at 24 kHz) in v1 and v2 shapes, so app-server clients configured with `transport = "websocket"` work against the same Flux session.

One protocol mismatch worth knowing: Codex appends caption deltas and never rewrites them (`codex-rs/core/src/realtime_history.rs:340-370`), while Flux `Update` events carry the whole running transcript and may revise the last word or two. The shim forwards only the prefix that stayed identical across two consecutive Updates, cut at a word boundary, and flushes the rest at `EndOfTurn`. A revised word can therefore appear once in the live caption ("Yeah. Is As as much as..." in the spacewalk run), but the text Codex acts on is the exact `EndOfTurn` transcript in `input_transcript`, so the task itself is never wrong. `--no-live-captions` turns the deltas off entirely.

Path B (a Rust fork) is not needed: no Codex code has to change. Path C stays as the fallback (app-server turn/start or codex exec, question 4); it is not built here.

Codex config that points the CLI at the shim (user-level file, or the same two keys as `-c` overrides, which is what `run-codex-with-flux.sh` does; not a project `.codex/config.toml`):

```toml
# ~/.codex/config.toml
experimental_realtime_webrtc_call_base_url = "http://127.0.0.1:8765/v1"
experimental_realtime_ws_base_url = "ws://127.0.0.1:8765/v1"
```

## Testing limits

No OpenAI login or microphone was available where this was written, so the real TUI-to-shim run is not recorded. The shim's WebRTC and sideband behavior was exercised headlessly with a fake Codex client built from the same source citations, inside Docker and in a host venv, against the live Flux API; the README lists what each check covers and the trimmed output is under `evidence/`. The auth headers Codex sends to the shim (ChatGPT bearer token, account id, attestation) are never logged or stored; the shim ignores them.

## Flux turn-detection settings

Chosen for the shim: `eot_threshold=0.8`, `eot_timeout_ms=6000`, no `eager_eot_threshold`.

Why: dictating a task to a coding agent is full of thinking pauses ("um", "so, uh"). In the live spacewalk run with Flux defaults (`evidence/host-wav-to-flux-defaults.txt`), `end_of_turn_confidence` peaked at 0.340 during the filler pauses, the last `Update` before the real end read 0.396, and the single `EndOfTurn` fired at 0.778, above the default 0.7. Raising the threshold to 0.8 asks for slightly more acoustic and semantic certainty before ending the turn, and lengthening the timeout to 6 s keeps a long silent pause from force-ending the turn while you think. Both are overridable with `--eot-threshold` and `--eot-timeout-ms` on the shim, and the README has the 10-minute test procedure. Flux's eager end-of-turn is left off because Codex has no way to take back a turn once the handoff is sent. Parameter names, defaults, and ranges come from Deepgram's End-of-Turn Detection Parameters page (`https://developers.deepgram.com/docs/flux/configuration`, fetched 2026-09-30): `eot_threshold` range 0.5 to 1.0, default 0.7; `eot_timeout_ms` range 500 to 60000, default 5000; `eager_eot_threshold` range 0.3 to 0.9, off by default. That page's own guidance is that 0.8 to 0.9 means "higher certainty required before ending a turn, fewer false positives, slightly increased latency", which is the trade a coding-task dictation wants.

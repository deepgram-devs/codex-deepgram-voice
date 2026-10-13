"""Check the plain WebSocket transport (no WebRTC): Codex app-server style audio append.

Codex clients configured with `[realtime] transport = "websocket"` open {ws}/realtime, send
`session.update`, then stream `input_audio_buffer.append` frames of base64 PCM16 mono at 24 kHz
(codex-api methods_common.rs REALTIME_AUDIO_SAMPLE_RATE). This script does that against the
shim for both dialects and reports the transcript events that come back:

  --dialect v1   session.type "quicksilver": expects conversation.input_transcript.turn_marked
                 and conversation.handoff.requested
  --dialect v2   session.type "transcription": expects
                 conversation.item.input_audio_transcription.completed (no handoff, by design)
  --dialect v2 --session-type realtime
                 the conversational v2 session (methods_v2.rs SessionType::Realtime): also
                 expects a conversation.item.done function_call named background_agent whose
                 arguments are JSON {"prompt": <transcript>}, the v2 handoff Codex acts on
  --dialect v3   realtime v3 ("frameless bidi", Codex 0.162) on {ws}/live with no call id. Sends
                 one input_audio.append before session.update and checks it is ignored (no
                 error, no close). Sends session.update in the v3 shape (no `type`,
                 methods_frameless_bidi.rs:46-93), requires session.started with a session id as
                 the first event, streams input_audio.append frames, and expects
                 input_transcript.added captions, exactly one turn.done (role user) and one
                 delegation.created whose input_text equals the turn.done transcript. Any other
                 event type fails the check. Then sends session.close and expects the shim to
                 close the socket.

Usage:
  DEEPGRAM_API_KEY=... python tests/ws_transport_check.py audio/spacewalk-16k.wav --dialect v1
  DEEPGRAM_API_KEY=... python tests/ws_transport_check.py audio/spacewalk-16k.wav --dialect v3
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import sys
import time
import wave
from pathlib import Path
from typing import Optional

import aiohttp
import av
import numpy as np
from aiohttp import web

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shim.server import Settings, build_app  # noqa: E402

FRAME_MS = 20
CODEX_RATE = 24_000


def wav_to_pcm24k(path: str) -> bytes:
    with wave.open(path, "rb") as wf:
        rate, ch, width = wf.getframerate(), wf.getnchannels(), wf.getsampwidth()
        raw = wf.readframes(wf.getnframes())
    if (ch, width) != (1, 2):
        raise SystemExit("need 16-bit mono WAV")
    samples = np.frombuffer(raw, dtype="<i2").reshape(1, -1)
    frame = av.AudioFrame.from_ndarray(samples, format="s16", layout="mono")
    frame.sample_rate = rate
    resampler = av.AudioResampler(format="s16", layout="mono", rate=CODEX_RATE)
    out = b"".join(f.to_ndarray().tobytes() for f in resampler.resample(frame))
    out += b"".join(f.to_ndarray().tobytes() for f in resampler.resample(None))
    return out + b"\x00" * (CODEX_RATE * 2 * 3)  # 3 s trailing silence


def v2_handoff_prompt(ev: dict) -> Optional[str]:
    """The prompt of a background_agent function_call item, or None if `ev` is not one."""
    item = ev.get("item") or {}
    if item.get("type") != "function_call" or item.get("name") != "background_agent":
        return None
    try:
        args = json.loads(item.get("arguments") or "")
    except json.JSONDecodeError:
        return None
    prompt = args.get("prompt") if isinstance(args, dict) else None
    return prompt if isinstance(prompt, str) else None


def delegation_text(ev: dict) -> Optional[str]:
    """The concatenated input_text of a v3 delegation.created item (protocol_frameless_bidi.rs:75-99),
    or None if the item is not a client delegation."""
    item = ev.get("item") if isinstance(ev.get("item"), dict) else {}
    if item.get("type") != "delegation" or item.get("target") != "client" or not item.get("id"):
        return None
    content = item.get("content") if isinstance(item.get("content"), list) else []
    texts = [p.get("text") for p in content
             if isinstance(p, dict) and p.get("type") == "input_text" and isinstance(p.get("text"), str)]
    return "".join(texts) if texts else None


async def run_v3(wav: str, ws_url: str) -> bool:
    """The v3 plain WebSocket transport: GET {ws}/live with no call id."""
    # The session JSON Codex 0.162 sends (methods_frameless_bidi.rs:46-93). There is no `type`.
    session = {"instructions": "x", "audio": {"output": {"voice": "cove"}},
               "delegation": {"type": "client"}, "model": "gpt-live-1-codex"}
    pcm = wav_to_pcm24k(wav)
    step = CODEX_RATE * 2 * FRAME_MS // 1000
    started: list[dict] = []
    deltas: list[str] = []
    done: list[dict] = []
    delegations: list[dict] = []
    problems: list[str] = []
    closed_after_close = False
    t0 = time.monotonic()

    async with aiohttp.ClientSession() as http:
        async with http.ws_connect(ws_url, heartbeat=20) as ws:
            print(f"connected {ws_url} (dialect v3, no call id)")

            # Audio before session.update must be ignored: no event, no error, no close.
            await ws.send_json({"type": "input_audio.append",
                                "audio": base64.b64encode(pcm[:step]).decode()})
            try:
                early = await asyncio.wait_for(ws.receive(), 1.0)
            except asyncio.TimeoutError:
                early = None
            if early is not None:
                desc = (json.loads(early.data).get("type") if early.type == aiohttp.WSMsgType.TEXT
                        else f"socket {early.type.name}")
                problems.append(f"input_audio.append before session.update answered with {desc}")
                print(f"[{time.monotonic() - t0:6.2f}s] before session.update: {desc}")
            else:
                print(f"[{time.monotonic() - t0:6.2f}s] input_audio.append before session.update: "
                      "ignored (nothing within 1 s, socket open)")

            if not ws.closed:
                await ws.send_json({"type": "session.update", "session": session})

            async def sender() -> None:
                for i in range(0, len(pcm), step):
                    await ws.send_json({"type": "input_audio.append",
                                        "audio": base64.b64encode(pcm[i:i + step]).decode()})
                    await asyncio.sleep(FRAME_MS / 1000)

            send_task: Optional[asyncio.Task] = None
            deadline = None
            while not ws.closed and not problems:
                try:
                    # wait_for, not receive(timeout=): aiohttp restarts that timeout on every
                    # heartbeat PING, so a silent shim would hang the check forever.
                    msg = await asyncio.wait_for(
                        ws.receive(), (max(0.1, deadline - time.monotonic()) if deadline else 90))
                except asyncio.TimeoutError:
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    problems.append(f"socket {msg.type.name} before the session finished")
                    break
                ev = json.loads(msg.data)
                kind = ev.get("type")
                ts = f"[{time.monotonic() - t0:6.2f}s]"
                if not started and kind != "session.started":
                    problems.append(f"{kind} arrived before session.started")
                    print(f"{ts} {kind} before session.started")
                    break
                if kind == "session.started":
                    sid = (ev.get("session") or {}).get("id")
                    print(f"{ts} session.started id={sid}")
                    if started:
                        problems.append("a second session.started")
                    elif not sid:
                        problems.append("session.started without session.id")
                    started.append(ev)
                    send_task = asyncio.create_task(sender())
                elif kind == "input_transcript.added":
                    text = (ev.get("item") or {}).get("text")
                    if not isinstance(text, str):
                        problems.append("input_transcript.added without item.text")
                    else:
                        deltas.append(text)
                        if len(deltas) <= 3:
                            print(f"{ts} input_transcript.added: {text!r}")
                elif kind == "turn.done":
                    turn = ev.get("turn") or {}
                    print(f"{ts} turn.done role={turn.get('role')}: {str(turn.get('transcript', ''))[:100]!r}")
                    done.append(ev)
                elif kind == "delegation.created":
                    text = delegation_text(ev)
                    print(f"{ts} delegation.created id={(ev.get('item') or {}).get('id')}: {(text or '')[:100]!r}")
                    if text is None:
                        problems.append("delegation.created item is not a client delegation "
                                        "(needs id, type=delegation, target=client)")
                    delegations.append(ev)
                    deadline = time.monotonic() + 3
                elif kind == "error":
                    problems.append(f"error: {ev.get('message') or (ev.get('error') or {}).get('message')}")
                    print(f"{ts} error: {problems[-1]}")
                else:
                    problems.append(f"unknown event type {kind!r}")
                    print(f"{ts} unknown event {kind}")
                if send_task is not None and send_task.done() and deadline is None:
                    deadline = time.monotonic() + 10
            if send_task is not None:
                send_task.cancel()

            if not ws.closed:
                await ws.send_json({"type": "session.close"})
                print(f"[{time.monotonic() - t0:6.2f}s] sent session.close")
                try:
                    while True:
                        msg = await asyncio.wait_for(ws.receive(), 5)
                        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                                        aiohttp.WSMsgType.CLOSED):
                            closed_after_close = True
                            print(f"[{time.monotonic() - t0:6.2f}s] shim closed the socket "
                                  f"(code {ws.close_code})")
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            kind = json.loads(msg.data).get("type")
                            problems.append(f"{kind} after session.close")
                except asyncio.TimeoutError:
                    problems.append("socket still open 5 s after session.close")

    transcript = ((done[0].get("turn") or {}).get("transcript") if len(done) == 1 else None)
    print("\nsummary:")
    print(f"  session.started: {len(started)}, input_transcript.added: {len(deltas)}, "
          f"turn.done: {len(done)}, delegation.created: {len(delegations)}, "
          f"closed after session.close: {closed_after_close}")
    print(f"  problems: {problems}")
    ok = (len(started) == 1 and not problems and bool(deltas) and len(done) == 1
          and (done[0].get("turn") or {}).get("role") == "user"
          and isinstance(transcript, str) and bool(transcript.strip())
          and len(delegations) == 1 and delegation_text(delegations[0]) == transcript
          and closed_after_close)
    if ok:
        print(f"  delegation text equals turn.done transcript ({len(transcript)} chars)")
    return ok


async def run(wav: str, dialect: str, session_type: str, shim_base: Optional[str]) -> int:
    runner = None
    if shim_base is None:
        runner = web.AppRunner(build_app(Settings()))
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        shim_base = f"http://127.0.0.1:{runner.addresses[0][1]}/v1"
    if dialect == "v3":
        try:
            ok = await run_v3(wav, "ws" + shim_base[len("http"):] + "/live")
        finally:
            if runner is not None:
                await runner.cleanup()
        print("RESULT:", "PASS" if ok else "FAIL")
        return 0 if ok else 1
    ws_url = "ws" + shim_base[len("http"):] + "/realtime"

    if dialect == "v1":
        session = {"type": "quicksilver", "instructions": "x",
                   "audio": {"input": {"format": {"type": "audio/pcm", "rate": CODEX_RATE}}, "output": {"voice": "marin"}}}
        ws_url += "?intent=quicksilver"
        want_done, want_handoff = "conversation.input_transcript.turn_marked", "conversation.handoff.requested"
    elif session_type == "transcription":
        session = {"type": "transcription",
                   "audio": {"input": {"format": {"type": "audio/pcm", "rate": CODEX_RATE},
                                       "transcription": {"model": "gpt-4o-mini-transcribe"}}}}
        want_done, want_handoff = "conversation.item.input_audio_transcription.completed", None
    else:
        # Trimmed from methods_v2.rs session_update_session(Conversational): the shim reads
        # only `type`, the rest is here so the shape matches what Codex sends.
        session = {"type": session_type, "instructions": "x", "output_modalities": ["audio"],
                   "audio": {"input": {"format": {"type": "audio/pcm", "rate": CODEX_RATE},
                                       "transcription": {"model": "gpt-4o-mini-transcribe"},
                                       "turn_detection": {"type": "server_vad"}},
                             "output": {"format": {"type": "audio/pcm", "rate": CODEX_RATE},
                                        "voice": "marin"}},
                   "tools": [{"type": "function", "name": "background_agent",
                              "parameters": {"type": "object",
                                             "properties": {"prompt": {"type": "string"}},
                                             "required": ["prompt"]}}],
                   "tool_choice": "auto"}
        want_done = "conversation.item.input_audio_transcription.completed"
        want_handoff = "conversation.item.done"

    pcm = wav_to_pcm24k(wav)
    step = CODEX_RATE * 2 * FRAME_MS // 1000
    got: dict[str, list] = {"updated": [], "deltas": [], "done": [], "handoff": [], "other": []}
    t0 = time.monotonic()

    async with aiohttp.ClientSession() as http:
        async with http.ws_connect(ws_url, heartbeat=20) as ws:
            print(f"connected {ws_url} (dialect {dialect}, session type {session['type']})")
            await ws.send_json({"type": "session.update", "session": session})

            async def sender() -> None:
                for i in range(0, len(pcm), step):
                    await ws.send_json({"type": "input_audio_buffer.append",
                                        "audio": base64.b64encode(pcm[i:i + step]).decode()})
                    await asyncio.sleep(FRAME_MS / 1000)

            send_task = asyncio.create_task(sender())
            deadline = None
            while True:
                try:
                    # wait_for, not receive(timeout=): aiohttp restarts that timeout on every
                    # heartbeat PING, so a silent shim would hang the check forever.
                    msg = await asyncio.wait_for(
                        ws.receive(), (max(0.1, deadline - time.monotonic()) if deadline else 90))
                except asyncio.TimeoutError:
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    break
                ev = json.loads(msg.data)
                kind = ev.get("type")
                ts = f"[{time.monotonic() - t0:6.2f}s]"
                if kind == "session.updated":
                    got["updated"].append(ev)
                    print(f"{ts} session.updated {ev['session']}")
                elif kind.endswith(".delta"):
                    got["deltas"].append(ev["delta"])
                    if len(got["deltas"]) <= 3:
                        print(f"{ts} {kind}: {ev['delta']!r}")
                elif kind == want_done:
                    got["done"].append(ev)
                    print(f"{ts} {kind}: {ev.get('transcript', '')[:100]!r}")
                    if want_handoff is None:
                        deadline = time.monotonic() + 3
                elif kind == "error":
                    got["other"].append(kind)
                    print(f"{ts} error: {ev.get('error', {}).get('message')}")
                    break  # the shim reported a Flux failure; nothing more will come
                elif kind == want_handoff and dialect == "v2":
                    prompt = v2_handoff_prompt(ev)
                    item = ev.get("item") or {}
                    print(f"{ts} {kind}: {item.get('type')} {item.get('name')} prompt={(prompt or '')[:100]!r}")
                    if prompt is None:
                        got["other"].append(f"{kind} (not a background_agent call)")
                    else:
                        got["handoff"].append(ev)
                        deadline = time.monotonic() + 3
                elif kind == want_handoff:
                    got["handoff"].append(ev)
                    print(f"{ts} {kind}: {ev.get('input_transcript', '')[:100]!r}")
                    deadline = time.monotonic() + 3
                else:
                    got["other"].append(kind)
                    print(f"{ts} {kind}")
                if send_task.done() and deadline is None:
                    deadline = time.monotonic() + 10
            send_task.cancel()
            await ws.send_json({"type": "session.close"})
    if runner is not None:
        await runner.cleanup()

    print("\nsummary:")
    print(f"  session.updated: {len(got['updated'])}, caption deltas: {len(got['deltas'])}, "
          f"done events: {len(got['done'])}, handoffs: {len(got['handoff'])}, other: {sorted(set(got['other']))}")
    ok = bool(got["updated"]) and any(e.get("transcript", "").strip() for e in got["done"])
    if want_handoff is not None:
        ok = ok and bool(got["handoff"])
    if want_handoff is not None and dialect == "v2":
        ok = ok and all((v2_handoff_prompt(e) or "").strip() for e in got["handoff"])
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wav")
    ap.add_argument("--dialect", choices=["v1", "v2", "v3"], default="v1")
    ap.add_argument("--session-type", default="transcription",
                    help='v2 session.type: "transcription" (default) or "realtime" (expects a handoff)')
    ap.add_argument("--shim", default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    return asyncio.run(run(args.wav, args.dialect, args.session_type, args.shim))


if __name__ == "__main__":
    raise SystemExit(main())

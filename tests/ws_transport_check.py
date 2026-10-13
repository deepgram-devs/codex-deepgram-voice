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

Usage:
  DEEPGRAM_API_KEY=... python tests/ws_transport_check.py audio/spacewalk-16k.wav --dialect v1
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


async def run(wav: str, dialect: str, session_type: str, shim_base: Optional[str]) -> int:
    runner = None
    if shim_base is None:
        runner = web.AppRunner(build_app(Settings()))
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        shim_base = f"http://127.0.0.1:{runner.addresses[0][1]}/v1"
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
    ap.add_argument("--dialect", choices=["v1", "v2"], default="v1")
    ap.add_argument("--session-type", default="transcription",
                    help='v2 session.type: "transcription" (default) or "realtime" (expects a handoff)')
    ap.add_argument("--shim", default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    return asyncio.run(run(args.wav, args.dialect, args.session_type, args.shim))


if __name__ == "__main__":
    raise SystemExit(main())

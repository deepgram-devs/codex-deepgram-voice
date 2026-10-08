"""Headless end-to-end check: a fake Codex CLI talks to the shim, the shim talks to live Flux.

The fake client does exactly what Codex does when you press F8 (citations in DECISION.md, Q2):
  1. builds a WebRTC offer with one Opus audio track (from a WAV file) and an `oai-events`
     data channel, like codex-voice-host;
  2. POSTs it as multipart (`sdp` + `session`) to {base}/realtime/calls?intent=quicksilver
     &architecture=avas and reads the call id from the Location header, like codex-api;
  3. applies the SDP answer, then opens the sideband WebSocket
     {ws}/realtime?call_id=<id>&intent=quicksilver with `openai-alpha: quicksilver=v1`
     and sends `session.update` with a `quicksilver` session, like codex-core;
  4. waits for `session.updated`, `conversation.input_transcript.delta`,
     `conversation.input_transcript.turn_marked`, and `conversation.handoff.requested`,
     and answers the handoff with `conversation.handoff.append` the way Codex would.

Exit code 0 when a handoff with a non-empty transcript arrived. By default the shim runs
in-process on a random port so the whole check fits in one container.

Usage:
  DEEPGRAM_API_KEY=... python tests/e2e_fake_codex.py audio/spacewalk-16k.wav [--shim http://127.0.0.1:8765/v1]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import tempfile
import time
import wave
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web
from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaPlayer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shim.server import Settings, build_app  # noqa: E402

log = logging.getLogger("fake-codex")

# The session Codex core sends for a v1 (quicksilver) WebRTC call: methods_v1.rs:51-79.
QUICKSILVER_SESSION = {
    "type": "quicksilver",
    "instructions": "You are Codex voice. Delegate coding work to the background agent.",
    "audio": {"input": {"format": {"type": "audio/pcm", "rate": 24000}}, "output": {"voice": "marin"}},
}


def padded_copy(wav_path: str, pad_seconds: float) -> str:
    """Copy a WAV with trailing silence so Flux can close the last turn on its own."""
    with wave.open(wav_path, "rb") as src:
        params = src.getparams()
        frames = src.readframes(src.getnframes())
    out = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    with wave.open(out.name, "wb") as dst:
        dst.setparams(params)
        dst.writeframes(frames)
        dst.writeframes(b"\x00" * int(params.framerate * params.sampwidth * params.nchannels * pad_seconds))
    return out.name


async def run(wav: str, shim_base: Optional[str], pad_seconds: float, wait_after: float) -> int:
    runner: Optional[web.AppRunner] = None
    if shim_base is None:
        app = build_app(Settings())
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        shim_base = f"http://127.0.0.1:{port}/v1"
        log.info("shim started in-process at %s", shim_base)
    ws_base = "ws" + shim_base[len("http"):]

    summary = {"session_updated": False, "deltas": 0, "turn_marked": [], "handoffs": [], "codex_ok": False}
    t0 = time.monotonic()
    pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
    player = MediaPlayer(padded_copy(wav, pad_seconds))
    pc.addTrack(player.audio)
    pc.createDataChannel("oai-events")  # codex-voice-host creates this and ignores it
    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    async with aiohttp.ClientSession() as http:
        form = aiohttp.FormData()
        form.add_field("sdp", pc.localDescription.sdp, content_type="application/sdp")
        form.add_field("session", json.dumps(QUICKSILVER_SESSION), content_type="application/json")
        url = f"{shim_base}/realtime/calls?intent=quicksilver&architecture=avas"
        async with http.post(url, data=form) as resp:
            body = await resp.text()
            location = resp.headers.get("Location", "")
            print(f"POST {url} -> {resp.status}, Location: {location}, answer {len(body)} bytes")
            if resp.status >= 300 or "rtc_" not in location:
                print("call creation failed:", body[:200])
                return 2
        call_id = location.rsplit("/", 1)[-1]
        await pc.setRemoteDescription(RTCSessionDescription(sdp=body, type="answer"))

        ws_url = f"{ws_base}/realtime?call_id={call_id}&intent=quicksilver"
        headers = {"openai-alpha": "quicksilver=v1", "x-session-id": "thread_fake_codex"}
        async with http.ws_connect(ws_url, headers=headers, heartbeat=20) as ws:
            print(f"sideband connected: {ws_url}")
            await ws.send_json({"type": "session.update", "session": QUICKSILVER_SESSION})
            deadline = None
            while True:
                timeout = None if deadline is None else max(0.1, deadline - time.monotonic())
                try:
                    # wait_for, not receive(timeout=): aiohttp restarts that timeout on every
                    # heartbeat PING, so a silent shim would hang the check forever.
                    msg = await asyncio.wait_for(ws.receive(), timeout or 90)
                except asyncio.TimeoutError:
                    print("done waiting")
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    print(f"sideband closed ({msg.type})")
                    break
                event = json.loads(msg.data)
                kind = event.get("type")
                ts = f"[{time.monotonic() - t0:6.2f}s]"
                if kind == "session.updated":
                    summary["session_updated"] = True
                    print(f"{ts} session.updated id={event['session'].get('id')}")
                elif kind == "conversation.input_transcript.delta":
                    summary["deltas"] += 1
                    if summary["deltas"] <= 5 or summary["deltas"] % 10 == 0:
                        print(f"{ts} input_transcript.delta #{summary['deltas']}: {event['delta']!r}")
                elif kind == "conversation.input_transcript.turn_marked":
                    summary["turn_marked"].append(event["transcript"])
                    print(f"{ts} input_transcript.turn_marked: {event['transcript'][:100]!r}")
                elif kind == "conversation.handoff.requested":
                    summary["handoffs"].append(event)
                    print(f"{ts} handoff.requested id={event['handoff_id']} input_transcript="
                          f"{event['input_transcript'][:100]!r}")
                    # Codex would now run the turn; it streams its reply back like this.
                    await ws.send_json({"type": "conversation.handoff.append",
                                        "handoff_id": event["handoff_id"],
                                        "output_text": "Starting on it."})
                    summary["codex_ok"] = True
                    deadline = time.monotonic() + wait_after
                elif kind == "error":
                    print(f"{ts} error: {event.get('error', {}).get('message')}")
                    break  # the shim reported a Flux failure; nothing more will come
                else:
                    print(f"{ts} {kind}: {json.dumps(event)[:160]}")
                if pc.connectionState in ("failed", "closed"):
                    print("peer connection ended")
                    break
            await ws.send_json({"type": "session.close"})

    await pc.close()
    if runner is not None:
        await runner.cleanup()

    print("\nsummary:")
    print(f"  session.updated received: {summary['session_updated']}")
    print(f"  live caption deltas:      {summary['deltas']}")
    print(f"  turn_marked events:       {len(summary['turn_marked'])}")
    print(f"  handoff.requested events: {len(summary['handoffs'])}")
    for h in summary["handoffs"]:
        print(f"    -> Codex would start on: {h['input_transcript']}")
    ok = summary["session_updated"] and any(h["input_transcript"].strip() for h in summary["handoffs"])
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wav", help="16 kHz mono PCM WAV to play as the microphone")
    ap.add_argument("--shim", default=None, help="external shim base, e.g. http://127.0.0.1:8765/v1")
    ap.add_argument("--pad-seconds", type=float, default=3.0, help="trailing silence appended to the WAV")
    ap.add_argument("--wait-after", type=float, default=6.0, help="seconds to keep listening after a handoff")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    for noisy in ("aioice", "aiortc", "websockets"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return asyncio.run(run(args.wav, args.shim, args.pad_seconds, args.wait_after))


if __name__ == "__main__":
    raise SystemExit(main())

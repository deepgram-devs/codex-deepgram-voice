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

That is realtime v1, what Codex 0.159 speaks (the default, `--dialect v1`). With
`--dialect v3` it speaks realtime v3 ("frameless bidi"), what Codex 0.162 speaks (DECISION.md,
"Realtime v3"):
  2. POSTs the same multipart offer to {base}/live with the v3 session JSON (no `type`) and
     reads the call id from the Location header, like realtime_call.rs;
  3. opens the sideband {ws}/live/<call_id> with `openai-alpha: quicksilver=v2` and sends
     nothing, like ws_methods.rs for a WebRTC sideband;
  4. requires `session.started` as the first event, then collects `input_transcript.added`,
     one `turn.done` (role user), and one `delegation.created` whose text equals the turn.done
     transcript, and answers with `delegation.context.append` and `session.close`.

Exit code 0 when a handoff with a non-empty transcript arrived (and, for v3, the checks
above held). By default the shim runs in-process on a random port so the whole check fits in
one container.

Usage:
  DEEPGRAM_API_KEY=... python tests/e2e_fake_codex.py audio/spacewalk-16k.wav \
      [--dialect v1|v3] [--shim http://127.0.0.1:8765/v1]
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

# The session Codex core sends for a v3 (frameless bidi) call: methods_frameless_bidi.rs:46-93.
LIVE_SESSION = {
    "instructions": "You are Codex voice. Delegate coding work to the background agent.",
    "audio": {"output": {"voice": "cove"}},
    "delegation": {"type": "client"},
    "model": "gpt-live-1-codex",
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


async def run(wav: str, shim_base: Optional[str], pad_seconds: float, wait_after: float,
              dialect: str = "v1") -> int:
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

    summary = {"session_updated": False, "deltas": 0, "turn_marked": [], "handoffs": [], "codex_ok": False,
               "first_event": None, "v3_shape_errors": [], "closed_after_close": False}
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
        v3 = dialect == "v3"
        form.add_field("session", json.dumps(LIVE_SESSION if v3 else QUICKSILVER_SESSION),
                       content_type="application/json")
        # v3 carries no query string (realtime_call.rs:213-224)
        url = f"{shim_base}/live" if v3 else f"{shim_base}/realtime/calls?intent=quicksilver&architecture=avas"
        async with http.post(url, data=form) as resp:
            body = await resp.text()
            location = resp.headers.get("Location", "")
            print(f"POST {url} -> {resp.status}, Location: {location}, answer {len(body)} bytes")
            if resp.status >= 300 or "rtc_" not in location:
                print("call creation failed:", body[:200])
                return 2
        call_id = location.rsplit("/", 1)[-1]
        await pc.setRemoteDescription(RTCSessionDescription(sdp=body, type="answer"))

        if v3:
            ws_url = f"{ws_base}/live/{call_id}"
            headers = {"openai-alpha": "quicksilver=v2", "x-session-id": "thread_fake_codex"}
        else:
            ws_url = f"{ws_base}/realtime?call_id={call_id}&intent=quicksilver"
            headers = {"openai-alpha": "quicksilver=v1", "x-session-id": "thread_fake_codex"}
        async with http.ws_connect(ws_url, headers=headers, heartbeat=20) as ws:
            print(f"sideband connected: {ws_url}")
            if not v3:  # a v3 WebRTC sideband sends no session.update (ws_methods.rs:1082-1112)
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
                if summary["first_event"] is None:
                    summary["first_event"] = kind
                ts = f"[{time.monotonic() - t0:6.2f}s]"
                if v3:
                    if kind == "session.started":
                        summary["session_updated"] = True
                        print(f"{ts} session.started id={event.get('session', {}).get('id')}")
                    elif kind == "input_transcript.added":
                        summary["deltas"] += 1
                        if summary["deltas"] <= 5 or summary["deltas"] % 10 == 0:
                            print(f"{ts} input_transcript.added #{summary['deltas']}: {event['item']['text']!r}")
                    elif kind == "turn.done":
                        turn_info = event.get("turn", {})
                        if turn_info.get("role") != "user":
                            summary["v3_shape_errors"].append(f"turn.done role {turn_info.get('role')!r}")
                        summary["turn_marked"].append(turn_info.get("transcript", ""))
                        print(f"{ts} turn.done role={turn_info.get('role')}: {turn_info.get('transcript', '')[:100]!r}")
                    elif kind == "delegation.created":
                        item = event.get("item", {})
                        content = item.get("content") or [{}]
                        text = content[0].get("text", "") if content[0].get("type") == "input_text" else ""
                        if item.get("type") != "delegation" or item.get("target") != "client":
                            summary["v3_shape_errors"].append(f"delegation.created item {item}")
                        if not summary["turn_marked"] or text != summary["turn_marked"][-1]:
                            summary["v3_shape_errors"].append("delegation.created text differs from turn.done")
                        summary["handoffs"].append({"handoff_id": item.get("id"), "input_transcript": text})
                        print(f"{ts} delegation.created id={item.get('id')} text={text[:100]!r}")
                        await ws.send_json({"type": "delegation.context.append",
                                            "delegation_item_id": item.get("id"), "channel": "commentary",
                                            "content": [{"type": "input_text", "text": "Starting on it."}]})
                        summary["codex_ok"] = True
                        deadline = time.monotonic() + wait_after
                    elif kind == "error":
                        print(f"{ts} error: {event.get('message')}")
                        break
                    else:
                        print(f"{ts} {kind}: {json.dumps(event)[:160]}")
                    if pc.connectionState in ("failed", "closed"):
                        print("peer connection ended")
                        break
                    continue
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
            if dialect == "v3":
                # ws_methods.rs:575-594: Codex treats any close other than 1000 as an error, and
                # sideband.rs reconnects on a socket that merely stays open; the shim must close.
                try:
                    while True:
                        msg = await asyncio.wait_for(ws.receive(), 5)
                        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                                        aiohttp.WSMsgType.CLOSED):
                            summary["closed_after_close"] = ws.close_code == 1000
                            print(f"shim closed the sideband after session.close (code {ws.close_code})")
                            break
                except asyncio.TimeoutError:
                    summary["v3_shape_errors"].append("sideband still open 5 s after session.close")

    await pc.close()
    if runner is not None:
        await runner.cleanup()

    print("\nsummary:")
    if dialect == "v3":
        print(f"  first event:               {summary['first_event']}")
        print(f"  session.started received:  {summary['session_updated']}")
        print(f"  input_transcript.added:    {summary['deltas']}")
        print(f"  turn.done events:          {len(summary['turn_marked'])}")
        print(f"  delegation.created events: {len(summary['handoffs'])}")
        print(f"  closed with 1000 after session.close: {summary['closed_after_close']}")
        for problem in summary["v3_shape_errors"]:
            print(f"  shape problem: {problem}")
    else:
        print(f"  session.updated received: {summary['session_updated']}")
        print(f"  live caption deltas:      {summary['deltas']}")
        print(f"  turn_marked events:       {len(summary['turn_marked'])}")
        print(f"  handoff.requested events: {len(summary['handoffs'])}")
    for h in summary["handoffs"]:
        print(f"    -> Codex would start on: {h['input_transcript']}")
    ok = summary["session_updated"] and any(h["input_transcript"].strip() for h in summary["handoffs"])
    if dialect == "v3":
        ok = (ok and summary["first_event"] == "session.started" and not summary["v3_shape_errors"]
              and len(summary["turn_marked"]) == 1 and len(summary["handoffs"]) == 1
              and summary["closed_after_close"])
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wav", help="16 kHz mono PCM WAV to play as the microphone")
    ap.add_argument("--dialect", choices=["v1", "v3"], default="v1",
                    help="v1: POST /realtime/calls + /realtime?call_id= (Codex 0.159); "
                         "v3: POST /live + /live/<call_id> (Codex 0.162)")
    ap.add_argument("--shim", default=None, help="external shim base, e.g. http://127.0.0.1:8765/v1")
    ap.add_argument("--pad-seconds", type=float, default=3.0, help="trailing silence appended to the WAV")
    ap.add_argument("--wait-after", type=float, default=6.0, help="seconds to keep listening after a handoff")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    for noisy in ("aioice", "aiortc", "websockets"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return asyncio.run(run(args.wav, args.shim, args.pad_seconds, args.wait_after, args.dialect))


if __name__ == "__main__":
    raise SystemExit(main())

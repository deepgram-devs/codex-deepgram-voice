"""Hardening checks for the shim. No valid key needed.

  missing-key    `python -m shim.server` with no DEEPGRAM_API_KEY exits 1 with a message
  origin         a browser-style Origin from another site gets 403 on the WebSocket and on
                 POST /realtime/calls; no Origin (Codex CLI) and a localhost Origin connect
  orphan         a call whose sideband never attaches is dropped after the expiry window
  bad-key        runs the real shim with `-v` and a fake key against live Flux, drives a
                 WebSocket session, and expects one Codex `error` event (Flux rejects the key)
                 and no trace of the key anywhere in the shim's output
  late-sideband  the path Codex uses: a WebRTC call whose audio flows (and fails against Flux
                 with the fake key) before the sideband attaches ~3 s later; expects the one
                 queued `error` to arrive after `session.updated`, one Flux connect attempt,
                 and no trace of the key in the shim's output

Usage:
  python tests/hardening_check.py [--key FAKE_KEY_SENTINEL] [--only origin ...]
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import socket
import subprocess
import sys
from pathlib import Path

import aiohttp
from aiohttp import web
from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaPlayer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shim.server import Settings, build_app  # noqa: E402

CODEX_RATE = 24_000
WAV = ROOT / "audio" / "spacewalk-16k.wav"
V1_SESSION = {"type": "quicksilver", "instructions": "x",
              "audio": {"input": {"format": {"type": "audio/pcm", "rate": CODEX_RATE}}}}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def start_in_process(settings: Settings) -> tuple[web.AppRunner, str]:
    runner = web.AppRunner(build_app(settings))
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, f"http://127.0.0.1:{runner.addresses[0][1]}"


def check_missing_key() -> bool:
    env = {k: v for k, v in os.environ.items() if k != "DEEPGRAM_API_KEY"}
    proc = subprocess.run([sys.executable, "-m", "shim.server", "--port", str(free_port())],
                          cwd=ROOT, env=env, capture_output=True, text=True, timeout=20)
    print(f"  exit code {proc.returncode}, stderr: {proc.stderr.strip()!r}")
    return proc.returncode == 1 and "DEEPGRAM_API_KEY" in proc.stderr


async def ws_status(http: aiohttp.ClientSession, url: str, origin: str | None) -> int:
    headers = {"Origin": origin} if origin else {}
    try:
        async with http.ws_connect(url, headers=headers) as ws:
            await ws.close()
            return 101
    except aiohttp.WSServerHandshakeError as exc:
        return exc.status


async def check_origin() -> bool:
    runner, base = await start_in_process(Settings())
    ws_url = "ws" + base[len("http"):] + "/v1/realtime"
    ok = True
    async with aiohttp.ClientSession() as http:
        for origin, want in [(None, 101), ("http://localhost:3000", 101), ("http://127.0.0.1:8765", 101),
                             ("http://[::1]:8765", 101), ("https://evil.example", 403),
                             ("http://localhost.evil.example", 403), ("null", 403)]:
            got = await ws_status(http, ws_url, origin)
            print(f"  WS   Origin={origin!r:32} -> {got} (want {want})")
            ok &= got == want
        for origin, want in [("https://evil.example", 403), (None, 400)]:
            headers = {"Origin": origin} if origin else {}
            # Not an SDP offer: a request that gets past the Origin check fails with 400.
            async with http.post(f"{base}/v1/realtime/calls", data="x", headers=headers) as resp:
                print(f"  POST Origin={origin!r:32} -> {resp.status} (want {want})")
                ok &= resp.status == want
    await runner.cleanup()
    return ok


async def session_count(http: aiohttp.ClientSession, base: str) -> int:
    async with http.get(f"{base}/healthz") as resp:
        return (await resp.json())["sessions"]


async def check_orphan(timeout_s: float = 1.5) -> bool:
    runner, base = await start_in_process(Settings(orphan_call_timeout_s=timeout_s))
    pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
    pc.addTransceiver("audio", direction="sendonly")
    pc.createDataChannel("oai-events")
    await pc.setLocalDescription(await pc.createOffer())
    async with aiohttp.ClientSession() as http:
        async with http.post(f"{base}/v1/realtime/calls", data=pc.localDescription.sdp,
                             headers={"Content-Type": "application/sdp"}) as resp:
            print(f"  POST /v1/realtime/calls -> {resp.status}, Location {resp.headers.get('Location')}")
            created = resp.status == 201
        before = await session_count(http, base)
        await asyncio.sleep(timeout_s + 1.0)  # never open the sideband
        after = await session_count(http, base)
    print(f"  sessions right after the call: {before}, after {timeout_s + 1.0:.1f}s: {after}")
    await pc.close()
    await runner.cleanup()
    return created and before == 1 and after == 0


async def start_shim_process(key: str) -> tuple[asyncio.subprocess.Process, int]:
    """The real shim at -v with `key`, so its whole output can be searched for the key."""
    port = free_port()
    env = dict(os.environ, DEEPGRAM_API_KEY=key, PYTHONUNBUFFERED="1")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "shim.server", "--port", str(port), "-v", cwd=ROOT, env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    async with aiohttp.ClientSession() as http:
        for _ in range(50):  # wait for the listener
            try:
                async with http.get(f"http://127.0.0.1:{port}/healthz"):
                    break
            except aiohttp.ClientConnectionError:
                await asyncio.sleep(0.2)
    return proc, port


async def stop_shim_process(proc: asyncio.subprocess.Process, key: str,
                            errors: list[dict]) -> tuple[str, int, bool]:
    """Stops the shim; returns (its output, Flux connect attempts, whether the key leaked)."""
    proc.terminate()
    out = (await proc.communicate())[0].decode("utf-8", "replace")
    connects = out.count("connecting to Flux STT")
    leaked = key in out or any(key in json.dumps(e) for e in errors)
    print(f"  shim output: {len(out.splitlines())} lines at -v, Flux connect attempts: {connects}, "
          f"key in output: {leaked}")
    if leaked:
        for line in out.splitlines():
            if key in line:
                print("  LEAK:", line.replace(key, "<KEY>"))
    return out, connects, leaked


async def check_bad_key(key: str) -> bool:
    proc, port = await start_shim_process(key)
    errors: list[dict] = []
    try:
        async with aiohttp.ClientSession() as http:
            async with http.ws_connect(f"ws://127.0.0.1:{port}/v1/realtime?intent=quicksilver") as ws:
                await ws.send_json({"type": "session.update", "session": V1_SESSION})
                silence = base64.b64encode(b"\x00" * (CODEX_RATE * 2 // 50)).decode()  # 20 ms
                for _ in range(50):  # 1 s of audio: the shim must not retry Flux per frame
                    await ws.send_json({"type": "input_audio_buffer.append", "audio": silence})
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.receive(), 8)  # see ws_transport_check.py
                    except asyncio.TimeoutError:
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        break
                    ev = json.loads(msg.data)
                    if ev.get("type") == "error":
                        errors.append(ev)
                        print(f"  Codex got: {json.dumps(ev)}")
                await ws.send_json({"type": "session.close"})
    finally:
        _, connects, leaked = await stop_shim_process(proc, key, errors)
    message = errors[0]["error"]["message"] if errors else ""
    return len(errors) == 1 and "API key" in message and connects == 1 and not leaked


async def check_late_sideband(key: str, late_s: float = 3.0) -> bool:
    proc, port = await start_shim_process(key)
    base = f"http://127.0.0.1:{port}/v1"
    pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
    player = MediaPlayer(str(WAV))  # like e2e_fake_codex.py: one audio track plus oai-events
    pc.addTrack(player.audio)
    pc.createDataChannel("oai-events")
    await pc.setLocalDescription(await pc.createOffer())
    kinds: list[str] = []
    errors: list[dict] = []
    try:
        async with aiohttp.ClientSession() as http:
            form = aiohttp.FormData()
            form.add_field("sdp", pc.localDescription.sdp, content_type="application/sdp")
            form.add_field("session", json.dumps(V1_SESSION), content_type="application/json")
            async with http.post(f"{base}/realtime/calls?intent=quicksilver", data=form) as resp:
                answer, location = await resp.text(), resp.headers.get("Location", "")
                print(f"  POST /v1/realtime/calls -> {resp.status}")
            if resp.status != 201:
                return False
            await pc.setRemoteDescription(RTCSessionDescription(sdp=answer, type="answer"))
            # Audio flows now; Flux rejects the key while there is no sideband to tell.
            await asyncio.sleep(late_s)
            print(f"  WebRTC state after {late_s:.0f}s: {pc.connectionState}; attaching the sideband")
            call_id = location.rsplit("/", 1)[-1]
            ws_url = f"ws://127.0.0.1:{port}/v1/realtime?call_id={call_id}&intent=quicksilver"
            async with http.ws_connect(ws_url, headers={"openai-alpha": "quicksilver=v1"}) as ws:
                await ws.send_json({"type": "session.update", "session": V1_SESSION})
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.receive(), 4)
                    except asyncio.TimeoutError:
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        break
                    ev = json.loads(msg.data)
                    kinds.append(ev.get("type"))
                    if ev.get("type") == "error":
                        errors.append(ev)
                        print(f"  Codex got: {json.dumps(ev)}")
                await ws.send_json({"type": "session.close"})
    finally:
        await pc.close()
        out, connects, leaked = await stop_shim_process(proc, key, errors)
    print(f"  sideband events in order: {kinds}")
    rejected_at, attached_at = out.find("rejected the API key"), out.find("sideband WebSocket attached")
    queued = 0 <= rejected_at < attached_at  # Flux failed while there was no sideband to tell
    print(f"  Flux rejected the key before the sideband attached: {queued}")
    error_after_update = ("session.updated" in kinds and "error" in kinds
                          and kinds.index("error") > kinds.index("session.updated"))
    return (queued and len(errors) == 1 and error_after_update and "API key" in errors[0]["error"]["message"]
            and connects == 1 and not leaked)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--key", default="FAKE_KEY_SENTINEL", help="invalid key for the bad-key check")
    ap.add_argument("--only", nargs="*", choices=["missing-key", "origin", "orphan", "bad-key", "late-sideband"])
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    checks = {
        "missing-key": check_missing_key,
        "origin": lambda: asyncio.run(check_origin()),
        "orphan": lambda: asyncio.run(check_orphan()),
        "bad-key": lambda: asyncio.run(check_bad_key(args.key)),
        "late-sideband": lambda: asyncio.run(check_late_sideband(args.key)),
    }
    results = {}
    for name, fn in checks.items():
        if args.only and name not in args.only:
            continue
        print(f"[{name}]")
        results[name] = fn()
        print(f"  -> {'PASS' if results[name] else 'FAIL'}")
    ok = all(results.values())
    print("RESULT:", "PASS" if ok else "FAIL", results)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

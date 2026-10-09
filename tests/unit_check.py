"""Offline checks for the shim's session logic. No key, no network, no Docker.

  prefix        stable_prefix() cuts at word boundaries and never indexes past either string
  origin        origin_allowed(): no Origin passes, localhost passes, other hosts and "null" fail
  ordering      Flux events that arrive before Codex's session.update wait in the queue and go
                out after session.updated, in order; later events go out directly
  captions      a Flux revision of an already-shown word appends from the stable prefix, and
                the handoff carries the exact final transcript; an empty EndOfTurn sends nothing
  double-close  FluxSession.close() called twice at once closes the socket once and raises nothing
  immediate-close
                FluxSession.close() right after start(), before the reader's first tick, and
                close() while the reader task is failing: the socket is closed exactly once,
                nothing is raised, and `closed` is set
  tail          the samples a resampler holds back reach Flux: input_audio_buffer.commit and
                session close drain the WebSocket resampler, the WebRTC pump drains its own, and
                audio after a commit still flows; before session.update, audio is ignored
  queue-cap     Flux events queued before session.update stop at the configured cap and the
                session closes instead of growing without bound
  loopback      is_loopback(): 127.0.0.1, 127.1.2.3, ::1, localhost pass; 0.0.0.0, LAN
                addresses, hostnames, and "" fail
  registry      a call whose sideband never attaches is dropped at the expiry window and leaves
                no entry behind (/healthz reports 0, a late sideband gets 404); an oversized
                offer is refused with 413 before it is read

Usage:
  python tests/unit_check.py
"""

from __future__ import annotations

import asyncio
import base64
import json
import sys
from pathlib import Path

import av
import numpy as np
from aiohttp.test_utils import TestClient, TestServer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shim.flux_client import FluxSession, TurnInfo  # noqa: E402
from shim.server import (  # noqa: E402
    Settings, VoiceSession, build_app, is_loopback, origin_allowed, stable_prefix,
)


class FakeSideband:
    """Stands in for aiohttp's WebSocketResponse: records every JSON event sent."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.closed = False

    async def send_json(self, event: dict) -> None:
        self.sent.append(event)

    async def close(self) -> None:
        self.closed = True


class FakeFluxSocket:
    """Stands in for the websockets client connection: records sends, yields no messages."""

    def __init__(self) -> None:
        self.closes = 0
        self.sent: list[bytes | str] = []

    async def send(self, data) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        self.closes += 1

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class StubFlux:
    """Stands in for FluxSession on a VoiceSession: counts the PCM bytes it is fed."""

    def __init__(self) -> None:
        self.fed = bytearray()
        self.closing = False
        self.closes = 0

    async def feed(self, pcm: bytes) -> None:
        self.fed.extend(pcm)

    async def close(self, wait_s: float = 8.0) -> None:
        self.closing = True
        self.closes += 1


def pcm_b64(samples: int) -> str:
    return base64.b64encode(np.zeros(samples, dtype="<i2").tobytes()).decode()


def turn(event: str, transcript: str, index: int = 0, conf: float = 0.5) -> TurnInfo:
    return TurnInfo(event=event, transcript=transcript, turn_index=index, end_of_turn_confidence=conf)


def check_prefix() -> bool:
    cases = [
        ("", "Yeah.", ""),
        ("Yeah.", "Yeah.", "Yeah."),
        ("Yeah. Is", "Yeah. As as much", "Yeah."),
        ("Yeah. As", "Yeah. As as much", "Yeah. As"),
        ("Yeah. As as much", "Yeah. As", "Yeah. As"),
        ("abc", "abd", ""),
    ]
    ok = True
    for a, b, want in cases:
        got = stable_prefix(a, b)
        if got != want:
            print(f"  stable_prefix({a!r}, {b!r}) = {got!r}, wanted {want!r}")
            ok = False
    return ok


def check_origin() -> bool:
    cases = [(None, True), ("http://localhost:3000", True), ("http://127.0.0.1", True),
             ("http://[::1]:8080", True), ("https://example.com", False), ("null", False)]
    ok = True
    for origin, want in cases:
        if origin_allowed(origin) != want:
            print(f"  origin_allowed({origin!r}) != {want}")
            ok = False
    return ok


async def check_ordering() -> bool:
    s = VoiceSession(Settings(), "rtc_test")
    ws = FakeSideband()
    s.ws = ws  # the sideband attached, but Codex has not sent session.update yet
    await s.on_turn(turn("StartOfTurn", ""))
    await s.on_turn(turn("Update", "Create a"))
    await s.on_turn(turn("Update", "Create a file"))
    await s.emit({"type": "error", "error": {"type": "flux_error", "message": "x"}})
    if ws.sent:
        print(f"  {len(ws.sent)} event(s) went out before session.updated: {[e['type'] for e in ws.sent]}")
        return False
    await s.handle_codex_message({"type": "session.update", "session": {"type": "quicksilver"}})
    await s.on_turn(turn("EndOfTurn", "Create a file called hello.txt", conf=0.9))
    types = [e["type"] for e in ws.sent]
    want = ["session.updated", "conversation.input_transcript.delta", "error",
            "conversation.input_transcript.delta", "conversation.input_transcript.turn_marked",
            "conversation.handoff.requested"]
    if types != want:
        print(f"  order was {types}\n  wanted   {want}")
        return False
    if ws.sent[-1]["input_transcript"] != "Create a file called hello.txt" or s.pending:
        print("  handoff transcript or leftover queue wrong")
        return False
    return True


async def check_captions() -> bool:
    s = VoiceSession(Settings(), "rtc_caps")
    ws = FakeSideband()
    s.ws = ws
    await s.handle_codex_message({"type": "session.update", "session": {"type": "quicksilver"}})
    await s.on_turn(turn("StartOfTurn", ""))
    await s.on_turn(turn("Update", "Yeah. Is"))
    await s.on_turn(turn("Update", "Yeah. Is it"))        # "Yeah." is now stable -> shown
    await s.on_turn(turn("Update", "Yeah. As as much"))   # Flux revised "Is" to "As"
    await s.on_turn(turn("Update", "Yeah. As as much as"))
    await s.on_turn(turn("EndOfTurn", "Yeah. As as much as, um, it's worth", conf=0.9))
    deltas = [e["delta"] for e in ws.sent if e["type"].endswith(".delta")]
    shown = "".join(deltas)
    handoff = [e for e in ws.sent if e["type"] == "conversation.handoff.requested"]
    marked = [e for e in ws.sent if e["type"] == "conversation.input_transcript.turn_marked"]
    ok = True
    if len(handoff) != 1 or handoff[0]["input_transcript"] != "Yeah. As as much as, um, it's worth":
        print(f"  handoff wrong: {handoff}")
        ok = False
    if len(marked) != 1 or not shown.endswith("it's worth"):
        print(f"  captions did not reach the final text: {shown!r}")
        ok = False
    before = len(ws.sent)
    await s.on_turn(turn("StartOfTurn", "", index=1))
    await s.on_turn(turn("EndOfTurn", "   ", index=1, conf=0.8))
    if len(ws.sent) != before:
        print(f"  empty EndOfTurn sent {[e['type'] for e in ws.sent[before:]]}")
        ok = False
    return ok


async def check_double_close() -> bool:
    f = FluxSession(lambda info: asyncio.sleep(0))
    sock = FakeFluxSocket()
    f._ws = sock  # type: ignore[assignment]
    f._reader = asyncio.create_task(asyncio.sleep(0.05))
    f._pending.extend(b"\x00" * 100)
    try:
        await asyncio.gather(f.close(wait_s=2.0), f.close(wait_s=2.0))
    except Exception as exc:  # noqa: BLE001
        print(f"  concurrent close raised {type(exc).__name__}: {exc}")
        return False
    if sock.closes != 1 or not f.closed.is_set():
        print(f"  socket closed {sock.closes} time(s); closed event set: {f.closed.is_set()}")
        return False
    if not any(isinstance(m, str) and json.loads(m).get("type") == "CloseStream" for m in sock.sent):
        print("  CloseStream was never sent")
        return False
    return True


async def check_immediate_close() -> bool:
    ok = True
    # start() then close() with no await in between: the reader has not run yet.
    f = FluxSession(lambda info: asyncio.sleep(0))
    sock = FakeFluxSocket()
    f._ws = sock  # type: ignore[assignment]
    f._reader = asyncio.create_task(f._read_loop(sock))  # type: ignore[arg-type]
    try:
        await f.close(wait_s=2.0)
    except Exception as exc:  # noqa: BLE001
        print(f"  immediate close raised {type(exc).__name__}: {exc}")
        ok = False
    if sock.closes != 1 or not f.closed.is_set():
        print(f"  immediate close: socket closed {sock.closes} time(s), closed set {f.closed.is_set()}")
        ok = False
    # The reader task fails: close() must still close the socket once and not raise.
    async def boom() -> None:
        await asyncio.sleep(0)
        raise RuntimeError("reader failed")
    f = FluxSession(lambda info: asyncio.sleep(0))
    sock = FakeFluxSocket()
    f._ws = sock  # type: ignore[assignment]
    f._reader = asyncio.create_task(boom())
    try:
        await f.close(wait_s=2.0)
    except Exception as exc:  # noqa: BLE001
        print(f"  close with a failing reader raised {type(exc).__name__}: {exc}")
        ok = False
    if sock.closes != 1 or not f.closed.is_set():
        print(f"  failing reader: socket closed {sock.closes} time(s), closed set {f.closed.is_set()}")
        ok = False
    return ok


def near(got_bytes: int, samples_in: int, rate_in: int, tolerance: int = 2) -> bool:
    want = samples_in * 16_000 / rate_in
    return abs(got_bytes / 2 - want) <= tolerance


async def check_tail() -> bool:
    ok = True
    s = VoiceSession(Settings(), "rtc_tail")
    ws = FakeSideband()
    s.ws = ws
    s.flux = StubFlux()  # type: ignore[assignment]
    # Audio before session.update is ignored.
    await s.handle_codex_message({"type": "input_audio_buffer.append", "audio": pcm_b64(2400)})
    if s.flux.fed:
        print(f"  {len(s.flux.fed)} bytes fed before session.update")
        ok = False
    await s.handle_codex_message({"type": "session.update", "session": {"type": "quicksilver"}})
    # 1234 samples at 24 kHz -> 822.67 at 16 kHz; the resampler holds some back until drained.
    await s.handle_codex_message({"type": "input_audio_buffer.append", "audio": pcm_b64(1234)})
    before = len(s.flux.fed)
    await s.handle_codex_message({"type": "input_audio_buffer.commit"})
    after = len(s.flux.fed)
    if after <= before:
        print(f"  commit drained nothing ({before} bytes before, {after} after)")
        ok = False
    if not near(after, 1234, 24_000):
        print(f"  commit: {after // 2} samples reached Flux, wanted ~{1234 * 16_000 / 24_000:.0f}")
        ok = False
    # An empty commit is a no-op, and audio keeps flowing after a commit.
    await s.handle_codex_message({"type": "input_audio_buffer.commit"})
    try:
        await s.handle_codex_message({"type": "input_audio_buffer.append", "audio": pcm_b64(1234)})
    except Exception as exc:  # noqa: BLE001
        print(f"  append after commit raised {type(exc).__name__}: {exc}")
        return False
    # Session close drains what the second append left behind (Codex never sends commit).
    await s.close()
    total = len(s.flux.fed)
    if not near(total, 2 * 1234, 24_000):
        print(f"  after close: {total // 2} samples reached Flux, wanted ~{2 * 1234 * 16_000 / 24_000:.0f}")
        ok = False
    if s.flux.closes != 1:
        print(f"  Flux closed {s.flux.closes} time(s) on session close")
        ok = False
    # The WebRTC pump's resampler: a 48 kHz frame that is not a multiple of the output frame.
    s2 = VoiceSession(Settings(), "rtc_tail2")
    s2.flux = StubFlux()  # type: ignore[assignment]
    resampler = av.AudioResampler(format="s16", layout="mono", rate=16_000)
    frame = av.AudioFrame.from_ndarray(np.zeros((1, 1001), dtype="<i2"), format="s16", layout="mono")
    frame.sample_rate = 48_000
    for out in resampler.resample(frame):
        await s2.feed_pcm16_16k(out.to_ndarray().tobytes())
    before = len(s2.flux.fed)
    await s2._drain(resampler)
    after = len(s2.flux.fed)
    if after <= before or not near(after, 1001, 48_000):
        print(f"  WebRTC drain: {before // 2} -> {after // 2} samples, wanted ~{1001 * 16_000 / 48_000:.0f}")
        ok = False
    return ok


async def check_queue_cap() -> bool:
    s = VoiceSession(Settings(pending_max_events=5), "rtc_cap")
    s.ws = FakeSideband()  # attached, but session.update never arrives
    for i in range(8):
        await s.emit({"type": "error", "error": {"type": "flux_error", "message": f"event {i}"}})
    await asyncio.sleep(0.05)  # the overflow close runs as its own task
    if len(s.pending) > 5 or not s.closed:
        print(f"  {len(s.pending)} queued (cap 5), session closed: {s.closed}")
        return False
    s2 = VoiceSession(Settings(pending_max_bytes=200), "rtc_cap2")
    s2.ws = FakeSideband()
    for _ in range(8):
        await s2.emit({"type": "error", "error": {"type": "flux_error", "message": "x" * 60}})
    await asyncio.sleep(0.05)
    if s2.pending_bytes > 200 or not s2.closed:
        print(f"  {s2.pending_bytes} bytes queued (cap 200), session closed: {s2.closed}")
        return False
    return True


def check_loopback() -> bool:
    cases = [("127.0.0.1", True), ("127.1.2.3", True), ("::1", True), ("localhost", True),
             ("0.0.0.0", False), ("::", False), ("192.168.1.5", False), ("example.com", False), ("", False)]
    ok = True
    for host, want in cases:
        if is_loopback(host) != want:
            print(f"  is_loopback({host!r}) != {want}")
            ok = False
    return ok


async def check_registry() -> bool:
    app = build_app(Settings(orphan_call_timeout_s=0.3))
    async with TestClient(TestServer(app)) as client:
        r = await client.post("/v1/realtime/calls", data="v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n",
                              headers={"Content-Type": "application/sdp"})
        status = r.status
        call_id = r.headers.get("Location", "").rsplit("/", 1)[-1]
        during = (await (await client.get("/healthz")).json())["sessions"]
        await asyncio.sleep(0.8)
        after = (await (await client.get("/healthz")).json())["sessions"]
        late = await client.get("/v1/realtime", params={"call_id": call_id})
        late_status = late.status
        big = await client.post("/v1/realtime/calls", data=b"", headers={
            "Content-Type": "application/sdp", "Content-Length": str(64 * 1024 * 1024)})
        big_status = big.status
    ok = True
    if status != 201 or during != 1:
        print(f"  call returned {status} and /healthz saw {during} session(s); wanted 201 and 1")
        ok = False
    if after != 0:
        print(f"  /healthz still reports {after} session(s) after the orphan window")
        ok = False
    if late_status != 404:
        print(f"  sideband for the expired call returned {late_status}, wanted 404")
        ok = False
    if big_status != 413:
        print(f"  oversized offer returned {big_status}, wanted 413")
        ok = False
    return ok


async def main() -> int:
    results = {
        "prefix": check_prefix(),
        "origin": check_origin(),
        "ordering": await check_ordering(),
        "captions": await check_captions(),
        "double-close": await check_double_close(),
        "immediate-close": await check_immediate_close(),
        "tail": await check_tail(),
        "queue-cap": await check_queue_cap(),
        "loopback": check_loopback(),
        "registry": await check_registry(),
    }
    for name, ok in results.items():
        print(f"[{name}] {'PASS' if ok else 'FAIL'}")
    print("RESULT:", "PASS" if all(results.values()) else "FAIL", results)
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

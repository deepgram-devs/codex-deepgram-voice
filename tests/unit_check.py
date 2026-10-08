"""Offline checks for the shim's session logic. No key, no network, no Docker.

  prefix        stable_prefix() cuts at word boundaries and never indexes past either string
  origin        origin_allowed(): no Origin passes, localhost passes, other hosts and "null" fail
  ordering      Flux events that arrive before Codex's session.update wait in the queue and go
                out after session.updated, in order; later events go out directly
  captions      a Flux revision of an already-shown word appends from the stable prefix, and
                the handoff carries the exact final transcript; an empty EndOfTurn sends nothing
  double-close  FluxSession.close() called twice at once closes the socket once and raises nothing
  registry      a call whose sideband never attaches is dropped at the expiry window and leaves
                no entry behind (/healthz reports 0, a late sideband gets 404); an oversized
                offer is refused with 413 before it is read

Usage:
  python tests/unit_check.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shim.flux_client import FluxSession, TurnInfo  # noqa: E402
from shim.server import Settings, VoiceSession, build_app, origin_allowed, stable_prefix  # noqa: E402


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
    def __init__(self) -> None:
        self.closes = 0
        self.sent: list[bytes | str] = []

    async def send(self, data) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        self.closes += 1


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
        "registry": await check_registry(),
    }
    for name, ok in results.items():
        print(f"[{name}] {'PASS' if ok else 'FAIL'}")
    print("RESULT:", "PASS" if all(results.values()) else "FAIL", results)
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

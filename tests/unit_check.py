"""Offline checks for the shim's session logic. No key, no network, no Docker.

  prefix        stable_prefix() cuts at word boundaries and never indexes past either string
  origin        origin_allowed(): no Origin passes, localhost passes, other hosts and "null" fail
  ordering      Flux events that arrive before Codex's session.update wait in the queue and go
                out after session.updated, in order; later events go out directly
  captions      a Flux revision of an already-shown word appends from the stable prefix, and
                the handoff carries the exact final transcript; an empty EndOfTurn sends nothing;
                a repeated StartOfTurn for the same turn does not re-send captions
  double-close  FluxSession.close() called twice at once closes the socket once and raises nothing
  immediate-close
                FluxSession.close() right after start(), before the reader's first tick; close()
                while the reader task is failing; close() called from inside the reader task; and
                Flux dropping the socket mid-stream: the socket is closed exactly once, nothing is
                raised, `closed` is set, and the drop reaches on_error
  tail          the samples a resampler holds back reach Flux: input_audio_buffer.commit and
                session close drain the WebSocket resampler, the WebRTC path drains its own at
                track end and at session close, and audio after a commit still flows; before
                session.update, audio is ignored
  queue-cap     Flux events queued before session.update stop at the configured cap and the
                session closes, driven by a real FluxSession reader so the close runs off the
                reader task and still closes the Flux socket
  loopback      is_loopback(): 127.0.0.1, 127.1.2.3, ::1, localhost pass; 0.0.0.0, LAN
                addresses, hostnames, and "" fail
  bad-messages  Codex messages with the right `type` but a string `session`, a `session.type`
                that is not a string, a string `item`, a non-string `audio`, a missing `audio`,
                or a null `output_text` raise no exception. A well-formed `session.update`
                still works on the same session afterwards
  close-paths   VoiceSession.close() still closes Flux and drops the registry entry when the
                peer connection refuses to close; that refusal logs one WARNING naming only the
                exception type, with no traceback. A close() cancelled mid-drain still releases
                the session
  registry      a call leaves no entry behind once its sideband closes (/healthz reports 0, a
                late WebSocket attempt gets 404); two sidebands opened at once for one call get
                one 101 and one 409; an oversized offer is refused with 413 before it is read
  v3-events     a v3 (/live) call sideband: on attach, session.started is the first event and the
                session is ready without a session.update; events queued before the attach follow
                it in order; the session.update deadline does not close it; Update and EndOfTurn
                become input_transcript.added, turn.done (role user), and delegation.created with
                the final text; StartOfTurn sends nothing; a Flux error carries a top-level
                `message`; delegation.context.append and session.context.append are logged,
                input_audio.append reaches Flux, and session.close ends the sideband
  dtls-role     the SDP answer takes the DTLS server role (`a=setup:passive`) when the offer says
                `actpass`, which is what Codex sends: its voice helper opens the SCTP association
                for its data channel only as the DTLS client, and aiortc opens it only as the ICE
                controlling side, so an `active` answer leaves both waiting and Codex 0.162 gives
                up after about 15 s (observed). An aiortc caller still opens a data channel against the passive
                answer; an offer that fixed its own role (`active` or `passive`) is answered with
                the complementary one

Usage:
  python tests/unit_check.py
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import av
import numpy as np
import websockets.exceptions
from aiohttp import WSMsgType, web
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
        self.prepared = True  # a sideband that completed its upgrade
        self.closed = False

    async def send_json(self, event: dict) -> None:
        self.sent.append(event)

    async def close(self) -> None:
        self.closed = True


class ScriptedSideband(FakeSideband):
    """A FakeSideband that run_ws() can iterate: yields the JSON messages pushed with send()."""

    def __init__(self) -> None:
        super().__init__()
        self.inbox: asyncio.Queue = asyncio.Queue()

    def push(self, message: dict) -> None:
        self.inbox.put_nowait(SimpleNamespace(type=WSMsgType.TEXT, data=json.dumps(message)))

    async def close(self, **_kwargs) -> None:  # the deadline passes code= and message=
        self.closed = True
        self.inbox.put_nowait(SimpleNamespace(type=WSMsgType.CLOSE, data=None))

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.inbox.get()


class FakeFluxSocket:
    """Stands in for the websockets client connection: records sends, yields `messages` (JSON
    text frames) to the reader, then ends the stream (or raises `final` if given)."""

    def __init__(self, messages: list[dict] | None = None, final: Exception | None = None) -> None:
        self.closes = 0
        self.sent: list[bytes | str] = []
        self.messages = list(messages or [])
        self.final = final

    async def send(self, data) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        self.closes += 1

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0)
        if self.messages:
            return json.dumps(self.messages.pop(0))
        if self.final is not None:
            raise self.final
        raise StopAsyncIteration


class CapturedLog:
    """Collects the records a logger emits at or above `level` while the block runs."""

    def __init__(self, name: str, level: int) -> None:
        self.logger = logging.getLogger(name)
        self.handler = logging.Handler(level)
        self.records: list[logging.LogRecord] = []
        self.handler.emit = self.records.append  # type: ignore[method-assign]

    def __enter__(self) -> "CapturedLog":
        self.logger.addHandler(self.handler)
        return self

    def __exit__(self, *exc) -> None:
        self.logger.removeHandler(self.handler)


class BrokenPeerConnection:
    async def close(self) -> None:
        raise RuntimeError("peer connection refused to close")


def flux_update(index: int, transcript: str) -> dict:
    return {"type": "TurnInfo", "event": "Update", "turn_index": index, "transcript": transcript,
            "end_of_turn_confidence": 0.1}


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


class BlockingFlux(StubFlux):
    """A Flux stub whose feed() blocks until released, to park close() inside its drain."""

    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.entered = asyncio.Event()  # set once feed() is running, so a test can wait for it

    async def feed(self, pcm: bytes) -> None:
        self.entered.set()
        await self.release.wait()
        await super().feed(pcm)


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
    # A second StartOfTurn for the same turn keeps the captions already shown.
    await s.on_turn(turn("StartOfTurn", "", index=2))
    await s.on_turn(turn("Update", "Open the", index=2))
    await s.on_turn(turn("Update", "Open the file", index=2))
    shown_before = len([e for e in ws.sent if e["type"].endswith(".delta")])
    await s.on_turn(turn("StartOfTurn", "", index=2))
    await s.on_turn(turn("Update", "Open the file", index=2))
    await s.on_turn(turn("Update", "Open the file now", index=2))
    deltas2 = [e["delta"] for e in ws.sent if e["type"].endswith(".delta")][shown_before:]
    if "".join(deltas2).lstrip().startswith("Open the"):
        print(f"  repeated StartOfTurn re-sent captions: {deltas2}")
        ok = False
    return ok


async def check_double_close() -> bool:
    f = FluxSession(lambda info: asyncio.sleep(0))
    sock = FakeFluxSocket([flux_update(0, "one"), flux_update(0, "one two")])
    f._ws = sock  # type: ignore[assignment]
    f._reader = asyncio.create_task(f._read_loop(sock))  # type: ignore[arg-type]
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
    # close() from inside the reader task (a Flux event handler decides to end the session):
    # the reader cannot wait on itself; the socket must still close once and nothing may raise.
    f = FluxSession(lambda info: asyncio.sleep(0))
    sock = FakeFluxSocket()
    f._ws = sock  # type: ignore[assignment]
    outcome: list[str] = []
    warnings = CapturedLog("flux", logging.WARNING)

    async def reader_that_closes() -> None:
        try:
            await f.close(wait_s=2.0)
            outcome.append("ok")
        except Exception as exc:  # noqa: BLE001
            outcome.append(f"{type(exc).__name__}: {exc}")
    with warnings:
        f._reader = asyncio.create_task(reader_that_closes())
        await asyncio.wait_for(f._reader, timeout=5.0)
    if outcome != ["ok"] or sock.closes != 1 or not f.closed.is_set() or warnings.records:
        print(f"  close from the reader task: {outcome}, socket closed {sock.closes} time(s), "
              f"warnings: {[r.getMessage() for r in warnings.records]}")
        ok = False
    # Flux drops the socket mid-stream: on_error is told once, and close() still works.
    errors: list[str] = []

    async def on_error(message: str) -> None:
        errors.append(message)
    f = FluxSession(lambda info: asyncio.sleep(0), on_error=on_error)
    sock = FakeFluxSocket(final=websockets.exceptions.ConnectionClosedError(None, None))
    f._ws = sock  # type: ignore[assignment]
    f._reader = asyncio.create_task(f._read_loop(sock))  # type: ignore[arg-type]
    await asyncio.wait_for(f._reader, timeout=5.0)
    await f.close(wait_s=2.0)
    if len(errors) != 1 or "unexpectedly" not in errors[0] or sock.closes != 1:
        print(f"  unexpected Flux drop: on_error got {errors}, socket closed {sock.closes} time(s)")
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
    # The WebRTC path: a 48 kHz frame that is not a multiple of the output frame, drained at
    # track end (what the pump does) and, on another session, by close() before the track ended.
    frame = av.AudioFrame.from_ndarray(np.zeros((1, 1001), dtype="<i2"), format="s16", layout="mono")
    frame.sample_rate = 48_000
    for label, finish in (("track end", lambda sess: sess._drain_rtc_audio()),
                          ("session close", lambda sess: sess.close())):
        s2 = VoiceSession(Settings(), "rtc_tail2")
        s2.flux = StubFlux()  # type: ignore[assignment]
        for out in s2._rtc_resampler.resample(frame):
            await s2.feed_pcm16_16k(out.to_ndarray().tobytes())
        before = len(s2.flux.fed)
        await finish(s2)
        after = len(s2.flux.fed)
        if after <= before or not near(after, 1001, 48_000):
            print(f"  WebRTC drain at {label}: {before // 2} -> {after // 2} samples, "
                  f"wanted ~{1001 * 16_000 / 48_000:.0f}")
            ok = False
    return ok


async def check_queue_cap() -> bool:
    ok = True
    for label, settings in (("5 events", Settings(pending_max_events=5)),
                            ("200 bytes", Settings(pending_max_bytes=200))):
        s = VoiceSession(settings, "rtc_cap")
        s.ws = FakeSideband()  # attached, but session.update never arrives
        # A real FluxSession whose reader delivers eight growing Updates, each a new caption
        # delta, so the overflow close is spawned from inside the Flux reader task.
        words = " ".join(f"word{i}" for i in range(12))
        sock = FakeFluxSocket([flux_update(0, words[: 6 * (i + 1)]) for i in range(8)])
        flux = FluxSession(s.on_turn, on_error=s.on_flux_error)
        flux._ws = sock  # type: ignore[assignment]
        flux._reader = asyncio.create_task(flux._read_loop(sock))  # type: ignore[arg-type]
        s.flux = flux
        await s.on_turn(turn("StartOfTurn", ""))
        await asyncio.wait_for(flux._reader, timeout=5.0)
        await asyncio.wait_for(asyncio.gather(*s._tasks), timeout=5.0)  # the spawned close
        cap_ok = len(s.pending) <= 5 if "events" in label else s.pending_bytes <= 200
        if not cap_ok or not s.closed or sock.closes != 1:
            print(f"  cap {label}: {len(s.pending)} events / {s.pending_bytes} bytes queued, "
                  f"session closed: {s.closed}, Flux socket closed {sock.closes} time(s)")
            ok = False
    return ok


def check_loopback() -> bool:
    cases = [("127.0.0.1", True), ("127.1.2.3", True), ("::1", True), ("localhost", True),
             ("0.0.0.0", False), ("::", False), ("192.168.1.5", False), ("example.com", False), ("", False)]
    ok = True
    for host, want in cases:
        if is_loopback(host) != want:
            print(f"  is_loopback({host!r}) != {want}")
            ok = False
    return ok


async def check_bad_messages() -> bool:
    s = VoiceSession(Settings(), "rtc_shapes")
    ws = FakeSideband()
    s.ws = ws
    s.flux = StubFlux()  # type: ignore[assignment]
    bad = [
        {"type": "session.update", "session": "bad"},
        {"type": "session.update", "session": {"type": 5}},
        {"type": "conversation.item.create", "item": "x"},
        {"type": "input_audio_buffer.append", "audio": 5},
        {"type": "input_audio_buffer.append"},
        {"type": "conversation.handoff.append", "output_text": None},
    ]
    for msg in bad:
        try:
            await s.handle_codex_message(msg)
        except Exception as exc:  # noqa: BLE001
            print(f"  {msg} raised {type(exc).__name__}: {exc}")
            return False
    updated = [e for e in ws.sent if e["type"] == "session.updated"]
    if len(updated) != 2 or any(e["session"]["type"] != "quicksilver" for e in updated):
        print(f"  session.updated for malformed session.update: {updated}")
        return False
    await s.handle_codex_message({"type": "session.update", "session": {"type": "transcription"}})
    if ws.sent[-1] != {"type": "session.updated", "session": {"id": s.session_id, "type": "transcription"}}:
        print(f"  well-formed session.update after bad ones gave {ws.sent[-1]}")
        return False
    return s.flux.fed == b""


async def check_close_paths() -> bool:
    ok = True
    # The peer connection raises on close: Flux is still closed and the registry hook still runs.
    s = VoiceSession(Settings(), "rtc_close1")
    s.flux = StubFlux()  # type: ignore[assignment]
    s.pc = BrokenPeerConnection()  # type: ignore[assignment]
    dropped: list[str] = []
    s.on_closed = lambda: dropped.append(s.tag)
    # The failure is one WARNING naming only the exception type, not an ERROR with a traceback.
    with CapturedLog("shim", logging.WARNING) as errors:
        try:
            await s.close()
        except Exception as exc:  # noqa: BLE001
            print(f"  close() raised {type(exc).__name__}: {exc}")
            ok = False
    warned = [r.getMessage() for r in errors.records
              if r.levelno == logging.WARNING and r.exc_info is None]
    if (not s.closed or s.flux.closes != 1 or dropped != ["rtc_close1"] or len(errors.records) != 1
            or warned != ["[rtc_close1] peer connection close failed (RuntimeError)"]):
        print(f"  broken pc: closed {s.closed}, Flux closes {s.flux.closes}, registry hook {dropped}, "
              f"records logged {[r.getMessage() for r in errors.records]}")
        ok = False
    # close() is cancelled while its final drain is blocked on Flux: the session still ends.
    s = VoiceSession(Settings(), "rtc_close2")
    flux = BlockingFlux()
    s.flux = flux  # type: ignore[assignment]
    dropped = []
    s.on_closed = lambda: dropped.append(s.tag)
    frame = av.AudioFrame.from_ndarray(np.zeros((1, 1001), dtype="<i2"), format="s16", layout="mono")
    frame.sample_rate = 48_000
    s._rtc_resampler.resample(frame)  # leaves a tail, so close() has something to drain
    closer = asyncio.create_task(s.close())
    await asyncio.wait_for(flux.entered.wait(), 2.0)  # close() is now blocked in feed()
    closer.cancel()
    try:
        await closer
        print("  cancelled close() did not raise CancelledError")
        ok = False
    except asyncio.CancelledError:
        pass
    if not s.closed or flux.closes != 1 or dropped != ["rtc_close2"]:
        print(f"  cancelled close: closed {s.closed}, Flux closes {flux.closes}, registry hook {dropped}")
        ok = False
    return ok


async def check_registry() -> bool:
    app = build_app(Settings(orphan_call_timeout_s=1.5))
    async with TestClient(TestServer(app)) as client:
        r = await client.post("/v1/realtime/calls", data="v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n",
                              headers={"Content-Type": "application/sdp"})
        status = r.status
        call_id = r.headers.get("Location", "").rsplit("/", 1)[-1]
        during = (await (await client.get("/healthz")).json())["sessions"]

        async def sideband_status() -> int:
            try:
                ws = await client.ws_connect("/v1/realtime", params={"call_id": call_id})
            except Exception as exc:  # noqa: BLE001
                return getattr(exc, "status", -1)
            await asyncio.sleep(0.5)  # hold it open while the other attempt is judged
            await ws.close()
            return 101
        # Slow the upgrade so both requests are in flight at once, which is the window the
        # second-sideband check has to cover.
        real_prepare = web.WebSocketResponse.prepare

        async def slow_prepare(self, request):
            await asyncio.sleep(0.05)
            return await real_prepare(self, request)
        web.WebSocketResponse.prepare = slow_prepare  # type: ignore[method-assign]
        try:
            pair = sorted(await asyncio.gather(sideband_status(), sideband_status()))
        finally:
            web.WebSocketResponse.prepare = real_prepare  # type: ignore[method-assign]
        # poll every 50 ms, up to 5 s, until /healthz reads 0
        loop = asyncio.get_running_loop()
        wait_until = loop.time() + 5.0
        after = (await (await client.get("/healthz")).json())["sessions"]
        while after != 0 and loop.time() < wait_until:
            await asyncio.sleep(0.05)
            after = (await (await client.get("/healthz")).json())["sessions"]
        try:  # a real WebSocket attempt: a plain GET is refused with 400 before the call lookup
            late_ws = await client.ws_connect("/v1/realtime", params={"call_id": call_id})
            await late_ws.close()
            late_status = 101
        except aiohttp.WSServerHandshakeError as exc:
            late_status = exc.status
        big = await client.post("/v1/realtime/calls", data=b"", headers={
            "Content-Type": "application/sdp", "Content-Length": str(64 * 1024 * 1024)})
        big_status = big.status
    ok = True
    if status != 201 or during != 1:
        print(f"  call returned {status} and /healthz saw {during} session(s); wanted 201 and 1")
        ok = False
    if after != 0:
        print(f"  /healthz still reports {after} session(s) after the sidebands closed")
        ok = False
    if late_status != 404:
        print(f"  late WebSocket attempt for the released call returned {late_status}, wanted 404")
        ok = False
    if pair != [101, 409]:
        print(f"  two sidebands at once returned {pair}, wanted [101, 409]")
        ok = False
    if big_status != 413:
        print(f"  oversized offer returned {big_status}, wanted 413")
        ok = False
    return ok


async def check_v3_events() -> bool:
    ok = True

    def fail(message: str) -> None:
        nonlocal ok
        print(f"  {message}")
        ok = False

    s = VoiceSession(Settings(session_update_timeout_s=0.2), "rtc_v3")
    s.dialect = "v3"  # what create_call sets for POST /live
    s.flux = StubFlux()  # type: ignore[assignment]
    # Flux events before the sideband attaches are queued.
    await s.on_turn(turn("StartOfTurn", ""))
    await s.on_turn(turn("Update", "Create a"))
    await s.on_turn(turn("Update", "Create a file"))
    queued = list(s.pending)
    if [e["type"] for e in queued] != ["input_transcript.added"]:
        fail(f"queued before attach: {queued}")
    ws = ScriptedSideband()
    s.ws = ws  # type: ignore[assignment]
    shim_log = logging.getLogger("shim")
    old_level = shim_log.level
    shim_log.setLevel(logging.INFO)
    try:
        with CapturedLog("shim", logging.INFO) as logged:
            runner = asyncio.create_task(s.run_ws(ws))  # type: ignore[arg-type]
            await asyncio.sleep(0.4)  # past the 0.2 s session.update deadline
            want_first = {"type": "session.started", "session": {"id": s.session_id}}
            if not ws.sent or ws.sent[0] != want_first:
                fail(f"first event {ws.sent[:1]}, wanted {want_first}")
            if ws.sent[1:] != queued or s.pending:
                fail(f"queued events after session.started: {ws.sent[1:]}, wanted {queued}")
            if not s.ready or ws.closed:
                fail(f"after attach: ready {s.ready}, closed by the deadline {ws.closed}")
            before = len(ws.sent)
            await s.on_turn(turn("EndOfTurn", "Create a file called hello.txt", conf=0.9))
            out = ws.sent[before:]
            final = "Create a file called hello.txt"
            if [e["type"] for e in out] != ["input_transcript.added", "turn.done", "delegation.created"]:
                fail(f"EndOfTurn sent {out}")
            else:
                shown = "".join(e["item"]["text"] for e in ws.sent if e["type"] == "input_transcript.added")
                if shown != final or set(out[0]) != {"type", "item"} or set(out[0]["item"]) != {"text"}:
                    fail(f"captions {shown!r} / {out[0]}")
                if out[1] != {"type": "turn.done", "turn": {"role": "user", "transcript": final}}:
                    fail(f"turn.done {out[1]}")
                item = out[2]["item"]
                handoff_id = item.get("id", "")
                want = {"type": "delegation.created",
                        "item": {"id": handoff_id, "type": "delegation", "target": "client",
                                 "content": [{"type": "input_text", "text": final}]}}
                if out[2] != want or not handoff_id.startswith("handoff_"):
                    fail(f"delegation.created {out[2]}")
            before = len(ws.sent)
            await s.on_turn(turn("StartOfTurn", "", index=1))
            if len(ws.sent) != before:
                fail(f"StartOfTurn sent {ws.sent[before:]}")
            await s.on_flux_error("boom")
            err = ws.sent[-1]
            if err.get("type") != "error" or err.get("message") != "boom":
                fail(f"error event {err}")
            ws.push({"type": "delegation.context.append", "delegation_item_id": "handoff_x",
                     "channel": "commentary", "content": [{"type": "input_text", "text": "Starting on it."}]})
            ws.push({"type": "delegation.context.append", "delegation_item_id": "handoff_y", "content": "bad"})
            ws.push({"type": "session.context.append", "channel": "speakable",
                     "content": [{"type": "input_text", "text": "ctx"}]})
            s.flux_failed = False  # the simulated error above stopped audio; let it flow again
            ws.push({"type": "input_audio.append", "audio": pcm_b64(2400)})
            ws.push({"type": "input_audio.append", "audio": 5})
            ws.push({"type": "session.close"})
            await asyncio.wait_for(runner, timeout=2.0)
        messages = [r.getMessage() for r in logged.records]
    finally:
        shim_log.setLevel(old_level)
    if not any("Codex reply for handoff_x: Starting on it." in m for m in messages):
        fail(f"delegation.context.append not logged: {messages}")
    if not any("Codex reply for handoff_y: (no text)" in m for m in messages):
        fail("delegation.context.append with a string content not handled")
    if not any("session context (speakable) ignored" in m for m in messages):
        fail("session.context.append not logged")
    if not any("Codex closed the session" in m for m in messages):
        fail("session.close not handled")
    if not s.flux.fed:
        fail("input_audio.append reached no Flux audio")
    if any(r.levelno >= logging.ERROR and "boom" not in r.getMessage() for r in logged.records):
        fail("unexpected ERROR records")
    return ok


async def check_dtls_role() -> bool:
    import re

    from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription

    ok = True

    def fail(message: str) -> None:
        nonlocal ok
        print(f"  {message}")
        ok = False

    async def offer(setup: str | None):
        pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
        pc.addTransceiver("audio", direction="sendrecv")
        channel = pc.createDataChannel("oai-events")  # the label Codex's helper uses
        await pc.setLocalDescription(await pc.createOffer())
        sdp = pc.localDescription.sdp  # aiortc offers a=setup:actpass, as Codex does
        if setup is not None:
            sdp = re.sub(r"a=setup:\w+", f"a=setup:{setup}", sdp)
        return pc, channel, sdp

    def setups(sdp: str) -> set[str]:
        return set(re.findall(r"a=setup:(\w+)", sdp))

    app = build_app(Settings(orphan_call_timeout_s=1.0))
    async with TestClient(TestServer(app)) as client:
        async def answer_for(sdp: str) -> tuple[int, str]:
            r = await client.post("/v1/live", data=sdp, headers={"Content-Type": "application/sdp"})
            return r.status, await r.text()

        # actpass offer (Codex): passive answer, and a caller can still open its data channel
        pc, channel, sdp = await offer(None)
        if setups(sdp) != {"actpass"}:
            fail(f"test offer setup {setups(sdp)}, expected actpass")
        status, answer = await answer_for(sdp)
        if status != 201 or setups(answer) != {"passive"}:
            fail(f"actpass offer: status {status}, answer setup {setups(answer)}, expected passive")
        # An aiortc caller is ICE controlling, so it sends the SCTP INIT in either DTLS role; this
        # guards that aiortc callers (the other checks, e2e) still work against a passive answer.
        # The `a=setup` assertion above is the one that pins the fix.
        opened = asyncio.Event()
        channel.on("open", opened.set)
        await pc.setRemoteDescription(RTCSessionDescription(sdp=answer, type="answer"))
        try:
            await asyncio.wait_for(opened.wait(), 5)
        except asyncio.TimeoutError:
            fail("data channel did not open against the passive answer")
        await pc.close()

        # an offer that fixed its own role gets the complementary one
        for offered, expected in (("passive", "active"), ("active", "passive")):
            pc, _, sdp = await offer(offered)
            status, answer = await answer_for(sdp)
            if status != 201 or setups(answer) != {expected}:
                fail(f"{offered} offer: status {status}, answer setup {setups(answer)}, expected {expected}")
            await pc.close()
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
        "bad-messages": await check_bad_messages(),
        "close-paths": await check_close_paths(),
        "registry": await check_registry(),
        "v3-events": await check_v3_events(),
        "dtls-role": await check_dtls_role(),
    }
    for name, ok in results.items():
        print(f"[{name}] {'PASS' if ok else 'FAIL'}")
    print("RESULT:", "PASS" if all(results.values()) else "FAIL", results)
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

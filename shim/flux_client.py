"""Deepgram Flux STT client: stream 16 kHz mono PCM in, get TurnInfo events out.

Wire contract (Deepgram /v2/listen, model flux-general-en):
  * connect: wss://api.deepgram.com/v2/listen?model=flux-general-en&encoding=linear16
             &sample_rate=16000[&eot_threshold=..&eot_timeout_ms=..]
  * auth:    Authorization: Token <DEEPGRAM_API_KEY>  (read from the environment only)
  * send:    binary frames of PCM16 LE; we send 80 ms chunks = 2560 bytes at 16 kHz mono
  * recv:    {"type":"Connected",...}
             {"type":"TurnInfo","event":"StartOfTurn|Update|EagerEndOfTurn|TurnResumed|EndOfTurn",
              "turn_index":n,"transcript":"...","end_of_turn_confidence":0.xx,...}
             {"type":"FatalError",...}
  * finish:  {"type":"CloseStream"} then wait for the last EndOfTurn
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional
from urllib.parse import urlencode

import websockets

log = logging.getLogger("flux")

FLUX_URL = "wss://api.deepgram.com/v2/listen"
MODEL = "flux-general-en"
SAMPLE_RATE = 16_000
CHANNELS = 1
BYTES_PER_SAMPLE = 2
CHUNK_MS = 80
CHUNK_BYTES = SAMPLE_RATE * CHANNELS * BYTES_PER_SAMPLE * CHUNK_MS // 1000  # 2560


@dataclass
class TurnInfo:
    event: str
    transcript: str
    turn_index: int
    end_of_turn_confidence: Optional[float]
    raw: dict = field(repr=False, default_factory=dict)


EventHandler = Callable[[TurnInfo], Awaitable[None]]
ErrorHandler = Callable[[str], Awaitable[None]]  # gets a human-readable message, never the key


def flux_url(
    *,
    eot_threshold: Optional[float] = None,
    eot_timeout_ms: Optional[int] = None,
    eager_eot_threshold: Optional[float] = None,
    keyterms: Optional[list[str]] = None,
) -> str:
    params: list[tuple[str, str]] = [
        ("model", MODEL),
        ("encoding", "linear16"),
        ("sample_rate", str(SAMPLE_RATE)),
    ]
    if eot_threshold is not None:
        params.append(("eot_threshold", f"{eot_threshold:g}"))
    if eot_timeout_ms is not None:
        params.append(("eot_timeout_ms", str(int(eot_timeout_ms))))
    if eager_eot_threshold is not None:
        params.append(("eager_eot_threshold", f"{eager_eot_threshold:g}"))
    for term in keyterms or []:
        params.append(("keyterm", term))
    return f"{FLUX_URL}?{urlencode(params)}"


def describe_connect_error(exc: BaseException) -> str:
    """A message safe to show the user for a failed Flux connect. Never echoes headers."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status in (401, 403):
        return (f"Deepgram rejected the API key (HTTP {status}). "
                "Check DEEPGRAM_API_KEY and restart the shim.")
    if status is not None:
        return f"Deepgram Flux refused the connection (HTTP {status})."
    if isinstance(exc, RuntimeError) and "DEEPGRAM_API_KEY" in str(exc):
        return str(exc)
    return f"Could not connect to Deepgram Flux ({type(exc).__name__})."


def api_key_from_env() -> str:
    key = os.environ.get("DEEPGRAM_API_KEY", "").strip()
    if not key:
        raise RuntimeError("DEEPGRAM_API_KEY is not set in the environment")
    return key


class FluxSession:
    """One Flux STT WebSocket. Feed PCM with `feed()`; TurnInfo arrives on `on_turn`."""

    def __init__(
        self,
        on_turn: EventHandler,
        *,
        eot_threshold: Optional[float] = None,
        eot_timeout_ms: Optional[int] = None,
        eager_eot_threshold: Optional[float] = None,
        keyterms: Optional[list[str]] = None,
        on_error: Optional[ErrorHandler] = None,
    ) -> None:
        self.on_turn = on_turn
        self.on_error = on_error
        self.url = flux_url(
            eot_threshold=eot_threshold,
            eot_timeout_ms=eot_timeout_ms,
            eager_eot_threshold=eager_eot_threshold,
            keyterms=keyterms,
        )
        self._ws: Optional[websockets.ClientConnection] = None
        self._pending = bytearray()
        self._reader: Optional[asyncio.Task] = None
        self.connected = asyncio.Event()
        self.closed = asyncio.Event()
        self.closing = False  # close() was called: later audio is dropped, not an error
        self.fatal: Optional[str] = None
        self.request_id: Optional[str] = None

    async def __aenter__(self) -> "FluxSession":
        await self.start()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def start(self) -> None:
        headers = {"Authorization": f"Token {api_key_from_env()}"}
        log.info("connecting to Flux STT: %s", self.url)
        ws = await websockets.connect(
            self.url, additional_headers=headers, max_size=None, ping_interval=20
        )
        self._ws = ws
        # The reader gets its own reference: close() clears self._ws, and the reader may not
        # have had its first tick by then.
        self._reader = asyncio.create_task(self._read_loop(ws), name="flux-reader")

    async def feed(self, pcm16: bytes) -> None:
        """Append PCM16 LE 16 kHz mono; sends complete 80 ms (2560 byte) chunks."""
        ws = self._ws  # close() may clear self._ws while a send below is awaiting
        if ws is None:
            raise RuntimeError("FluxSession not started")
        self._pending.extend(pcm16)
        while len(self._pending) >= CHUNK_BYTES:
            chunk = bytes(self._pending[:CHUNK_BYTES])
            del self._pending[:CHUNK_BYTES]
            await ws.send(chunk)

    async def flush(self, ws: Optional[websockets.ClientConnection] = None) -> None:
        """Send a trailing partial chunk, padded with silence to 80 ms."""
        ws = ws or self._ws
        if ws is None or not self._pending:
            return
        chunk = bytes(self._pending) + b"\x00" * (CHUNK_BYTES - len(self._pending))
        self._pending.clear()
        await ws.send(chunk)

    async def close(self, wait_s: float = 8.0) -> None:
        """CloseStream, then wait for Flux to finish the last turn and close.

        Safe to call more than once, including concurrently: the first caller owns the
        socket, later callers wait for it to finish.
        """
        self.closing = True
        ws = self._ws
        if ws is None:
            if self._reader is not None and not self.closed.is_set():
                try:
                    await asyncio.wait_for(self.closed.wait(), timeout=wait_s)
                except asyncio.TimeoutError:
                    pass
            return
        self._ws = None  # claim the socket; a concurrent close() takes the branch above
        try:
            try:
                await self.flush(ws)
                await ws.send(json.dumps({"type": "CloseStream"}))
            except websockets.ConnectionClosed:
                pass
            reader = self._reader
            if reader is not None and reader is not asyncio.current_task():
                # (a task cannot await itself: a close() from inside a Flux event handler
                # just sends CloseStream and lets the reader finish on its own)
                try:
                    await asyncio.wait_for(reader, timeout=wait_s)
                except asyncio.TimeoutError:
                    log.warning("Flux reader did not finish within %.1fs; forcing close", wait_s)
                except Exception:  # noqa: BLE001
                    pass  # _read_loop already logged it; the socket still gets closed below
        finally:
            try:
                await ws.close()
            finally:
                self.closed.set()

    async def _read_loop(self, ws: websockets.ClientConnection) -> None:
        try:
            async for message in ws:
                if isinstance(message, (bytes, bytearray)):
                    continue
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    log.warning("non-JSON text frame from Flux: %r", message[:80])
                    continue
                kind = data.get("type")
                if kind == "Connected":
                    self.request_id = data.get("request_id")
                    self.connected.set()
                    log.info("Flux connected")
                elif kind == "TurnInfo":
                    info = TurnInfo(
                        event=data.get("event", ""),
                        transcript=data.get("transcript", "") or "",
                        turn_index=int(data.get("turn_index", 0) or 0),
                        end_of_turn_confidence=data.get("end_of_turn_confidence"),
                        raw=data,
                    )
                    await self.on_turn(info)
                elif kind in ("FatalError", "Error"):
                    self.fatal = json.dumps(data)
                    detail = data.get("description") or data.get("code") or kind
                    log.error("Flux fatal error: %s", detail)
                    if self.on_error is not None:
                        await self.on_error(f"Deepgram Flux error: {detail}")
                else:
                    log.debug("Flux message: %s", kind)
        except websockets.ConnectionClosed as exc:
            log.info("Flux socket closed: %s", exc.code)
        except Exception as exc:  # noqa: BLE001
            # A bug in a handler or an unexpected frame must not end the session silently.
            log.exception("Flux reader failed")
            if self.on_error is not None and not self.closing:
                await self.on_error(f"Deepgram Flux stream failed ({type(exc).__name__}). "
                                    "Press F8 to start a new voice session.")
        finally:
            self.closed.set()

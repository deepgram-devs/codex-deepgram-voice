"""Local realtime shim: Codex CLI voice in, Deepgram Flux STT underneath.

Codex CLI starts voice as a WebRTC call plus a sideband WebSocket (see DECISION.md,
question 2). This server stands in for OpenAI's realtime backend on localhost:

  POST {prefix}/realtime/calls           WebRTC offer in (multipart `sdp` + `session`, or
                                         raw application/sdp), SDP answer out, call id in
                                         the Location header
  GET  {prefix}/realtime?call_id=<id>    sideband WebSocket for that call (v1 "quicksilver")
  GET  {prefix}/realtime                 plain WebSocket transport (v1 or v2 shapes, audio as
                                         input_audio_buffer.append, base64 PCM16 @ 24 kHz)
  GET  /healthz

Audio from either path is resampled to 16 kHz mono PCM16 and streamed to Flux in 80 ms
chunks. Flux `Update` events become live caption deltas; Flux `EndOfTurn` becomes the
finished transcript plus a handoff request, which is what makes Codex start the turn.

Auth headers Codex sends (ChatGPT tokens, account ids) are ignored and never logged.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import secrets
import sys
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlsplit

import av
import numpy as np
import websockets
from aiohttp import WSMsgType, web
from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription
from aiortc.mediastreams import MediaStreamError

from .flux_client import SAMPLE_RATE, FluxSession, TurnInfo, describe_connect_error

log = logging.getLogger("shim")

CODEX_WS_PCM_RATE = 24_000  # codex-api methods_common.rs REALTIME_AUDIO_SAMPLE_RATE
V2_HANDOFF_TOOL = "background_agent"  # codex-api protocol_v2.rs BACKGROUND_AGENT_TOOL_NAME
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


@dataclass
class Settings:
    eot_threshold: Optional[float] = 0.8
    eot_timeout_ms: Optional[int] = 6000
    eager_eot_threshold: Optional[float] = None
    keyterms: list[str] = field(default_factory=list)
    live_captions: bool = True
    location_prefix: str = "/v1"
    orphan_call_timeout_s: float = 30.0  # drop a call whose sideband never attaches


def stable_prefix(a: str, b: str) -> str:
    """Longest common prefix of two transcripts, cut back to a word boundary."""
    n = 0
    for x, y in zip(a, b, strict=False):  # stops at the shorter one on purpose
        if x != y:
            break
        n += 1
    if n == len(a) == len(b):
        return a
    if n == len(a) and b[n] == " ":
        return a  # a is a whole-word prefix of b
    if n == len(b) and a[n] == " ":
        return b  # b is a whole-word prefix of a
    cut = a.rfind(" ", 0, n)
    return a[:cut] if cut > 0 else ""


def origin_allowed(origin: Optional[str]) -> bool:
    """Codex CLI sends no Origin. A browser always does; only accept local pages."""
    if origin is None:
        return True
    try:
        host = urlsplit(origin).hostname
    except ValueError:
        return False
    return host in LOCAL_HOSTS


def trim(text: str, n: int = 120) -> str:
    text = " ".join(text.split())
    return text if len(text) <= n else text[: n - 3] + "..."


class VoiceSession:
    """One Codex voice session: an audio source, one Flux stream, one sideband socket."""

    def __init__(self, settings: Settings, call_id: Optional[str]) -> None:
        self.settings = settings
        self.call_id = call_id
        self.session_id = f"sess_{secrets.token_hex(8)}"
        self.tag = call_id or self.session_id
        self.flux: Optional[FluxSession] = None
        self.ws: Optional[web.WebSocketResponse] = None
        self.pc: Optional[RTCPeerConnection] = None
        self.pending: list[dict] = []
        self.dialect = "v1"  # "v1" (quicksilver) or "v2" (public realtime shape)
        self.session_type = "quicksilver"
        self.last_sent = ""  # caption text already appended in Codex for the current turn
        self.prev_update = ""  # previous Flux Update transcript, to find the stable prefix
        self.turn_item_id = ""
        self.handoffs = 0
        self.closed = False
        self.flushing = False  # draining `pending` after session.update
        self.attached = False  # a sideband WebSocket has connected at least once
        self.flux_failed = False  # Flux could not start or died; stop feeding, error sent once
        self._tasks: set[asyncio.Task] = set()  # strong refs to the pump and expiry tasks
        self._ws_resampler = av.AudioResampler(format="s16", layout="mono", rate=SAMPLE_RATE)
        self._started = time.monotonic()

    # ---- audio in -----------------------------------------------------------------

    def spawn(self, coro, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def ensure_flux(self) -> bool:
        """Open Flux on first audio. False (once logged and reported) if it cannot."""
        if self.flux_failed:
            return False
        if self.flux is None:
            s = self.settings
            flux = FluxSession(
                self.on_turn,
                eot_threshold=s.eot_threshold,
                eot_timeout_ms=s.eot_timeout_ms,
                eager_eot_threshold=s.eager_eot_threshold,
                keyterms=s.keyterms or None,
                on_error=self.on_flux_error,
            )
            try:
                await flux.start()
            except Exception as exc:  # noqa: BLE001
                await self.on_flux_error(describe_connect_error(exc))
                return False
            if self.closed:  # the session ended while Flux was connecting; nobody will close it
                await flux.close(wait_s=0.5)
                return False
            self.flux = flux
            log.info("[%s] Flux STT stream opened (eot_threshold=%s eot_timeout_ms=%s)",
                     self.tag, s.eot_threshold, s.eot_timeout_ms)
        return True

    async def on_flux_error(self, message: str) -> None:
        """Report a Flux failure to Codex once and stop sending audio for this session."""
        if self.flux_failed:
            return
        self.flux_failed = True
        log.error("[%s] %s", self.tag, message)
        await self.emit({"type": "error", "error": {"type": "flux_error", "message": message}})

    async def feed_pcm16_16k(self, pcm: bytes) -> None:
        if self.closed or not pcm:
            return
        if not await self.ensure_flux():
            return
        flux = self.flux
        assert flux is not None
        if flux.closing:
            return  # we already ended this stream (the track ended); later audio is dropped
        try:
            await flux.feed(pcm)
        except websockets.ConnectionClosed:
            if not flux.closing:
                await self.on_flux_error("Deepgram Flux closed the stream unexpectedly.")

    async def feed_codex_ws_audio(self, b64: str) -> None:
        """input_audio_buffer.append: base64 PCM16 mono at 24 kHz from the WebSocket transport."""
        raw = base64.b64decode(b64)
        if len(raw) < 2:
            return
        samples = np.frombuffer(raw[: len(raw) - len(raw) % 2], dtype="<i2").reshape(1, -1)
        frame = av.AudioFrame.from_ndarray(samples, format="s16", layout="mono")
        frame.sample_rate = CODEX_WS_PCM_RATE
        for out in self._ws_resampler.resample(frame):
            await self.feed_pcm16_16k(out.to_ndarray().tobytes())

    async def start_webrtc(self, offer_sdp: str) -> str:
        pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
        self.pc = pc

        @pc.on("track")
        def on_track(track) -> None:
            log.info("[%s] WebRTC %s track received", self.tag, track.kind)
            if track.kind == "audio":
                self.spawn(self._pump_track(track), f"pump-{self.tag}")

        @pc.on("datachannel")
        def on_datachannel(channel) -> None:
            log.info("[%s] data channel %r opened by Codex", self.tag, channel.label)

        @pc.on("connectionstatechange")
        async def on_state() -> None:
            log.info("[%s] WebRTC connection state: %s", self.tag, pc.connectionState)
            if pc.connectionState in ("failed", "closed"):
                await self.close()

        await pc.setRemoteDescription(RTCSessionDescription(sdp=offer_sdp, type="offer"))
        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)
        return pc.localDescription.sdp

    async def _pump_track(self, track) -> None:
        resampler = av.AudioResampler(format="s16", layout="mono", rate=SAMPLE_RATE)
        frames = 0
        try:
            while not self.closed:
                frame = await track.recv()
                frames += 1
                if frames == 1:
                    log.info("[%s] first audio frame: %s Hz, %s, %d samples", self.tag,
                             frame.sample_rate, frame.layout.name, frame.samples)
                for out in resampler.resample(frame):
                    await self.feed_pcm16_16k(out.to_ndarray().tobytes())
        except MediaStreamError:
            log.info("[%s] audio track ended after %d frames", self.tag, frames)
        except Exception:  # noqa: BLE001
            log.exception("[%s] audio pump failed", self.tag)
        if self.flux is not None and not self.closed and not self.flux_failed:
            # Let Flux settle the final turn before the sideband goes away.
            await self.flux.close()

    # ---- Flux events out -----------------------------------------------------------

    async def on_turn(self, info: TurnInfo) -> None:
        conf = info.end_of_turn_confidence
        conf_s = f"{conf:.3f}" if isinstance(conf, (int, float)) else "n/a"
        if info.event == "StartOfTurn":
            self.last_sent = ""
            self.prev_update = ""
            self.turn_item_id = f"item_{secrets.token_hex(6)}"
            log.info("[%s] turn %d StartOfTurn", self.tag, info.turn_index)
            if self.dialect == "v2":
                await self.emit({"type": "input_audio_buffer.speech_started", "item_id": self.turn_item_id})
            return
        if info.event == "Update":
            log.debug("[%s] turn %d Update conf=%s %r", self.tag, info.turn_index, conf_s, trim(info.transcript, 60))
            if not self.settings.live_captions:
                return
            # Codex appends caption deltas and never rewrites them, while Flux may revise its
            # most recent words. Only forward the prefix that stayed the same across two
            # consecutive Updates, cut at a word boundary.
            stable = stable_prefix(self.prev_update, info.transcript)
            self.prev_update = info.transcript
            await self.send_caption_up_to(stable)
            return
        if info.event == "EndOfTurn":
            text = info.transcript.strip()
            if not text:
                self.last_sent = ""
                self.prev_update = ""
                log.info("[%s] turn %d EndOfTurn with empty transcript (conf=%s), nothing to send",
                         self.tag, info.turn_index, conf_s)
                return
            if self.settings.live_captions:
                await self.send_caption_up_to(text)  # flush the tail so the history is complete
            self.last_sent = ""
            self.prev_update = ""
            self.handoffs += 1
            log.info("[%s] turn %d EndOfTurn conf=%s -> Codex: %s", self.tag, info.turn_index, conf_s, trim(text))
            await self.emit(self.done_event(text))
            handoff = self.handoff_event(text)
            if handoff is not None:
                await self.emit(handoff)
            return
        log.debug("[%s] turn %d %s conf=%s", self.tag, info.turn_index, info.event, conf_s)

    async def send_caption_up_to(self, text: str) -> None:
        """Append whatever of `text` Codex has not seen yet for this turn."""
        if not text or self.last_sent.startswith(text):
            return  # nothing new, or Codex already shows more than this
        if text.startswith(self.last_sent):
            delta = text[len(self.last_sent):]
        else:
            # Flux revised a word we already showed. Codex cannot rewrite, so append from the
            # common prefix onward; the finished transcript in the handoff is still exact.
            delta = text[len(stable_prefix(self.last_sent, text)):]
            if delta and not delta[0].isspace():
                delta = " " + delta
        if not delta:
            return
        self.last_sent = text
        await self.emit(self.delta_event(delta))

    def delta_event(self, delta: str) -> dict:
        if self.dialect == "v1":
            return {"type": "conversation.input_transcript.delta", "delta": delta}
        return {"type": "conversation.item.input_audio_transcription.delta",
                "item_id": self.turn_item_id, "delta": delta}

    def done_event(self, text: str) -> dict:
        if self.dialect == "v1":
            return {"type": "conversation.input_transcript.turn_marked", "transcript": text}
        return {"type": "conversation.item.input_audio_transcription.completed",
                "item_id": self.turn_item_id, "transcript": text}

    def handoff_event(self, text: str) -> Optional[dict]:
        handoff_id = f"handoff_{secrets.token_hex(6)}"
        item_id = self.turn_item_id or f"item_{secrets.token_hex(6)}"
        if self.dialect == "v1":
            return {"type": "conversation.handoff.requested", "handoff_id": handoff_id,
                    "item_id": item_id, "input_transcript": text}
        if self.session_type == "transcription":
            return None  # v2 transcription sessions carry no handoff; Codex only records the text
        call_id = f"call_{secrets.token_hex(6)}"
        return {"type": "conversation.item.done",
                "item": {"id": item_id, "type": "function_call", "name": V2_HANDOFF_TOOL,
                         "call_id": call_id, "arguments": json.dumps({"prompt": text})}}

    async def emit(self, event: dict) -> None:
        if self.ws is None or self.ws.closed or self.flushing:
            self.pending.append(event)
            return
        try:
            await self.ws.send_json(event)
        except ConnectionResetError:
            self.pending.append(event)

    # ---- sideband in ---------------------------------------------------------------

    async def run_ws(self, ws: web.WebSocketResponse) -> None:
        self.ws = ws
        self.attached = True
        log.info("[%s] sideband WebSocket attached", self.tag)
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    log.warning("[%s] non-JSON text frame from Codex", self.tag)
                    continue
                if await self.handle_codex_message(data):
                    break
            elif msg.type == WSMsgType.BINARY:
                await self.feed_pcm16_16k(bytes(msg.data))  # not a Codex shape; accept anyway
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                break
        log.info("[%s] sideband WebSocket closed", self.tag)
        self.ws = None

    async def handle_codex_message(self, data: dict) -> bool:
        """Returns True when the session should end."""
        kind = data.get("type")
        if kind == "session.update":
            session = data.get("session") or {}
            stype = session.get("type") or "quicksilver"
            self.session_type = stype
            self.dialect = "v1" if stype == "quicksilver" else "v2"
            log.info("[%s] session.update type=%s -> dialect %s", self.tag, stype, self.dialect)
            await self.emit({"type": "session.updated",
                             "session": {"id": self.session_id, "type": stype}})
            # Events emitted during the flush queue behind it, so Codex sees them in order.
            self.flushing = True
            try:
                while self.pending:
                    try:
                        await self.ws.send_json(self.pending[0])  # type: ignore[union-attr]
                    except ConnectionResetError:
                        break  # the sideband went away: keep the rest, in order, for the next one
                    self.pending.pop(0)
            finally:
                self.flushing = False
            return False
        if kind == "input_audio_buffer.append":
            await self.feed_codex_ws_audio(data.get("audio", ""))
            return False
        if kind == "conversation.handoff.append":
            text = data.get("output_text", "")
            log.info("[%s] Codex reply for %s: %s", self.tag, data.get("handoff_id"), trim(text))
            return False
        if kind == "conversation.item.create":
            item = data.get("item") or {}
            log.info("[%s] Codex context item (%s) ignored", self.tag, item.get("role") or item.get("type"))
            return False
        if kind == "session.close":
            log.info("[%s] Codex closed the session", self.tag)
            return True
        if kind in ("response.create", "input_audio_buffer.commit"):
            return False
        log.debug("[%s] unhandled Codex message %s", self.tag, kind)
        return False

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self.flux is not None:
            try:
                await self.flux.close(wait_s=3.0)
            except Exception:  # noqa: BLE001
                log.exception("[%s] Flux close failed", self.tag)
        if self.pc is not None:
            await self.pc.close()
        if self.ws is not None and not self.ws.closed:
            await self.ws.close()
        current = asyncio.current_task()
        for task in list(self._tasks):
            if task is not current:
                task.cancel()  # the pump is idle once `closed` is set; this just reaps it
        log.info("[%s] session closed after %.0fs, %d handoff(s)", self.tag,
                 time.monotonic() - self._started, self.handoffs)


# ---- HTTP layer --------------------------------------------------------------------

def build_app(settings: Settings) -> web.Application:
    app = web.Application(client_max_size=4 * 1024 * 1024)
    app["settings"] = settings
    app["sessions"] = {}

    async def healthz(_request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "sessions": len(app["sessions"])})

    async def dispatch(request: web.Request) -> web.StreamResponse:
        path = request.path.rstrip("/")
        if not origin_allowed(request.headers.get("Origin")):
            # Blocks browser pages (any site you visit) from opening Flux sessions on your key.
            log.warning("rejected %s %s from non-local Origin", request.method, path)
            return web.Response(status=403, text="cross-origin requests are not allowed")
        if request.method == "POST" and path.endswith("/realtime/calls"):
            return await create_call(request)
        if request.method == "GET" and (path.endswith("/realtime") or path == "/realtime"):
            return await realtime_ws(request)
        if request.method == "GET" and path.endswith("/live"):
            log.warning("realtime v3 (/live) requested; only v1 and v2 are supported")
            return web.Response(status=404, text="realtime v3 (/live) is not supported by this shim")
        return web.Response(status=404, text="not found")

    async def create_call(request: web.Request) -> web.Response:
        sdp: Optional[str] = None
        session_json: dict = {}
        ctype = request.content_type
        if ctype == "multipart/form-data":
            reader = await request.multipart()
            async for part in reader:
                if part.name == "sdp":
                    sdp = (await part.read(decode=False)).decode("utf-8", "replace")
                elif part.name == "session":
                    try:
                        session_json = json.loads(await part.text())
                    except json.JSONDecodeError:
                        session_json = {}
        elif ctype == "application/json":
            body = await request.json()
            sdp = body.get("sdp")
            session_json = body.get("session") or {}
        else:
            sdp = await request.text()
        if not sdp or not sdp.startswith("v=0"):
            return web.Response(status=400, text="expected an SDP offer")

        call_id = f"rtc_{secrets.token_hex(12)}"
        session = VoiceSession(settings, call_id)
        session.session_type = session_json.get("type") or "quicksilver"
        session.dialect = "v1" if session.session_type == "quicksilver" else "v2"
        app["sessions"][call_id] = session
        log.info("[%s] call created (session type %s, query %s)", call_id, session.session_type,
                 dict(request.query) or "{}")
        try:
            answer = await session.start_webrtc(sdp)
        except Exception:  # noqa: BLE001
            log.exception("[%s] WebRTC answer failed", call_id)
            app["sessions"].pop(call_id, None)
            await session.close()  # releases the peer connection
            return web.Response(status=500, text="webrtc negotiation failed")
        session.spawn(expire_if_orphaned(session), f"expire-{call_id}")
        location = f"{settings.location_prefix}/realtime/calls/{call_id}"
        return web.Response(status=201, body=answer.encode(), content_type="application/sdp",
                            headers={"Location": location})

    async def expire_if_orphaned(session: VoiceSession) -> None:
        """Codex opens the sideband right after the answer; a call without one is dead."""
        await asyncio.sleep(settings.orphan_call_timeout_s)
        if session.attached or session.closed:
            return
        log.warning("[%s] no sideband after %.0fs; dropping the call", session.tag,
                    settings.orphan_call_timeout_s)
        app["sessions"].pop(session.call_id, None)
        await session.close()

    async def realtime_ws(request: web.Request) -> web.StreamResponse:
        call_id = request.query.get("call_id")
        if call_id:
            session = app["sessions"].get(call_id)
            if session is None:
                log.warning("sideband for unknown call id")
                return web.Response(status=404, text="unknown call_id")
        else:
            session = VoiceSession(settings, None)
            app["sessions"][session.session_id] = session
            log.info("[%s] WebSocket transport session (no call)", session.session_id)
        ws = web.WebSocketResponse(heartbeat=20.0, max_msg_size=8 * 1024 * 1024)
        await ws.prepare(request)
        try:
            await session.run_ws(ws)
        finally:
            await session.close()
            app["sessions"].pop(session.call_id or session.session_id, None)
        return ws

    app.router.add_get("/healthz", healthz)
    app.router.add_route("*", "/{tail:.*}", dispatch)
    return app


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Codex CLI voice -> Deepgram Flux STT shim")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--eot-threshold", type=float, default=0.8,
                    help="Flux eot_threshold (default 0.8; Flux default is 0.7)")
    ap.add_argument("--eot-timeout-ms", type=int, default=6000,
                    help="Flux eot_timeout_ms (default 6000; Flux default is 5000)")
    ap.add_argument("--eager-eot-threshold", type=float, default=None)
    ap.add_argument("--keyterm", action="append", default=[], help="repeatable Flux keyterm")
    ap.add_argument("--no-live-captions", action="store_true",
                    help="only send the finished transcript, no live deltas")
    ap.add_argument("--verbose", "-v", action="store_true")
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    if not os.environ.get("DEEPGRAM_API_KEY", "").strip():
        print("error: DEEPGRAM_API_KEY is not set. Export your Deepgram API key and start the "
              "shim again.", file=sys.stderr)
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("aioice").setLevel(logging.WARNING)
    logging.getLogger("aiortc").setLevel(logging.WARNING)
    # websockets logs request headers at DEBUG, including `Authorization: Token <key>`.
    logging.getLogger("websockets").setLevel(logging.INFO)
    settings = Settings(
        eot_threshold=args.eot_threshold,
        eot_timeout_ms=args.eot_timeout_ms,
        eager_eot_threshold=args.eager_eot_threshold,
        keyterms=args.keyterm,
        live_captions=not args.no_live_captions,
    )
    app = build_app(settings)
    log.info("Codex voice shim listening on http://%s:%d  (calls: /v1/realtime/calls, sideband: /v1/realtime)",
             args.host, args.port)
    log.info("Codex config: experimental_realtime_webrtc_call_base_url = \"http://%s:%d/v1\", "
             "experimental_realtime_ws_base_url = \"ws://%s:%d/v1\"", args.host, args.port, args.host, args.port)
    web.run_app(app, host=args.host, port=args.port, print=None, access_log=None)


if __name__ == "__main__":
    main()

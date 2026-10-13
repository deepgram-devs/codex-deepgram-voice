"""Local realtime shim: Codex CLI voice in, Deepgram Flux STT underneath.

Codex CLI starts voice as a WebRTC call plus a sideband WebSocket (see DECISION.md,
question 2 and "Realtime v3"). Codex 0.159 speaks realtime v1 ("quicksilver"); 0.162 speaks
v3 ("frameless bidi", /live). This server stands in for OpenAI's realtime backend on localhost
and serves both:

  POST {prefix}/realtime/calls           v1 call: WebRTC offer in (multipart `sdp` + `session`,
                                         or raw application/sdp), SDP answer out, call id in
                                         the Location header
  GET  {prefix}/realtime?call_id=<id>    sideband WebSocket for that call (v1 "quicksilver")
  GET  {prefix}/realtime                 plain WebSocket transport (v1 or v2 shapes, audio as
                                         input_audio_buffer.append, base64 PCM16 @ 24 kHz)
  POST {prefix}/live                     v3 call: same body and answer as the v1 call
  GET  {prefix}/live/<call_id>           sideband WebSocket for a v3 call; the shim sends
                                         session.started first, Codex sends no session.update
  GET  {prefix}/live                     plain WebSocket transport in the v3 shape (audio as
                                         input_audio.append, base64 PCM16 @ 24 kHz)
  GET  /healthz

Audio from either path is resampled to 16 kHz mono PCM16 and streamed to Flux in 80 ms
chunks. Flux `Update` events become live caption deltas; Flux `EndOfTurn` becomes the
finished transcript plus a handoff request (v3: `delegation.created`), which is what makes
Codex start the turn.

Auth headers Codex sends (ChatGPT tokens, account ids) are ignored and never logged.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import ipaddress
import json
import logging
import os
import secrets
import sys
import time
from dataclasses import dataclass, field
from typing import Callable, Optional
from urllib.parse import urlsplit

import av
import numpy as np
import aiohttp.http_exceptions
import websockets
from aiohttp import BodyPartReader, WSMsgType, web
from aiohttp.web import RequestPayloadError
from aiortc import RTCConfiguration, RTCPeerConnection, RTCSessionDescription
from aiortc.mediastreams import MediaStreamError
from aiortc.sdp import SessionDescription

from .flux_client import SAMPLE_RATE, FluxSession, TurnInfo, describe_connect_error

log = logging.getLogger("shim")

CODEX_WS_PCM_RATE = 24_000  # codex-api methods_common.rs REALTIME_AUDIO_SAMPLE_RATE
V2_HANDOFF_TOOL = "background_agent"  # codex-api protocol_v2.rs BACKGROUND_AGENT_TOOL_NAME
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
MAX_CALL_BODY = 4 * 1024 * 1024  # an SDP offer plus the session JSON is a few KB


@dataclass
class Settings:
    eot_threshold: Optional[float] = 0.8
    eot_timeout_ms: Optional[int] = 6000
    eager_eot_threshold: Optional[float] = None
    keyterms: list[str] = field(default_factory=list)
    live_captions: bool = True
    location_prefix: str = "/v1"
    # A sideband that never sends session.update is closed after this long.
    session_update_timeout_s: float = 10.0
    # Flux events queued before session.updated (the sideband can lag the call by up to
    # orphan_call_timeout_s, and Flux sends several Updates a second); past either cap the
    # session is closed rather than letting a client that never finishes the handshake grow it.
    pending_max_events: int = 2048
    pending_max_bytes: int = 1 << 20
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


def is_loopback(host: str) -> bool:
    """True only for a bind address that other machines cannot reach."""
    if host in LOCAL_HOSTS:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False  # "", "0.0.0.0", hostnames: aiohttp would bind every interface or resolve it


def as_dict(value: object) -> dict:
    """A JSON member that must be an object: anything else counts as absent."""
    return value if isinstance(value, dict) else {}


def session_type_of(session: dict) -> str:
    """The `type` of a Codex session object; Codex's v1 shape (`quicksilver`) when missing."""
    stype = session.get("type")
    return stype if isinstance(stype, str) and stype else "quicksilver"


def is_live(path: str) -> bool:
    """The v3 endpoint (`{prefix}/live`, realtime_call.rs:66-75), trailing slash already stripped."""
    return path == "/live" or path.endswith("/live")


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
        self.pending_bytes = 0  # serialized size of `pending`, against Settings.pending_max_bytes
        self.dialect = "v1"  # "v1" (quicksilver), "v2" (public realtime shape), "v3" (/live)
        self.session_type = "quicksilver"
        self.last_sent = ""  # caption text already appended in Codex for the current turn
        self.prev_update = ""  # previous Flux Update transcript, to find the stable prefix
        self.turn_item_id = ""
        self.handoffs = 0
        self.closed = False
        self._closing = False  # close() has started (it drains audio before it sets `closed`)
        self.flushing = False  # draining `pending` after session.update
        self.ready = False  # session.updated (v3: session.started) sent on the current sideband
        self.attached = False  # a sideband WebSocket has connected at least once
        self.handshake_failed = False  # prepare() raised; _teardown closes a sent 101 with 1011
        self.on_closed: Optional[Callable[[], None]] = None  # set by the app: drops the registry entry
        self._flux_lock = asyncio.Lock()  # one Flux stream per session, even with two audio sources
        self.flux_failed = False  # Flux could not start or died; stop feeding, error sent once
        self._tasks: set[asyncio.Task] = set()  # strong refs to the pump and expiry tasks
        self._ws_resampler = av.AudioResampler(format="s16", layout="mono", rate=SAMPLE_RATE)
        self._rtc_resampler = av.AudioResampler(format="s16", layout="mono", rate=SAMPLE_RATE)
        self.turn_index = -1  # Flux turn_index of the turn in progress, -1 between turns
        self._deadline: Optional[asyncio.Task] = None  # session.update deadline for the sideband
        self._audio_before_update_warned = False
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
        if self.flux is not None:
            return True
        async with self._flux_lock:
            if self.flux_failed or self.closed:
                return False
            if self.flux is not None:  # another audio source opened it while we waited
                return True
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
        event: dict = {"type": "error", "error": {"type": "flux_error", "message": message}}
        if self.dialect == "v3":
            event["message"] = message  # protocol_common.rs:68-83 reads `message` first
        await self.emit(event)

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
                await self.on_flux_error("Deepgram Flux closed the stream unexpectedly. "
                                         "Press F8 to start a new voice session.")

    async def feed_codex_ws_audio(self, b64: str) -> None:
        """input_audio_buffer.append (v1/v2) or input_audio.append (v3): base64 PCM16 mono at
        24 kHz from the WebSocket transport."""
        try:
            raw = base64.b64decode(b64, validate=True)
        except (ValueError, TypeError):
            log.warning("[%s] audio append carried invalid base64; frame dropped", self.tag)
            return
        if len(raw) < 2:
            return
        samples = np.frombuffer(raw[: len(raw) - len(raw) % 2], dtype="<i2").reshape(1, -1)
        frame = av.AudioFrame.from_ndarray(samples, format="s16", layout="mono")
        frame.sample_rate = CODEX_WS_PCM_RATE
        for out in self._ws_resampler.resample(frame):
            await self.feed_pcm16_16k(out.to_ndarray().tobytes())

    async def _drain(self, resampler: av.AudioResampler) -> None:
        """Forward the samples a resampler still holds. The resampler is finished afterwards."""
        if self.closed or self.flux is None or self.flux_failed:
            return
        try:
            tail = resampler.resample(None)
        except Exception:  # noqa: BLE001
            log.exception("[%s] resampler drain failed", self.tag)
            return
        for out in tail:
            await self.feed_pcm16_16k(out.to_ndarray().tobytes())

    def _fresh(self) -> av.AudioResampler:
        return av.AudioResampler(format="s16", layout="mono", rate=SAMPLE_RATE)

    async def _drain_ws_audio(self) -> None:
        """End of the WebSocket audio stream: drain, then start a fresh resampler for any more."""
        # The swap and the drain share no await, so a frame that arrives meanwhile goes to the
        # fresh resampler; a drained one raises EOFError on reuse.
        resampler, self._ws_resampler = self._ws_resampler, self._fresh()
        await self._drain(resampler)

    async def _drain_rtc_audio(self) -> None:
        """End of the WebRTC audio stream, or session close before the track ended."""
        resampler, self._rtc_resampler = self._rtc_resampler, self._fresh()
        await self._drain(resampler)

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
        frames = 0
        try:
            while not self.closed:
                frame = await track.recv()
                frames += 1
                if frames == 1:
                    log.info("[%s] first audio frame: %s Hz, %s, %d samples", self.tag,
                             frame.sample_rate, frame.layout.name, frame.samples)
                for out in self._rtc_resampler.resample(frame):
                    await self.feed_pcm16_16k(out.to_ndarray().tobytes())
        except MediaStreamError:
            log.info("[%s] audio track ended after %d frames", self.tag, frames)
        except Exception:  # noqa: BLE001
            log.exception("[%s] audio pump failed", self.tag)
        await self._drain_rtc_audio()  # the resampler can hold the last few samples back
        if self.flux is not None and not self.closed and not self.flux_failed:
            # Let Flux settle the final turn before the sideband goes away.
            await self.flux.close()

    # ---- Flux events out -----------------------------------------------------------

    async def on_turn(self, info: TurnInfo) -> None:
        conf = info.end_of_turn_confidence
        conf_s = f"{conf:.3f}" if isinstance(conf, (int, float)) else "n/a"
        if info.event == "StartOfTurn":
            if info.turn_index >= 0 and info.turn_index == self.turn_index:
                # A second StartOfTurn for the turn in progress (Flux numbers turns from 0 and
                # never reuses an index within a stream): keep the captions already shown. An
                # event without an index always starts a new turn.
                log.debug("[%s] turn %d repeated StartOfTurn ignored", self.tag, info.turn_index)
                return
            self.turn_index = info.turn_index
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
            self.turn_index = -1
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
            # Flux revised a word we already showed. A delta cannot rewrite, so append from the
            # common prefix onward; the caption then holds the stale words plus the new tail.
            # What happens at EndOfTurn depends on the dialect (codex-api realtime_websocket
            # methods.rs:777-824, apply_transcript_done and append_handoff_input, rust-v0.162.1):
            # - v1/v2: turn_marked / transcription.completed replaces the accumulated caption
            #   with the exact final transcript.
            # - v3: turn.done replaces it only when the final starts with the caption, which a
            #   revision breaks, so the stale caption stays. delegation.created then adds the
            #   exact final as a second user entry, because no entry matches it, and both
            #   entries travel in the handoff's transcript.
            # Either way the text Codex acts on (the handoff/delegation input) is exact.
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
        if self.dialect == "v3":
            return {"type": "input_transcript.added", "item": {"text": delta}}
        return {"type": "conversation.item.input_audio_transcription.delta",
                "item_id": self.turn_item_id, "delta": delta}

    def done_event(self, text: str) -> dict:
        if self.dialect == "v1":
            return {"type": "conversation.input_transcript.turn_marked", "transcript": text}
        if self.dialect == "v3":
            return {"type": "turn.done", "turn": {"role": "user", "transcript": text}}
        return {"type": "conversation.item.input_audio_transcription.completed",
                "item_id": self.turn_item_id, "transcript": text}

    def handoff_event(self, text: str) -> Optional[dict]:
        handoff_id = f"handoff_{secrets.token_hex(6)}"
        item_id = self.turn_item_id or f"item_{secrets.token_hex(6)}"
        if self.dialect == "v1":
            return {"type": "conversation.handoff.requested", "handoff_id": handoff_id,
                    "item_id": item_id, "input_transcript": text}
        if self.dialect == "v3":
            # protocol_frameless_bidi.rs:75-99: the item id is the handoff id Codex replies to
            return {"type": "delegation.created",
                    "item": {"id": handoff_id, "type": "delegation", "target": "client",
                             "content": [{"type": "input_text", "text": text}]}}
        if self.session_type == "transcription":
            return None  # v2 transcription sessions carry no handoff; Codex only records the text
        call_id = f"call_{secrets.token_hex(6)}"
        return {"type": "conversation.item.done",
                "item": {"id": item_id, "type": "function_call", "name": V2_HANDOFF_TOOL,
                         "call_id": call_id, "arguments": json.dumps({"prompt": text})}}

    async def emit(self, event: dict) -> None:
        # Nothing goes out before `session.updated`, and nothing overtakes the queue while it
        # drains, so Codex always sees events in the order Flux produced them.
        if self.ws is None or self.ws.closed or not self.ready or self.flushing:
            self._queue(event)
            return
        try:
            await self.ws.send_json(event)
        except (ConnectionResetError, RuntimeError):  # aiohttp: closing transport / socket not prepared
            self._queue(event)

    def _queue(self, event: dict) -> None:
        size = len(json.dumps(event))
        s = self.settings
        if len(self.pending) >= s.pending_max_events or self.pending_bytes + size > s.pending_max_bytes:
            if not self.closed and not self._closing:
                log.warning("[%s] %d events (%d bytes) queued and the session never started on a "
                            "sideband; closing the session", self.tag, len(self.pending), self.pending_bytes)
                # emit() runs on the Flux reader task; close() must not wait on it from here.
                self.spawn(self.close(), f"overflow-close-{self.tag}")
            return
        self.pending.append(event)
        self.pending_bytes += size

    # ---- sideband in ---------------------------------------------------------------

    async def run_ws(self, ws: web.WebSocketResponse) -> None:
        self.ws = ws
        self.attached = True
        if self.dialect == "v3" and self.call_id:
            # A v3 WebRTC sideband carries no session.update (ws_methods.rs:1082-1112): Codex
            # waits for session.started instead, so send it now and skip the deadline.
            log.info("[%s] sideband WebSocket attached (dialect v3); sending session.started", self.tag)
            await self._start_and_flush(ws, self.started_event())
        else:
            log.info("[%s] sideband WebSocket attached", self.tag)
            self._deadline = self.spawn(self._session_update_deadline(ws), f"deadline-{self.tag}")
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except (json.JSONDecodeError, RecursionError):  # not JSON, or nested past the parser
                    log.warning("[%s] non-JSON text frame from Codex", self.tag)
                    continue
                if not isinstance(data, dict):
                    log.warning("[%s] Codex message is not a JSON object; ignored", self.tag)
                    continue
                if await self.handle_codex_message(data):
                    break
            elif msg.type == WSMsgType.BINARY:
                if self._audio_allowed():
                    await self.feed_pcm16_16k(bytes(msg.data))  # not a Codex shape; accepted once initialized
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                break
        log.info("[%s] sideband WebSocket closed", self.tag)
        if self.ws is ws:  # a second sideband that slipped in before this one ended keeps its state
            self._cancel_deadline()
            self.ws = None
            self.ready = False

    async def _session_update_deadline(self, ws: web.WebSocketResponse) -> None:
        await asyncio.sleep(self.settings.session_update_timeout_s)
        if self.ready or self.ws is not ws or ws.closed:
            return
        log.warning("[%s] no session.update after %.0fs; closing the sideband", self.tag,
                    self.settings.session_update_timeout_s)
        await ws.close(code=1008, message=b"session.update not received")  # ends run_ws's loop

    def _cancel_deadline(self) -> None:
        if self._deadline is not None:
            self._deadline.cancel()
            self._deadline = None

    def _audio_allowed(self) -> bool:
        """WebSocket-transport audio counts only after session.update (WebRTC audio is not gated:
        the call exists before the sideband does, and the orphan timer covers a call without one)."""
        if self.ready:
            return True
        if not self._audio_before_update_warned:
            self._audio_before_update_warned = True
            log.warning("[%s] audio before session.update ignored", self.tag)
        return False

    def started_event(self) -> dict:
        """v3's answer to a new sideband or a session.update (protocol_common.rs:27-44)."""
        return {"type": "session.started", "session": {"id": self.session_id}}

    async def _start_and_flush(self, ws: web.WebSocketResponse, first: dict) -> None:
        """Send `first` (session.updated or session.started), then the queued events in order,
        then mark the sideband ready. Events emitted during the flush queue behind it, so Codex
        sees them in the order Flux produced them."""
        self.flushing = True
        try:
            await ws.send_json(first)
            while self.pending:
                try:
                    await ws.send_json(self.pending[0])
                except (ConnectionResetError, RuntimeError):
                    return  # the sideband went away mid-flush; the handler closes the session
                self.pending_bytes -= len(json.dumps(self.pending.pop(0)))
            self.ready = True
            self._cancel_deadline()
        except (ConnectionResetError, RuntimeError):
            return
        finally:
            self.flushing = False

    async def handle_codex_message(self, data: dict) -> bool:
        """Returns True when the session should end."""
        kind = data.get("type")
        if kind == "session.update":
            session = as_dict(data.get("session"))  # a non-object `session` is treated as empty
            ws = self.ws
            if self.dialect == "v3":
                # v3 sessions carry no `type`; the dialect came from the /live path
                log.info("[%s] session.update -> dialect v3", self.tag)
                if ws is not None and not ws.closed:
                    await self._start_and_flush(ws, self.started_event())
                return False
            stype = session_type_of(session)
            self.session_type = stype
            self.dialect = "v1" if stype == "quicksilver" else "v2"
            log.info("[%s] session.update type=%s -> dialect %s", self.tag, stype, self.dialect)
            if ws is None or ws.closed:
                return False
            await self._start_and_flush(
                ws, {"type": "session.updated", "session": {"id": self.session_id, "type": stype}})
            return False
        if kind in ("input_audio_buffer.append", "input_audio.append"):
            if self._audio_allowed():
                await self.feed_codex_ws_audio(data.get("audio", ""))  # non-base64 input is dropped there
            return False
        if kind == "input_audio_buffer.commit":
            await self._drain_ws_audio()
            return False
        if kind == "conversation.handoff.append":
            text = data.get("output_text")
            log.info("[%s] Codex reply for %s: %s", self.tag, data.get("handoff_id"),
                     trim(text) if isinstance(text, str) else "(no text)")
            return False
        if kind == "delegation.context.append":
            content = data.get("content")
            parts = content if isinstance(content, list) else []
            text = "".join(p.get("text") for p in parts
                           if isinstance(p, dict) and isinstance(p.get("text"), str))
            log.info("[%s] Codex reply for %s: %s", self.tag, data.get("delegation_item_id"),
                     trim(text) if text else "(no text)")
            return False
        if kind == "session.context.append":
            log.info("[%s] Codex session context (%s) ignored", self.tag,
                     data.get("channel") if isinstance(data.get("channel"), str) else "no channel")
            return False
        if kind == "conversation.item.create":
            item = as_dict(data.get("item"))
            log.info("[%s] Codex context item (%s) ignored", self.tag, item.get("role") or item.get("type"))
            return False
        if kind == "session.close":
            log.info("[%s] Codex closed the session", self.tag)
            return True
        if kind == "response.create":
            return False
        log.debug("[%s] unhandled Codex message %s", self.tag, kind)
        return False

    async def close(self) -> None:
        if self.closed or self._closing:
            return
        self._closing = True
        try:
            if self.flux is not None and not self.flux_failed:
                try:
                    # Codex's WebSocket transport never sends commit, and a call can end before
                    # its track does: send what both resamplers still hold. Audio that arrives
                    # after this point belongs to no turn and is dropped.
                    await self._drain_ws_audio()
                    await self._drain_rtc_audio()
                except Exception:  # noqa: BLE001
                    log.exception("[%s] final drain failed", self.tag)
        finally:
            # Everything below runs even if the drain was cancelled: the Flux socket, the peer
            # connection, and the registry entry must not outlive the session.
            self.closed = True
            self._cancel_deadline()
            await self._teardown()

    async def _teardown(self) -> None:
        try:
            if self.flux is not None:
                try:
                    await self.flux.close(wait_s=3.0)
                except Exception:  # noqa: BLE001
                    log.exception("[%s] Flux close failed", self.tag)
            if self.pc is not None:
                try:
                    await self.pc.close()
                except Exception as exc:  # noqa: BLE001
                    # aiortc can fail to close a connection whose negotiation failed; log only
                    # the type, since the traceback can quote the offer
                    log.warning("[%s] peer connection close failed (%s)", self.tag,
                                type(exc).__name__)
            # close only after prepare() has created the response writer: close() on an
            # unprepared WebSocketResponse raises
            if self.ws is not None and self.ws.prepared and not self.ws.closed:
                try:
                    # prepare() raised; close with 1011 (the 101 may have been sent)
                    if self.handshake_failed:
                        await self.ws.close(code=1011, message=b"handshake failed")
                    else:
                        await self.ws.close()
                except Exception:  # noqa: BLE001
                    log.exception("[%s] sideband close failed", self.tag)
            current = asyncio.current_task()
            for task in list(self._tasks):
                if task is not current:
                    task.cancel()  # the pump is idle once `closed` is set; this just reaps it
            log.info("[%s] session closed after %.0fs, %d handoff(s)", self.tag,
                     time.monotonic() - self._started, self.handoffs)
        finally:
            if self.on_closed is not None:  # the registry entry goes whatever happened above
                self.on_closed()


# ---- HTTP layer --------------------------------------------------------------------

def build_app(settings: Settings) -> web.Application:
    app = web.Application(client_max_size=MAX_CALL_BODY)
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
        parent, _, last = path.rpartition("/")
        if request.method == "POST" and path.endswith("/realtime/calls"):
            return await create_call(request, "v1")
        if request.method == "POST" and is_live(path):
            return await create_call(request, "v3")  # the dialect comes from the path: v3 has no session.type
        if request.method == "GET" and (path.endswith("/realtime") or path == "/realtime"):
            return await realtime_ws(request, request.query.get("call_id"))
        if request.method == "GET" and is_live(path):
            return await realtime_ws(request, None, dialect="v3")
        if request.method == "GET" and is_live(parent) and last:
            return await realtime_ws(request, last)  # ws_methods.rs:1226-1260: call id as a path segment
        return web.Response(status=404, text="not found")

    def register(session: VoiceSession) -> None:
        key = session.call_id or session.session_id
        app["sessions"][key] = session
        session.on_closed = lambda: app["sessions"].pop(key, None)

    async def create_call(request: web.Request, dialect: str) -> web.Response:
        if (request.content_length or 0) > MAX_CALL_BODY:
            return web.Response(status=413, text="offer too large")
        # Codex sends multipart (`sdp` + `session` parts); JSON and raw SDP are accepted too.
        # Nothing here may raise on a malformed body: the client gets a 400 with the reason. A body
        # over the size cap raises aiohttp's 413. The body itself is never logged.
        def reject(reason: str, detail: str | None = None) -> web.Response:
            # `detail` goes to the log line only, never to the client
            if detail:
                log.warning("call request rejected: %s (%s)", reason, detail)
            else:
                log.warning("call request rejected: %s", reason)
            return web.Response(status=400, text=reason)

        sdp: object = None
        session_json: object = None
        ctype = request.content_type
        try:
            if ctype == "multipart/form-data":
                total = 0
                async for part in await request.multipart():
                    if not isinstance(part, BodyPartReader):
                        return reject("nested multipart is not accepted")
                    # read in chunks so the size cap applies to all parts together. The count is of
                    # bytes as read from the request body: after Content-Encoding, before any
                    # Content-Transfer-Encoding.
                    data = bytearray()
                    while chunk := await part.read_chunk(64 * 1024):
                        data.extend(chunk)
                        total += len(chunk)
                        if total > MAX_CALL_BODY:
                            raise web.HTTPRequestEntityTooLarge(max_size=MAX_CALL_BODY, actual_size=total)
                    if part.name == "sdp":
                        sdp = bytes(part.decode(data)).decode("utf-8", "replace")
                    elif part.name == "session":
                        charset = part.get_charset(default="utf-8")
                        session_json = json.loads(bytes(part.decode(data)).decode(charset))
            elif ctype == "application/json":
                body = await request.json()
                if not isinstance(body, dict):
                    return reject("call request must be a JSON object")
                sdp = body.get("sdp")
                session_json = body.get("session")
            else:
                sdp = await request.text()
        except RequestPayloadError:
            # The body does not match its Content-Encoding (e.g. gzip declared, plain bytes
            # sent). aiohttp logs the payload error a second time after the response unless
            # the connection is closed and the payload is ended here.
            resp = reject("call request body does not match its Content-Encoding")
            resp.force_close()
            request.content.feed_eof()
            return resp
        except ConnectionError:  # the client disconnected before the body was complete
            # the client has disconnected, so this 400 is not delivered; it keeps aiohttp from
            # logging the reset as a server error
            return reject("client disconnected mid-body")
        except json.JSONDecodeError:
            return reject("call request is not valid JSON")
        except UnicodeDecodeError:
            return reject("call request body is not valid text for its charset")
        except RecursionError:
            return reject("call request JSON is nested too deep")
        except LookupError:
            return reject("unknown charset in Content-Type")
        except RuntimeError:
            return reject("unknown Content-Transfer-Encoding in a multipart part")
        except ValueError:
            # the reason depends on the content type; a multipart body gets the multipart reason
            # even when the ValueError came from JSON inside a part
            return reject("malformed multipart body" if ctype == "multipart/form-data"
                          else "call request could not be parsed")
        except aiohttp.http_exceptions.HttpProcessingError:  # e.g. a part header without a colon
            return reject("malformed multipart body")
        if session_json is None:
            session_json = {}
        if not isinstance(session_json, dict):
            return reject("session must be a JSON object")
        if not isinstance(sdp, str) or not sdp.startswith("v=0"):
            return reject("sdp must be a string starting with v=0")
        try:
            # json.loads accepts a lone surrogate escape; aiortc would negotiate it and echo it
            # into an answer that cannot be encoded
            sdp.encode("utf-8")
        except UnicodeEncodeError:
            return reject("sdp is not valid text")
        # aiortc fails a BUNDLE token with no media line only after its background connect task
        # has started, and that task then fails with an unretrieved exception; check it here with
        # aiortc's own parser, so the mids are the ones aiortc will see
        try:
            desc = SessionDescription.parse(sdp)
        except Exception as exc:  # the parser raises assorted types on a malformed offer
            return reject("SDP offer could not be parsed", detail=type(exc).__name__)
        mids = {m.rtp.muxId for m in desc.media if m.rtp.muxId}
        for group in desc.group:
            if group.semantic == "BUNDLE" and any(str(item) not in mids for item in group.items):
                return reject("BUNDLE group names a mid that no media line has")
        # aioice keeps retrying a check to a port-0 candidate, and the retry fails with an ERROR
        # when the session closes; aiortc's parser also passes ports past 65535,
        # which fail with "Fatal write error on datagram transport" ERRORs
        for m in desc.media:
            for c in m.ice_candidates:
                if not 1 <= c.port <= 65535:
                    return reject("ICE candidate port out of range")

        call_id = f"rtc_{secrets.token_hex(12)}"
        session = VoiceSession(settings, call_id)
        if dialect == "v3":
            session.dialect = "v3"
            session.session_type = "live"
            register(session)
            log.info("[%s] call created (dialect v3, query %s)", call_id, dict(request.query) or "{}")
        else:
            session.session_type = session_type_of(session_json)
            session.dialect = "v1" if session.session_type == "quicksilver" else "v2"
            register(session)
            log.info("[%s] call created (session type %s, query %s)", call_id, session.session_type,
                     dict(request.query) or "{}")
        try:
            answer = (await session.start_webrtc(sdp)).encode()
        except asyncio.CancelledError:
            await session.close()  # cancelled at shutdown: release the session, then propagate
            raise
        except Exception as exc:  # noqa: BLE001
            # aiortc raises ValueError, KeyError, IndexError, or AssertionError depending on
            # which line of a bad offer it trips on; all of them are the client's offer. Encoding
            # the answer is inside the try so any failure after register() releases the session.
            # Only the exception type is logged: the message can quote the offer.
            await session.close()  # releases the peer connection and the registry entry
            return reject("SDP offer could not be negotiated", type(exc).__name__)
        session.spawn(expire_if_orphaned(session), f"expire-{call_id}")
        if dialect == "v3":
            location = f"{settings.location_prefix}/live/{call_id}"
        else:
            location = f"{settings.location_prefix}/realtime/calls/{call_id}"
        return web.Response(status=201, body=answer, content_type="application/sdp",
                            headers={"Location": location})

    async def expire_if_orphaned(session: VoiceSession) -> None:
        """Codex opens the sideband right after the answer; a call without one is dead."""
        await asyncio.sleep(settings.orphan_call_timeout_s)
        if session.attached or session.closed:
            return
        log.warning("[%s] no sideband after %.0fs; dropping the call", session.tag,
                    settings.orphan_call_timeout_s)
        await session.close()

    async def realtime_ws(request: web.Request, call_id: Optional[str],
                          dialect: str = "v1") -> web.StreamResponse:
        """A call's sideband when `call_id` is given (the call's dialect applies), else a plain
        WebSocket transport session in `dialect` (v1 here means v1 or v2, set by session.update)."""
        ws = web.WebSocketResponse(heartbeat=20.0, max_msg_size=8 * 1024 * 1024)
        if not ws.can_prepare(request).ok:
            # Checked before any session is looked up, registered, or claimed, so a plain GET
            # leaves no state behind and cannot block the real sideband of a call.
            log.warning("realtime endpoint request rejected: request is not a WebSocket upgrade")
            return web.Response(status=400, text="request is not a WebSocket upgrade")
        if call_id:
            session = app["sessions"].get(call_id)
            if session is None or session.closed:
                log.warning("sideband for unknown or expired call id")
                return web.Response(status=404, text="unknown call_id")
            if session.ws is not None and not session.ws.closed:
                log.warning("[%s] second sideband for the same call refused", session.tag)
                return web.Response(status=409, text="this call already has a sideband")
            session.attached = True  # claim it before the upgrade so the orphan timer cannot fire mid-attach
        else:
            session = VoiceSession(settings, None)
            session.dialect = dialect
            register(session)
            log.info("[%s] WebSocket transport session (no call, dialect %s)", session.session_id,
                     "v3" if dialect == "v3" else "v1/v2")
        session.ws = ws  # claimed before the upgrade awaits, so a concurrent second sideband sees it
        try:
            # If prepare() fails, the 400 or 500 below reaches the client only when the 101 was
            # not sent; if prepare() raised after sending the 101, _teardown closes the socket
            # with code 1011. session.ws stays set either way: _teardown closes it only if
            # prepare() got as far as creating the response writer.
            try:
                await ws.prepare(request)
            except OSError as exc:  # includes ConnectionError: the client disconnected mid-handshake
                session.handshake_failed = True
                log.warning("[%s] WebSocket handshake failed (%s)", session.tag, type(exc).__name__)
                return web.Response(status=400, text="WebSocket handshake failed")
            except Exception:  # noqa: BLE001  # a shim bug: keep the traceback visible
                session.handshake_failed = True
                log.exception("[%s] WebSocket handshake failed inside the shim", session.tag)
                return web.Response(status=500, text="WebSocket handshake failed inside the shim")
            await session.run_ws(ws)
        finally:
            await session.close()  # releases the claim and the registry entry either way
        return ws

    # aioice 0.10.2: a STUN check to a candidate that never answers schedules
    # Transaction.__retry with call_later. When the retry fires after the session closed the
    # transport, it raises AttributeError ('NoneType' object has no attribute
    # 'call_exception_handler'), which asyncio logs as an ERROR "Exception in callback
    # Transaction.__retry()". Only that failure is filtered: the callback's module must be
    # `aioice.stun`, its qualified name must end in `Transaction.__retry`, the exception must
    # be an AttributeError, and its message must contain "'NoneType' object has no attribute".
    # A bound method's __module__ and __qualname__ are its function's, and the __qualname__ is
    # `Transaction.__retry`, unmangled. The module check keeps same-named classes elsewhere out.
    # The match is not on the handle's repr, which also quotes the callback's arguments. The
    # first occurrence per loop logs one WARNING; later ones are dropped. The wrapper
    # delegates everything it does not match to the loop's previous handler. It is installed
    # once per loop and stays for the loop's lifetime, because aioice retries can fire after
    # the app that created the connection has been cleaned up.
    async def install_loop_exception_handler(_app: web.Application) -> None:
        loop = asyncio.get_running_loop()
        if getattr(loop, "_codex_shim_aioice_handler", None) is not None:
            return
        previous = loop.get_exception_handler()
        warned = False

        def handler(loop_: asyncio.AbstractEventLoop, context: dict) -> None:
            nonlocal warned
            cb = getattr(context.get("handle"), "_callback", None)
            name = getattr(cb, "__qualname__", "")
            exc = context.get("exception")
            if (getattr(cb, "__module__", "") == "aioice.stun"
                    and isinstance(name, str) and name.endswith("Transaction.__retry")
                    and isinstance(exc, AttributeError)
                    and "'NoneType' object has no attribute" in str(exc)):
                if not warned:
                    warned = True
                    log.warning("aioice STUN retry fired after its transport closed "
                                "(AttributeError); not an error")
                return
            if previous is not None:
                previous(loop_, context)
            else:
                loop_.default_exception_handler(context)

        loop.set_exception_handler(handler)
        loop._codex_shim_aioice_handler = handler  # type: ignore[attr-defined]

    app.on_startup.append(install_loop_exception_handler)
    app.router.add_get("/healthz", healthz)
    app.router.add_route("*", "/{tail:.*}", dispatch)
    return app


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Codex CLI voice -> Deepgram Flux STT shim. Serves Codex realtime v1 "
                    "(POST /v1/realtime/calls, sideband /v1/realtime?call_id=..., used by Codex 0.159) "
                    "and v3 (POST /v1/live, sideband /v1/live/<call_id>, used by Codex 0.162), "
                    "plus the plain WebSocket transport on /v1/realtime and /v1/live.")
    ap.add_argument("--host", default="127.0.0.1",
                    help="address to bind (default 127.0.0.1; anything non-loopback needs "
                         "--allow-unauthenticated-remote)")
    ap.add_argument("--port", type=int, default=8765, help="port to listen on (default 8765)")
    ap.add_argument("--eot-threshold", type=float, default=0.8,
                    help="Flux eot_threshold (default 0.8; Flux default is 0.7)")
    ap.add_argument("--eot-timeout-ms", type=int, default=6000,
                    help="Flux eot_timeout_ms (default 6000; Flux default is 5000)")
    ap.add_argument("--eager-eot-threshold", type=float, default=None,
                    help="Flux eager_eot_threshold (off by default: Codex cannot take a turn back)")
    ap.add_argument("--keyterm", action="append", default=[], help="repeatable Flux keyterm")
    ap.add_argument("--no-live-captions", action="store_true",
                    help="only send the finished transcript, no live deltas")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="debug logging, including every Flux turn event")
    ap.add_argument("--allow-unauthenticated-remote", action="store_true",
                    help="bind a non-loopback --host. The shim has no client authentication: anyone "
                         "who can reach the port can stream audio on your DEEPGRAM_API_KEY. Meant "
                         "only for a container whose port is published on 127.0.0.1.")
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    if not is_loopback(args.host) and not args.allow_unauthenticated_remote:
        print(f"error: --host {args.host!r} is not a loopback address. The shim has no client "
              "authentication, so it only binds 127.0.0.1 by default. Pass "
              "--allow-unauthenticated-remote only inside a container whose port is published on 127.0.0.1.",
              file=sys.stderr)
        sys.exit(2)
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
    if not is_loopback(args.host):
        log.warning("binding %s: no client authentication; anyone who can reach this port can use "
                    "your Deepgram key. Publish it on 127.0.0.1 only.", args.host)
    log.info("Codex voice shim listening on http://%s:%d  (calls: /v1/realtime/calls and /v1/live, "
             "sideband: /v1/realtime and /v1/live/<call_id>)", args.host, args.port)
    log.info("Codex config: experimental_realtime_webrtc_call_base_url = \"http://%s:%d/v1\", "
             "experimental_realtime_ws_base_url = \"ws://%s:%d/v1\"", args.host, args.port, args.host, args.port)
    web.run_app(app, host=args.host, port=args.port, print=None, access_log=None)


if __name__ == "__main__":
    main()

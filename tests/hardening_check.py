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
  no-session-update
                 a WebSocket client that streams audio but never sends session.update: the
                 audio is ignored (no Flux connection), the shim closes the socket with code
                 1008 at the deadline, and the session leaves the registry
  remote-bind    `--host 0.0.0.0` exits 2 without `--allow-unauthenticated-remote`; with the
                 flag the shim starts and answers /healthz
  bad-call-body  malformed call requests get a 400 naming the problem, except the 413
                 cases, the disconnect case, and the ICE-candidate-never-answers offer below,
                 and no server error.
                 JSON shape and syntax: a JSON array, non-JSON, JSON nested past the parser, a
                 string `session`, and a non-string `sdp` each get their own reason. A JSON
                 integer of 5000 digits, past Python's digit limit, gets "call request could
                 not be parsed". The same integer inside a multipart `session` part gets
                 "malformed multipart body".
                 Encoding and charset: bad UTF-8 as JSON and as SDP gets "call request body is
                 not valid text for its charset".
                 A JSON body that does not match its gzip or deflate Content-Encoding gets
                 "call request body does not match its Content-Encoding". A multipart body
                 that does not match gzip gets the same reason. An unknown Content-Encoding
                 whose bytes are not JSON gets "call request is not valid JSON".
                 Multipart: a string `session` part and a non-JSON `session` part get the
                 same 400s as their JSON counterparts. An unknown charset, an unknown
                 Content-Transfer-Encoding, and a nested part each get their own reason. A
                 part header without a colon and a body cut short are malformed multipart
                 bodies. A gzip multipart body truncated to 30 bytes gets either "malformed
                 multipart body" or "call request body does not match its Content-Encoding".
                 Offers: eleven offers are checked here, in five groups. Four offers that
                 aiortc's SDP parser rejects (a bad setup line, a bare media line after a full
                 session header, `v=0` then a bare media line, and garbage after `v=0`) each get
                 "SDP offer could not be parsed". Three offers whose BUNDLE group names a mid
                 that no media line has are found by the same parser. They are an unknown token,
                 a session-level `a=mid:9` before the `m=` line, and a second `a=mid:9` in the
                 audio section. Each gets "BUNDLE group names a mid that no media line has"
                 before negotiation. One otherwise valid offer whose mid and BUNDLE group are a
                 lone surrogate (`\\ud800` in the JSON) gets "sdp is not valid text". Two
                 minimal audio offers with an ICE candidate on port 0 and on port 70000 each get
                 "ICE candidate port out of range". One offer with an ICE candidate that never
                 answers (`127.0.0.1 65535`) gets 201. aioice's STUN retry then fires a few
                 seconds after the session closes. The shim's loop exception handler logs it as one
                 WARNING (the test asserts exactly one), so no ERROR is logged when the session
                 closes. None of these offers logs a server error.
                 Size cap and disconnect: a chunked JSON body or multipart part over the 4 MB
                 cap gets 413, and so do two 3 MB multipart parts that are over it together.
                 A client that disconnects mid-body (it promises a 1000-byte
                 application/json body and disconnects after 5 bytes) logs a warning and no
                 server error.
                 What still gets 201: a well-formed JSON call, a valid gzip one, and a
                 multipart call whose `sdp` part is base64 with a Content-Transfer-Encoding
                 header.
                 br and zstd are not exercised: aiohttp decodes them only if `brotli` or
                 `zstandard` is installed, and requirements.txt installs neither. Without
                 them, aiohttp answers those encodings with its own 400 and logs an ERROR on
                 aiohttp.server before the handler runs, so this check does not send them.
                 A chunked body with an invalid chunk-size line is likewise rejected by aiohttp
                 before the handler runs. aiohttp logs its own ERROR for it, so this check does
                 not send it.
                 A request header containing an invalid byte (for example a DEL character in a
                 header value) is rejected by aiohttp on every route. aiohttp answers it with its
                 own 400 and logs an ERROR on aiohttp.server. This check does not send it.
                 A truncated deflate body (declared Content-Encoding: deflate, bytes
                 b"\\x78\\x9c\\x00") is rejected by aiohttp's parser before the handler runs,
                 whatever its content type. aiohttp answers it with its own 400 and logs an ERROR
                 on aiohttp.server (ContentEncodingError in feed_eof). This check therefore does
                 not send it.
  bad-ws-frame   a sideband text frame nested too deep to parse is dropped like any non-JSON
                 frame, and a session.update on the same socket still gets session.updated
  bad-upgrade    a GET on the realtime endpoint without a WebSocket upgrade gets 400 "request is
                 not a WebSocket upgrade" and leaves no state. No standalone session is
                 registered, and a plain GET with a call_id neither blocks the real sideband nor
                 stops the orphan timer. An unknown call_id also gets 400 without the upgrade
                 (the upgrade check runs first). A handshake failure is injected by patching
                 `WebSocketResponse.prepare`, on a standalone WebSocket and on a call sideband;
                 every variant expects the session released. ConnectionResetError and
                 OSError("boom") before the 101 expect 400 and no server error.
                 TypeError("boom") stands in for a shim bug: it expects 500 and exactly one
                 ERROR "WebSocket handshake failed inside the shim" per attempt, which the
                 server-error count then excludes. OSError("late") raised after the real
                 prepare() sent the 101 expects a CLOSE with code 1011 and no server error.

Usage:
  python tests/hardening_check.py [--key FAKE_KEY_SENTINEL] [--only origin ...]
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import gc
import gzip
import json
import logging
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Callable

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


async def check_no_session_update(deadline_s: float = 1.0) -> bool:
    runner, base = await start_in_process(Settings(session_update_timeout_s=deadline_s))
    ws_url = "ws" + base[len("http"):] + "/v1/realtime"
    silence = base64.b64encode(b"\x00" * (CODEX_RATE * 2 // 10)).decode()  # 100 ms at 24 kHz
    ok = True
    async with aiohttp.ClientSession() as http:
        async with http.ws_connect(ws_url) as ws:
            for _ in range(5):
                await ws.send_json({"type": "input_audio_buffer.append", "audio": silence})
                await asyncio.sleep(0.1)
            sessions = list(runner.app["sessions"].values())
            session = sessions[0] if len(sessions) == 1 else None
            flux_opened = session is not None and session.flux is not None
            texts = []
            try:
                async with asyncio.timeout(deadline_s + 2.0):
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            texts.append(json.loads(msg.data).get("type"))
            except TimeoutError:
                print("  the shim never closed the socket")
                ok = False
            code = ws.close_code
        await asyncio.sleep(0.2)
        after = await session_count(http, base)
    await runner.cleanup()
    print(f"  sessions while streaming: {len(sessions)}, Flux opened: {flux_opened}, "
          f"frames received: {texts}, close code: {code}, sessions after: {after}")
    return ok and len(sessions) == 1 and not flux_opened and code == 1008 and after == 0 and not texts


async def check_remote_bind() -> bool:
    env = dict(os.environ, DEEPGRAM_API_KEY="FAKE_KEY_SENTINEL")  # never reaches Flux: no audio flows
    proc = subprocess.run([sys.executable, "-m", "shim.server", "--host", "0.0.0.0", "--port", str(free_port())],
                          cwd=ROOT, env=env, capture_output=True, text=True, timeout=20)
    print(f"  without the flag: exit code {proc.returncode}, stderr: {proc.stderr.strip()!r}")
    refused = proc.returncode == 2 and "loopback" in proc.stderr
    port = free_port()
    server = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "shim.server", "--host", "0.0.0.0", "--port", str(port),
        "--allow-unauthenticated-remote", cwd=ROOT, env=dict(env, PYTHONUNBUFFERED="1"),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    healthy = False
    async with aiohttp.ClientSession() as http:
        for _ in range(50):
            try:
                async with http.get(f"http://127.0.0.1:{port}/healthz") as resp:
                    healthy = resp.status == 200
                    break
            except aiohttp.ClientConnectionError:
                await asyncio.sleep(0.2)
    server.terminate()
    out = (await server.communicate())[0].decode("utf-8", "replace")
    warned = "no client authentication" in out
    print(f"  with the flag: /healthz {'answered' if healthy else 'did not answer'}, "
          f"startup warning logged: {warned}")
    return refused and healthy and warned


class ErrorLog:
    """Collects records at `level` or above (ERROR by default) on the root logger, so every
    logger that propagates counts: the shim, aiohttp's request handler, aiortc, aioice, and
    asyncio (a task exception never retrieved)."""

    def __init__(self, level: int = logging.ERROR) -> None:
        self.records: list[logging.LogRecord] = []
        self.handler = logging.Handler(level)
        self.handler.emit = self.records.append  # type: ignore[method-assign]
        self.root = logging.getLogger()

    def __enter__(self) -> "ErrorLog":
        # a root level above `level` would drop the records before the handler sees them, so
        # lower it for the duration instead of relying on basicConfig in main()
        self.saved_level = self.root.level
        if self.root.level > self.handler.level:
            self.root.setLevel(self.handler.level)
        self.root.addHandler(self.handler)
        return self

    def __exit__(self, *exc) -> None:
        self.root.removeHandler(self.handler)
        self.root.setLevel(self.saved_level)


MINIMAL_OFFER = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n"


async def check_bad_call_body(orphan_s: float = 0.5) -> bool:
    runner, base = await start_in_process(Settings(orphan_call_timeout_s=orphan_s))
    url = f"{base}/v1/realtime/calls"
    json_ct = {"Content-Type": "application/json"}

    def multipart(session_part: bytes, charset: str | None = None) -> aiohttp.FormData:
        form = aiohttp.FormData()
        form.add_field("sdp", MINIMAL_OFFER, content_type="application/sdp")
        ct = "application/json" + (f"; charset={charset}" if charset else "")
        form.add_field("session", session_part, content_type=ct)
        return form

    # aiohttp's form builder drops an unknown Content-Transfer-Encoding, so this one is raw bytes.
    raw_boundary = "raw-boundary"
    raw_bogus_cte = ("--" + raw_boundary + "\r\nContent-Disposition: form-data; name=\"sdp\"\r\n"
                     "Content-Type: application/sdp\r\n\r\n" + MINIMAL_OFFER + "\r\n"
                     "--" + raw_boundary + "\r\nContent-Disposition: form-data; name=\"session\"\r\n"
                     "Content-Type: application/json\r\nContent-Transfer-Encoding: x-bogus\r\n\r\n{}\r\n"
                     "--" + raw_boundary + "--\r\n").encode()
    raw_ct = {"Content-Type": f"multipart/form-data; boundary={raw_boundary}"}
    # the sdp part base64-encoded with a Content-Transfer-Encoding header: decoded like the
    # session part, so the call is created
    raw_b64_sdp = ("--" + raw_boundary + "\r\nContent-Disposition: form-data; name=\"sdp\"\r\n"
                   "Content-Type: application/sdp\r\nContent-Transfer-Encoding: base64\r\n\r\n"
                   + base64.b64encode(MINIMAL_OFFER.encode()).decode() + "\r\n"
                   "--" + raw_boundary + "--\r\n").encode()
    raw_no_colon = b"--XX\r\nNoColonHeader\r\n\r\nabc\r\n--XX--"
    raw_no_colon_ct = {"Content-Type": "multipart/form-data; boundary=XX"}
    raw_cut_short = raw_bogus_cte.split(b"\r\n--" + raw_boundary.encode() + b"\r\nContent-Disposition: form-data; name=\"session\"")[0]
    fingerprint = ":".join(["AB"] * 32)

    def audio_offer(group: str, mid: str, session_extra: str = "", media_extra: str = "") -> str:
        """A minimal audio offer with the given BUNDLE group and media id. `session_extra` goes
        before the `m=` line, `media_extra` right after the section's `a=mid:` line."""
        return (MINIMAL_OFFER + session_extra + f"a=group:BUNDLE {group}\r\n"
                "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\n"
                f"a=mid:{mid}\r\n{media_extra}a=sendrecv\r\na=rtcp-mux\r\na=rtpmap:111 opus/48000/2\r\n"
                "a=ice-ufrag:abcd\r\na=ice-pwd:abcdefghijklmnopqrstuvwx\r\n"
                f"a=fingerprint:sha-256 {fingerprint}\r\na=setup:actpass\r\n")

    # offers that pass the `v=0` check but that aiortc's SDP parser rejects outright
    bad_offers = {"bad setup line": MINIMAL_OFFER + "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=setup:bogus\r\n",
                  "bare media line": MINIMAL_OFFER + "m=audio\r\n",
                  "v=0 then a bare media line": "v=0\r\nm=audio\r\n",
                  "garbage after v=0": "v=0 garbage"}
    # each bad BUNDLE group names a mid that no media line has: the shim rejects it before
    # aiortc negotiates, since aiortc's background connect task would fail with an unretrieved
    # exception that asyncio logs as an ERROR. The shim reads mids with aiortc's own parser, so
    # an `a=mid:` line outside a media section does not count. When a section has two
    # `a=mid:` lines the parser keeps the last one in the section and ignores the earlier one.
    bad_bundle_offers = {
        "bad BUNDLE group": audio_offer("zz 0", "0"),
        "session-level mid": audio_offer("9 0", "0", session_extra="a=mid:9\r\n"),
        "duplicate mid in section": audio_offer("9 0", "0", media_extra="a=mid:9\r\n"),
    }
    surrogate_offer = audio_offer("\ud800", "\ud800")
    def candidate_offer(port: int) -> str:
        return audio_offer("0", "0", media_extra=f"a=candidate:1 1 udp 1 127.0.0.1 {port} typ host\r\n")

    # aioice would retry a check to port 0 and log an ERROR when the session closes; port 70000
    # passes aiortc's parser and fails with "Fatal write error on datagram transport" ERRORs
    port0_offer = candidate_offer(0)
    port70000_offer = candidate_offer(70000)
    # a valid port that nothing answers on: the call is created, and aioice's STUN retry fires
    # after the session closed its transport
    silent_candidate_offer = candidate_offer(65535)

    nested = aiohttp.MultipartWriter("mixed")
    nested.append("x")
    outer = aiohttp.MultipartWriter("form-data")
    outer.append(nested)

    cases = [
        ("JSON array", dict(data=b"[]", headers=json_ct), "call request must be a JSON object"),
        ("non-JSON", dict(data=b"not json", headers=json_ct), "call request is not valid JSON"),
        ("deeply nested JSON", dict(data=b"[" * 100_000 + b"]" * 100_000, headers=json_ct),
         "call request JSON is nested too deep"),
        ("string session", dict(data=b'{"sdp": "v=0", "session": "bad"}', headers=json_ct),
         "session must be a JSON object"),
        ("non-string sdp", dict(data=b'{"sdp": 5}', headers=json_ct), "sdp must be a string starting with v=0"),
        ("bad UTF-8 as JSON", dict(data=b"\xff\xfe", headers=json_ct),
         "call request body is not valid text for its charset"),
        ("bad UTF-8 as SDP", dict(data=b"\xff\xfe", headers={"Content-Type": "application/sdp"}),
         "call request body is not valid text for its charset"),
        ("multipart string session", dict(data=multipart(b'"bad"')), "session must be a JSON object"),
        ("multipart non-JSON session", dict(data=multipart(b"{not json")), "call request is not valid JSON"),
        ("multipart unknown charset", dict(data=multipart(b"{}", charset="bogus")),
         "unknown charset in Content-Type"),
        ("multipart bogus transfer enc", dict(data=raw_bogus_cte, headers=raw_ct),
         "unknown Content-Transfer-Encoding in a multipart part"),
        ("multipart cut short", dict(data=raw_cut_short, headers=raw_ct), "malformed multipart body"),
        ("multipart header, no colon", dict(data=raw_no_colon, headers=raw_no_colon_ct),
         "malformed multipart body"),
        ("nested multipart part", dict(data=outer), "nested multipart is not accepted"),
        ("JSON, gzip header, not gzip", dict(data=b"not gzip", headers={**json_ct, "Content-Encoding": "gzip"}),
         "call request body does not match its Content-Encoding"),
        ("JSON, deflate hdr, bad bytes", dict(data=b"not gzip", headers={**json_ct, "Content-Encoding": "deflate"}),
         "call request body does not match its Content-Encoding"),
        ("multipart, gzip hdr, bad", dict(data=raw_bogus_cte, headers={**raw_ct, "Content-Encoding": "gzip"}),
         "call request body does not match its Content-Encoding"),
        ("multipart, gzip cut to 30 B",
         dict(data=gzip.compress(raw_bogus_cte)[:30], headers={**raw_ct, "Content-Encoding": "gzip"}),
         ("malformed multipart body", "call request body does not match its Content-Encoding")),
        ("JSON int over 4300 digits", dict(data=b'{"sdp": ' + b"9" * 5000 + b"}", headers=json_ct),
         "call request could not be parsed"),
        ("multipart int, 5000 digits", dict(data=multipart(b'{"type": ' + b"9" * 5000 + b"}")),
         "malformed multipart body"),
        ("JSON, unknown encoding", dict(data=b"not json", headers={**json_ct, "Content-Encoding": "bogus"}),
         "call request is not valid JSON"),
    ] + [
        (f"offer: {label}", dict(data=json.dumps({"sdp": offer}).encode(), headers=json_ct),
         "SDP offer could not be parsed")
        for label, offer in bad_offers.items()
    ] + [
        (f"offer: {label}", dict(data=json.dumps({"sdp": offer}).encode(), headers=json_ct),
         "BUNDLE group names a mid that no media line has")
        for label, offer in bad_bundle_offers.items()
    ] + [
        # a valid offer except that its media id and BUNDLE group are a lone surrogate:
        # json.loads accepts the escape, aiortc would negotiate it, and the answer could not be
        # encoded
        ("offer: lone surrogate in mid",
         dict(data=json.dumps({"sdp": surrogate_offer}, ensure_ascii=True).encode(), headers=json_ct),
         "sdp is not valid text"),
        ("offer: ICE candidate on port 0",
         dict(data=json.dumps({"sdp": port0_offer}).encode(), headers=json_ct),
         "ICE candidate port out of range"),
        ("offer: ICE candidate port 70000",
         dict(data=json.dumps({"sdp": port70000_offer}).encode(), headers=json_ct),
         "ICE candidate port out of range"),
    ]
    ok = True
    errors = ErrorLog()

    async def never_answers(http: aiohttp.ClientSession) -> bool:
        """An offer whose ICE candidate never answers gets 201. aioice's STUN retry fires a few
        seconds after the orphan timer closes the session; the shim's loop exception handler logs
        the first occurrence of that AttributeError as one WARNING (the test asserts exactly one),
        so no ERROR is logged."""
        errors_before = len(errors.records)
        warnings = ErrorLog(level=logging.WARNING)
        with warnings:
            async with http.post(url, data=json.dumps({"sdp": silent_candidate_offer}).encode(),
                                 headers=json_ct) as resp:
                status = resp.status
                await resp.read()
            sessions = await wait_sessions(http, base, 0, deadline_s=orphan_s + 3.5)
            # the retry fires a few seconds after the session closes; poll for up to 8 s
            poll_end = asyncio.get_running_loop().time() + 8.0
            while asyncio.get_running_loop().time() < poll_end:
                if any(rec.name == "shim" and "aioice STUN retry" in rec.getMessage()
                       for rec in warnings.records):
                    break
                await asyncio.sleep(0.05)
            gc.collect()
        new_errors = len(errors.records) - errors_before
        retry_warnings = [rec for rec in warnings.records if rec.levelno == logging.WARNING
                          and rec.name == "shim" and "aioice STUN retry" in rec.getMessage()]
        print(f"  {'offer: ICE candidate that never answers':28} -> {status}, sessions now {sessions}, "
              f"server errors {new_errors}, STUN retry warnings {len(retry_warnings)} "
              f"(want 201, 0, 0, 1)")
        return status == 201 and sessions == 0 and new_errors == 0 and len(retry_warnings) == 1

    with errors:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as http:
            for label, kwargs, want in cases:
                wants = (want,) if isinstance(want, str) else want  # a tuple: any one of them
                errors_before_case = len(errors.records)
                async with http.post(url, **kwargs) as resp:
                    text = (await resp.text()).strip()
                case_errors = len(errors.records) - errors_before_case
                print(f"  {label:28} -> {resp.status} {text!r} (want 400 {' or '.join(map(repr, wants))})"
                      + (f", server errors {case_errors} (want 0)" if case_errors else ""))
                ok &= resp.status == 400 and text in wants and case_errors == 0
            # a chunked body over the size cap keeps aiohttp's own 413 (no Content-Length to check up front)
            async def five_mb():
                for _ in range(5 * 16):
                    yield b"x" * 65536
            errors_before_big = len(errors.records)
            async with http.post(url, data=five_mb(), headers=json_ct) as resp:
                big_status = resp.status
                await resp.read()
            big_errors = len(errors.records) - errors_before_big
            big_ok = big_status == 413 and big_errors == 0
            print(f"  {'chunked body over the size cap':28} -> {big_status}, server errors {big_errors} "
                  f"(want 413 and 0)")
            # the same cap for a multipart part sent chunked: its 5 MB `sdp` part gets 413 too
            mp_boundary = "big-boundary"

            async def five_mb_part():
                yield (f"--{mp_boundary}\r\nContent-Disposition: form-data; name=\"sdp\"\r\n"
                       "Content-Type: application/sdp\r\n\r\n").encode()
                for _ in range(5 * 16):
                    yield b"x" * 65536
                yield f"\r\n--{mp_boundary}--\r\n".encode()
            errors_before_part = len(errors.records)
            async with http.post(url, data=five_mb_part(),
                                 headers={"Content-Type": f"multipart/form-data; boundary={mp_boundary}"}) as resp:
                part_status = resp.status
                await resp.read()
            part_errors = len(errors.records) - errors_before_part
            big_ok &= part_status == 413 and part_errors == 0
            print(f"  {'chunked multipart part over the size cap':28} -> {part_status}, server errors "
                  f"{part_errors} (want 413 and 0)")
            # two 3 MB parts, each under the cap, over it together: the cap is on all parts together

            async def two_3mb_parts():
                for name in ("sdp", "pad"):
                    yield (f"--{mp_boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n"
                           "Content-Type: application/octet-stream\r\n\r\n").encode()
                    for _ in range(3 * 16):
                        yield b"x" * 65536
                    yield b"\r\n"
                yield f"--{mp_boundary}--\r\n".encode()
            errors_before_parts = len(errors.records)
            async with http.post(url, data=two_3mb_parts(),
                                 headers={"Content-Type": f"multipart/form-data; boundary={mp_boundary}"}) as resp:
                parts_status = resp.status
                await resp.read()
            parts_errors = len(errors.records) - errors_before_parts
            big_ok &= parts_status == 413 and parts_errors == 0
            print(f"  {'two 3 MB parts, over the cap together':28} -> {parts_status}, server errors "
                  f"{parts_errors} (want 413 and 0)")
            # a client that disconnects mid-body: it promises 1000 bytes and sends 5
            errors_before_cut = len(errors.records)
            host, port = base.rsplit("//", 1)[1].rsplit(":", 1)
            cut_warnings = ErrorLog(level=logging.WARNING)
            with cut_warnings:
                _, writer = await asyncio.open_connection(host, int(port))
                writer.write(b"POST /v1/realtime/calls HTTP/1.1\r\nHost: x\r\n"
                             b"Content-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"sdp")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                # poll every 50 ms, up to 2 s, until the warning arrives; an ERROR ends it early too
                loop = asyncio.get_running_loop()
                wait_until = loop.time() + 2.0
                while (loop.time() < wait_until and len(errors.records) == errors_before_cut
                       and not any(rec.levelno == logging.WARNING and rec.name == "shim"
                                   for rec in cut_warnings.records)):
                    await asyncio.sleep(0.05)
            cut_errors = len(errors.records) - errors_before_cut
            cut_warned = [rec for rec in cut_warnings.records
                          if rec.levelno == logging.WARNING and rec.name == "shim"]
            cut_warn_ok = (len(cut_warned) == 1
                           and "client disconnected mid-body" in cut_warned[0].getMessage())
            cut_quiet = cut_errors == 0 and cut_warn_ok
            cut_sessions = await wait_sessions(http, base, 0, deadline_s=0.5)
            print(f"  {'client disconnects mid-body':28} -> server errors {cut_errors} (want 0); "
                  f"sessions now {cut_sessions}")
            print(f"  {'mid-body disconnect warning':28} -> {len(cut_warned)} "
                  f"{[rec.getMessage() for rec in cut_warned]!r} (want 1 'client disconnected mid-body')")
            body = json.dumps({"sdp": MINIMAL_OFFER, "session": {"type": "quicksilver"}}).encode()
            async with http.post(url, data=body, headers=json_ct) as resp:
                created = resp.status == 201
                print(f"  well-formed JSON call          -> {resp.status} (want 201)")
            async with http.post(url, data=gzip.compress(body),
                                 headers={**json_ct, "Content-Encoding": "gzip"}) as resp:
                created &= resp.status == 201
                print(f"  well-formed gzip JSON call     -> {resp.status} (want 201)")
            async with http.post(url, data=raw_b64_sdp, headers=raw_ct) as resp:
                created &= resp.status == 201
                print(f"  multipart base64 sdp part      -> {resp.status} (want 201)")
            await asyncio.sleep(orphan_s + 0.5)
            after = await wait_sessions(http, base, 0, deadline_s=orphan_s + 2.0)
            silent_ok = await never_answers(http)
        await runner.cleanup()  # inside the capture: a handler's error is logged as it finishes
        gc.collect()  # inside the capture: a task destroyed with an unretrieved exception logs here
    print(f"  server errors logged: {len(errors.records)} (want 0), sessions after the orphan window: {after}")
    for rec in errors.records:
        print("  ERROR:", rec.getMessage())
    return (ok and big_ok and cut_quiet and created and after == 0 and silent_ok
            and not errors.records)


async def check_bad_ws_frame() -> bool:
    runner, base = await start_in_process(Settings())
    ws_url = "ws" + base[len("http"):] + "/v1/realtime"
    errors = ErrorLog()
    with errors:
        async with aiohttp.ClientSession() as http:
            async with http.ws_connect(ws_url) as ws:
                await ws.send_str("[" * 100_000 + "]" * 100_000)
                await ws.send_json({"type": "session.update", "session": {"type": "quicksilver"}})
                msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                got = json.loads(msg.data).get("type") if msg.type == aiohttp.WSMsgType.TEXT else str(msg.type)
        await runner.cleanup()  # inside the capture: a handler's error is logged as it finishes
    print(f"  after a 100k-deep frame, session.update answered with: {got}; server errors: {len(errors.records)}")
    return got == "session.updated" and not errors.records


async def session_count(http: aiohttp.ClientSession, base: str) -> int:
    async with http.get(f"{base}/healthz") as resp:
        return (await resp.json())["sessions"]


async def wait_sessions(http: aiohttp.ClientSession, base: str, want: int, deadline_s: float = 3.0) -> int:
    """Polls /healthz until it reports `want` sessions or the deadline passes; returns the last count."""
    loop = asyncio.get_running_loop()
    end = loop.time() + deadline_s
    while True:
        n = await session_count(http, base)
        if n == want or loop.time() >= end:
            return n
        await asyncio.sleep(0.05)


async def check_bad_upgrade(orphan_s: float = 1.5) -> bool:
    runner, base = await start_in_process(Settings(orphan_call_timeout_s=orphan_s))
    rt = f"{base}/v1/realtime"
    ws_rt = "ws" + rt[len("http"):]
    ok = True

    def step(label: str, passed: bool) -> None:
        nonlocal ok
        print(f"  {label:60} {'ok' if passed else 'WRONG'}")
        ok &= passed

    async def create_call(http: aiohttp.ClientSession) -> str:
        async with http.post(f"{base}/v1/realtime/calls", data=MINIMAL_OFFER,
                             headers={"Content-Type": "application/sdp"}) as resp:
            step(f"create call -> {resp.status} (want 201)", resp.status == 201)
            return resp.headers.get("Location", "").rsplit("/", 1)[-1]

    async def plain_get(http: aiohttp.ClientSession, params: dict | None = None) -> int:
        """Returns the status, or -1 if the 400 is not "request is not a WebSocket upgrade"."""
        async with http.get(rt, params=params) as resp:
            text = (await resp.text()).strip()
            if resp.status == 400 and text != "request is not a WebSocket upgrade":
                print(f"  unexpected 400 text: {text!r}")
                return -1
            return resp.status

    async def ws_handshake_status(http: aiohttp.ClientSession, params: dict | None = None) -> int:
        try:
            async with http.ws_connect(ws_rt, params=params):
                return 101
        except aiohttp.WSServerHandshakeError as exc:
            return exc.status

    async def ws_close_after_101(http: aiohttp.ClientSession, params: dict | None = None) -> str:
        """For a failure after the 101: connects, then reports the first frame and the close code."""
        try:
            async with http.ws_connect(ws_rt, params=params) as ws:
                msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                return f"{msg.type.name} {ws.close_code}"
        except aiohttp.WSServerHandshakeError as exc:
            return f"handshake {exc.status}"

    errors = ErrorLog()
    with errors:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as http:
            # 1. standalone: a plain GET registers nothing
            status = await plain_get(http)
            step(f"plain GET /v1/realtime -> {status} (want 400)", status == 400)
            n = await session_count(http, base)
            step(f"sessions after it: {n} (want 0)", n == 0)

            # 2. a plain GET with a call_id does not claim the call's sideband
            call_id = await create_call(http)
            status = await plain_get(http, {"call_id": call_id})
            step(f"plain GET with the call_id -> {status} (want 400)", status == 400)
            n = await session_count(http, base)
            step(f"sessions after it: {n} (want 1)", n == 1)
            got = None
            try:
                async with http.ws_connect(ws_rt, params={"call_id": call_id}) as ws:
                    await ws.send_json({"type": "session.update", "session": {"type": "quicksilver"}})
                    msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                    got = json.loads(msg.data).get("type") if msg.type == aiohttp.WSMsgType.TEXT else str(msg.type)
            except aiohttp.WSServerHandshakeError as exc:
                got = f"handshake {exc.status}"
            step(f"real sideband afterwards answers session.update with: {got}", got == "session.updated")
            n = await wait_sessions(http, base, 0)
            step(f"sessions after the sideband closes: {n} (want 0)", n == 0)

            # 3. the failed upgrade does not stop the orphan timer
            call_id = await create_call(http)
            status = await plain_get(http, {"call_id": call_id})
            step(f"plain GET with a second call_id -> {status} (want 400)", status == 400)
            n = await session_count(http, base)
            step(f"sessions right after it: {n} (want 1, not closed early)", n == 1)
            n = await wait_sessions(http, base, 0, deadline_s=orphan_s + 2.0)
            step(f"sessions after the orphan window: {n} (want 0)", n == 0)

            # 4. an unknown call_id without the upgrade gets the same 400
            status = await plain_get(http, {"call_id": "rtc_unknown"})
            step(f"plain GET with an unknown call_id -> {status} (want 400)", status == 400)

            # 5. a handshake failure injected by patching prepare() releases the session.
            #    OSError or ConnectionResetError before the 101: 400.
            #    TypeError before the 101 (a shim bug): 500 and one ERROR with its traceback,
            #    expected here. OSError after the real prepare() sent the 101: the client's
            #    ws_connect succeeds, its first frame is a CLOSE with code 1011, no ERROR.
            expected = []  # the ERROR records the TypeError variant is meant to produce
            original = web.WebSocketResponse.prepare

            def raising(make_exc: Callable[[], BaseException]):
                async def failing_prepare(self, request):  # noqa: ANN001
                    raise make_exc()  # a fresh exception per attempt, so each traceback is separate
                return failing_prepare

            async def late_prepare(self, request):  # noqa: ANN001
                await original(self, request)
                raise OSError("late")

            variants = [("ConnectionResetError", raising(lambda: ConnectionResetError("simulated")), "400", False),
                        ("OSError", raising(lambda: OSError("boom")), "400", False),
                        ("TypeError", raising(lambda: TypeError("boom")), "500", True),
                        ("OSError after the 101", late_prepare, "CLOSE 1011", False)]
            for name, patched, want, shim_bug in variants:
                late = want.startswith("CLOSE")

                def logged_since(mark: int) -> None:
                    new = errors.records[mark:]
                    if shim_bug:
                        hits = [r for r in new if r.name == "shim"
                                and "WebSocket handshake failed inside the shim" in r.getMessage()]
                        step(f"  ERROR records for it: {len(hits)} on shim (want 1)", len(hits) == 1)
                        traced = [r for r in hits if r.exc_info and r.exc_info[0] is not None]
                        step(f"  with a traceback: {len(traced)} (want 1)", len(traced) == len(hits) == 1)
                        expected.extend(hits)
                    else:
                        step(f"  ERROR records for it: {len(new)} (want 0)", not new)

                async def attempt(params: dict | None = None) -> str:
                    if late:
                        return await ws_close_after_101(http, params)
                    return str(await ws_handshake_status(http, params))

                web.WebSocketResponse.prepare = patched  # type: ignore[method-assign]
                try:
                    mark = len(errors.records)
                    got = await attempt()
                    step(f"standalone WebSocket, {name} -> {got} (want {want})", got == want)
                    n = await wait_sessions(http, base, 0)
                    step(f"sessions after it: {n} (want 0)", n == 0)
                    logged_since(mark)
                    call_id = await create_call(http)
                    mark = len(errors.records)
                    got = await attempt({"call_id": call_id})
                    step(f"call sideband, {name} -> {got} (want {want})", got == want)
                    n = await wait_sessions(http, base, 0)
                    step(f"sessions after it (call closed): {n} (want 0)", n == 0)
                    logged_since(mark)
                finally:
                    web.WebSocketResponse.prepare = original  # type: ignore[method-assign]
        await runner.cleanup()  # inside the capture: a handler's error is logged as it finishes
        gc.collect()  # inside the capture: a task destroyed with an unretrieved exception logs here
    unexpected = [r for r in errors.records if not any(r is e for e in expected)]
    print(f"  server errors logged: {len(unexpected)} (want 0; {len(expected)} expected from the "
          f"simulated shim bug not counted)")
    for rec in unexpected:
        print("  ERROR:", rec.getMessage())
    return ok and not unexpected and len(errors.records) == len(expected)


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
    ap.add_argument("--only", nargs="*", choices=["missing-key", "origin", "orphan", "bad-key", "late-sideband",
                                                 "no-session-update", "remote-bind", "bad-call-body",
                                                 "bad-ws-frame", "bad-upgrade"])
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    checks = {
        "missing-key": check_missing_key,
        "origin": lambda: asyncio.run(check_origin()),
        "orphan": lambda: asyncio.run(check_orphan()),
        "bad-key": lambda: asyncio.run(check_bad_key(args.key)),
        "late-sideband": lambda: asyncio.run(check_late_sideband(args.key)),
        "no-session-update": lambda: asyncio.run(check_no_session_update()),
        "remote-bind": lambda: asyncio.run(check_remote_bind()),
        "bad-call-body": lambda: asyncio.run(check_bad_call_body()),
        "bad-ws-frame": lambda: asyncio.run(check_bad_ws_frame()),
        "bad-upgrade": lambda: asyncio.run(check_bad_upgrade()),
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

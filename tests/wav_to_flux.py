"""Live check: stream a 16 kHz mono PCM WAV to Deepgram Flux STT in real time and print TurnInfo.

Usage:
  DEEPGRAM_API_KEY=... python tests/wav_to_flux.py audio/spacewalk-16k.wav [--realtime] [--eot 0.8] [--timeout-ms 3000]

Prints one line per TurnInfo event with the end_of_turn_confidence, and a final EndOfTurn summary.
Exit code 0 when at least one EndOfTurn with a non-empty transcript arrived.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
import wave
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shim.flux_client import CHUNK_BYTES, CHUNK_MS, SAMPLE_RATE, FluxSession, TurnInfo  # noqa: E402


def read_pcm16_16k_mono(path: str) -> bytes:
    with wave.open(path, "rb") as wf:
        if (wf.getframerate(), wf.getnchannels(), wf.getsampwidth()) != (SAMPLE_RATE, 1, 2):
            raise SystemExit(
                f"{path}: need 16 kHz mono 16-bit PCM, got "
                f"{wf.getframerate()} Hz {wf.getnchannels()} ch {8 * wf.getsampwidth()}-bit"
            )
        return wf.readframes(wf.getnframes())


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("wav")
    ap.add_argument("--realtime", action="store_true", help="pace chunks at 80 ms wall clock")
    ap.add_argument("--eot", type=float, default=None, help="eot_threshold")
    ap.add_argument("--timeout-ms", type=int, default=None, help="eot_timeout_ms")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    pcm = read_pcm16_16k_mono(args.wav)
    print(f"audio: {len(pcm)} bytes = {len(pcm) / (SAMPLE_RATE * 2):.1f}s, chunk={CHUNK_BYTES}B/{CHUNK_MS}ms")

    ends: list[TurnInfo] = []
    t0 = time.monotonic()

    async def on_turn(info: TurnInfo) -> None:
        conf = info.end_of_turn_confidence
        conf_s = f"{conf:.3f}" if isinstance(conf, (int, float)) else "n/a"
        text = info.transcript if len(info.transcript) <= 90 else info.transcript[:87] + "..."
        print(f"[{time.monotonic() - t0:6.2f}s] TurnInfo {info.event:<14} turn={info.turn_index} eot_conf={conf_s} {text!r}")
        if info.event == "EndOfTurn":
            ends.append(info)

    async with FluxSession(on_turn, eot_threshold=args.eot, eot_timeout_ms=args.timeout_ms) as session:
        print(f"url: {session.url}")
        await asyncio.wait_for(session.connected.wait(), timeout=15)
        for i in range(0, len(pcm), CHUNK_BYTES):
            await session.feed(pcm[i : i + CHUNK_BYTES])
            if args.realtime:
                await asyncio.sleep(CHUNK_MS / 1000)
        # Trailing second of silence lets Flux close the final turn on its own timeout.
        await session.feed(b"\x00" * (SAMPLE_RATE * 2))
    print(f"EndOfTurn events: {len(ends)}")
    for e in ends:
        print(f"  turn {e.turn_index} conf={e.end_of_turn_confidence}: {e.transcript}")
    ok = any(e.transcript.strip() for e in ends)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

# Validation and shim image. The API key is never baked in; pass it at run time:
#   docker run --rm -e DEEPGRAM_API_KEY codex-flux-voice python tests/wav_to_flux.py audio/spacewalk-16k.wav
FROM python:3.12-slim

# aiortc wheels bundle ffmpeg/opus/srtp, so only tiny runtime libs are needed.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libgomp1 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY shim ./shim
COPY tests ./tests
COPY audio ./audio

ENV PYTHONUNBUFFERED=1
EXPOSE 8765
# Default: run the shim. Override the command for tests.
# A container must bind 0.0.0.0 for the port mapping to work, and the shim refuses a non-loopback
# bind without this flag. It has no client authentication. The compose port mapping on 127.0.0.1 is
# what keeps it off your network; never publish it with a bare `-p 8765:8765`.
CMD ["python", "-m", "shim.server", "--host", "0.0.0.0", "--port", "8765", "--allow-unauthenticated-remote"]

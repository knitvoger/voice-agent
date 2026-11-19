"""Optionally synthesize five English test WAVs; real Azure TTS charges apply."""

import argparse
import hashlib
import io
import json
import os
import wave
from pathlib import Path
from urllib.parse import urlsplit
from xml.sax.saxutils import escape

import httpx
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

INPUTS = {
    "hello.wav": "Hello!",
    "billing-1.wav": "Why did my bill increase by thirty yuan this month?",
    "billing-2.wav": "How were those extra thirty yuan calculated?",
    "booking-1.wav": "What is the status of the simulated reservation BK1001?",
    "booking-2.wav": "What is the cancellation policy for that reservation?",
}


def main() -> None:
    load_dotenv(Path(__file__).with_name(".env"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--speech-endpoint", default=os.getenv("SPEECH_ENDPOINT"), required=not os.getenv("SPEECH_ENDPOINT")
    )
    parser.add_argument("--output", type=Path, default=Path("audio"))
    args = parser.parse_args()
    parsed = urlsplit(args.speech_endpoint)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
    ):
        parser.error("Use an HTTPS Speech resource root without a path or embedded credentials")
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = []
    with DefaultAzureCredential() as credential, httpx.Client(timeout=60) as client:
        for filename, text in INPUTS.items():
            token = credential.get_token("https://cognitiveservices.azure.com/.default").token
            response = client.post(
                args.speech_endpoint.rstrip("/") + "/tts/cognitiveservices/v1",
                headers={
                    "Authorization": "Bearer " + token,
                    "Content-Type": "application/ssml+xml",
                    "X-Microsoft-OutputFormat": "riff-24khz-16bit-mono-pcm",
                },
                content=(
                    '<speak version="1.0" xml:lang="en-US"><voice name="en-US-GuyNeural">'
                    + escape(text)
                    + "</voice></speak>"
                ).encode(),
            )
            response.raise_for_status()
            with wave.open(io.BytesIO(response.content), "rb") as wav:
                if (wav.getframerate(), wav.getnchannels(), wav.getsampwidth()) != (24000, 1, 2):
                    raise ValueError("Expected 24kHz mono 16-bit PCM WAV")
            (args.output / filename).write_bytes(response.content)
            manifest.append({"file": filename, "text": text, "sha256": hashlib.sha256(response.content).hexdigest()})
            (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(filename)


if __name__ == "__main__":
    main()

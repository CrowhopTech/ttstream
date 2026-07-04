#!/usr/bin/env python3
"""CLI tool: convert audio to WAV via ffmpeg, then inject into Redis queue."""
import argparse
import os
import shutil
import subprocess
import sys

import numpy as np
import redis

# ─── helpers ───────────────────────────────────────────────────────────────
ffmpeg_path = shutil.which("ffmpeg") or "ffmpeg"


def convert_to_wav(input_path: str) -> str:
    """Use ffmpeg to decode any audio format into a raw WAV file path.

    Returns the path to the created temp WAV file.  Cleans up on exit.
    """
    output = os.path.join("/tmp", "injector_audio.wav")

    cmd = [
        ffmpeg_path, "-y",
        "-i", input_path,
        "-ar", "24000",
        "-ac", "1",
        "-sample_fmt", "s16",
        output,
    ]

    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")
        print(f"Error converting {input_path} to WAV:\n{stderr}", file=sys.stderr)
        raise subprocess.CalledProcessError(result.returncode, cmd, stderr=stderr)

    return output


def read_wav_to_float32(wav_path: str) -> bytes:
    """Read a WAV file and return raw float32 LE PCM bytes (consumer format)."""
    import wave

    with wave.open(wav_path, "rb") as wf:
        n_frames = wf.getnframes()
        raw_data = wf.readframes(n_frames)

    # WAV is s16le by default → normalize to [-1, 1] float32
    samples = np.frombuffer(raw_data, dtype=np.int16)
    samples = samples.astype(np.float32) / 32768.0

    if samples.ndim > 1:
        samples = samples[:, 0]
    if samples.size % 2 == 1:
        samples = samples[:-1]

    return samples.tobytes()


# ─── main ──────────────────────────────────────────────────────────────────
def inject_audio_to_redis(audio_file: str, redis_host: str, port: int) -> bytes:
    """Convert any audio file to float32 LE PCM and push to Redis queue."""
    wav_path = convert_to_wav(audio_file)
    audio_bytes = read_wav_to_float32(wav_path)
    return audio_bytes


def main():
    parser = argparse.ArgumentParser(
        description="Read an audio file, convert it to float32 LE PCM, and inject it into a Redis queue."
    )
    parser.add_argument("audio_file", help="Path to input audio file (any format)")
    parser.add_argument(
        "--format",
        choices=["wav", "raw"],
        default="wav",
        help="Output format (default: wav)",
    )
    parser.add_argument(
        "--no-convert", action="store_true", help="Skip ffmpeg conversion (use as-is)"
    )
    parser.add_argument(
        "--redis-host", default="localhost", help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--redis-port", type=int, default=6379, help="Redis port (default: 6379)"
    )
    args = parser.parse_args()

    if args.no_convert and args.format == "wav":
        print(f"Skipping conversion, pushing raw bytes from: {args.audio_file}")
        with open(args.audio_file, "rb") as f:
            audio_bytes = f.read()
    elif args.format == "wav":
        wav_path = convert_to_wav(args.audio_file)
        audio_bytes = read_wav_to_float32(wav_path)
    else:
        print("Raw output not supported in this mode; use --format wav", file=sys.stderr)
        sys.exit(1)

    r = redis.Redis(host=args.redis_host, port=args.redis_port)
    r.rpush("generated_audio_bytes", audio_bytes)
    print(f"Audio pushed to Redis queue: {len(audio_bytes)} bytes, {wav_path if 'wav_path' in dir() else args.audio_file}")


if __name__ == "__main__":
    main()

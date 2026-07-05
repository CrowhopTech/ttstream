import asyncio
import argparse
import redis
import numpy as np
from environs import env
import subprocess
from yaspin import yaspin
from yaspin.spinners import Spinners

async def main():
    env.read_env()

    parser = argparse.ArgumentParser(prog="speaker_audio_player")
    parser.add_argument("-d", "--delay", type=float, default=0.5, help="Time to wait between playing each audio sample")
    redis_address = env.str("REDIS_ADDRESS", default="localhost")
    redis_port = env.int("REDIS_PORT", default=6379)
    input_queue = env.str("REDIS_AUDIO_INPUT_QUEUE_NAME", default="generated_audio_bytes")
    icecast_address = env.str("ICECAST_ADDRESS", default="localhost")
    icecast_port = env.str("ICECAST_PORT", default=8069)
    icecast_pass = env.str("ICECAST_PASSWORD")
    args = parser.parse_args()

    r = redis.Redis(host=redis_address, port=redis_port, decode_responses=False)
    running_ffmpeg = subprocess.Popen([
        "ffmpeg", "-y",
        "-f", "f32le", "-ar", "24000", "-ac", "1", "-i", "pipe:0",
        "-c:a", "libmp3lame",
        "-b:a", "128k",
        "-content_type", "audio/mpeg",
        "-f", "mp3", f"icecast://source:{icecast_pass}@{icecast_address}:{icecast_port}/stream.mp3"
    ], stdin=subprocess.PIPE, stdout=None, stderr=None)

    has_written_once = False

    while True:
        try:
            with yaspin(text="Waiting for text to play over speaker...", spinner=Spinners.sand):
                while True:
                    next_audio: bytes = r.rpop(input_queue)
                    if next_audio is not None:
                        break
                    
                    if has_written_once:
                        silence = construct_silence(0.1, 24000, 1)
                        play_audio(running_ffmpeg, silence)

                    await asyncio.sleep(0.5)

            print(f"Playing {len(next_audio)} bytes")
            next_audio_ndarray = np.frombuffer(next_audio, dtype=np.float32)
            with yaspin(text="Playing audio over speaker!", spinner=Spinners.dotsCircle):
                play_audio(running_ffmpeg, next_audio_ndarray)
                has_written_once = True
                await asyncio.sleep(args.delay) # TODO: add some jitter here so it sounds less predictable
        except KeyboardInterrupt:
            break
    
    running_ffmpeg.stdin.close()
    running_ffmpeg.wait()

FRAME_SIZE = 1024
TRAILING_SILENCE_FRAMES = 10  # extra frames to force encoder to flush the real last frame

def construct_silence(chunk_duration_sec: float, sample_rate: int, channels: int=1) -> np.ndarray:
    n_samples = int(sample_rate * chunk_duration_sec)
    silence = np.zeros(n_samples * channels, dtype=np.float32)
    return silence

def pad_to_frame_size(audio_ndarray: np.ndarray, frame_size: int = FRAME_SIZE, extra_frames: int = TRAILING_SILENCE_FRAMES) -> np.ndarray:
    remainder = len(audio_ndarray) % frame_size
    pad_amount = (frame_size - remainder if remainder != 0 else 0) + extra_frames * frame_size
    return np.concatenate([audio_ndarray, np.zeros(pad_amount, dtype=np.float32)])

def play_audio(running_ffmpeg: subprocess.Popen[bytes], audio_data: np.ndarray):
    audio_data = pad_to_frame_size(audio_data)
    running_ffmpeg.stdin.write(audio_data.tobytes())
    running_ffmpeg.stdin.flush()

if __name__ == "__main__":
    asyncio.run(main())

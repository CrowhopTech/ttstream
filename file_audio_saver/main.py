import asyncio
import argparse
import redis
import sounddevice as sd
import soundfile as sf
import numpy as np
from environs import env
import sys
from yaspin import yaspin
from yaspin.spinners import Spinners
from datetime import datetime

async def main():
    env.read_env()

    parser = argparse.ArgumentParser(prog="speaker_audio_player")
    parser.add_argument("-f", "--file", default=None, type=argparse.FileType('r'), help="File to play sound from instead of queueing from Redis")
    parser.add_argument("-d", "--delay", default=0.5, type=float, help="Time to wait between playing each audio sample")
    redis_address = env.str("REDIS_ADDRESS", default="localhost")
    redis_port = env.int("REDIS_PORT", default=6379)
    input_queue = env.str("REDIS_AUDIO_INPUT_QUEUE_NAME", default="generated_audio_bytes")
    args = parser.parse_args()

    r = redis.Redis(host=redis_address, port=redis_port, decode_responses=False)
    
    if args.file is not None:
        with open(args.file, "rb") as manual_file:
            sd.play(manual_file.read())
            sys.exit(0)
    
    while True:
        try:
            with yaspin(text="Waiting for text to save...", spinner=Spinners.sand):
                while True:
                    next_audio: bytes = r.rpop(input_queue)
                    if next_audio is not None:
                        break
                    await asyncio.sleep(0.5)

            next_audio_ndarray = np.frombuffer(next_audio, dtype=np.float32)
            sf.write(f"{datetime.now()}.wav", next_audio_ndarray, 24000)
        except KeyboardInterrupt:
            break

def play_audio(audio_data):
    sd.play(audio_data, 24000)
    sd.wait()

if __name__ == "__main__":
    asyncio.run(main())

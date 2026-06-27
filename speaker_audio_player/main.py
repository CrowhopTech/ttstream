import asyncio
import argparse
import redis
import torch
import sounddevice as sd
import numpy as np
from environs import env
import sys

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
            next_audio: bytes = r.rpop(input_queue)
            if next_audio is None:
                print("No items to play over speaker... sleeping")
                await asyncio.sleep(0.5)
                continue
            
            print("Got new audio snippet, playing...")

            next_audio_ndarray = np.frombuffer(next_audio, dtype=np.float32)
            play_audio(next_audio_ndarray)
            await asyncio.sleep(args.delay) # TODO: add some jitter here so it sounds less predictable
        except KeyboardInterrupt:
            break

def play_audio(audio_data):
    sd.play(audio_data, 24000)
    sd.wait()

if __name__ == "__main__":
    asyncio.run(main())

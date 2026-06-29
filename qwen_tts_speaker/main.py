import asyncio
import argparse
import redis
from environs import env
import torch
import numpy as np
from faster_qwen3_tts import FasterQwen3TTS
import sys, os
from yaspin import yaspin
from yaspin.spinners import Spinners

EXPECTED_SAMPLE_RATE = 24000
SAMPLES_RELDIR="audio_samples"
SAMPLES_DIR: str

def get_samples_dir() -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), SAMPLES_RELDIR)

async def main():
    env.read_env()

    parser = argparse.ArgumentParser(prog="qwen_tts_speaker")
    parser.add_argument("-m", "--model", default="")
    parser.add_argument("-p", "--voice-prompt", default="")
    parser.add_argument("-t", "--text", default="", help="Text to generate to speech instead of fetching from Redis")
    parser.add_argument("-v", "--voice", default="", help="Which voice sample to use as a source")
    redis_address = env.str("REDIS_ADDRESS", default="localhost")
    redis_port = env.int("REDIS_PORT", default=6379)
    input_queue = env.str("REDIS_TEXT_INPUT_QUEUE_NAME", default="generated_text")
    output_queue = env.str("REDIS_AUDIO_OUTPUT_QUEUE_NAME", default="generated_audio_bytes") # TODO: standardize the format of this output stream
    max_gpu_memory_gb = env.float("MAX_GPU_MEMORY_GB", default=12)
    args = parser.parse_args()

    load_kwargs: dict[str, object] = {
        "device_map": "cuda:0",
        "dtype": torch.bfloat16,
    }
    if max_gpu_memory_gb is not None:
        load_kwargs["max_memory"] = {0: f"{max_gpu_memory_gb:.1f}GiB", "cpu": "64GiB"}
    
    qwen_model: FasterQwen3TTS = FasterQwen3TTS.from_pretrained(args.model)

    assert args.model != "", "--model required but not specified"
    assert args.voice != "" or args.voice_prompt != "", "one of --voice or --voice-prompt required but not specified"

    r = redis.Redis(host=redis_address, port=redis_port)

    # TODO: publish audio information such as bitrate to special keys in redis

    if args.text != "":
        with yaspin(text=f"Generating sound for text {args.text}...", spinner=Spinners.dotsCircle):
            result = generate_speech(qwen_model, args.text, args.voice_prompt, args.voice)
        print(f"Pushing bytes for text {args.text} to redis queue {output_queue}...")
        push_bytes_to_queue(result, r, output_queue)
        print(f"Successfully pushed audio for text {args.text} to redis, exiting.")
        sys.exit(0)

    # Loop until system interrupt (handle them cleanly):
    # while true, poll latest event from queue. If nothing, wait 500ms and try again. If something, generate speech, feed to redis, loop again
    while True:
        try:
            with yaspin(text="Waiting for text to render to audio...", spinner=Spinners.sand):
                while True:
                    raw = r.rpop(input_queue)
                    if raw is not None:
                        break
                    await asyncio.sleep(0.5)
            next_text = raw.decode("UTF-8")
            
            with yaspin(text=f"Generating audio for text '{next_text}'...", spinner=Spinners.dotsCircle):
                generated = generate_speech(qwen_model, next_text, args.voice_prompt, args.voice)
                push_bytes_to_queue(generated, r, output_queue)
        except KeyboardInterrupt:
            break

def generate_speech(model: FasterQwen3TTS, input: str, voice_prompt: str="", voice: str="") -> np.ndarray:
    assert voice != "" or voice_prompt != "", "One of voice or voice_prompt required for generate_speech"

    # Generates Tuple[list of wavs (NP arrays, see "dtype" above), sample rate]
    if voice_prompt != "":
        wavs, sample_rate = model.generate_voice_design(  # type: ignore[return-value]
            text=input,
            instruct=voice_prompt,
            language="english",
        )
    else:
        samples_dir = get_samples_dir()
        voice_txt = os.path.join(samples_dir, f"{voice}.txt")
        voice_wav = os.path.join(samples_dir, f"{voice}.wav")
        with open(voice_txt, "r") as ref_text_file:
            loaded_ref_text = ref_text_file.read()
            wavs, sample_rate = model.generate_voice_clone(
                text=input,
                ref_text=loaded_ref_text,
                ref_audio=voice_wav,
                language="english",
                xvec_only=False,
            )
    assert sample_rate == EXPECTED_SAMPLE_RATE, "Sample rate returned by generate_voice_design does not match expected"
    return wavs[0]

def push_bytes_to_queue(data: np.ndarray, r: redis.Redis, q: str) -> None:
    r.lpush(q, data.tobytes())

if __name__ == "__main__":
    asyncio.run(main())
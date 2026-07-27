import asyncio
import argparse
import redis
from environs import env
import torch
import numpy as np
from faster_qwen3_tts import FasterQwen3TTS
import sys, os
import json
from yaspin import yaspin
from yaspin.spinners import Spinners
from datetime import datetime

EXPECTED_SAMPLE_RATE = 24000
SAMPLES_RELDIR="audio_samples"
SAMPLES_DIR: str

class QwenTTSSpeakerStatus:
    def __init__(self, status: str, as_of: datetime=datetime.now()):
        self.as_of = as_of.timestamp()
        self.status = status
    
    def json(self) -> str:
        return json.dumps(self.__dict__)

    def __str__(self) -> str:
        return self.json()

def get_samples_dir() -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), SAMPLES_RELDIR)

async def main():
    env.read_env()

    parser = argparse.ArgumentParser(prog="qwen_tts_speaker")
    parser.add_argument("-t", "--text", default="", help="Text to generate to speech instead of fetching from Redis")
    model = env.str("QWEN_TTS_MODEL")
    tts_voice_id = env.str("TTS_VOICE_ID")
    redis_address = env.str("REDIS_ADDRESS", default="localhost")
    redis_port = env.int("REDIS_PORT", default=6379)
    input_queue = env.str("REDIS_TEXT_INPUT_QUEUE_NAME", default="queues:generated_text")
    output_queue = env.str("REDIS_AUDIO_OUTPUT_QUEUE_NAME", default="queues:generated_audio_bytes") # TODO: standardize and document the format of this output stream
    redis_status_output_name = env.str("REDIS_STATUS_OUTPUT_NAME", default="statuses:qwen_tts_speaker")
    redis_trigger_key_name = env.str("REDIS_TRIGGER_KEY_NAME", default="webpage.keepalive")
    max_gpu_memory_gb = env.float("MAX_GPU_MEMORY_GB", default=12)
    redis_voices_key_name = env.str("REDIS_VOICES_KEY_NAME", default="tts_voices")
    voices_file_path = env.str("VOICES_FILE", default="voices.json")
    args = parser.parse_args()

    assert model != "", "--model required but not specified"
    assert tts_voice_id != "", "TTS_VOICE_ID required but not specified"

    r = redis.Redis(host=redis_address, port=redis_port)

    # Load in the voices.json file, parse it, and put it into a key in redis if valid
    parsed_voices: dict[str, object]
    with open(voices_file_path) as voices_file:
        parsed_voices = json.load(voices_file)
        r.set(redis_voices_key_name, json.dumps(parsed_voices))

    assert tts_voice_id in parsed_voices, f"Voice {tts_voice_id} not found in voices file {voices_file_path}"
    voice_id = parsed_voices[tts_voice_id]["internal_id"]

    load_kwargs: dict[str, object] = {
        "device_map": "cuda:0",
        "dtype": torch.bfloat16,
    }
    if max_gpu_memory_gb is not None:
        load_kwargs["max_memory"] = {0: f"{max_gpu_memory_gb:.1f}GiB", "cpu": "4GiB"}
    
    qwen_model: FasterQwen3TTS = FasterQwen3TTS.from_pretrained(model)

    def set_status(new_status: str) -> None:
        r.set(redis_status_output_name, QwenTTSSpeakerStatus(status=new_status).json())
    
    set_status("Starting up...")

    # TODO: publish audio information such as bitrate to special keys in redis

    if args.text != "":
        with yaspin(text=f"Generating audio for text '{args.text}'...", spinner=Spinners.dotsCircle):
            result = generate_speech(qwen_model, args.text, voice=voice_id)
        print(f"Pushing bytes for text {args.text} to redis queue {output_queue}...")
        push_bytes_to_queue(result, r, output_queue)
        print(f"Successfully pushed audio for text {args.text} to redis, exiting.")
        sys.exit(0)

    # Preload qwen-tts by generating a tiny chunk of text, to prevent it from trying to provision GPU memory later when it's already
    # taken up by the giant llama-server model
    _ = generate_speech(qwen_model, "Warmup", voice=voice_id)

    # Loop until system interrupt (handle them cleanly):
    # while true, poll latest event from queue. If nothing, wait 500ms and try again. If something, generate speech, feed to redis, loop again
    while True:
        try:
            if not r.get(redis_trigger_key_name):
                # Stop generation, clear the queues
                set_status(f"Waiting for trigger key to be set to true...")
                print("Clearing generated audio queue")
                r.delete(output_queue)
                while r.get(redis_trigger_key_name) == "false":
                    await asyncio.sleep(1.0)

            set_status("Waiting for text to render to audio...")
            with yaspin(text="Waiting for text to render to audio...", spinner=Spinners.sand):
                while True:
                    raw = r.rpop(input_queue)
                    if raw is not None:
                        break
                    await asyncio.sleep(0.1)
            next_text = raw.decode("UTF-8")
            
            set_status(f"Generating audio for text '{next_text}'...")
            with yaspin(text=f"Generating audio for text '{next_text}'...", spinner=Spinners.dotsCircle):
                generated = generate_speech(qwen_model, next_text, voice=voice_id)
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
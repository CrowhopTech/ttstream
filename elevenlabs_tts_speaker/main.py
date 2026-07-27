import asyncio
import argparse
import redis
from environs import env
import numpy as np
import sys, os
import json
from yaspin import yaspin
from yaspin.spinners import Spinners
from datetime import datetime

from elevenlabs.client import ElevenLabs

EXPECTED_SAMPLE_RATE = 24000
SAMPLES_RELDIR = "audio_samples"
SAMPLES_DIR: str

# ElevenLabs only ever emits PCM as 16-bit signed integers over the wire --
# there is no float32 output_format on their API. We convert to float32 in
# [-1, 1] after the fact so the output queue's dtype matches Qwen3-TTS.
OUTPUT_DTYPE = np.float32
_INT16_MAX = 32768.0


class TTSSpeakerStatus:
    def __init__(self, status: str, as_of: datetime = datetime.now()):
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

    api_key = env.str("ELEVENLABS_API_KEY")
    model = env.str("ELEVENLABS_MODEL", default="eleven_turbo_v2_5")
    voice_id_param = env.str("TTS_VOICE_ID") # TODO: this is going to be passed in by Redis key instead, so we can change voices on the fly. Also add a "wipe the queue" key separate from this, for both audio and text
    redis_address = env.str("REDIS_ADDRESS", default="localhost")
    redis_port = env.int("REDIS_PORT", default=6379)
    input_queue = env.str("REDIS_TEXT_INPUT_QUEUE_NAME", default="queues:generated_text")
    output_queue = env.str("REDIS_AUDIO_OUTPUT_QUEUE_NAME", default="queues:generated_audio_bytes")
    redis_status_output_name = env.str("REDIS_STATUS_OUTPUT_NAME", default="statuses:qwen_tts_speaker")
    redis_trigger_key_name = env.str("REDIS_TRIGGER_KEY_NAME", default="webpage.keepalive")
    redis_voices_key_name = env.str("REDIS_VOICES_KEY_NAME", default="tts_voices")
    voices_file_path = env.str("VOICES_FILE", default="voices.json")

    assert api_key != "", "ELEVENLABS_API_KEY required but not specified"
    assert voice_id_param != "", \
        "TTS_VOICE_ID required but not specified"

    print("Constructing elevenlabs client")
    client = ElevenLabs(api_key=api_key)
    print("Constructed elevenlabs client")
    r = redis.Redis(host=redis_address, port=redis_port)

    def set_status(new_status: str) -> None:
        print(f"Setting status: {new_status}")
        r.set(redis_status_output_name, TTSSpeakerStatus(status=new_status).json())

    set_status("Starting up...")

    # Load in the voices.json file, parse it, and put it into a key in redis if valid
    parsed_voices: dict[str, object]
    with open(voices_file_path) as voices_file:
        parsed_voices = json.load(voices_file)
        r.set(redis_voices_key_name, json.dumps(parsed_voices))

    # Resolve a concrete voice_id ONCE up front, rather than per-utterance.
    # The original script re-ran generate_voice_design()/generate_voice_clone()
    # (with the reference sample) on every call, which is fine for a local
    # model but would mean re-cloning or re-designing a voice on every single
    # line of text here -- slow and wasteful against a hosted API. Instead we
    # resolve voice_id once (voice cloning or voice design), then reuse it,
    # which is the ElevenLabs-idiomatic equivalent of "load the voice."
    #
    # TTS_VOICE_ID takes priority over both: if you already have a voice
    # created in ElevenLabs (cloned, designed, or from their library), just
    # point at it directly and skip cloning/design altogether.
    elevenlabs_voice_id = parsed_voices[voice_id_param]["elevenlabs_voice_id"]
    print("Resolved voice ID")

    # Preload / sanity-check: generate a tiny chunk of text up front so that
    # auth or voice-config errors surface immediately instead of mid-loop.
    _ = generate_speech(client, model, "Warmup", elevenlabs_voice_id)

    while True:
        try:
            redis_trigger_val = r.get(redis_trigger_key_name)
            if redis_trigger_val is None or redis_trigger_val.decode("UTF-8") == "false":
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

            print("Generating audio for text")
            set_status(f"Generating audio for text '{next_text}'...")
            with yaspin(text=f"Generating audio for text '{next_text}'...", spinner=Spinners.dotsCircle):
                generated = generate_speech(client, model, next_text, elevenlabs_voice_id)
                push_bytes_to_queue(generated, r, output_queue)
        except KeyboardInterrupt:
            break


def resolve_voice_id(client: ElevenLabs, voice_id_param: str, voice_prompt: str, voice: str) -> str:
    """Turn TTS_VOICE_ID / TTS_VOICE_PROMPT / TTS_VOICE into a usable ElevenLabs voice_id.

    - voice_id_param -> already have a voice_id (cloned, designed, or from
      ElevenLabs' voice library) -- use it as-is, no cloning/design API
      calls needed. Takes priority if set.
    - voice_prompt -> ElevenLabs "voice design": describe a voice in words
      and get a synthesized voice back (analogous to generate_voice_design).
    - voice -> Instant Voice Cloning from the {voice}.wav sample in
      audio_samples/ (analogous to generate_voice_clone). The {voice}.txt
      reference transcript that Qwen3-TTS needs is not required by
      ElevenLabs' cloning API and is ignored here if present.
    """
    if voice_id_param != "":
        return voice_id_param

    if voice_prompt != "":
        previews = client.text_to_voice.design(
            voice_description=voice_prompt,
            text="This is a preview of the requested voice design.",
        )
        generated_voice_id = previews.previews[0].generated_voice_id
        created = client.text_to_voice.create(
            voice_name=f"designed-{generated_voice_id[:8]}",
            voice_description=voice_prompt,
            generated_voice_id=generated_voice_id,
        )
        return created.voice_id

    samples_dir = get_samples_dir()
    voice_wav = os.path.join(samples_dir, f"{voice}.wav")
    with open(voice_wav, "rb") as ref_audio_file:
        created = client.voices.ivc.create(
            name=voice,
            files=[ref_audio_file],
        )
    return created.voice_id


def generate_speech(client: ElevenLabs, model: str, input: str, voice_id: str) -> np.ndarray:
    print("Trying to generate speech...")
    audio_stream = client.text_to_speech.convert(
        voice_id=voice_id,
        model_id=model,
        text=input,
        output_format="pcm_24000",
    )
    print("Generated speech!")
    pcm_bytes = b"".join(audio_stream)
    pcm_i16 = np.frombuffer(pcm_bytes, dtype=np.int16)
    return (pcm_i16.astype(OUTPUT_DTYPE)) / _INT16_MAX


def push_bytes_to_queue(data: np.ndarray, r: redis.Redis, q: str) -> None:
    r.lpush(q, data.tobytes())


if __name__ == "__main__":
    asyncio.run(main())
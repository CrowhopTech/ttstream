import asyncio
import redis
from environs import env
import numpy as np
import os
import json
from datetime import datetime
from loguru import logger

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
    redis_address = env.str("REDIS_ADDRESS", default="localhost")
    redis_port = env.int("REDIS_PORT", default=6379)
    redis_status_output_name = env.str("REDIS_STATUS_OUTPUT_NAME", default="statuses:elevenlabs_tts_speaker")
    redis_trigger_key_name = env.str("REDIS_TRIGGER_KEY_NAME", default="session:keepalive")
    redis_session_id_key_name = env.str("REDIS_SESSION_ID_KEY_NAME", default="session:id")
    redis_session_info_key_name = env.str("REDIS_SESSION_INFO_KEY_NAME", default="session:info")
    redis_voices_key_name = env.str("REDIS_VOICES_KEY_NAME", default="options:tts_voices")
    input_queue_base = env.str("REDIS_TEXT_INPUT_QUEUE_NAME", default="queues:generated_text")
    output_queue_base = env.str("REDIS_AUDIO_OUTPUT_QUEUE_NAME", default="queues:generated_audio_bytes")
    voices_file_path = env.str("VOICES_FILE", default="voices.json")

    assert api_key != "", "ELEVENLABS_API_KEY required but not specified"

    logger.debug("Constructing elevenlabs client")
    client = ElevenLabs(api_key=api_key)
    logger.debug("Constructed elevenlabs client")
    r = redis.Redis(host=redis_address, port=redis_port)

    def set_status(new_status: str) -> None:
        logger.info(new_status)
        r.set(redis_status_output_name, TTSSpeakerStatus(status=new_status).json())

    # Session monitoring: wait for session to be set
    logger.info("ElevenLabs TTS speaker waiting for session initialization...")
    while True:
        session_id = r.get(redis_session_id_key_name)
        if session_id is not None:
            logger.info(f"Session initialized: {session_id.decode('UTF-8')}")
            break
        logger.debug("Waiting for session to be set...")
        await asyncio.sleep(1.0)

    # Derive queue names from session ID
    current_session_id = r.get(redis_session_id_key_name).decode("UTF-8")
    input_queue = f"{input_queue_base}:{current_session_id}"
    output_queue = f"{output_queue_base}:{current_session_id}"

    # Delete old queue if session info changed state
    session_info = r.get(redis_session_info_key_name)
    session_info_changed_state = True
    while session_info_changed_state:
        new_session_info = r.get(redis_session_info_key_name)
        if new_session_info is None:
            session_info_changed_state = False
        else:
            try:
                info_dict = json.loads(new_session_info.decode("UTF-8"))
                if info_dict.get("session_info_change_state", False):
                    logger.info(f"Session ID changed, deleting old queue: {output_queue}")
                    r.delete(output_queue)
                    session_info_changed_state = False
                else:
                    session_info_changed_state = False
            except (json.JSONDecodeError, UnicodeDecodeError):
                session_info_changed_state = False

    def get_queue(session_id: str) -> str:
        return f"{output_queue_base}:{session_id}"

    set_status("ElevenLabs TTS speaker is ready")

    # Load in the voices.json file, parse it, and put it into a key in redis if valid
    parsed_voices: dict[str, object]
    with open(voices_file_path) as voices_file:
        parsed_voices = json.load(voices_file)
        r.set(redis_voices_key_name, json.dumps(parsed_voices))
        logger.debug(f"Loaded voices JSON file: '{json.dumps(parsed_voices)}'")

    # Fetch voice_id from Redis (session:info) - no hard-coded voice IDs at startup
    session_info = r.get(redis_session_info_key_name)
    if session_info is not None:
        info_dict = json.loads(session_info.decode("UTF-8"))
        if info_dict.get("voice_id"):
            elevenlabs_voice_id = info_dict["voice_id"]["elevenlabs_voice_id"]
        else:
            # Fallback: use first available voice from voices.json
            if parsed_voices:
                first_voice_key = list(parsed_voices.keys())[0]
                elevenlabs_voice_id = parsed_voices[first_voice_key]["elevenlabs_voice_id"]
                logger.warning(f"No voice_id in session info, using first available voice: {first_voice_key}")
            else:
                raise ValueError("No voices available in voices.json")
    else:
        # Fallback: use first available voice from voices.json
        if parsed_voices:
            first_voice_key = list(parsed_voices.keys())[0]
            elevenlabs_voice_id = parsed_voices[first_voice_key]["elevenlabs_voice_id"]
            logger.warning(f"No session info found, using first available voice: {first_voice_key}")
        else:
            raise ValueError("No voices available in voices.json")

    # Preload / sanity-check: generate a tiny chunk of text up front so that
    # auth or voice-config errors surface immediately instead of mid-loop.
    _ = generate_speech(client, model, "Warmup", elevenlabs_voice_id)

    while True:
        try:
            redis_trigger_val = r.get(redis_trigger_key_name)
            if redis_trigger_val is None or redis_trigger_val.decode("UTF-8") == "false":
                # Stop generation, clear the queues
                set_status(f"Waiting for trigger key to be set to true...")
                logger.warning("Clearing generated audio queue")
                r.delete(output_queue)
                while r.get(redis_trigger_key_name) == "false":
                    await asyncio.sleep(1.0)

                # Re-check session ID after trigger reset
                current_session_id = r.get(redis_session_id_key_name).decode("UTF-8")
                input_queue = f"{input_queue_base}:{current_session_id}"
                output_queue = f"{output_queue_base}:{current_session_id}"

                # Re-fetch voice_id in case session info changed
                session_info = r.get(redis_session_info_key_name)
                if session_info is not None:
                    info_dict = json.loads(session_info.decode("UTF-8"))
                    if info_dict.get("voice_id"):
                        elevenlabs_voice_id = info_dict["voice_id"]["elevenlabs_voice_id"]
                    else:
                        first_voice_key = list(parsed_voices.keys())[0]
                        elevenlabs_voice_id = parsed_voices[first_voice_key]["elevenlabs_voice_id"]

            set_status("Waiting for text to render to audio...")
            while True:
                raw = r.rpop(input_queue)
                if raw is not None:
                    break
                await asyncio.sleep(0.1)
            next_text = raw.decode("UTF-8")

            set_status(f"Generating audio for text '{next_text}'...")
            generated = generate_speech(client, model, next_text, elevenlabs_voice_id)
            push_bytes_to_queue(generated, r, output_queue)
            logger.info(f"Successfully generated audio for text '{next_text}'")
        except KeyboardInterrupt:
            break


def generate_speech(client: ElevenLabs, model: str, input: str, voice_id: str) -> np.ndarray:
    audio_stream = client.text_to_speech.convert(
        voice_id=voice_id,
        model_id=model,
        text=input,
        output_format="pcm_24000",
    )
    logger.debug("Speech bytes generated, outputting to np buffer...")
    pcm_bytes = b"".join(audio_stream)
    pcm_i16 = np.frombuffer(pcm_bytes, dtype=np.int16)
    return (pcm_i16.astype(OUTPUT_DTYPE)) / _INT16_MAX


def push_bytes_to_queue(data: np.ndarray, r: redis.Redis, q: str) -> None:
    r.lpush(q, data.tobytes)


if __name__ == "__main__":
    asyncio.run(main())

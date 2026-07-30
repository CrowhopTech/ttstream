"""
Keepalive orchestrator — exposes /webpage-keepalive and flips a Redis flag.
"""

import time
import threading
import json
import redis
from flask import Flask, request
from flask_cors import CORS
from environs import env
from loguru import logger

app = Flask(__name__)
CORS(app, origins="*")

r = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
KEEPALIVE_TTL_SECONDS = 5  # Hits every 1 second, this gives a little buffer

last_keepalive_time = -1


@app.route("/webpage-keepalive", methods=["POST"])
def keepalive():
    global webpage_keepalive_key, last_keepalive_time
    r.set(webpage_keepalive_key, "true")
    last_keepalive_time = time.time()
    logger.debug("Got a keepalive ping")
    return "ok", 200


@app.route("/webpage_status.json", methods=["GET"])
def webpage_status():
    """Get status of TTStream background services."""    
    now = time.time()
    
    # Check OpenAI text generator status
    global openai_text_generator_status_key
    openai_status = r.get(openai_text_generator_status_key)
    
    if openai_status:
        openai_status_obj = json.loads(openai_status)
    else:
        openai_status_obj = None
    
    # Check Qwen TTS speaker status
    global qwen_tts_speaker_status_key
    qwen_status = r.get(qwen_tts_speaker_status_key)
    
    if qwen_status:
        qwen_status_obj = json.loads(qwen_status)
    else:
        qwen_status_obj = None
    
    status_data = {
        "openai_text_generator_status": {
            "as_of": now,
            "status": openai_status_obj.get("status", "unknown") if openai_status_obj else "unknown"
        },
        "qwen_tts_speaker_status": {
            "as_of": now,
            "status": qwen_status_obj.get("status", "unknown") if qwen_status_obj else "unknown"
        }
    }
    
    return status_data, 200


@app.route("/update_session", methods=["POST"])
def update_session():
    """Update session ID and info (voice/prompt IDs) for the active stream."""
    global session_id_key, session_info_key
    
    voice_id = request.args.get("voice", default=None)
    prompt_id = request.args.get("prompt", default=None)
    
    if not voice_id or not prompt_id:
        logger.warning(f"Missing voice or prompt ID. voice={voice_id}, prompt={prompt_id}")
        return "Missing voice or prompt parameter", 400
    
    now = str(time.time())
    
    # Update session ID
    r.set(session_id_key, prompt_id)
    
    # Update session info with voice and prompt IDs (prompt_id is stored as the key for openai_text_generator)
    session_info = {
        "as_of": now,
        "voice_id": voice_id,
        "prompt_id": prompt_id
    }
    r.set(session_info_key, json.dumps(session_info))
    
    logger.info(f"Updated session: voice={voice_id}, prompt={prompt_id}, id={prompt_id}")
    return "ok", 200


@app.route("/speech_options", methods=["GET"])
def speech_options():
    """
    Get available TTS voice options (and prepare for prompts).
    
    Reads from Redis key 'options:tts_voices' which should contain
    the list of available voice options from the Qwen TTS service.
    
    Returns JSON structure with 'voices' field and 'prompts' field (reserved for future).
    """
    # Fetch voice options from Redis
    voices_key = "options:tts_voices"
    voices_data = r.get(voices_key)
    
    if voices_data:
        try:
            voices = json.loads(voices_data)
        except json.JSONDecodeError:
            voices = []
    else:
        voices = []
    
    # Prepare structure for prompts (to be populated later)
    prompts_data = r.get("options:prompts")
    if prompts_data:
        try:
            prompts = json.loads(prompts_data)
        except json.JSONDecodeError:
            prompts = []
    else:
        prompts = None  # Not yet available
    
    # Return structure (prompts will be added when ready)
    return {
        "voices": voices,
        "prompts": prompts
    }, 200


def _keepalive_monitor():
    global last_keepalive_time, webpage_keepalive_key
    while True:
        now = time.time()
        elapsed = now - last_keepalive_time
        
        if r.get(webpage_keepalive_key) != "false" and elapsed >= KEEPALIVE_TTL_SECONDS:
            logger.info(f"Haven't gotten a keepalive heartbeat in {elapsed} seconds, setting {webpage_keepalive_key} to 'False'")
            r.set(webpage_keepalive_key, "false")
        
        time.sleep(1)


if __name__ == "__main__":
    env.read_env()
    global qwen_tts_speaker_status_key, openai_text_generator_status_key, webpage_keepalive_key
    qwen_tts_speaker_status_key = env.str("QWEN_TTS_SPEAKER_STATUS_KEY", default="statuses:qwen_tts_speaker")
    openai_text_generator_status_key = env.str("OPENAI_TEXT_GENERATOR_STATUS_KEY", default="statuses:openai_text_generator")
    webpage_keepalive_key = env.str("WEBPAGE_KEEPALIVE_KEY", default="session:keepalive")
    session_id_key = env.str("SESSION_ID_KEY", default="session:id")
    session_info_key = env.str("SESSION_INFO_KEY", default="session:info")
    
    t = threading.Thread(target=_keepalive_monitor, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8888)

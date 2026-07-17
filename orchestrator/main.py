"""
Keepalive orchestrator — exposes /webpage-keepalive and flips a Redis flag.
"""

import time
import threading

import redis
from flask import Flask, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins="*")

r = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
KEEPALIVE_KEY = "webpage.keepalive"
KEEPALIVE_TTL_SECONDS = 5  # Hits every 1 second, this gives a little buffer

last_keepalive_time = -1


@app.route("/webpage-keepalive", methods=["POST"])
def keepalive():
    r.set(KEEPALIVE_KEY, "true")
    return "ok", 200


@app.route("/webpage_status.json", methods=["GET"])
def webpage_status():
    """Get status of TTStream background services."""
    import json
    
    now = time.time()
    
    # Check OpenAI text generator status
    openai_status_key = "openai_text_generator_status"
    openai_status = r.get(openai_status_key)
    
    if openai_status:
        openai_status_obj = json.loads(openai_status)
        print(f"Got OpenAI status object: {openai_status_obj}")
    else:
        openai_status_obj = None
    
    # Check Qwen TTS speaker status
    qwen_status_key = "qwen_tts_speaker_status"
    qwen_status = r.get(qwen_status_key)
    
    if qwen_status:
        qwen_status_obj = json.loads(qwen_status)
        print(f"Got Qwen TTS status object: {qwen_status_obj}")
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


def _keepalive_monitor():
    while True:
        now = time.time()
        elapsed = now - last_keepalive_time
        
        if r.get(KEEPALIVE_KEY) != "false" and elapsed >= KEEPALIVE_TTL_SECONDS:
            print(f"Haven't gotten a keepalive heartbeat in {elapsed} seconds, setting {KEEPALIVE_KEY} to 'False'")
            r.set(KEEPALIVE_KEY, "false")
        
        time.sleep(1)


if __name__ == "__main__":
    t = threading.Thread(target=_keepalive_monitor, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=8888)

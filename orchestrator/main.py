"""
Keepalive orchestrator — exposes /webpage-keepalive and flips a Redis flag.
"""

import time
import threading

import redis
from flask import Flask, request

app = Flask(__name__)

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
KEEPALIVE_KEY = "webpage.keepalive"
KEEPALIVE_TTL_SECONDS = 7 # Hits every 5 seconds, this gives a little buffer

last_keepalive_time = -1


@app.route("/webpage-keepalive", methods=["POST"])
def keepalive():
    r.set(KEEPALIVE_KEY, "true", ex=KEEPALIVE_TTL_SECONDS)
    return "ok", 200


def _keepalive_monitor():
    while True:
        now = time.time()
        elapsed = now - last_keepalive_time

        if elapsed >= KEEPALIVE_TTL_SECONDS:
            print(f"Haven't gotten a keepalive heartbeat in {elapsed} seconds, setting {KEEPALIVE_KEY} to 'False'")
            r.set(KEEPALIVE_KEY, "false")

        time.sleep(1)


if __name__ == "__main__":
    t = threading.Thread(target=_keepalive_monitor, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)

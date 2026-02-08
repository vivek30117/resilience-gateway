from fastapi import FastAPI, HTTPException
import time
import random

app = FastAPI()

# -------------------------------
# In-memory state
# -------------------------------
processed_requests = set()
request_timestamps = []

failure_count = 0
circuit_state = "CLOSED"
last_failure_time = 0

# -------------------------------
# Config
# -------------------------------
RATE_LIMIT = 5
WINDOW_SECONDS = 10
FAILURE_THRESHOLD = 3
COOLDOWN_SECONDS = 10
MAX_RETRIES = 3


def allow_request():
    global request_timestamps
    now = time.time()

    request_timestamps = [t for t in request_timestamps if now - t < WINDOW_SECONDS]

    if len(request_timestamps) >= RATE_LIMIT:
        return False

    request_timestamps.append(now)
    return True


def circuit_allows_call():
    global circuit_state, last_failure_time
    now = time.time()

    if circuit_state == "OPEN":
        if now - last_failure_time > COOLDOWN_SECONDS:
            circuit_state = "HALF_OPEN"
        else:
            return False
    return True


def unstable_service_call():
    return random.choice([True, False])


def call_with_retry():
    delay = 1
    for _ in range(MAX_RETRIES):
        if unstable_service_call():
            return True
        time.sleep(delay)
        delay *= 2
    return False


@app.get("/process")
def process(request_id: str):
    global failure_count, circuit_state, last_failure_time

    if request_id in processed_requests:
        return {"status": "duplicate_blocked"}

    if not allow_request():
        raise HTTPException(status_code=429, detail="rate_limit_exceeded")

    if not circuit_allows_call():
        return {"status": "blocked_by_open_circuit"}

    success = call_with_retry()

    if success:
        processed_requests.add(request_id)
        failure_count = 0
        circuit_state = "CLOSED"
        return {"status": "success", "circuit_state": circuit_state}

    failure_count += 1
    last_failure_time = time.time()

    if failure_count >= FAILURE_THRESHOLD:
        circuit_state = "OPEN"

    return {
        "status": "failure",
        "failure_count": failure_count,
        "circuit_state": circuit_state
    }

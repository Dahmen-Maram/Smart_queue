from fastapi import FastAPI, Response
from pydantic import BaseModel
from typing import Dict, Any, Union, Optional
import os
import requests
import time

app = FastAPI(title="IA Service")

# ---------------------------
# Dapr config (sidecar ia-service)
# ---------------------------
DAPR_HTTP_PORT = os.getenv("DAPR_HTTP_PORT", "3502")
PUBSUB_NAME = "pubsub"
IN_TOPIC = "patient.added"
OUT_TOPIC = "patient.scored"
ALERT_TOPIC = "alert.critical"   # optionnel

# ---------------------------
# CloudEvent wrapper
# ---------------------------
class CloudEvent(BaseModel):
    data: Optional[Union[Dict[str, Any], str]] = None

# ---------------------------
# Dapr subscription
# ---------------------------
@app.get("/dapr/subscribe")
def dapr_subscribe():
    return [{"pubsubname": PUBSUB_NAME, "topic": IN_TOPIC, "route": "/process_patient"}]

# ---------------------------
# IA scoring logic
# ---------------------------
def score_patient(patient: Dict[str, Any]) -> Dict[str, Any]:
    speciality = (patient.get("speciality") or "").lower()

    base_priority = {
        "cardio": 3,
        "urgence": 5,
        "pediatrie": 4,
        "general": 2,
    }.get(speciality, 2)

    eta_minutes = max(5, 30 - base_priority * 4)

    return {
        **patient,
        "priority": base_priority,
        "eta_minutes": eta_minutes,
        "scored_at": int(time.time()),
    }

# ---------------------------
# Helpers publish Dapr
# ---------------------------
def dapr_publish(topic: str, payload: Dict[str, Any]):
    url = f"http://127.0.0.1:{DAPR_HTTP_PORT}/v1.0/publish/{PUBSUB_NAME}/{topic}"
    r = requests.post(url, json=payload, timeout=3)
    return r

# ---------------------------
# Main handler
# ---------------------------
@app.post("/process_patient")
def process_patient(event: CloudEvent):
    payload = event.data or {}

    # 1) SAFE payload: ignore non-dict (ex: "" ou string)
    if not isinstance(payload, dict):
        print("[IA] WARN: event.data not dict -> ignored:", repr(payload))
        return Response(status_code=200)

    # 2) Minimal validation
    required = ["patient_id", "name", "speciality"]
    missing = [k for k in required if not payload.get(k)]
    if missing:
        print("[IA] WARN: missing fields -> ignored:", missing, "payload=", payload)
        return Response(status_code=200)

    # 3) Score
    scored = score_patient(payload)
    print("[IA] scored:", scored)

    # 4) Publish patient.scored
    try:
        r = dapr_publish(OUT_TOPIC, scored)
        if r.status_code >= 400:
            print("[IA] publish FAILED:", r.status_code, r.text)
        else:
            print("[IA] publish OK:", r.status_code)
    except Exception as e:
        print("[IA] publish EXCEPTION:", repr(e))

    # 5) (Optionnel) Publish critical alert
    try:
        speciality = (scored.get("speciality") or "").lower()
        pr = int(scored.get("priority") or 0)

        if speciality == "urgence" or pr >= 5:
            alert_payload = {
                "type": "CRITICAL",
                "message": f"Critical patient detected: {scored.get('name')}",
                "patient": scored,
                "created_at": int(time.time()),
            }
            r2 = dapr_publish(ALERT_TOPIC, alert_payload)
            if r2.status_code >= 400:
                print("[IA] alert publish FAILED:", r2.status_code, r2.text)
            else:
                print("[IA] alert publish OK:", r2.status_code)
    except Exception as e:
        print("[IA] alert publish EXCEPTION:", repr(e))

    return Response(status_code=200)

# ---------------------------
# Health
# ---------------------------
@app.get("/")
def health():
    return {"status": "IA Service running"}

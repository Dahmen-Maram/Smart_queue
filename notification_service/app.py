from fastapi import FastAPI, Response
from pydantic import BaseModel
from typing import Any, Dict, Optional, Union

app = FastAPI(title="Notification Service")

class CloudEvent(BaseModel):
    data: Optional[Union[Dict[str, Any], str]] = None

def as_dict(evt: CloudEvent) -> Dict[str, Any]:
    return evt.data if isinstance(evt.data, dict) else {}

@app.get("/dapr/subscribe")
def dapr_subscribe():
    return [
        {"pubsubname": "pubsub", "topic": "patient.scored",    "route": "/notify/patient-scored"},
        {"pubsubname": "pubsub", "topic": "queue.next",        "route": "/notify/queue-next"},
        {"pubsubname": "pubsub", "topic": "queue.updated",     "route": "/notify/queue-updated"},
        {"pubsubname": "pubsub", "topic": "patient.cancelled", "route": "/notify/patient-cancelled"},
        {"pubsubname": "pubsub", "topic": "alert.critical",    "route": "/notify/alert-critical"},
    ]

@app.post("/notify/patient-scored")
def notify_patient_scored(event: CloudEvent):
    p = as_dict(event)
    if not p:
        print("[NOTIF] WARN patient.scored empty payload")
        return Response(status_code=200)

    pr = int(p.get("priority", 0) or 0)
    name = p.get("name")
    spec = p.get("speciality")
    eta = p.get("eta_minutes")

    if pr >= 4:
        print(f"[NOTIF]  URGENT | {name} | {spec} | priority={pr} | ETA={eta}min")
    else:
        print(f"[NOTIF]  Scored | {name} | {spec} | priority={pr} | ETA={eta}min")
    return Response(status_code=200)

@app.post("/notify/queue-next")
def notify_queue_next(event: CloudEvent):
    payload = as_dict(event)
    # attendu: {"speciality":"Cardio","next":{...},"remaining":2}
    print(f"[NOTIF]  NEXT patient called: {payload}")
    return Response(status_code=200)

@app.post("/notify/queue-updated")
def notify_queue_updated(event: CloudEvent):
    payload = as_dict(event)
    spec = payload.get("speciality")
    size = int(payload.get("size", 0) or 0)

    if size >= 10:
        print(f"[NOTIF]  SATURATION | {spec} queue size={size}")
    elif size >= 5:
        print(f"[NOTIF]  Busy queue | {spec} queue size={size}")
    else:
        # option: si tu vex silence sous 5, commente la ligne
        print(f"[NOTIF] Queue updated | {spec} size={size}")

    return Response(status_code=200)

@app.post("/notify/patient-cancelled")
def notify_cancel(event: CloudEvent):
    payload = as_dict(event)
    print(f"[NOTIF]  Patient cancelled: {payload}")
    return Response(status_code=200)

@app.post("/notify/alert-critical")
def notify_critical(event: CloudEvent):
    payload = as_dict(event)
    print(f"[NOTIF]  CRITICAL ALERT: {payload}")
    return Response(status_code=200)

@app.get("/")
def health():
    return {"status": "Notification Service running"}

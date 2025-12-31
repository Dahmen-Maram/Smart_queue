from fastapi import FastAPI, Response
from pydantic import BaseModel
from typing import Any, Dict, Optional, Union, List
import os
import requests

app = FastAPI(title="Queue Service")

# --- Dapr config ---
DAPR_HTTP_PORT = os.getenv("DAPR_HTTP_PORT", "3501")   # sidecar queue-service
STATESTORE = "statestore"
PUBSUB = "pubsub"

TOPIC_ADDED = "patient.added"
TOPIC_SCORED = "patient.scored"

TOPIC_QUEUE_UPDATED = "queue.updated"
TOPIC_QUEUE_NEXT = "queue.next"
TOPIC_CANCELLED = "patient.cancelled"
TOPIC_CRITICAL = "alert.critical"

# --- Stats index key (pour /stats) ---
INDEX_KEY = "queues:index"   # contient la liste des spécialités vues

# --- Models ---
class CloudEvent(BaseModel):
    data: Optional[Union[Dict[str, Any], str]] = None

def as_dict(evt: CloudEvent) -> Dict[str, Any]:
    return evt.data if isinstance(evt.data, dict) else {}

# --- Dapr helpers ---
def state_get(key: str):
    url = f"http://127.0.0.1:{DAPR_HTTP_PORT}/v1.0/state/{STATESTORE}/{key}"
    r = requests.get(url, timeout=3)
    if r.status_code == 204 or r.text.strip() == "":
        return None
    return r.json()

def state_set(key: str, value: Any):
    url = f"http://127.0.0.1:{DAPR_HTTP_PORT}/v1.0/state/{STATESTORE}"
    r = requests.post(url, json=[{"key": key, "value": value}], timeout=3)
    r.raise_for_status()

def publish(topic: str, data: Dict[str, Any]):
    url = f"http://127.0.0.1:{DAPR_HTTP_PORT}/v1.0/publish/{PUBSUB}/{topic}"
    try:
        r = requests.post(url, json=data, timeout=3)
        if r.status_code >= 400:
            print(f"[QUEUE] publish {topic} FAILED {r.status_code}: {r.text}", flush=True)
        return r.status_code
    except Exception as e:
        print(f"[QUEUE] publish {topic} EXCEPTION: {e}", flush=True)
        return None

# --- Queue helpers ---
def normalize_speciality_str(s: str) -> str:
    # Evite Cardio/cardio/CARDIO -> "Cardio"
    s = (s or "General").strip()
    if not s:
        return "General"
    return s[:1].upper() + s[1:].lower()

def normalize_speciality(p: Dict[str, Any]) -> str:
    return normalize_speciality_str(p.get("speciality") or "General")

def queue_key(speciality: str) -> str:
    return f"queue:{normalize_speciality_str(speciality)}"

def sort_queue_if_scored(q: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    - Les scorés d'abord
    - priority desc
    - scored_at asc (plus ancien d'abord)
    - Les non-scorés à la fin (FIFO entre eux conservé globalement via append)
    """
    def key(x):
        pr = x.get("priority")
        scored_at = x.get("scored_at")
        is_scored = 0 if pr is not None else 1
        pr_val = int(pr) if pr is not None else -1
        scored_at_val = int(scored_at) if scored_at is not None else 10**18
        return (is_scored, -pr_val, scored_at_val)

    return sorted(q, key=key)

def emit_queue_updated(speciality: str, size: int):
    speciality = normalize_speciality_str(speciality)
    publish(TOPIC_QUEUE_UPDATED, {"speciality": speciality, "size": size})

def index_add_speciality(speciality: str):
    speciality = normalize_speciality_str(speciality)
    idx = state_get(INDEX_KEY) or []
    if speciality not in idx:
        idx.append(speciality)
        state_set(INDEX_KEY, idx)

# --- Subscriptions ---
@app.get("/dapr/subscribe")
def dapr_subscribe():
    return [
        {"pubsubname": PUBSUB, "topic": TOPIC_ADDED,  "route": "/patient-added"},
        {"pubsubname": PUBSUB, "topic": TOPIC_SCORED, "route": "/patient-scored"},
    ]

# --- Handlers ---
@app.post("/patient-added")
def handle_patient_added(event: CloudEvent):
    p = as_dict(event)
    if not p:
        print("[QUEUE] WARN patient.added empty payload")
        return Response(status_code=200)

    speciality = normalize_speciality(p)
    index_add_speciality(speciality)
    key = queue_key(speciality)

    q: List[Dict[str, Any]] = state_get(key) or []
    pid = p.get("patient_id")

    # ✅ anti-duplication
    if pid and any(it.get("patient_id") == pid for it in q):
        print("[QUEUE] patient already in queue, ignore patient.added:", pid)
        return Response(status_code=200)

    q.append(p)         # FIFO
    state_set(key, q)

    print("[QUEUE] Patient reçu:", p)
    print("[QUEUE] Saved in", key, "size =", len(q))

    emit_queue_updated(speciality, len(q))
    return Response(status_code=200)

@app.post("/patient-scored")
def handle_patient_scored(event: CloudEvent):
    scored = as_dict(event)
    if not scored:
        print("[QUEUE] WARN patient.scored empty payload")
        return Response(status_code=200)

    speciality = normalize_speciality(scored)
    index_add_speciality(speciality)
    key = queue_key(speciality)

    q: List[Dict[str, Any]] = state_get(key) or []
    pid = scored.get("patient_id")

    replaced = False
    if pid:
        for i, it in enumerate(q):
            if it.get("patient_id") == pid:
                q[i] = scored
                replaced = True
                break
    if not replaced:
        q.append(scored)

    q = sort_queue_if_scored(q)
    state_set(key, q)

    print("[QUEUE] scored patient:", scored)
    print("[QUEUE] Updated", key, "size =", len(q), "replaced=", replaced)

    emit_queue_updated(speciality, len(q))

    # Alerte critique si urgence ou priorité max
    pr = int(scored.get("priority", 0) or 0)
    if speciality.lower() == "Urgence".lower() or pr >= 5:
        publish(TOPIC_CRITICAL, {
            "message": "Patient critique détecté",
            "patient": scored,
            "speciality": speciality,
            "priority": pr
        })

    return Response(status_code=200)

# --- API endpoints ---
@app.get("/queue/{speciality}")
def get_queue(speciality: str):
    speciality = normalize_speciality_str(speciality)
    key = queue_key(speciality)
    q = state_get(key) or []
    return {"key": key, "size": len(q), "queue": q}

@app.post("/next/{speciality}")
def pop_next(speciality: str):
    speciality = normalize_speciality_str(speciality)
    key = queue_key(speciality)
    q: List[Dict[str, Any]] = state_get(key) or []

    if not q:
        return {"message": "queue empty", "speciality": speciality, "remaining": 0}

    nxt = q.pop(0)
    state_set(key, q)

    payload = {"speciality": speciality, "next": nxt, "remaining": len(q)}
    publish(TOPIC_QUEUE_NEXT, payload)
    emit_queue_updated(speciality, len(q))

    return payload

@app.delete("/cancel/{speciality}/{patient_id}")
def cancel_patient(speciality: str, patient_id: str):
    speciality = normalize_speciality_str(speciality)
    key = queue_key(speciality)
    q: List[Dict[str, Any]] = state_get(key) or []

    new_q = [p for p in q if p.get("patient_id") != patient_id]
    removed = len(q) - len(new_q)
    state_set(key, new_q)

    publish(TOPIC_CANCELLED, {
        "speciality": speciality,
        "patient_id": patient_id,
        "removed": removed
    })
    emit_queue_updated(speciality, len(new_q))

    return {"speciality": speciality, "patient_id": patient_id, "removed": removed, "remaining": len(new_q)}

# ✅ 1) RESET par spécialité
@app.delete("/reset/{speciality}")
def reset_queue(speciality: str):
    speciality = normalize_speciality_str(speciality)
    index_add_speciality(speciality)
    key = queue_key(speciality)

    state_set(key, [])
    emit_queue_updated(speciality, 0)

    return {"message": "queue reset", "speciality": speciality, "key": key, "size": 0}

# ✅ 2) STATS (taille par spécialité)
@app.get("/stats")
def stats():
    idx = state_get(INDEX_KEY) or []
    queues = []
    total = 0

    for spec in idx:
        key = queue_key(spec)
        q = state_get(key) or []
        size = len(q)
        total += size
        queues.append({"speciality": spec, "size": size})

    queues.sort(key=lambda x: x["size"], reverse=True)
    return {"total_patients": total, "queues": queues}

@app.get("/")
def health():
    return {"status": "Queue Service running"}

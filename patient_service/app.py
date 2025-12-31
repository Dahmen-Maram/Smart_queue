from fastapi import FastAPI, HTTPException
import requests
import uuid
import os

app = FastAPI(title="Patient Service")

DAPR_HTTP_PORT = os.getenv("DAPR_HTTP_PORT", "3500")
PUBSUB_NAME = "pubsub"
TOPIC_NAME = "patient.added"


@app.post("/patients")
def create_patient(name: str, speciality: str):
    patient_id = str(uuid.uuid4())

    event = {
        "patient_id": patient_id,
        "name": name,
        "speciality": speciality
    }

    url = f"http://127.0.0.1:{DAPR_HTTP_PORT}/v1.0/publish/{PUBSUB_NAME}/{TOPIC_NAME}"

    try:
        r = requests.post(url, json=event, timeout=3)
        print(f"[PATIENT] publish -> {TOPIC_NAME} status={r.status_code} payload={event}", flush=True)

        if r.status_code >= 400:
            print(f"[PATIENT] response body: {r.text}", flush=True)
        r.raise_for_status()

    except requests.exceptions.ConnectionError:
        raise HTTPException(
            status_code=503,
            detail=f"Dapr sidecar not reachable on port {DAPR_HTTP_PORT}. Is dapr run started?"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"message": "Patient created", "patient_id": patient_id}


@app.get("/")
def health():
    return {"status": "Patient Service running"}

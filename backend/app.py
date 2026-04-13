"""
CDM StreamGuard — FastAPI Backend
===================================
Privacy Gateway intégré dans /api/ask
Pattern ØllamØnym adapté Capital Markets CDM

Flux /api/ask :
  Question employé
      ↓ pseudonymize_text()
  Question avec tokens
      ↓ Qwen génère SQL
  SQL avec tokens
      ↓ Dremio exécute
  Données avec tokens
      ↓ deanonymize()
  Réponse avec vraies valeurs

Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM
"""

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional

import requests
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from privacy_gateway import StreamGuardGateway

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BACKEND] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────────
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_URL         = os.getenv("DB_URL", "postgresql://cdm_user:CDM_PG_2026!@localhost:5432/cdm_metadata")
DREMIO_HOST    = os.getenv("DREMIO_HOST",    "localhost")
DREMIO_PORT    = os.getenv("DREMIO_PORT",    "9047")
DREMIO_USER    = os.getenv("DREMIO_USER",    "cdm_user")
DREMIO_PASS    = os.getenv("DREMIO_PASSWORD","CDM_Dremio_2026!")
OLLAMA_URL     = os.getenv("OLLAMA_URL",     "http://localhost:11434")
OLLAMA_MODEL   = os.getenv("OLLAMA_MODEL",   "qwen2.5:7b-instruct")
TOPIC_SECURE   = os.getenv("TOPIC_SECURE",   "cdm.sisdm.kondor.dbo.deals.secure")
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

DREMIO_BASE = f"http://{DREMIO_HOST}:{DREMIO_PORT}"

# ── WEBSOCKET MANAGER ─────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        if not self.active:
            return
        msg  = json.dumps(data, ensure_ascii=False, default=str)
        dead = []
        for ws in self.active:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active.remove(ws)


manager = ConnectionManager()

# ── DREMIO CLIENT ─────────────────────────────────────────────
class DremioClient:
    def __init__(self):
        self._token       : Optional[str] = None
        self._token_expiry: float         = 0

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expiry:
            return self._token
        try:
            resp = requests.post(
                f"{DREMIO_BASE}/apiv2/login",
                json={"userName": DREMIO_USER, "password": DREMIO_PASS},
                timeout=10,
            )
            resp.raise_for_status()
            self._token        = resp.json()["token"]
            self._token_expiry = time.time() + 3600
            return self._token
        except Exception as e:
            logger.error(f"Dremio auth failed: {e}")
            raise

    def query(self, sql: str) -> list[dict]:
        try:
            token   = self._get_token()
            headers = {"Authorization": f"Bearer {token}"}
            job     = requests.post(
                f"{DREMIO_BASE}/api/v3/sql",
                headers=headers,
                json={"sql": sql},
                timeout=30,
            )
            job.raise_for_status()
            job_id = job.json()["id"]

            for _ in range(60):
                status_r = requests.get(
                    f"{DREMIO_BASE}/api/v3/job/{job_id}",
                    headers=headers,
                    timeout=10,
                )
                state = status_r.json().get("jobState")
                if state == "COMPLETED":
                    break
                if state == "FAILED":
                    raise Exception(f"Dremio job failed")
                time.sleep(0.5)

            res = requests.get(
                f"{DREMIO_BASE}/api/v3/job/{job_id}/results",
                headers=headers,
                timeout=10,
            )
            return res.json().get("rows", [])
        except Exception as e:
            logger.warning(f"Dremio query failed: {e}")
            return []


dremio = DremioClient()

# ── OLLAMA / QWEN ─────────────────────────────────────────────
def call_ollama(prompt: str) -> str:
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["response"].strip()
    except Exception as e:
        logger.error(f"Ollama error: {e}")
        return ""


def generate_sql(question_pseudo: str) -> str:
    """
    Qwen génère le SQL à partir de la question pseudonymisée.
    Les tokens CDM sont dans la question — Qwen les utilise dans le SQL.
    """
    prompt = f"""Tu es un expert SQL pour la Salle des Marchés du Crédit du Maroc.

Tables disponibles (Dremio — Gold Layer) :
- gold.positions_nettes  : devise, position_mad, nb_deals, derniere_maj
- gold.pnl_journalier    : date_trade, devise, pnl_brut, nb_deals
- gold.mtm_intraday      : deal_id, taux, taux_marche, mtm_value, devise
- silver.deals_clean     : deal_id, montant, taux, devise, trader, ts_silver
- silver.anomalies       : deal_id, anomaly_type, score, detected_at

IMPORTANT : Les valeurs dans la question sont des tokens pseudonymisés.
Utilise-les exactement tels quels dans le SQL.

Question : "{question_pseudo}"

Génère UNIQUEMENT la requête SQL. Rien d'autre."""

    return call_ollama(prompt)


def format_response(question_orig: str, data: list) -> str:
    """
    Qwen formate la réponse finale en français.
    La question originale et les données sont dépseudonymisées.
    """
    if not data:
        return "Aucune donnée trouvée pour cette requête."

    prompt = f"""Tu es l'assistant IA de la Salle des Marchés du Crédit du Maroc.

Question posée : "{question_orig}"
Données : {json.dumps(data, ensure_ascii=False, default=str)}

Formule une réponse claire et naturelle en français.
Sois précis avec les chiffres. Pas de JSON. Pas de SQL."""

    result = call_ollama(prompt)
    if not result:
        return f"Données récupérées : {json.dumps(data, default=str)}"
    return result


# ── KAFKA CONSUMER ASYNC ──────────────────────────────────────
async def kafka_stream_task():
    consumer = Consumer({
        "bootstrap.servers" : KAFKA_SERVERS,
        "group.id"          : "cdm-backend-stream",
        "auto.offset.reset" : "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_SECURE])
    logger.info(f"Kafka consumer started — topic={TOPIC_SECURE}")
    loop = asyncio.get_event_loop()

    try:
        while True:
            msg = await loop.run_in_executor(
                None, lambda: consumer.poll(timeout=0.1)
            )
            if msg is None:
                await asyncio.sleep(0.05)
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                debezium = payload.get("payload", payload)
                deal     = debezium.get("after", debezium)

                await manager.broadcast({
                    "type"     : "deal",
                    "deal"     : deal,
                    "offset"   : msg.offset(),
                    "timestamp": time.time(),
                })
            except Exception as e:
                logger.error(f"Message processing error: {e}")
    finally:
        consumer.close()


# ── LIFESPAN ──────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(kafka_stream_task())
    logger.info("CDM StreamGuard Backend started ✅")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ── FASTAPI APP ───────────────────────────────────────────────
app = FastAPI(
    title       = "CDM StreamGuard API",
    description = "Privacy-First Capital Markets Intelligence — CDM DSIG",
    version     = "1.0.0",
    lifespan    = lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# ── MODELS ────────────────────────────────────────────────────
class AskRequest(BaseModel):
    question  : str
    model     : str = "qwen"
    session_id: str = "default"

class AskResponse(BaseModel):
    question        : str
    question_pseudo : str
    sql             : str
    data_raw        : list
    data_real       : list
    response        : str
    model_used      : str
    latency_ms      : float
    entities_found  : int

# ── ROUTES ────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Statut de tous les services."""
    services = {}

    # Kafka
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})
        admin.list_topics(timeout=3)
        services["kafka"] = "ok"
    except Exception as e:
        services["kafka"] = f"error: {e}"

    # Dremio
    try:
        dremio._get_token()
        services["dremio"] = "ok"
    except Exception as e:
        services["dremio"] = f"error: {e}"

    # Ollama
    try:
        resp = requests.get(f"{OLLAMA_URL}/api/tags", timeout=3)
        models = [m["name"] for m in resp.json().get("models", [])]
        services["ollama"] = f"ok — {models}"
    except Exception as e:
        services["ollama"] = f"error: {e}"

    all_ok = all("ok" in str(v) for v in services.values())
    return JSONResponse(
        content={"status": "ok" if all_ok else "degraded", "services": services},
        status_code=200 if all_ok else 207,
    )


@app.get("/api/positions")
async def get_positions():
    try:
        sql  = "SELECT devise, position_mad, nb_deals FROM CDM_DATALAKE.gold.positions_nettes WHERE DATE(derniere_maj) = CURRENT_DATE ORDER BY position_mad DESC"
        data = dremio.query(sql)
        return {"positions": data, "source": "dremio"}
    except Exception as e:
        return {"positions": [
            {"devise": "EUR/MAD", "position_mad": 490350000, "nb_deals": 12},
            {"devise": "USD/MAD", "position_mad": 120240000, "nb_deals": 4},
        ], "source": "demo"}


@app.get("/api/pnl")
async def get_pnl():
    try:
        sql  = "SELECT devise, SUM(pnl_brut) AS pnl_total FROM CDM_DATALAKE.gold.pnl_journalier WHERE date_trade = CURRENT_DATE GROUP BY devise"
        data = dremio.query(sql)
        return {"pnl": data, "source": "dremio"}
    except Exception as e:
        return {"pnl": [
            {"devise": "EUR/MAD", "pnl_total":  1240000},
            {"devise": "USD/MAD", "pnl_total": -320000},
        ], "source": "demo"}


@app.get("/api/anomalies")
async def get_anomalies():
    try:
        sql  = "SELECT deal_id, anomaly_type, score, detected_at FROM CDM_DATALAKE.silver.anomalies WHERE DATE(detected_at) = CURRENT_DATE ORDER BY score ASC LIMIT 20"
        data = dremio.query(sql)
        return {"anomalies": data, "source": "dremio"}
    except Exception as e:
        return {"anomalies": [
            {"deal_id": "DEAL_a3f9", "anomaly_type": "RATE_OUTLIER",   "score": -0.91},
            {"deal_id": "DEAL_b2e1", "anomaly_type": "AMOUNT_OUTLIER", "score": -0.85},
        ], "source": "demo"}


@app.post("/api/ask", response_model=AskResponse)
async def ask(request: AskRequest):
    """
    Assistant IA Privacy-First.

    Flux complet — Pattern ØllamØnym adapté CDM :

    1. StreamGuard pseudonymise la question
    2. Qwen génère le SQL avec les tokens
    3. Dremio exécute le SQL
    4. StreamGuard dépseudonymise les résultats
    5. Qwen formate la réponse finale en français
    """
    t_start  = time.time()
    question = request.question.strip()

    # ── Étape 1 : Pseudonymise la question ───────────────────
    session_id = request.session_id or f"sess_{uuid.uuid4().hex[:8]}"
    gateway    = StreamGuardGateway(session_id=session_id)

    question_pseudo, mapping = gateway.pseudonymize(question)

    logger.info(
        f"Question pseudonymisée — "
        f"{len(mapping)} entités — "
        f"session={session_id}"
    )
    logger.info(f"Original  : {question}")
    logger.info(f"Pseudo    : {question_pseudo}")

    # ── Étape 2 : Qwen génère le SQL ─────────────────────────
    if request.model == "claude" and ANTHROPIC_KEY:
        sql        = await _ask_claude(question_pseudo)
        model_used = "claude-sonnet-4-6"
    else:
        sql        = generate_sql(question_pseudo)
        model_used = OLLAMA_MODEL

    # Nettoyage du SQL généré
    sql = sql.strip().strip("```sql").strip("```").strip()
    logger.info(f"SQL généré : {sql}")

    # ── Étape 3 : Dremio exécute ──────────────────────────────
    data_raw = dremio.query(sql) if sql else []

    # ── Étape 4 : Dépseudonymise les résultats ────────────────
    data_real = gateway.deanonymize_data(data_raw, mapping)

    logger.info(f"Données brutes : {data_raw}")
    logger.info(f"Données réelles : {data_real}")

    # ── Étape 5 : Formate la réponse en français ──────────────
    response = format_response(question, data_real)

    latency_ms = (time.time() - t_start) * 1000

    return AskResponse(
        question        = question,
        question_pseudo = question_pseudo,
        sql             = sql,
        data_raw        = data_raw,
        data_real       = data_real,
        response        = response,
        model_used      = model_used,
        latency_ms      = round(latency_ms, 2),
        entities_found  = len(mapping),
    )


async def _ask_claude(prompt: str) -> str:
    """Appelle Claude API via StreamGuard Gateway."""
    import anthropic
    client  = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    message = client.messages.create(
        model      = "claude-sonnet-4-6",
        max_tokens = 500,
        messages   = [{"role": "user", "content": prompt}],
    )
    return message.content[0].text


# ── WEBSOCKET ─────────────────────────────────────────────────
@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    """
    WebSocket temps réel.
    Le dashboard Next.js se connecte ici.
    Chaque deal pseudonymisé depuis Kafka
    est poussé instantanément.
    """
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ── ENTRY POINT ───────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

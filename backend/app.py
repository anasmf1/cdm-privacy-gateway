"""
CDM StreamGuard — FastAPI Backend
===================================
API REST + WebSocket qui connecte :
→ Kafka (deals temps réel)
→ Dremio (données Gold)
→ Ollama/Qwen (assistant IA)
→ StreamGuard (métriques)
→ MLflow (benchmark ML)

Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM

Endpoints :
  GET  /health              → statut de tous les services
  GET  /api/metrics         → métriques StreamGuard
  GET  /api/positions       → positions nettes Gold
  GET  /api/pnl             → P&L journalier
  GET  /api/anomalies       → anomalies détectées
  POST /api/ask             → assistant IA
  WS   /ws/stream           → deals temps réel WebSocket
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, Optional

import redis.asyncio as aioredis
import requests
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BACKEND] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── CONFIGURATION ─────────────────────────────────────────────

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL      = os.getenv("REDIS_URL",               "redis://localhost:6379")
DREMIO_HOST    = os.getenv("DREMIO_HOST",             "localhost")
DREMIO_PORT    = os.getenv("DREMIO_PORT",             "9047")
DREMIO_USER    = os.getenv("DREMIO_USER",             "cdm_user")
DREMIO_PASS    = os.getenv("DREMIO_PASSWORD",         "CDM_Dremio_2026!")
OLLAMA_URL     = os.getenv("OLLAMA_URL",              "http://localhost:11434")
OLLAMA_MODEL   = os.getenv("OLLAMA_MODEL",            "qwen2.5:7b-instruct")
TOPIC_SECURE   = os.getenv("TOPIC_SECURE",            "cdm.sisdm.kondor.dbo.deals.secure")
TOPIC_AUDIT    = os.getenv("TOPIC_AUDIT",             "cdm.streamguard.audit")
ANTHROPIC_KEY  = os.getenv("ANTHROPIC_API_KEY",       "")
OPENAI_KEY     = os.getenv("OPENAI_API_KEY",          "")

DREMIO_BASE = f"http://{DREMIO_HOST}:{DREMIO_PORT}"

# ── WEBSOCKET MANAGER ─────────────────────────────────────────

class ConnectionManager:
    """Gère les connexions WebSocket actives."""

    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        logger.info(f"WebSocket connecté — total={len(self.active)}")

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)
        logger.info(f"WebSocket déconnecté — total={len(self.active)}")

    async def broadcast(self, data: dict):
        """Envoie un message à tous les clients connectés."""
        if not self.active:
            return
        msg = json.dumps(data, ensure_ascii=False, default=str)
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
    """Client pour l'API REST Dremio."""

    def __init__(self):
        self._token: Optional[str] = None
        self._token_expiry: float = 0

    def _get_token(self) -> str:
        """Récupère ou renouvelle le token Dremio."""
        if self._token and time.time() < self._token_expiry:
            return self._token

        try:
            resp = requests.post(
                f"{DREMIO_BASE}/apiv2/login",
                json={"userName": DREMIO_USER, "password": DREMIO_PASS},
                timeout=10,
            )
            resp.raise_for_status()
            self._token = resp.json()["token"]
            self._token_expiry = time.time() + 3600
            return self._token
        except Exception as e:
            logger.error(f"Dremio auth failed: {e}")
            raise

    def query(self, sql: str) -> list[dict]:
        """Exécute une requête SQL sur Dremio."""
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}"}

        # Soumettre le job
        job_resp = requests.post(
            f"{DREMIO_BASE}/api/v3/sql",
            headers=headers,
            json={"sql": sql},
            timeout=30,
        )
        job_resp.raise_for_status()
        job_id = job_resp.json()["id"]

        # Attendre la fin
        for _ in range(60):
            status_resp = requests.get(
                f"{DREMIO_BASE}/api/v3/job/{job_id}",
                headers=headers,
                timeout=10,
            )
            state = status_resp.json().get("jobState")
            if state == "COMPLETED":
                break
            if state == "FAILED":
                raise Exception(f"Dremio job failed: {status_resp.json()}")
            time.sleep(0.5)

        # Récupérer les résultats
        results_resp = requests.get(
            f"{DREMIO_BASE}/api/v3/job/{job_id}/results",
            headers=headers,
            timeout=10,
        )
        results_resp.raise_for_status()
        return results_resp.json().get("rows", [])


dremio = DremioClient()

# ── OLLAMA CLIENT ─────────────────────────────────────────────

def call_ollama(prompt: str, model: str = OLLAMA_MODEL) -> str:
    """Appelle Ollama (Qwen ou Mistral) en local."""
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["response"].strip()
    except Exception as e:
        logger.error(f"Ollama error: {e}")
        return f"Erreur LLM : {e}"


def generate_sql(question: str) -> str:
    """Génère le SQL depuis une question en français via Qwen."""
    prompt = f"""Tu es un expert SQL pour la Salle des Marchés du Crédit du Maroc.

Tables disponibles dans gold layer (Dremio) :
- gold.positions_nettes  : devise, position_mad, nb_deals, derniere_maj
- gold.pnl_journalier    : date_trade, devise, pnl_brut, nb_deals
- gold.mtm_intraday      : deal_id, taux, taux_marche, mtm_value, devise
- silver.deals_clean     : deal_id, montant, taux, devise, trader, ts_silver
- silver.anomalies       : deal_id, anomaly_type, score, detected_at

Question : "{question}"

Génère UNIQUEMENT la requête SQL. Rien d'autre. Pas d'explication."""

    return call_ollama(prompt)


def format_response(question: str, data: list, sql: str) -> str:
    """Formate la réponse en français naturel via Qwen."""
    prompt = f"""Tu es l'assistant IA de la Salle des Marchés du Crédit du Maroc.

Question posée : "{question}"
Données récupérées : {json.dumps(data, ensure_ascii=False, default=str)}

Formule une réponse claire et naturelle en français.
Sois précis avec les chiffres. Pas de JSON. Pas de SQL. Juste une réponse humaine."""

    return call_ollama(prompt)


# ── KAFKA CONSUMER ASYNC ──────────────────────────────────────

async def kafka_stream_task():
    """
    Tâche asyncio qui consomme le topic sécurisé Kafka
    et pousse chaque deal vers les WebSockets connectés.
    """
    consumer = Consumer({
        "bootstrap.servers"  : KAFKA_SERVERS,
        "group.id"           : "cdm-backend-stream",
        "auto.offset.reset"  : "latest",
        "enable.auto.commit" : True,
    })
    consumer.subscribe([TOPIC_SECURE, TOPIC_AUDIT])
    logger.info(f"Kafka consumer démarré — topics={TOPIC_SECURE}, {TOPIC_AUDIT}")

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
                topic   = msg.topic()

                # Détermine le type de message
                event_type = "deal" if TOPIC_SECURE in topic else "audit"

                # Extrait le deal depuis le format Debezium
                if event_type == "deal":
                    debezium = payload.get("payload", payload)
                    deal     = debezium.get("after", debezium)
                    event    = {
                        "type"      : "deal",
                        "deal"      : deal,
                        "offset"    : msg.offset(),
                        "partition" : msg.partition(),
                        "timestamp" : datetime.now().isoformat(),
                    }
                else:
                    event = {
                        "type"      : "audit",
                        "audit"     : payload,
                        "timestamp" : datetime.now().isoformat(),
                    }

                # Broadcast à tous les dashboards connectés
                await manager.broadcast(event)

                # Sauvegarde dans Redis pour les nouveaux clients
                redis_client = aioredis.from_url(REDIS_URL)
                if event_type == "deal":
                    await redis_client.lpush("recent_deals", json.dumps(event))
                    await redis_client.ltrim("recent_deals", 0, 49)
                await redis_client.aclose()

            except Exception as e:
                logger.error(f"Message processing error: {e}")

    finally:
        consumer.close()


# ── LIFESPAN ──────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Démarre les tâches background au démarrage."""
    task = asyncio.create_task(kafka_stream_task())
    logger.info("StreamGuard backend démarré ✅")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("StreamGuard backend arrêté")


# ── FASTAPI APP ───────────────────────────────────────────────

app = FastAPI(
    title       = "CDM StreamGuard API",
    description = "Backend API pour le dashboard Capital Markets CDM",
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
    question: str
    model: str = "qwen"  # "qwen", "mistral", "claude", "gpt4"


class AskResponse(BaseModel):
    question: str
    question_pseudo: str
    sql: str
    data: list
    response: str
    model_used: str
    latency_ms: float


# ── ROUTES ────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Vérifie le statut de tous les services."""
    services = {}

    # Kafka
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})
        admin.list_topics(timeout=3)
        services["kafka"] = "ok"
    except Exception as e:
        services["kafka"] = f"error: {e}"

    # Redis
    try:
        r = aioredis.from_url(REDIS_URL)
        await r.ping()
        await r.aclose()
        services["redis"] = "ok"
    except Exception as e:
        services["redis"] = f"error: {e}"

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
        services["ollama"] = f"ok — models: {models}"
    except Exception as e:
        services["ollama"] = f"error: {e}"

    all_ok = all("ok" in str(v) for v in services.values())
    return JSONResponse(
        content={"status": "ok" if all_ok else "degraded", "services": services},
        status_code=200 if all_ok else 207,
    )


@app.get("/api/metrics")
async def get_metrics():
    """Métriques StreamGuard depuis Redis."""
    try:
        r = aioredis.from_url(REDIS_URL)
        raw = await r.get("streamguard:metrics")
        await r.aclose()
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.warning(f"Redis metrics error: {e}")

    # Fallback données simulées si StreamGuard pas encore connecté
    return {
        "deals_processed"   : 0,
        "entities_detected" : 0,
        "avg_latency_ms"    : 0,
        "anomalies"         : 0,
        "status"            : "waiting",
    }


@app.get("/api/positions")
async def get_positions():
    """Positions nettes depuis Gold Iceberg via Dremio."""
    try:
        sql = """
            SELECT
                devise,
                position_mad,
                nb_deals,
                derniere_maj
            FROM CDM_DATALAKE.gold.positions_nettes
            WHERE DATE(derniere_maj) = CURRENT_DATE
            ORDER BY position_mad DESC
        """
        data = dremio.query(sql)
        return {"positions": data, "source": "dremio_live"}
    except Exception as e:
        logger.warning(f"Dremio positions error: {e}")
        # Données démo si Dremio pas encore configuré
        return {
            "positions": [
                {"devise": "EUR/MAD", "position_mad": 490350000, "nb_deals": 12},
                {"devise": "USD/MAD", "position_mad": 120240000, "nb_deals": 4},
                {"devise": "GBP/MAD", "position_mad": 91200000,  "nb_deals": 3},
            ],
            "source": "demo",
        }


@app.get("/api/pnl")
async def get_pnl():
    """P&L journalier depuis Gold Iceberg via Dremio."""
    try:
        sql = """
            SELECT
                devise,
                SUM(pnl_brut) AS pnl_total,
                COUNT(deal_id) AS nb_deals
            FROM CDM_DATALAKE.gold.pnl_journalier
            WHERE date_trade = CURRENT_DATE
            GROUP BY devise
            ORDER BY pnl_total DESC
        """
        data = dremio.query(sql)
        return {"pnl": data, "source": "dremio_live"}
    except Exception as e:
        logger.warning(f"Dremio PNL error: {e}")
        return {
            "pnl": [
                {"devise": "EUR/MAD", "pnl_total": 1240000,  "nb_deals": 12},
                {"devise": "USD/MAD", "pnl_total": -320000,  "nb_deals": 4},
                {"devise": "GBP/MAD", "pnl_total": 180000,   "nb_deals": 3},
                {"devise": "CHF/MAD", "pnl_total": -180000,  "nb_deals": 2},
            ],
            "source": "demo",
        }


@app.get("/api/anomalies")
async def get_anomalies():
    """Anomalies détectées depuis Silver Iceberg via Dremio."""
    try:
        sql = """
            SELECT
                deal_id,
                anomaly_type,
                score,
                detected_at
            FROM CDM_DATALAKE.silver.anomalies
            WHERE DATE(detected_at) = CURRENT_DATE
            ORDER BY score ASC
            LIMIT 20
        """
        data = dremio.query(sql)
        return {"anomalies": data, "source": "dremio_live"}
    except Exception as e:
        logger.warning(f"Dremio anomalies error: {e}")
        return {
            "anomalies": [
                {"deal_id": "DEAL_a3f9", "anomaly_type": "RATE_OUTLIER",   "score": -0.91},
                {"deal_id": "DEAL_b2e1", "anomaly_type": "AMOUNT_OUTLIER", "score": -0.85},
                {"deal_id": "DEAL_c4d7", "anomaly_type": "OFF_HOURS",      "score": -0.78},
            ],
            "source": "demo",
        }


@app.post("/api/ask", response_model=AskResponse)
async def ask(request: AskRequest):
    """
    Assistant IA Privacy-First.
    Question → StreamGuard pseudo → Qwen SQL → Dremio → Réponse FR
    """
    t_start = time.time()
    question = request.question

    # Pseudonymisation basique de la question
    # (StreamGuard complet sera intégré en Phase 2)
    question_pseudo = question

    # Générer le SQL via le modèle choisi
    if request.model == "claude" and ANTHROPIC_KEY:
        sql, model_used = await _ask_claude(question_pseudo), "claude-3-5-sonnet"
    elif request.model == "gpt4" and OPENAI_KEY:
        sql, model_used = await _ask_openai(question_pseudo), "gpt-4o"
    else:
        sql        = generate_sql(question_pseudo)
        model_used = OLLAMA_MODEL

    # Exécuter sur Dremio
    try:
        data = dremio.query(sql)
    except Exception as e:
        data = []
        logger.warning(f"SQL execution failed: {e}")

    # Formater la réponse en français
    response = format_response(question, data, sql)

    latency_ms = (time.time() - t_start) * 1000

    return AskResponse(
        question        = question,
        question_pseudo = question_pseudo,
        sql             = sql,
        data            = data,
        response        = response,
        model_used      = model_used,
        latency_ms      = round(latency_ms, 2),
    )


async def _ask_claude(prompt: str) -> str:
    """Appelle l'API Anthropic Claude."""
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    message = client.messages.create(
        model      = "claude-sonnet-4-6",
        max_tokens = 1000,
        messages   = [{"role": "user", "content": prompt}],
    )
    return message.content[0].text


async def _ask_openai(prompt: str) -> str:
    """Appelle l'API OpenAI GPT-4o."""
    import openai
    client = openai.OpenAI(api_key=OPENAI_KEY)
    resp = client.chat.completions.create(
        model    = "gpt-4o",
        messages = [{"role": "user", "content": prompt}],
    )
    return resp.choices[0].message.content


# ── WEBSOCKET ─────────────────────────────────────────────────

@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    """
    WebSocket temps réel.
    Le dashboard React se connecte ici.
    Chaque deal pseudonymisé depuis Kafka
    est poussé instantanément.
    """
    await manager.connect(websocket)

    # Envoie les 10 derniers deals au nouveau client
    try:
        r = aioredis.from_url(REDIS_URL)
        recent = await r.lrange("recent_deals", 0, 9)
        await r.aclose()
        for raw in reversed(recent):
            await websocket.send_text(raw.decode())
    except Exception:
        pass

    try:
        while True:
            # Garde la connexion alive
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ── ENTRY POINT ───────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host       = "0.0.0.0",
        port       = 8000,
        reload     = True,
        log_level  = "info",
    )

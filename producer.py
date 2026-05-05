"""
producer.py — IncidentIQ REST API (Producer)
=============================================
RabbitMQ patterns demonstrated:

  1. Default (direct-to-queue)       → POST /api/tickets
  2. Pub/Sub (Fanout exchange)        → POST /api/test-pubsub
  3. Routing (Direct exchange)        → POST /api/test-routing/{severity}
  4. Topic Exchange                   → POST /api/test-topic/{routing_key}
  5. Dead Letter Queue (DLQ)          → POST /api/test-dlq
  6. Message TTL                      → POST /api/test-ttl
  7. Priority Queue                   → POST /api/test-priority
  8. ACK / NACK modes                 → POST /api/test-ack-modes
"""

import json
import os
import uuid
from datetime import datetime

import pika
import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ── Constants ────────────────────────────────────────────────────────────────

RABBITMQ_HOST      = os.environ["RABBITMQ_HOST"]
RABBITMQ_USER      = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD  = os.environ["RABBITMQ_PASSWORD"]
POSTGRES_HOST      = os.environ["POSTGRES_HOST"]
POSTGRES_DB        = os.environ.get("POSTGRES_DB", "incident_db")

# Existing queues / exchanges
QUEUE_INCIDENT     = "py_incident_queue"
EXCHANGE_BROADCAST = "incident_broadcast"     # fanout
EXCHANGE_ROUTING   = "incident_routing"       # direct

# New
EXCHANGE_TOPIC     = "incident_topic"         # topic — wildcard keys
QUEUE_TOPIC        = "incident_topic_queue"

EXCHANGE_DLQ       = "dlq_exchange"           # main exchange for DLQ demo
QUEUE_DLQ_MAIN     = "dlq_main_queue"         # messages published here
EXCHANGE_DLQ_DEAD  = "dlq_dead_exchange"      # receives failed/expired messages
QUEUE_DLQ_DEAD     = "dlq_dead_queue"         # stores dead-lettered messages

QUEUE_TTL          = "ttl_queue"              # messages expire after N seconds
QUEUE_PRIORITY     = "priority_queue"         # higher priority consumed first

# ── Application ──────────────────────────────────────────────────────────────

app = FastAPI(
    title="IncidentIQ Producer API",
    description=(
        "Demonstrates 8 RabbitMQ messaging patterns:\n\n"
        "1. Default Queue\n2. Pub/Sub (Fanout)\n3. Routing (Direct)\n"
        "4. Topic Exchange\n5. Dead Letter Queue\n6. Message TTL\n"
        "7. Priority Queue\n8. ACK / NACK Modes"
    ),
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Logging ──────────────────────────────────────────────────────────────────

def log(req_id: str, level: str, msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level:<5}] [REQ:{req_id}] {msg}", flush=True)

def log_info(req_id, msg):  log(req_id, "INFO",  msg)
def log_warn(req_id, msg):  log(req_id, "WARN",  msg)
def log_error(req_id, msg): log(req_id, "ERROR", msg)

# ── Infrastructure helpers ────────────────────────────────────────────────────

def get_db_connection():
    try:
        return psycopg2.connect(
            dbname=POSTGRES_DB,
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=POSTGRES_HOST,
        )
    except Exception as exc:
        log_error("SYS", f"PostgreSQL connection failed → {exc}")
        raise


def get_rabbitmq_channel():
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        parameters  = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=30,
        )
        connection = pika.BlockingConnection(parameters)
        channel    = connection.channel()
        channel.queue_declare(queue=QUEUE_INCIDENT, durable=True)
        return connection, channel
    except Exception as exc:
        log_error("SYS", f"RabbitMQ connection failed → {exc}")
        raise


def _publish(channel, *, exchange: str, routing_key: str, body: dict,
             priority: int = None, expiration_ms: int = None) -> None:
    """Serialise and publish a persistent message with optional priority/TTL."""
    props = pika.BasicProperties(delivery_mode=2)
    if priority      is not None: props.priority   = priority
    if expiration_ms is not None: props.expiration = str(expiration_ms)
    channel.basic_publish(
        exchange=exchange, routing_key=routing_key,
        body=json.dumps(body), properties=props,
    )


def _log_to_db(req_id: str, pattern: str, exchange: str,
               routing_key: str, outcome: str) -> None:
    """Persist a pattern event via the log_pattern_event stored procedure."""
    try:
        conn = get_db_connection()
        cur  = conn.cursor()
        cur.execute(
            "CALL log_pattern_event(%s::VARCHAR, %s::VARCHAR, %s::VARCHAR, %s::VARCHAR, %s::VARCHAR);",
            (req_id, pattern, exchange, routing_key, outcome),
        )
        conn.commit(); cur.close(); conn.close()
        log_info(req_id, f"DB event logged → pattern={pattern} outcome={outcome}")
    except Exception as exc:
        log_warn(req_id, f"DB log skipped (non-fatal) | {exc}")

# ── Pattern 1 — Default Queue ─────────────────────────────────────────────────

@app.post("/api/tickets", summary="[Pattern 1] Default queue — direct-to-queue")
async def create_ticket(
    email: str       = Query(..., description="Requester email"),
    description: str = Query(..., description="Issue description"),
):
    """
    **Pattern 1 — Default Exchange (direct-to-queue)**

    Publishes to the default exchange with `routing_key='py_incident_queue'`.
    No exchange topology needed — RabbitMQ routes directly to the named queue.

        Producer → (default exchange) → py_incident_queue → Consumer Pod
    """
    req_id = str(uuid.uuid4())[:8].upper()
    ticket = {"request_id": req_id, "email": email, "description": description}
    log_info(req_id, f"Pattern=DEFAULT | exchange=(default) | queue={QUEUE_INCIDENT} | email={email}")
    try:
        conn, channel = get_rabbitmq_channel()
        _publish(channel, exchange="", routing_key=QUEUE_INCIDENT, body=ticket)
        conn.close()
        log_info(req_id, f"✔ Published → (default exchange) → queue={QUEUE_INCIDENT}")
        _log_to_db(req_id, "default", "(default)", QUEUE_INCIDENT, "published")
        return {"status": "queued", "trace_id": req_id, "queue": QUEUE_INCIDENT}
    except Exception as exc:
        log_error(req_id, f"Publish failed | {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

# ── Pattern 2 — Pub/Sub (Fanout) ─────────────────────────────────────────────

@app.post("/api/test-pubsub", summary="[Pattern 2] Pub/Sub — Fanout exchange")
async def simulate_pubsub(description: str = Query(default="Emergency Broadcast")):
    """
    **Pattern 2 — Pub/Sub (Fanout exchange)**

    Every queue bound to `incident_broadcast` gets a copy.
    The routing key is completely ignored — fanout is pure broadcast.

    Each consumer pod declares its own exclusive queue bound to this exchange,
    so one published message is delivered to ALL consumer pods simultaneously.

        Producer → incident_broadcast (fanout) → fanout_queue_pod-1 → consumer-1
                                              → fanout_queue_pod-2 → consumer-2
                                              → fanout_queue_pod-3 → consumer-3
    """
    req_id = "BCAST-" + str(uuid.uuid4())[:6].upper()
    ticket = {"request_id": req_id, "email": "BROADCAST@all-systems", "description": description}
    log_info(req_id, f"Pattern=FANOUT | exchange={EXCHANGE_BROADCAST} | routing_key=(ignored — fanout)")
    try:
        conn, channel = get_rabbitmq_channel()
        channel.exchange_declare(exchange=EXCHANGE_BROADCAST, exchange_type="fanout", durable=True)
        _publish(channel, exchange=EXCHANGE_BROADCAST, routing_key="", body=ticket)
        conn.close()
        log_info(req_id, f"✔ Fanout published → all queues bound to {EXCHANGE_BROADCAST} receive a copy")
        _log_to_db(req_id, "fanout", EXCHANGE_BROADCAST, "(empty)", "published")
        return {"status": "published", "pattern": "Pub/Sub (Fanout)",
                "exchange": EXCHANGE_BROADCAST, "routing_key": "(empty)", "trace_id": req_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

# ── Pattern 3 — Routing (Direct) ─────────────────────────────────────────────

@app.post("/api/test-routing/{severity}", summary="[Pattern 3] Routing — Direct exchange by severity")
async def simulate_routing(severity: str, description: str = Query(default="Routing Test")):
    """
    **Pattern 3 — Routing (Direct exchange)**

    Only queues bound with a matching key receive the message.

    Consumer bindings: `critical` ✓  `high` ✓  `low` ✗ (dropped)

        Producer → incident_routing (direct) --[severity]--> matched queues only
    """
    req_id      = "ROUTE-" + str(uuid.uuid4())[:6].upper()
    routing_key = severity.lower()
    ticket      = {"request_id": req_id, "email": f"{routing_key}@routing.internal",
                   "description": description, "severity": routing_key}
    bound_keys  = ["critical", "high"]
    will_route  = routing_key in bound_keys
    log_info(req_id, (f"Pattern=DIRECT | exchange={EXCHANGE_ROUTING} | "
                      f"routing_key={routing_key!r} | will_route={will_route}"))
    try:
        conn, channel = get_rabbitmq_channel()
        channel.exchange_declare(exchange=EXCHANGE_ROUTING, exchange_type="direct", durable=True)
        _publish(channel, exchange=EXCHANGE_ROUTING, routing_key=routing_key, body=ticket)
        conn.close()
        outcome = "routed" if will_route else "dropped"
        log_info(req_id, f"✔ Direct published → key={routing_key!r} | {outcome}")
        _log_to_db(req_id, "direct", EXCHANGE_ROUTING, routing_key, outcome)
        return {"status": "published", "pattern": "Routing (Direct)",
                "exchange": EXCHANGE_ROUTING, "routing_key": routing_key,
                "will_route": will_route, "bound_keys": bound_keys, "trace_id": req_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

# ── Pattern 4 — Topic Exchange ────────────────────────────────────────────────

@app.post("/api/test-topic/{routing_key:path}", summary="[Pattern 4] Topic — wildcard routing keys")
async def simulate_topic(
    routing_key: str,
    description: str = Query(default="Topic routing test"),
):
    """
    **Pattern 4 — Topic Exchange**

    Routing keys use dot-notation. Two wildcards:
    - `*` matches exactly ONE word
    - `#` matches zero or more words

    Try these routing keys:
    - `incident.critical.network`  → matches `incident.#` and `*.critical.*` and `incident.*.network`
    - `incident.low.hardware`      → matches `incident.#` only
    - `alert.critical.database`    → matches `*.critical.*` only
    - `random.key`                 → matches nothing — message dropped

    Consumer bindings on `incident_topic_queue`:
    - `incident.#`           all incidents
    - `*.critical.*`         anything critical
    - `incident.*.network`   all network incidents

        Producer → incident_topic (topic) --[wildcard key]--> matched queues
    """
    req_id  = "TOPIC-" + str(uuid.uuid4())[:6].upper()
    ticket  = {"request_id": req_id, "email": "topic@test.internal",
               "description": description, "routing_key": routing_key}
    parts   = routing_key.split(".")

    bindings = {
        "incident.#":          routing_key.startswith("incident.") or routing_key == "incident",
        "*.critical.*":        len(parts) == 3 and parts[1] == "critical",
        "incident.*.network":  len(parts) == 3 and parts[0] == "incident" and parts[2] == "network",
    }
    matched = [k for k, v in bindings.items() if v]

    log_info(req_id, (
        f"Pattern=TOPIC | exchange={EXCHANGE_TOPIC} | routing_key={routing_key!r} | "
        f"matched_bindings={matched if matched else ['NONE — will be dropped']}"
    ))

    try:
        conn, channel = get_rabbitmq_channel()
        channel.exchange_declare(exchange=EXCHANGE_TOPIC, exchange_type="topic", durable=True)
        channel.queue_declare(queue=QUEUE_TOPIC, durable=True)
        for binding in bindings:
            channel.queue_bind(exchange=EXCHANGE_TOPIC, queue=QUEUE_TOPIC, routing_key=binding)
        _publish(channel, exchange=EXCHANGE_TOPIC, routing_key=routing_key, body=ticket)
        conn.close()

        outcome = f"matched:{','.join(matched)}" if matched else "dropped"
        log_info(req_id, f"✔ Topic published → key={routing_key!r} | matched {len(matched)} binding(s): {matched}")
        _log_to_db(req_id, "topic", EXCHANGE_TOPIC, routing_key, outcome)

        return {
            "status":           "published",
            "pattern":          "Topic Exchange",
            "exchange":         EXCHANGE_TOPIC,
            "routing_key":      routing_key,
            "queue":            QUEUE_TOPIC,
            "all_bindings":     list(bindings.keys()),
            "matched_bindings": matched,
            "will_be_consumed": len(matched) > 0,
            "trace_id":         req_id,
        }
    except Exception as exc:
        log_error(req_id, f"Topic publish failed | {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

# ── Pattern 5 — Dead Letter Queue ────────────────────────────────────────────

@app.post("/api/test-dlq", summary="[Pattern 5] Dead Letter Queue — failed messages rerouted")
async def simulate_dlq(
    force_fail:  bool = Query(default=True,  description="NACK the message so it lands in DLQ"),
    description: str  = Query(default="DLQ test message"),
):
    """
    **Pattern 5 — Dead Letter Queue (DLQ)**

    When a message is rejected (NACK requeue=False), RabbitMQ automatically
    reroutes it to a Dead Letter Exchange instead of dropping it silently.

    Topology:
    ```
    dlq_exchange → dlq_main_queue  →  [NACK]  →  dlq_dead_exchange → dlq_dead_queue
                                    [normal]  →  ACK, processed, done
    ```

    - `force_fail=true`  → consumer NACKs → message lands in DLQ  ✗
    - `force_fail=false` → consumer ACKs  → message processed normally ✓

    After sending with force_fail=true, call `GET /api/test-dlq/inspect`
    to see the dead-lettered message.
    """
    req_id = "DLQ-" + str(uuid.uuid4())[:6].upper()
    ticket = {"request_id": req_id, "email": "dlq@test.internal",
              "description": description, "force_fail": force_fail}

    log_info(req_id, (
        f"Pattern=DLQ | force_fail={force_fail} | "
        f"main={QUEUE_DLQ_MAIN} | dead={QUEUE_DLQ_DEAD} | "
        f"fate={'→ NACK → DLQ' if force_fail else '→ ACK → processed'}"
    ))

    try:
        conn, channel = get_rabbitmq_channel()

        channel.exchange_declare(exchange=EXCHANGE_DLQ_DEAD, exchange_type="direct", durable=True)
        channel.queue_declare(queue=QUEUE_DLQ_DEAD, durable=True)
        channel.queue_bind(exchange=EXCHANGE_DLQ_DEAD, queue=QUEUE_DLQ_DEAD, routing_key=QUEUE_DLQ_DEAD)

        channel.queue_declare(
            queue=QUEUE_DLQ_MAIN, durable=True,
            arguments={
                "x-dead-letter-exchange":    EXCHANGE_DLQ_DEAD,
                "x-dead-letter-routing-key": QUEUE_DLQ_DEAD,
            },
        )

        channel.exchange_declare(exchange=EXCHANGE_DLQ, exchange_type="direct", durable=True)
        channel.queue_bind(exchange=EXCHANGE_DLQ, queue=QUEUE_DLQ_MAIN, routing_key=QUEUE_DLQ_MAIN)

        _publish(channel, exchange=EXCHANGE_DLQ, routing_key=QUEUE_DLQ_MAIN, body=ticket)
        conn.close()

        fate = "NACK'd → routed to DLQ" if force_fail else "ACK'd → processed normally"
        log_info(req_id, f"✔ DLQ message published → expected fate: {fate}")
        _log_to_db(req_id, "dlq", EXCHANGE_DLQ, QUEUE_DLQ_MAIN, "force_fail" if force_fail else "normal")

        return {
            "status":        "published",
            "pattern":       "Dead Letter Queue",
            "main_queue":    QUEUE_DLQ_MAIN,
            "dead_queue":    QUEUE_DLQ_DEAD,
            "force_fail":    force_fail,
            "expected_fate": fate,
            "trace_id":      req_id,
            "next_step":     "GET /api/test-dlq/inspect to see dead-lettered messages",
        }
    except Exception as exc:
        log_error(req_id, f"DLQ publish failed | {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/test-dlq/inspect", summary="[Pattern 5] Peek at messages in the Dead Letter Queue")
async def inspect_dlq():
    """Peeks at up to 10 messages in `dlq_dead_queue` without permanently consuming them."""
    req_id = "DLQ-PEEK"
    try:
        conn, channel = get_rabbitmq_channel()
        messages = []
        for _ in range(10):
            method, props, body = channel.basic_get(queue=QUEUE_DLQ_DEAD, auto_ack=False)
            if method is None:
                break
            messages.append({
                "delivery_tag": method.delivery_tag,
                "routing_key":  method.routing_key,
                "redelivered":  method.redelivered,
                "body":         json.loads(body),
            })
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        conn.close()
        log_info(req_id, f"DLQ inspect → {len(messages)} message(s) in {QUEUE_DLQ_DEAD}")
        return {"dead_queue": QUEUE_DLQ_DEAD, "message_count": len(messages), "messages": messages}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

# ── Pattern 6 — Message TTL ───────────────────────────────────────────────────

@app.post("/api/test-ttl", summary="[Pattern 6] TTL — messages expire if not consumed in time")
async def simulate_ttl(
    ttl_seconds: int = Query(default=8, ge=1, le=60, description="Expire after N seconds"),
    description: str = Query(default="TTL test message"),
):
    """
    **Pattern 6 — Message TTL (Time-To-Live)**

    A message published with a TTL expires if no consumer picks it up
    in time. Expired messages are routed to the Dead Letter Queue
    so you can inspect them.

    How to observe:
    1. Scale consumers to 0: `kubectl scale deployment/consumer-deployment --replicas=0`
    2. Call this endpoint with `ttl_seconds=8`
    3. Wait 8 seconds
    4. Scale consumers back: `kubectl scale deployment/consumer-deployment --replicas=3`
    5. Call `GET /api/test-dlq/inspect` — the expired message is there

        Producer → ttl_queue (TTL=Ns) → [expires] → dlq_dead_exchange → dlq_dead_queue
    """
    req_id = "TTL-" + str(uuid.uuid4())[:6].upper()
    ttl_ms = ttl_seconds * 1000
    ticket = {"request_id": req_id, "email": "ttl@test.internal",
              "description": description, "ttl_seconds": ttl_seconds}

    log_info(req_id, (
        f"Pattern=TTL | queue={QUEUE_TTL} | ttl={ttl_seconds}s ({ttl_ms}ms) | "
        f"expires if not consumed → routed to {QUEUE_DLQ_DEAD}"
    ))

    try:
        conn, channel = get_rabbitmq_channel()

        channel.exchange_declare(exchange=EXCHANGE_DLQ_DEAD, exchange_type="direct", durable=True)
        channel.queue_declare(queue=QUEUE_DLQ_DEAD, durable=True)
        channel.queue_bind(exchange=EXCHANGE_DLQ_DEAD, queue=QUEUE_DLQ_DEAD, routing_key=QUEUE_DLQ_DEAD)

        channel.queue_declare(
            queue=QUEUE_TTL, durable=True,
            arguments={
                "x-dead-letter-exchange":    EXCHANGE_DLQ_DEAD,
                "x-dead-letter-routing-key": QUEUE_DLQ_DEAD,
            },
        )

        _publish(channel, exchange="", routing_key=QUEUE_TTL, body=ticket, expiration_ms=ttl_ms)
        conn.close()

        log_info(req_id, (
            f"✔ TTL message published → queue={QUEUE_TTL} | "
            f"expires in {ttl_seconds}s | on expiry → {QUEUE_DLQ_DEAD}"
        ))
        _log_to_db(req_id, "ttl", "(default)", QUEUE_TTL, f"ttl={ttl_seconds}s")

        return {
            "status":      "published",
            "pattern":     "Message TTL",
            "queue":       QUEUE_TTL,
            "ttl_seconds": ttl_seconds,
            "on_expiry":   f"→ {QUEUE_DLQ_DEAD}",
            "trace_id":    req_id,
            "next_step":   f"Wait {ttl_seconds}s then GET /api/test-dlq/inspect",
        }
    except Exception as exc:
        log_error(req_id, f"TTL publish failed | {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

# ── Pattern 7 — Priority Queue ────────────────────────────────────────────────

@app.post("/api/test-priority", summary="[Pattern 7] Priority queue — higher priority processed first")
async def simulate_priority(
    priority:    int = Query(default=5, ge=1, le=10, description="1=lowest, 10=highest"),
    description: str = Query(default="Priority test message"),
):
    """
    **Pattern 7 — Priority Queue**

    Messages with higher priority are delivered to consumers first,
    even if lower-priority messages arrived earlier.

    How to observe the ordering:
    1. Scale consumers to 0: `kubectl scale deployment/consumer-deployment --replicas=0`
    2. Send three messages: priority=1, priority=5, priority=10 (in that order)
    3. Scale consumers back: `kubectl scale deployment/consumer-deployment --replicas=3`
    4. Watch logs — priority=10 is consumed FIRST despite arriving last

        Queue (before):  [p=1 arrived first] [p=5] [p=10 arrived last]
        Consumer order:  p=10 → p=5 → p=1
    """
    req_id = "PRIO-" + str(uuid.uuid4())[:6].upper()
    ticket = {"request_id": req_id, "email": f"priority_{priority}@test.internal",
              "description": description, "priority": priority}

    label = "CRITICAL" if priority >= 9 else "HIGH" if priority >= 7 else "MEDIUM" if priority >= 4 else "LOW"

    log_info(req_id, (
        f"Pattern=PRIORITY | queue={QUEUE_PRIORITY} | "
        f"priority={priority}/10 ({label}) | "
        f"higher priority = consumed first regardless of arrival order"
    ))

    try:
        conn, channel = get_rabbitmq_channel()
        channel.queue_declare(
            queue=QUEUE_PRIORITY, durable=True,
            arguments={"x-max-priority": 10},
        )
        _publish(channel, exchange="", routing_key=QUEUE_PRIORITY, body=ticket, priority=priority)
        conn.close()

        log_info(req_id, f"✔ Priority message published → queue={QUEUE_PRIORITY} priority={priority}/10 ({label})")
        _log_to_db(req_id, "priority", "(default)", QUEUE_PRIORITY, f"priority={priority}")

        return {
            "status":         "published",
            "pattern":        "Priority Queue",
            "queue":          QUEUE_PRIORITY,
            "priority":       priority,
            "priority_label": label,
            "trace_id":       req_id,
            "tip": (
                "Scale consumers to 0, send p=1/5/10, scale back to 3. "
                "p=10 is always consumed first."
            ),
        }
    except Exception as exc:
        log_error(req_id, f"Priority publish failed | {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

# ── Pattern 8 — ACK / NACK Modes ─────────────────────────────────────────────

@app.post("/api/test-ack-modes", summary="[Pattern 8] ACK modes — message safety vs loss")
async def simulate_ack_modes(
    mode: str = Query(
        default="manual_ack",
        description="manual_ack | manual_nack_requeue | manual_nack_drop | auto_ack_risk",
    ),
    description: str = Query(default="ACK mode test"),
):
    """
    **Pattern 8 — Acknowledgement Modes**

    Controls what happens to a message after the consumer receives it.

    | Mode                   | Behaviour                                               |
    |------------------------|---------------------------------------------------------|
    | `manual_ack`           | Consumer ACKs → message safely removed from queue       |
    | `manual_nack_requeue`  | Consumer NACKs → message goes BACK to queue (retry)     |
    | `manual_nack_drop`     | Consumer NACKs requeue=False → message sent to DLQ      |
    | `auto_ack_risk`        | RabbitMQ removes on delivery → crash = **DATA LOSS**    |

    The `ack_mode` field is read by the consumer which behaves accordingly,
    so you can observe the difference in RabbitMQ UI and consumer logs.
    """
    valid = ["manual_ack", "manual_nack_requeue", "manual_nack_drop", "auto_ack_risk"]
    if mode not in valid:
        raise HTTPException(status_code=400, detail=f"Invalid mode. Choose: {valid}")

    req_id = "ACK-" + str(uuid.uuid4())[:6].upper()
    ticket = {"request_id": req_id, "email": "ack@test.internal",
              "description": description, "ack_mode": mode}

    explanations = {
        "manual_ack":          "Consumer ACKs → safe, message removed after confirmed processing",
        "manual_nack_requeue": "Consumer NACKs + requeue=True → back to queue, will be retried",
        "manual_nack_drop":    "Consumer NACKs + requeue=False → sent to Dead Letter Queue",
        "auto_ack_risk":       "RabbitMQ removes on delivery — crash mid-process = message LOST forever",
    }

    log_info(req_id, (
        f"Pattern=ACK_MODE | mode={mode!r} | queue={QUEUE_INCIDENT} | "
        f"behaviour: {explanations[mode]}"
    ))

    try:
        conn, channel = get_rabbitmq_channel()
        _publish(channel, exchange="", routing_key=QUEUE_INCIDENT, body=ticket)
        conn.close()

        log_info(req_id, f"✔ ACK-mode message published → mode={mode!r}")
        _log_to_db(req_id, "ack_mode", "(default)", QUEUE_INCIDENT, mode)

        return {
            "status":    "published",
            "pattern":   "ACK Modes",
            "mode":      mode,
            "behaviour": explanations[mode],
            "queue":     QUEUE_INCIDENT,
            "trace_id":  req_id,
        }
    except Exception as exc:
        log_error(req_id, f"ACK-mode publish failed | {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

# ── Utility endpoints ─────────────────────────────────────────────────────────

@app.get("/api/tickets/{email}", summary="Fetch ticket history by email")
async def get_tickets_by_email(email: str):
    req_id = "READ-" + str(uuid.uuid4())[:6].upper()
    try:
        conn = get_db_connection(); cur = conn.cursor()
        cur.execute("SELECT * FROM get_user_ticket_history(%s::VARCHAR);", (email,))
        rows = cur.fetchall(); conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    if not rows:
        raise HTTPException(status_code=404, detail="No tickets found.")
    results = [
        {"request_id": r[0], "description": r[1], "category": r[2],
         "status": r[3], "handled_by_pod": r[4], "processing_time_sec": r[5]}
        for r in rows
    ]
    log_info(req_id, f"Returned {len(results)} tickets for {email}")
    return {"user": email, "ticket_count": len(results), "history": results}


@app.get("/api/metrics", summary="System-wide processing metrics")
async def get_metrics():
    try:
        conn = get_db_connection(); cur = conn.cursor()
        cur.execute("SELECT get_system_metrics();")
        row = cur.fetchone(); conn.close()
        data = row[0]
        if isinstance(data, str): data = json.loads(data)
        return data
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/pattern-events", summary="View all pattern events logged to PostgreSQL")
async def get_pattern_events():
    """Returns every RabbitMQ pattern event recorded by log_pattern_event()."""
    try:
        conn = get_db_connection(); cur = conn.cursor()
        cur.execute("SELECT * FROM get_pattern_events();")
        rows = cur.fetchall(); conn.close()
        results = [
            {"id": r[0], "request_id": r[1], "pattern": r[2], "exchange": r[3],
             "routing_key": r[4], "outcome": r[5], "created_at": str(r[6])}
            for r in rows
        ]
        return {"total": len(results), "events": results}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/health", summary="Health check")
async def health_check():
    status = {"api": "online", "database": "offline", "rabbitmq": "offline"}
    try:
        conn = get_db_connection(); conn.close()
        status["database"] = "online"
    except: pass
    try:
        conn, _ = get_rabbitmq_channel(); conn.close()
        status["rabbitmq"] = "online"
    except: pass
    if "offline" in status.values():
        raise HTTPException(status_code=503, detail=status)
    return status


@app.post("/api/admin/simulate-burst", summary="Flood the default queue with synthetic tickets")
async def simulate_burst(count: int = Query(default=20, ge=1, le=500)):
    batch_id = "BURST-" + str(uuid.uuid4())[:6].upper()
    try:
        conn, channel = get_rabbitmq_channel()
        for i in range(count):
            ticket = {"request_id": f"{batch_id}-{i:04d}",
                      "email": f"burst_{i:04d}@load-test.internal",
                      "description": f"Burst ticket #{i+1} of {count}"}
            _publish(channel, exchange="", routing_key=QUEUE_INCIDENT, body=ticket)
            if (i + 1) % 10 == 0 or (i + 1) == count:
                log_info(batch_id, f"Published {i+1}/{count} → queue={QUEUE_INCIDENT}")
        conn.close()
        return {"status": "success", "batch_id": batch_id, "messages_published": count}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.delete("/api/admin/clear-database", summary="Truncate all data (admin)")
async def clear_database():
    try:
        conn = get_db_connection(); cur = conn.cursor()
        cur.execute("CALL admin_clear_database();")
        conn.commit(); conn.close()
        log_warn("ADMIN", "⚠️  All data cleared")
        return {"status": "success", "message": "All tickets and events deleted."}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
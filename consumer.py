import json
import os
import random
import sys
import time
from datetime import datetime

import pika
from pika import channel
from pika import channel
import psycopg2

# ── Constants ────────────────────────────────────────────────────────────────

RABBITMQ_HOST      = os.environ["RABBITMQ_HOST"]
RABBITMQ_USER      = os.environ["RABBITMQ_USER"]
RABBITMQ_PASSWORD  = os.environ["RABBITMQ_PASSWORD"]
POSTGRES_HOST      = os.environ["POSTGRES_HOST"]
POSTGRES_DB        = os.environ.get("POSTGRES_DB", "incident_db")

# Queues this consumer listens on
QUEUE_INCIDENT     = "py_incident_queue"
EXCHANGE_BROADCAST = "incident_broadcast"
EXCHANGE_ROUTING   = "incident_routing"
ROUTING_KEYS       = ["critical", "high"]

EXCHANGE_TOPIC     = "incident_topic"
QUEUE_TOPIC        = "incident_topic_queue"
TOPIC_BINDINGS     = ["incident.#", "*.critical.*", "incident.*.network"]

EXCHANGE_DLQ       = "dlq_exchange"
QUEUE_DLQ_MAIN     = "dlq_main_queue"
EXCHANGE_DLQ_DEAD  = "dlq_dead_exchange"
QUEUE_DLQ_DEAD     = "dlq_dead_queue"

QUEUE_TTL          = "ttl_queue"
QUEUE_PRIORITY     = "priority_queue"

CATEGORIES = ["Hardware Issue", "Network Outage", "Software Bug", "Access Request", "Billing Inquiry"]

# ── Logging ──────────────────────────────────────────────────────────────────

def log(req_id: str, pod_id: str, level: str, msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level:<5}] [POD:{pod_id}] [REQ:{req_id}] {msg}", flush=True)

def log_info(req_id, pod_id, msg):  log(req_id, pod_id, "INFO",  msg)
def log_warn(req_id, pod_id, msg):  log(req_id, pod_id, "WARN",  msg)
def log_error(req_id, pod_id, msg): log(req_id, pod_id, "ERROR", msg)

# ── Helpers ───────────────────────────────────────────────────────────────────

def detect_routing_path(method) -> str:
    """Return a human-readable description of how this message arrived."""
    exchange = method.exchange or ""
    rk       = method.routing_key or "(none)"

    if exchange == "":
        return f"DEFAULT exchange → routing_key={rk!r} → queue={QUEUE_INCIDENT}"
    if exchange == EXCHANGE_BROADCAST:
        return f"FANOUT exchange={EXCHANGE_BROADCAST!r} → routing_key=(ignored) → queue={QUEUE_INCIDENT}"
    if exchange == EXCHANGE_ROUTING:
        return f"DIRECT exchange={EXCHANGE_ROUTING!r} → routing_key={rk!r} → queue={QUEUE_INCIDENT}"
    if exchange == EXCHANGE_TOPIC:
        return f"TOPIC exchange={EXCHANGE_TOPIC!r} → routing_key={rk!r} → queue={QUEUE_TOPIC}"
    if exchange == EXCHANGE_DLQ:
        return f"DLQ exchange={EXCHANGE_DLQ!r} → routing_key={rk!r} → queue={QUEUE_DLQ_MAIN}"
    return f"exchange={exchange!r} routing_key={rk!r}"


def persist_ticket(req_id: str, pod_id: str, email: str, description: str,
                   category: str, duration: float) -> bool:
    """Call the create_incident_ticket stored procedure. Returns True on success."""
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=POSTGRES_HOST,
        )
        cur = conn.cursor()
        cur.execute(
            "CALL create_incident_ticket(%s::VARCHAR, %s::TEXT, %s::VARCHAR, %s::VARCHAR, %s::FLOAT);",
            (email, description, category, pod_id, duration),
        )
        conn.commit(); cur.close(); conn.close()
        log_info(req_id, pod_id, f"✔ Persisted → category={category!r} duration={duration}s")
        return True
    except Exception as exc:
        log_error(req_id, pod_id, f"✘ PostgreSQL error | {exc}")
        return False

# ── Queue callbacks ───────────────────────────────────────────────────────────

def process_ticket(ch, method, properties, body) -> None:
    """
    Main callback for py_incident_queue.
    Handles patterns 1, 2, 3, and 8 (ACK modes).
    """
    pod_id = os.getenv("HOSTNAME", "unknown-pod")

    try:
        ticket = json.loads(body)
    except json.JSONDecodeError as exc:
        log_error("PARSE-ERR", pod_id, f"Invalid JSON — dropping | {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    req_id      = ticket.get("request_id", "UNKNOWN")
    email       = ticket.get("email",       "unknown@test.com")
    description = ticket.get("description", "")
    severity    = ticket.get("severity",    "n/a")
    ack_mode    = ticket.get("ack_mode",    "manual_ack")   # Pattern 8 field

    routing_path = detect_routing_path(method)

    log_info(req_id, pod_id, (
        f"▶ RECEIVED | path: {routing_path} | "
        f"delivery_tag={method.delivery_tag} | redelivered={method.redelivered} | "
        f"email={email!r} | severity={severity!r} | ack_mode={ack_mode!r} | "
        f"body={len(body)}B"
    ))

    # ── Pattern 8: ACK mode simulation ───────────────────────────────────────
    if ack_mode == "manual_nack_requeue":
        log_warn(req_id, pod_id,
                 f"⚠ ACK_MODE=manual_nack_requeue → NACK + requeue=True | "
                 f"message goes BACK to queue and will be retried")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    if ack_mode == "manual_nack_drop":
        log_warn(req_id, pod_id,
                 f"⚠ ACK_MODE=manual_nack_drop → NACK + requeue=False | "
                 f"message sent to Dead Letter Queue (not retried)")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    if ack_mode == "auto_ack_risk":
        log_warn(req_id, pod_id,
                 f"⚠ ACK_MODE=auto_ack_risk → RabbitMQ already removed this message on delivery | "
                 f"if we crash NOW the message is LOST forever (simulating risk)")
        # Fall through to normal processing — RabbitMQ already ACK'd on delivery

    # ── Normal processing (manual_ack + auto_ack_risk fall-through) ───────────
    start_ts   = time.time()
    sleep_secs = random.uniform(1.5, 3.5)
    log_info(req_id, pod_id, f"⚙ Processing | simulated_work={sleep_secs:.2f}s ...")
    time.sleep(sleep_secs)

    category = random.choice(CATEGORIES)
    duration = round(time.time() - start_ts, 3)
    log_info(req_id, pod_id, f"✔ Categorised → {category!r} | duration={duration}s")

    ok = persist_ticket(req_id, pod_id, email, description, category, duration)

    if not ok:
        log_error(req_id, pod_id, "✘ Persist failed → NACK + requeue=True")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    if ack_mode != "auto_ack_risk":
        ch.basic_ack(delivery_tag=method.delivery_tag)

    log_info(req_id, pod_id, (
        f"✔ ACK sent | ack_mode={ack_mode!r} | "
        f"category={category!r} | total_time={duration}s | path: {routing_path}"
    ))


def process_topic(ch, method, properties, body) -> None:
    """
    Callback for incident_topic_queue (Pattern 4 — Topic exchange).
    Logs which wildcard binding matched the routing key.
    """
    pod_id = os.getenv("HOSTNAME", "unknown-pod")
    try:
        ticket = json.loads(body)
    except json.JSONDecodeError:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    req_id      = ticket.get("request_id", "UNKNOWN")
    routing_key = ticket.get("routing_key", method.routing_key or "")
    parts       = routing_key.split(".")

    # Determine which binding(s) matched
    matched = []
    if routing_key.startswith("incident.") or routing_key == "incident":
        matched.append("incident.#")
    if len(parts) == 3 and parts[1] == "critical":
        matched.append("*.critical.*")
    if len(parts) == 3 and parts[0] == "incident" and parts[2] == "network":
        matched.append("incident.*.network")

    log_info(req_id, pod_id, (
        f"▶ TOPIC RECEIVED | "
        f"exchange={EXCHANGE_TOPIC!r} | routing_key={routing_key!r} | "
        f"matched_bindings={matched} | "
        f"delivery_tag={method.delivery_tag} | redelivered={method.redelivered}"
    ))

    time.sleep(random.uniform(0.5, 1.5))
    category = random.choice(CATEGORIES)
    duration = round(random.uniform(0.5, 1.5), 3)

    ok = persist_ticket(req_id, pod_id,
                        ticket.get("email", "topic@test.internal"),
                        ticket.get("description", ""),
                        f"[TOPIC:{','.join(matched)}] {category}", duration)

    if ok:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log_info(req_id, pod_id,
                 f"✔ ACK | topic key={routing_key!r} matched {len(matched)} binding(s): {matched}")
    else:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def process_dlq_main(ch, method, properties, body) -> None:
    """
    Callback for dlq_main_queue (Pattern 5 — DLQ).
    If force_fail=True: NACK without requeue → message goes to dlq_dead_exchange.
    Otherwise: normal ACK processing.
    """
    pod_id = os.getenv("HOSTNAME", "unknown-pod")
    try:
        ticket = json.loads(body)
    except json.JSONDecodeError:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    req_id     = ticket.get("request_id", "UNKNOWN")
    force_fail = ticket.get("force_fail", False)

    log_info(req_id, pod_id, (
        f"▶ DLQ-MAIN RECEIVED | queue={QUEUE_DLQ_MAIN} | "
        f"force_fail={force_fail} | delivery_tag={method.delivery_tag} | "
        f"redelivered={method.redelivered}"
    ))

    if force_fail:
        log_warn(req_id, pod_id, (
            f"⚠ DLQ force_fail=True → NACK + requeue=False | "
            f"RabbitMQ will route this to {EXCHANGE_DLQ_DEAD} → {QUEUE_DLQ_DEAD}"
        ))
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    # Normal processing
    time.sleep(random.uniform(0.5, 1.0))
    category = random.choice(CATEGORIES)
    duration = round(random.uniform(0.5, 1.0), 3)
    ok = persist_ticket(req_id, pod_id,
                        ticket.get("email", "dlq@test.internal"),
                        ticket.get("description", ""),
                        f"[DLQ-NORMAL] {category}", duration)
    if ok:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log_info(req_id, pod_id, f"✔ DLQ-MAIN ACK | force_fail=False → processed normally")
    else:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def process_priority(ch, method, properties, body) -> None:
    """
    Callback for priority_queue (Pattern 7).
    Logs the message priority so you can verify high-priority messages
    are consumed before lower-priority ones.
    """
    pod_id = os.getenv("HOSTNAME", "unknown-pod")
    try:
        ticket = json.loads(body)
    except json.JSONDecodeError:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    req_id   = ticket.get("request_id", "UNKNOWN")
    priority = ticket.get("priority", properties.priority or 0)
    label    = "CRITICAL" if priority >= 9 else "HIGH" if priority >= 7 else "MEDIUM" if priority >= 4 else "LOW"

    log_info(req_id, pod_id, (
        f"▶ PRIORITY RECEIVED | queue={QUEUE_PRIORITY} | "
        f"priority={priority}/10 ({label}) | "
        f"delivery_tag={method.delivery_tag} | redelivered={method.redelivered} | "
        f"note: higher priority messages arrived AFTER this but were consumed FIRST"
    ))

    time.sleep(random.uniform(0.5, 1.5))
    category = random.choice(CATEGORIES)
    duration = round(random.uniform(0.5, 1.5), 3)
    ok = persist_ticket(req_id, pod_id,
                        ticket.get("email", "priority@test.internal"),
                        ticket.get("description", ""),
                        f"[PRIORITY:{priority}/{label}] {category}", duration)
    if ok:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log_info(req_id, pod_id, f"✔ PRIORITY ACK | priority={priority}/10 ({label})")
    else:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

# ── Startup & bindings ────────────────────────────────────────────────────────

def main() -> None:
    pod_id = os.getenv("HOSTNAME", "unknown-pod")
    log_info("SYS-BOOT", pod_id,
             f"Connecting to RabbitMQ → host={RABBITMQ_HOST} user={RABBITMQ_USER}")

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters  = pika.ConnectionParameters(
        host=RABBITMQ_HOST, credentials=credentials,
        heartbeat=60, blocked_connection_timeout=30, connection_attempts=1,
    )
    connection = pika.BlockingConnection(parameters)
    channel    = connection.channel()
    log_info("SYS-BOOT", pod_id, f"✔ Connected → host={RABBITMQ_HOST}")

    # ── 1. Default / py_incident_queue ───────────────────────────────────────────
    channel.queue_declare(queue=QUEUE_INCIDENT, durable=True)
    log_info("SYS-BOOT", pod_id, f"Queue → {QUEUE_INCIDENT!r} (default exchange + fanout + direct)")

    # ── 2. Fanout binding ─────────────────────────────────────────────────────
    channel.exchange_declare(exchange=EXCHANGE_BROADCAST, exchange_type="fanout", durable=True)
    pod_id         = os.getenv("HOSTNAME", "unknown-pod")
    fanout_queue   = f"fanout_queue_{pod_id}"   # unique per pod

    channel.queue_declare(queue=fanout_queue, durable=False, exclusive=True)
    channel.queue_bind(exchange=EXCHANGE_BROADCAST, queue=fanout_queue)
    channel.basic_consume(queue=fanout_queue, on_message_callback=process_ticket, auto_ack=False)    
    log_info("SYS-BOOT", pod_id,
             f"Binding → {EXCHANGE_BROADCAST!r} (fanout) → {QUEUE_INCIDENT!r} | key=(all)")

    # ── 3. Direct routing binding ─────────────────────────────────────────────
    channel.exchange_declare(exchange=EXCHANGE_ROUTING, exchange_type="direct", durable=True)
    for key in ROUTING_KEYS:
        channel.queue_bind(exchange=EXCHANGE_ROUTING, queue=QUEUE_INCIDENT, routing_key=key)
        log_info("SYS-BOOT", pod_id,
                 f"Binding → {EXCHANGE_ROUTING!r} (direct) → {QUEUE_INCIDENT!r} | key={key!r}")

    # ── 4. Topic exchange binding ─────────────────────────────────────────────
    channel.exchange_declare(exchange=EXCHANGE_TOPIC, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_TOPIC, durable=True)
    for binding in TOPIC_BINDINGS:
        channel.queue_bind(exchange=EXCHANGE_TOPIC, queue=QUEUE_TOPIC, routing_key=binding)
        log_info("SYS-BOOT", pod_id,
                 f"Binding → {EXCHANGE_TOPIC!r} (topic) → {QUEUE_TOPIC!r} | key={binding!r}")

    # ── 5. DLQ setup ──────────────────────────────────────────────────────────
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
    log_info("SYS-BOOT", pod_id,
             f"DLQ → {QUEUE_DLQ_MAIN!r} (NACK → {EXCHANGE_DLQ_DEAD!r} → {QUEUE_DLQ_DEAD!r})")

    # ── 6. TTL queue ──────────────────────────────────────────────────────────
    channel.queue_declare(
        queue=QUEUE_TTL, durable=True,
        arguments={
            "x-dead-letter-exchange":    EXCHANGE_DLQ_DEAD,
            "x-dead-letter-routing-key": QUEUE_DLQ_DEAD,
        },
    )
    log_info("SYS-BOOT", pod_id,
             f"TTL queue → {QUEUE_TTL!r} (expired → {QUEUE_DLQ_DEAD!r})")

    # ── 7. Priority queue ─────────────────────────────────────────────────────
    channel.queue_declare(
        queue=QUEUE_PRIORITY, durable=True,
        arguments={"x-max-priority": 10},
    )
    log_info("SYS-BOOT", pod_id,
             f"Priority queue → {QUEUE_PRIORITY!r} (x-max-priority=10)")

    # ── QoS ───────────────────────────────────────────────────────────────────
    channel.basic_qos(prefetch_count=1)
    log_info("SYS-BOOT", pod_id, "QoS → prefetch_count=1 (fair dispatch)")

    # ── Start consuming all queues ────────────────────────────────────────────
    channel.basic_consume(queue=QUEUE_INCIDENT,  on_message_callback=process_ticket,   auto_ack=False)
    channel.basic_consume(queue=QUEUE_TOPIC,     on_message_callback=process_topic,    auto_ack=False)
    channel.basic_consume(queue=QUEUE_DLQ_MAIN,  on_message_callback=process_dlq_main, auto_ack=False)
    channel.basic_consume(queue=QUEUE_PRIORITY,  on_message_callback=process_priority, auto_ack=False)
    # TTL queue: consumer intentionally NOT registered here — messages expire and land in DLQ
    # to demonstrate the TTL pattern. Uncomment below to consume TTL messages normally:
    # channel.basic_consume(queue=QUEUE_TTL, on_message_callback=process_ticket, auto_ack=False)

    log_info("SYS-BOOT", pod_id, (
        f"✔ Consumer READY — listening on 4 queues: "
        f"{QUEUE_INCIDENT} | {QUEUE_TOPIC} | {QUEUE_DLQ_MAIN} | {QUEUE_PRIORITY}"
    ))

    channel.start_consuming()


if __name__ == "__main__":
    pod_id      = os.getenv("HOSTNAME", "unknown-pod")
    max_retries = 10
    retry_delay = 5

    for attempt in range(1, max_retries + 1):
        try:
            log_info("SYS-BOOT", pod_id,
                     f"Startup attempt {attempt}/{max_retries} — connecting to {RABBITMQ_HOST} ...")
            main()
            break
        except Exception as exc:
            log_warn("SYS-BOOT", pod_id, f"Attempt {attempt}/{max_retries} failed → {exc}")
            if attempt == max_retries:
                log_error("SYS-BOOT", pod_id, f"All {max_retries} attempts exhausted — exiting.")
                sys.exit(1)
            wait = retry_delay * attempt
            log_info("SYS-BOOT", pod_id, f"Retrying in {wait}s ...")
            time.sleep(wait)

"""RSU application: wires rsu_core.py to MQTT, SQS, SNS and HTTP.

Environment variables are listed in docs/CONTRACTS.md section 10. Extra, for local tests only:
  START_BROKER=0     do not start Mosquitto (use an external broker on localhost:1883)
  UPLINK_DRY_RUN=1   log events instead of sending them to SQS (no AWS needed)
"""
import asyncio
import json
import logging
import os
import subprocess
import threading
import time
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

import boto3
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, Request

import rsu_core as core

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rsu")

RSU_ID = os.environ.get("RSU_ID", "rsu-1")
EVENTS_QUEUE = os.environ.get("EVENTS_QUEUE", "v2x-events")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "v2x-alerts")
AGG_WINDOW_S = float(os.environ.get("AGG_WINDOW_S", "5"))
BUFFER_MAX = int(os.environ.get("BUFFER_MAX", "5000"))
LOCAL_BRAKE_DECEL = float(os.environ.get("LOCAL_BRAKE_DECEL", "4"))
DRY_RUN = os.environ.get("UPLINK_DRY_RUN", "0") == "1"
SCHEMA_DIR = Path(os.environ.get("SCHEMA_DIR", "schema"))
MQTT_HOST, MQTT_PORT = "localhost", 1883   # the broker lives inside this container

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"rsu-app-{RSU_ID}")


def publish_alert(alert):
    """Send an alert to the vehicles connected to this RSU."""
    try:
        mqtt_client.publish(f"v2x/{RSU_ID}/alerts", json.dumps(alert), qos=1)
        log.info("ALERT -> vehicles: %s %s (origin=%s)", alert["alert_type"], alert["alert_id"], alert["origin"])
    except Exception:
        log.exception("could not publish alert")


rsu = core.Rsu(
    RSU_ID,
    json.loads((SCHEMA_DIR / "v2x-event.schema.json").read_text()),
    json.loads((SCHEMA_DIR / "v2x-alert.schema.json").read_text()),
    publish_alert,
    buffer=core.UplinkBuffer(BUFFER_MAX),
    local_brake_decel=LOCAL_BRAKE_DECEL,
    agg_window_s=AGG_WINDOW_S,
)


# ---- MQTT: vehicles -> RSU --------------------------------------------------------------------
def on_connect(client, userdata, flags, reason_code, properties):
    log.info("MQTT connected (%s), subscribing to v2x/%s/up", reason_code, RSU_ID)
    client.subscribe(f"v2x/{RSU_ID}/up", qos=1)


def on_message(client, userdata, msg):
    rsu.on_message(msg.payload)


mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


# ---- uplink: RSU -> SQS, with buffering and retry (CONTRACTS.md section 7) -----------------------
class SqsSender:
    def __init__(self):
        self.sqs = boto3.client("sqs")
        self.url = None

    def send(self, events):
        """Send one batch (max 10). Returns the events that failed; raises if the whole call failed."""
        if self.url is None:
            self.url = self.sqs.get_queue_url(QueueName=EVENTS_QUEUE)["QueueUrl"]
        entries = [{"Id": str(i), "MessageBody": json.dumps(ev)} for i, ev in enumerate(events)]
        resp = self.sqs.send_message_batch(QueueUrl=self.url, Entries=entries)
        return [events[int(f["Id"])] for f in resp.get("Failed", [])]


class DryRunSender:
    def send(self, events):
        for ev in events:
            log.info("FORWARD %s", json.dumps(ev))
        return []


def uplink_loop():
    sender = DryRunSender() if DRY_RUN else SqsSender()
    backoff = 0.5
    while True:
        if not rsu.uplink_up:                      # uplink switched off: events stay in the buffer
            time.sleep(0.2)
            continue
        batch = rsu.buffer.take(10)
        if not batch:
            time.sleep(0.05)
            continue
        for ev in batch:
            ev["trace"]["rsu_forward_time"] = core.now_iso()
        try:
            failed = sender.send(batch)
        except Exception as e:
            log.warning("uplink failed (%s), %d events kept, retry in %.1fs", e, len(batch), backoff)
            rsu.buffer.put_back(batch)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        rsu.record_forwarded(len(batch) - len(failed))
        if failed:
            rsu.buffer.put_back(failed)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        else:
            backoff = 0.5


def density_loop():
    while True:
        time.sleep(AGG_WINDOW_S)
        rsu.flush_density()


# ---- alert path: SNS -> RSU (HTTP) --------------------------------------------------------------
def imds_public_ip():
    """Public IP of the EC2 instance (IMDSv2); None if not on EC2 or not reachable from the container."""
    try:
        req = urllib.request.Request("http://169.254.169.254/latest/api/token", method="PUT",
                                     headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
        token = urllib.request.urlopen(req, timeout=2).read().decode()
        req = urllib.request.Request("http://169.254.169.254/latest/meta-data/public-ipv4",
                                     headers={"X-aws-ec2-metadata-token": token})
        return urllib.request.urlopen(req, timeout=2).read().decode()
    except Exception:
        return None


def subscribe_to_alerts():
    host = os.environ.get("PUBLIC_HOST") or imds_public_ip()
    port = os.environ.get("PUBLIC_HTTP_PORT", "8080")
    if not host or not port:
        log.warning("PUBLIC_HOST / PUBLIC_HTTP_PORT not set: NOT subscribing to SNS")
        return
    time.sleep(3)                                  # let the HTTP server start: SNS calls /sns right away
    sns = boto3.client("sns")
    for _ in range(30):
        try:
            arn = sns.create_topic(Name=ALERTS_TOPIC)["TopicArn"]   # idempotent: returns the existing topic
            sns.subscribe(TopicArn=arn, Protocol="http", Endpoint=f"http://{host}:{port}/sns")
            log.info("subscribed http://%s:%s/sns to %s", host, port, arn)
            return
        except Exception as e:
            log.warning("SNS subscribe failed (%s), retrying in 10s", e)
            time.sleep(10)


# ---- process start-up / shutdown ---------------------------------------------------------------
@asynccontextmanager
async def lifespan(app):
    broker = None
    if os.environ.get("START_BROKER", "1") == "1":
        broker = subprocess.Popen(["mosquitto", "-c", "/etc/mosquitto/mosquitto.conf"])
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=5)
    mqtt_client.connect_async(MQTT_HOST, MQTT_PORT)
    mqtt_client.loop_start()
    for target in (uplink_loop, density_loop, subscribe_to_alerts):
        threading.Thread(target=target, daemon=True).start()
    log.info("RSU %s started (dry_run=%s)", RSU_ID, DRY_RUN)
    yield
    mqtt_client.loop_stop()
    if broker:
        broker.terminate()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/stats")
def stats():
    return rsu.snapshot()


@app.post("/admin/uplink")
def admin_uplink(up: bool):
    """Simulate losing (up=false) or restoring (up=true) the connection to AWS."""
    rsu.uplink_up = up
    log.warning("uplink switched %s", "ON" if up else "OFF")
    return {"uplink_up": up}


@app.post("/sns")
async def sns_endpoint(request: Request):
    """SNS delivers with Content-Type text/plain, so parse the raw body ourselves."""
    try:
        body = json.loads(await request.body())
    except ValueError:
        raise HTTPException(400, "body is not JSON")
    if not str(body.get("TopicArn", "")).endswith(":" + ALERTS_TOPIC):
        raise HTTPException(403, "unexpected topic")
    kind = request.headers.get("x-amz-sns-message-type")
    if kind == "SubscriptionConfirmation":
        url = urlparse(body.get("SubscribeURL", ""))
        if url.scheme != "https" or not (url.hostname or "").endswith(".amazonaws.com"):
            raise HTTPException(400, "bad SubscribeURL")
        await asyncio.to_thread(urllib.request.urlopen, body["SubscribeURL"], None, 5)
        log.info("SNS subscription confirmed")
    elif kind == "Notification":
        try:
            alert = json.loads(body["Message"])
        except (KeyError, ValueError):
            raise HTTPException(400, "Message is not JSON")
        rsu.on_cloud_alert(alert)
    return {"ok": True}

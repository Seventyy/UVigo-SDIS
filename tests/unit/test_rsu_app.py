"""HTTP-level tests of the RSU application (services/rsu/rsu_app.py). No MQTT broker, no AWS."""
import json
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
pytest.importorskip("boto3")
pytest.importorskip("paho.mqtt.client")

ROOT = Path(__file__).resolve().parents[2]
os.environ.update(RSU_ID="rsu-1", UPLINK_DRY_RUN="1", SCHEMA_DIR=str(ROOT / "schema"))
sys.path.insert(0, str(ROOT / "services" / "rsu"))

from fastapi.testclient import TestClient  # noqa: E402
import rsu_app  # noqa: E402

TOPIC = "arn:aws:sns:us-east-1:123456789012:v2x-alerts"
ALERT = {"alert_id": "brake:abc", "alert_type": "sudden_braking", "origin": "cloud", "severity": 1,
         "created_time": "2026-11-14T10:00:00.000Z", "location": {"segment_id": "S02", "rsu_id": "rsu-1"},
         "cause_event_ids": ["abc"], "schema_version": "1.0", "message": "Sudden braking"}


@pytest.fixture
def client(monkeypatch):
    published = []
    monkeypatch.setattr(rsu_app.mqtt_client, "publish", lambda topic, payload, qos=0: published.append((topic, payload)))
    c = TestClient(rsu_app.app)       # no "with": start-up threads / broker are not started
    c.published = published
    return c


def notification(alert):
    return json.dumps({"Type": "Notification", "TopicArn": TOPIC, "Message": json.dumps(alert)})


def test_health_and_stats(client):
    assert client.get("/health").json() == {"ok": True}
    stats = client.get("/stats").json()
    for key in ("received", "invalid", "duplicate", "forwarded", "buffered", "dropped_overflow", "local_alerts",
                "cloud_alerts_received", "cloud_alerts_relayed", "alerts_filtered", "uplink_up"):
        assert key in stats


def test_admin_uplink_switch(client):
    assert client.post("/admin/uplink?up=false").json() == {"uplink_up": False}
    assert client.get("/stats").json()["uplink_up"] is False
    assert client.post("/admin/uplink?up=true").json() == {"uplink_up": True}


def test_sns_notification_is_relayed_over_mqtt(client):
    r = client.post("/sns", content=notification(ALERT),
                    headers={"x-amz-sns-message-type": "Notification", "content-type": "text/plain"})
    assert r.status_code == 200
    topic, payload = client.published[-1]
    assert topic == "v2x/rsu-1/alerts" and json.loads(payload)["alert_id"] == "brake:abc"


def test_sns_subscription_confirmation_visits_subscribe_url(client, monkeypatch):
    visited = []
    monkeypatch.setattr(rsu_app.urllib.request, "urlopen", lambda url, data=None, timeout=None: visited.append(url))
    url = "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&Token=t"
    body = json.dumps({"Type": "SubscriptionConfirmation", "TopicArn": TOPIC, "SubscribeURL": url})
    r = client.post("/sns", content=body, headers={"x-amz-sns-message-type": "SubscriptionConfirmation"})
    assert r.status_code == 200 and visited == [url]


def test_sns_rejects_foreign_topic_and_foreign_subscribe_url(client):
    bad_topic = json.dumps({"TopicArn": "arn:aws:sns:us-east-1:1:other", "Message": "{}"})
    assert client.post("/sns", content=bad_topic, headers={"x-amz-sns-message-type": "Notification"}).status_code == 403
    evil = json.dumps({"TopicArn": TOPIC, "SubscribeURL": "https://evil.example.com/confirm"})
    assert client.post("/sns", content=evil, headers={"x-amz-sns-message-type": "SubscriptionConfirmation"}).status_code == 400


def test_subscribes_to_sns_with_default_port_8080(monkeypatch):
    calls = {}

    class FakeSns:
        def create_topic(self, Name):
            return {"TopicArn": TOPIC}

        def subscribe(self, **kw):
            calls.update(kw)

    monkeypatch.setenv("PUBLIC_HOST", "3.4.5.6")
    monkeypatch.delenv("PUBLIC_HTTP_PORT", raising=False)
    monkeypatch.setattr(rsu_app.time, "sleep", lambda s: None)
    monkeypatch.setattr(rsu_app.boto3, "client", lambda name: FakeSns())
    rsu_app.subscribe_to_alerts()
    assert calls == {"TopicArn": TOPIC, "Protocol": "http", "Endpoint": "http://3.4.5.6:8080/sns"}

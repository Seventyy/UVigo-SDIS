import json
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services" / "v2x-service"))
from alerts import build_alert, publish_alert  # noqa: E402

REQUIRED = {"alert_id", "alert_type", "origin", "severity", "created_time", "location",
            "cause_event_ids", "message", "schema_version"}


def brake(decel, event_id="e1"):
    return {"event_id": event_id, "source_id": "veh-0001", "event_type": "brake",
            "location": {"segment_id": "S03", "rsu_id": "rsu-1"},
            "payload": {"decel_mps2": decel, "speed_before_mps": 28.0}}


class FakeSns:
    def __init__(self, failures=0):
        self.failures = failures
        self.calls = []

    def publish(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) <= self.failures:
            raise RuntimeError("throttled")
        return {"MessageId": "m"}


def test_hard_braking_builds_a_complete_alert():
    a = build_alert(brake(5.2), "2026-11-14T10:00:00.000Z")
    assert REQUIRED <= set(a)
    assert a["origin"] == "cloud" and a["alert_type"] == "sudden_braking" and a["severity"] == 1
    assert a["cause_event_ids"] == ["e1"] and a["location"] == {"segment_id": "S03", "rsu_id": "rsu-1"}


def test_severity_and_threshold():
    assert build_alert(brake(8.0), "t")["severity"] == 2
    assert build_alert(brake(4.0), "t") is not None
    assert build_alert(brake(3.9), "t") is None


def test_works_with_decimal_payloads_like_the_service_parses_them():
    a = build_alert(brake(Decimal("6.5")), "t")
    json.dumps(a)                       # must stay serialisable for SNS
    assert a["severity"] == 1


def test_other_event_types_do_not_alert():
    ev = {"event_id": "p", "source_id": "veh-0001", "event_type": "position",
          "location": {"segment_id": "S00", "rsu_id": "rsu-1"}, "payload": {"speed_mps": 30}}
    assert build_alert(ev, "t") is None


def test_same_event_gives_the_same_alert_id():
    assert build_alert(brake(5), "t1")["alert_id"] == build_alert(brake(5), "t2")["alert_id"]


def test_publish_sends_the_alert_as_json_with_attributes():
    sns, alert = FakeSns(), build_alert(brake(5), "t")
    assert publish_alert(sns, "arn:topic", alert) is True
    call = sns.calls[0]
    assert call["TopicArn"] == "arn:topic" and json.loads(call["Message"]) == alert
    assert call["MessageAttributes"]["origin"]["StringValue"] == "cloud"


def test_publish_retries_with_backoff_then_succeeds():
    sns, waits = FakeSns(failures=2), []
    assert publish_alert(sns, "arn", build_alert(brake(5), "t"), sleep=waits.append) is True
    assert len(sns.calls) == 3 and waits == [0.2, 0.4]


def test_publish_gives_up_without_raising():
    sns, waits = FakeSns(failures=99), []
    assert publish_alert(sns, "arn", build_alert(brake(5), "t"), retries=3, sleep=waits.append) is False
    assert len(sns.calls) == 4 and len(waits) == 3

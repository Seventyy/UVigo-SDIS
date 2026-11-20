"""Unit tests for the RSU logic (services/rsu/rsu_core.py). No network, no AWS."""
import json
import sys
import uuid
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "rsu"))
import rsu_core as core  # noqa: E402

EVENT_SCHEMA = json.loads((ROOT / "schema" / "v2x-event.schema.json").read_text())
ALERT_SCHEMA = json.loads((ROOT / "schema" / "v2x-alert.schema.json").read_text())


def make_event(event_type="position", payload=None, segment="S03", rsu="rsu-1", source="veh-0001"):
    return {"event_id": str(uuid.uuid4()), "source_id": source, "source_type": "vehicle",
            "event_type": event_type, "event_time": "2026-11-14T10:00:00.000Z",
            "location": {"segment_id": segment, "rsu_id": rsu},
            "payload": payload if payload is not None else {"speed_mps": 20.0},
            "schema_version": "1.0", "seq": 0}


def make_cloud_alert(rsu="rsu-1", severity=1):
    return {"alert_id": f"brake:{uuid.uuid4()}", "alert_type": "sudden_braking", "origin": "cloud",
            "severity": severity, "created_time": "2026-11-14T10:00:00.000Z",
            "location": {"segment_id": "S03", "rsu_id": rsu}, "cause_event_ids": ["x"],
            "schema_version": "1.0", "message": "Sudden braking"}


@pytest.fixture
def alerts():
    return []


@pytest.fixture
def rsu(alerts):
    return core.Rsu("rsu-1", EVENT_SCHEMA, ALERT_SCHEMA, alerts.append)


def send(rsu, ev):
    rsu.on_message(json.dumps(ev))


def drain(rsu):
    return rsu.buffer.take(10_000)


# ---- validation and deduplication ---------------------------------------------------------------
@pytest.mark.parametrize("bad", ["not json", "[]", "{}", '{"event_id": "x"}'])
def test_invalid_messages_are_dropped(rsu, bad):
    rsu.on_message(bad)
    assert rsu.stats["invalid"] == 1 and len(rsu.buffer) == 0


def test_duplicate_event_ids_are_dropped(rsu):
    ev = make_event("hazard", {"kind": "debris"})
    send(rsu, ev)
    send(rsu, ev)
    assert rsu.stats["duplicate"] == 1 and len(rsu.buffer) == 1


def test_trace_receive_time_is_added(rsu):
    send(rsu, make_event("hazard", {"kind": "debris"}))
    assert "rsu_received_time" in drain(rsu)[0]["trace"]


# ---- edge processing ----------------------------------------------------------------------------
def test_positions_are_aggregated_not_forwarded(rsu):
    send(rsu, make_event("position", {"speed_mps": 10.0}, source="veh-0001"))
    send(rsu, make_event("position", {"speed_mps": 20.0}, source="veh-0002"))
    send(rsu, make_event("position", {"speed_mps": 30.0}, source="veh-0001"))   # last speed of veh-0001 counts
    assert len(rsu.buffer) == 0
    out = rsu.flush_density()
    assert len(out) == 1
    ev = out[0]
    jsonschema.validate(ev, EVENT_SCHEMA)
    assert ev["source_id"] == "rsu-1" and ev["source_type"] == "rsu" and ev["event_type"] == "density"
    assert ev["payload"]["vehicle_count"] == 2 and ev["payload"]["mean_speed_mps"] == 25.0
    assert rsu.flush_density() == []                    # window was reset


def test_density_seq_increases(rsu):
    send(rsu, make_event("position"))
    first = rsu.flush_density()[0]
    send(rsu, make_event("position"))
    assert rsu.flush_density()[0]["seq"] == first["seq"] + 1


def test_critical_events_go_first(rsu):
    send(rsu, make_event("position"))
    rsu.flush_density()                                 # routine event is queued first
    send(rsu, make_event("hazard", {"kind": "debris"}))
    assert [e["event_type"] for e in drain(rsu)] == ["hazard", "density"]


# ---- local alerts ---------------------------------------------------------------------------------
def test_crash_raises_local_alert(rsu, alerts):
    ev = make_event("crash", {})
    send(rsu, ev)
    assert len(alerts) == 1 and rsu.stats["local_alerts"] == 1
    jsonschema.validate(alerts[0], ALERT_SCHEMA)
    assert alerts[0]["alert_type"] == "local_crash" and alerts[0]["severity"] == 3
    assert alerts[0]["alert_id"] == f"local-crash:{ev['event_id']}"
    assert len(rsu.buffer) == 1                         # the event is still forwarded to the cloud


@pytest.mark.parametrize("decel,severity", [(4.0, 1), (6.9, 1), (7.0, 2), (9.0, 2)])
def test_hard_brake_raises_local_alert(rsu, alerts, decel, severity):
    send(rsu, make_event("brake", {"decel_mps2": decel, "speed_before_mps": 25.0}))
    jsonschema.validate(alerts[0], ALERT_SCHEMA)
    assert alerts[0]["alert_type"] == "local_hard_braking" and alerts[0]["severity"] == severity


def test_soft_brake_has_no_local_alert_but_is_forwarded(rsu, alerts):
    send(rsu, make_event("brake", {"decel_mps2": 2.0, "speed_before_mps": 15.0}))
    assert alerts == [] and len(rsu.buffer) == 1


# ---- cloud alerts: dedup + relevance rule -----------------------------------------------------------
def test_cloud_alert_for_own_zone_is_relayed(rsu, alerts):
    rsu.on_cloud_alert(make_cloud_alert(rsu="rsu-1"))
    assert len(alerts) == 1 and rsu.stats["cloud_alerts_relayed"] == 1


def test_cloud_alert_for_other_zone_low_severity_is_filtered(rsu, alerts):
    rsu.on_cloud_alert(make_cloud_alert(rsu="rsu-2", severity=1))
    assert alerts == [] and rsu.stats["alerts_filtered"] == 1


def test_cloud_alert_for_other_zone_high_severity_is_relayed(rsu, alerts):
    rsu.on_cloud_alert(make_cloud_alert(rsu="rsu-2", severity=2))
    assert len(alerts) == 1


def test_duplicate_and_invalid_cloud_alerts_are_not_relayed(rsu, alerts):
    a = make_cloud_alert()
    rsu.on_cloud_alert(a)
    rsu.on_cloud_alert(a)
    rsu.on_cloud_alert({"alert_id": "broken"})
    assert len(alerts) == 1 and rsu.stats["duplicate_alerts"] == 1 and rsu.stats["invalid_alerts"] == 1


# ---- buffer policy (CONTRACTS.md section 7) ---------------------------------------------------------
def test_overflow_drops_oldest_routine_first():
    buf = core.UplinkBuffer(max_size=3)
    r = [make_event("density", {}) for _ in range(3)]
    c = make_event("hazard", {})
    for ev in r:
        buf.put(ev)
    buf.put(c)
    assert buf.dropped_overflow == 1
    assert buf.take(10) == [c, r[1], r[2]]


def test_overflow_drops_oldest_critical_when_no_routine_left():
    buf = core.UplinkBuffer(max_size=2)
    c = [make_event("hazard", {}) for _ in range(3)]
    for ev in c:
        buf.put(ev)
    assert buf.dropped_overflow == 1 and buf.take(10) == [c[1], c[2]]


def test_put_back_keeps_order():
    buf = core.UplinkBuffer()
    evs = [make_event("hazard", {}) for _ in range(4)]
    for ev in evs:
        buf.put(ev)
    batch = buf.take(2)
    buf.put_back(batch)
    assert buf.take(10) == evs

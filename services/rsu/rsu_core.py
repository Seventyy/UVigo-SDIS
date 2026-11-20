"""RSU core logic: validation, dedup, edge processing, local alerts, uplink buffer, cloud-alert relay.

No network code here (MQTT/SQS/SNS/HTTP live in rsu_app.py), so everything is unit-testable.
The behaviour implemented here is specified in docs/CONTRACTS.md (sections 4, 5 and 7).
"""
import json
import threading
import uuid
from collections import OrderedDict, deque
from datetime import datetime, timezone

import jsonschema

CRITICAL = {"brake", "crash", "hazard", "emergency", "collision_risk"}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def is_critical(ev):
    return ev["event_type"] in CRITICAL


class UplinkBuffer:
    """Two FIFO queues (critical / routine) with one total size limit (CONTRACTS.md section 7)."""

    def __init__(self, max_size=5000):
        self.max_size = max_size
        self.critical, self.routine = deque(), deque()
        self.dropped_overflow = 0
        self._lock = threading.Lock()

    def __len__(self):
        return len(self.critical) + len(self.routine)

    def put(self, ev):
        with self._lock:
            (self.critical if is_critical(ev) else self.routine).append(ev)
            while len(self.critical) + len(self.routine) > self.max_size:
                (self.routine or self.critical).popleft()  # drop oldest routine first, then oldest critical
                self.dropped_overflow += 1

    def take(self, n):
        """Remove and return up to n events, critical first, oldest first inside a class."""
        with self._lock:
            out = []
            for q in (self.critical, self.routine):
                while q and len(out) < n:
                    out.append(q.popleft())
            return out

    def put_back(self, events):
        """Return events that could not be sent to the FRONT of their queues, keeping their order."""
        with self._lock:
            for ev in reversed(events):
                (self.critical if is_critical(ev) else self.routine).appendleft(ev)


class Rsu:
    def __init__(self, rsu_id, event_schema, alert_schema, publish_alert, buffer=None, now=now_iso,
                 local_brake_decel=4.0, agg_window_s=5.0, dedupe_size=10000):
        self.rsu_id = rsu_id
        self.publish_alert = publish_alert          # callable(alert_dict): sends the alert to vehicles over MQTT
        self.buffer = buffer if buffer is not None else UplinkBuffer()
        self.now = now
        self.local_brake_decel = local_brake_decel
        self.agg_window_s = agg_window_s
        self.dedupe_size = dedupe_size
        self.uplink_up = True
        self.stats = dict.fromkeys(
            ["received", "invalid", "duplicate", "forwarded", "local_alerts", "cloud_alerts_received",
             "cloud_alerts_relayed", "alerts_filtered", "invalid_alerts", "duplicate_alerts"], 0)
        self._event_validator = jsonschema.Draft202012Validator(event_schema)
        self._alert_validator = jsonschema.Draft202012Validator(alert_schema)
        self._seen_events, self._seen_alerts = OrderedDict(), OrderedDict()
        self._positions = {}    # segment_id -> {vehicle id: last speed} for the current density window
        self._seq = 0
        self._lock = threading.Lock()

    # ---- helpers ---------------------------------------------------------------------------------
    def _seen(self, cache, key):
        if key in cache:
            return True
        cache[key] = None
        if len(cache) > self.dedupe_size:
            cache.popitem(last=False)
        return False

    def record_forwarded(self, n):
        with self._lock:
            self.stats["forwarded"] += n

    def snapshot(self):
        with self._lock:
            s = dict(self.stats)
            s.update(rsu_id=self.rsu_id, buffered=len(self.buffer),
                     dropped_overflow=self.buffer.dropped_overflow, uplink_up=self.uplink_up)
            return s

    # ---- event path: vehicle -> RSU -> SQS -------------------------------------------------------
    def on_message(self, payload):
        """Handle one MQTT message (bytes or str) received on v2x/<rsu_id>/up."""
        with self._lock:
            self.stats["received"] += 1
            try:
                ev = json.loads(payload)
                self._event_validator.validate(ev)
            except (ValueError, jsonschema.ValidationError):
                self.stats["invalid"] += 1
                return
            if self._seen(self._seen_events, ev["event_id"]):
                self.stats["duplicate"] += 1
                return
            ev.setdefault("trace", {})["rsu_received_time"] = self.now()
            if ev["event_type"] == "position":
                speed = float(ev["payload"].get("speed_mps", 0.0))
                self._positions.setdefault(ev["location"]["segment_id"], {})[ev["source_id"]] = speed
                return                                   # aggregated, not forwarded individually
            if is_critical(ev):
                self._local_alert(ev)
            self.buffer.put(ev)

    def flush_density(self):
        """Turn the positions seen in the last window into one density event per segment."""
        with self._lock:
            positions, self._positions = self._positions, {}
            out = []
            for segment_id, vehicles in positions.items():
                speeds = list(vehicles.values())
                ev = {"event_id": str(uuid.uuid4()), "source_id": self.rsu_id, "source_type": "rsu",
                      "event_type": "density", "event_time": self.now(),
                      "location": {"segment_id": segment_id, "rsu_id": self.rsu_id},
                      "payload": {"vehicle_count": len(speeds),
                                  "mean_speed_mps": round(sum(speeds) / len(speeds), 2),
                                  "window_s": self.agg_window_s},
                      "schema_version": "1.0", "seq": self._seq, "trace": {"rsu_received_time": self.now()}}
                self._seq += 1
                self.buffer.put(ev)
                out.append(ev)
            return out

    # ---- local alerts: RSU -> vehicles, no cloud -------------------------------------------------
    def _local_alert(self, ev):
        et, p = ev["event_type"], ev["payload"]
        if et == "crash":
            atype, severity, prefix, what = "local_crash", 3, "local-crash", "Crash"
        elif et == "brake" and p.get("decel_mps2", 0) >= self.local_brake_decel:
            atype, prefix, what = "local_hard_braking", "local-brake", "Hard braking"
            severity = 2 if p["decel_mps2"] >= 7 else 1
        else:
            return
        seg = ev["location"]["segment_id"]
        alert = {"alert_id": f"{prefix}:{ev['event_id']}", "alert_type": atype, "origin": "rsu",
                 "severity": severity, "created_time": self.now(),
                 "location": {"segment_id": seg, "rsu_id": ev["location"]["rsu_id"]},
                 "cause_event_ids": [ev["event_id"]], "schema_version": "1.0",
                 "message": f"{what} by {ev['source_id']} on {seg} (local)"}
        self.stats["local_alerts"] += 1
        self.publish_alert(alert)

    # ---- alert path: SNS -> RSU -> vehicles ------------------------------------------------------
    def on_cloud_alert(self, alert):
        """Handle one alert received from SNS: validate, dedup, apply the relevance rule, relay."""
        with self._lock:
            self.stats["cloud_alerts_received"] += 1
            try:
                self._alert_validator.validate(alert)
            except jsonschema.ValidationError:
                self.stats["invalid_alerts"] += 1
                return
            if self._seen(self._seen_alerts, alert["alert_id"]):
                self.stats["duplicate_alerts"] += 1
                return
            if alert["location"]["rsu_id"] == self.rsu_id or alert["severity"] >= 2:
                self.stats["cloud_alerts_relayed"] += 1
                self.publish_alert(alert)
            else:
                self.stats["alerts_filtered"] += 1

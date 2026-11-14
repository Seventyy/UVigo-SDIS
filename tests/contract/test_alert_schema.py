"""Contract test: the alert schema accepts the alerts the system really produces and rejects broken ones."""
import copy
import json
from pathlib import Path

import jsonschema
import pytest

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = json.loads((ROOT / "schema" / "v2x-alert.schema.json").read_text())

# Built exactly as services/v2x-service/app.py does today (cloud alert for a hard brake).
CLOUD_ALERT = {
    "alert_id": "brake:3f2b8c1e-0000-4000-8000-000000000001", "alert_type": "sudden_braking", "origin": "cloud",
    "severity": 2, "created_time": "2026-11-14T10:15:30.123Z",
    "location": {"segment_id": "S03", "rsu_id": "rsu-1"},
    "cause_event_ids": ["3f2b8c1e-0000-4000-8000-000000000001"], "schema_version": "1.0",
    "message": "Sudden braking by veh-0001 on S03",
}

RSU_BRAKE_ALERT = {**CLOUD_ALERT, "alert_id": "local-brake:3f2b8c1e-0000-4000-8000-000000000001",
                   "alert_type": "local_hard_braking", "origin": "rsu", "severity": 1,
                   "message": "Hard braking by veh-0001 on S03 (local)"}

RSU_CRASH_ALERT = {**CLOUD_ALERT, "alert_id": "local-crash:3f2b8c1e-0000-4000-8000-000000000002",
                   "alert_type": "local_crash", "origin": "rsu", "severity": 3,
                   "message": "Crash of veh-0002 on S03 (local)"}


def test_schema_itself_is_valid():
    jsonschema.Draft202012Validator.check_schema(SCHEMA)


@pytest.mark.parametrize("alert", [CLOUD_ALERT, RSU_BRAKE_ALERT, RSU_CRASH_ALERT])
def test_real_alerts_are_accepted(alert):
    jsonschema.validate(alert, SCHEMA)


def _without(key):
    def mutate(a):
        del a[key]
    return mutate


@pytest.mark.parametrize("mutate", [
    _without("origin"),
    _without("alert_id"),
    lambda a: a.update(origin="edge"),
    lambda a: a.update(severity=0),
    lambda a: a.update(severity=4),
    lambda a: a.update(cause_event_ids=[]),
    lambda a: a["location"].update(rsu_id="rsu-3"),
    lambda a: a.update(unexpected_field=1),
], ids=["no-origin", "no-alert-id", "bad-origin", "severity-0", "severity-4",
        "no-cause", "bad-rsu", "extra-field"])
def test_broken_alerts_are_rejected(mutate):
    alert = copy.deepcopy(CLOUD_ALERT)
    mutate(alert)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(alert, SCHEMA)

"""Guards for the two Mosquitto pitfalls that cost us time (see docs/CONTRACTS.md section 3)."""
import re
from pathlib import Path

CONF = (Path(__file__).resolve().parents[2] / "services" / "rsu" / "mosquitto.conf").read_text()


def setting(name):
    m = re.search(rf"^{name}\s+(\S+)", CONF, re.MULTILINE)
    return m.group(1) if m else None


def test_broker_accepts_remote_anonymous_clients():
    assert setting("listener") == "1883" and "0.0.0.0" in CONF
    assert setting("allow_anonymous") == "true"


def test_broker_queue_limits_survive_a_burst():
    # Mosquitto defaults (20 in flight / 1000 queued) made it drop messages during a 1800-message burst.
    assert int(setting("max_queued_messages")) >= 10000
    assert int(setting("max_inflight_messages")) >= 100

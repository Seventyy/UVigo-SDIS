import json
from pathlib import Path
from types import SimpleNamespace


from simulator import scenario
from simulator.engine import Simulator
from simulator.transport import MqttTransport

ROOT = Path(__file__).resolve().parents[2]
ACCIDENT = str(ROOT / "simulator" / "scenarios" / "accident.yaml")


class FakeInfo:
    def __init__(self, acked=True):
        self.acked = acked
        self.waited = False

    def is_published(self):
        return self.acked

    def wait_for_publish(self, timeout=None):
        self.waited = True


class FakeClient:
    """Stands in for a paho client: records what the transport does, never touches the network."""

    def __init__(self, client_id, acked=True):
        self.client_id = client_id
        self.acked = acked
        self.published = []      # (topic, payload, qos)
        self.subscribed = []
        self.infos = []
        self.connected_to = None
        self.looping = False
        self.disconnected = False
        self.on_connect = None
        self.on_message = None

    def connect(self, host, port, keepalive):
        self.connected_to = (host, port)

    def loop_start(self):
        self.looping = True

    def loop_stop(self):
        self.looping = False

    def disconnect(self):
        self.disconnected = True

    def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))

    def publish(self, topic, payload, qos=0):
        self.published.append((topic, payload, qos))
        info = FakeInfo(self.acked)
        self.infos.append(info)
        return info


def make(acked=True):
    clients = {}

    def factory(client_id):
        clients[client_id] = FakeClient(client_id, acked)
        return clients[client_id]

    t = MqttTransport({"rsu-1": ("h1", 1883), "rsu-2": ("h2", 1884)}, client_factory=factory, linger_s=0)
    return t, clients


def event(source_type="vehicle", rsu="rsu-1", event_id="e1"):
    return {"event_id": event_id, "source_id": "veh-0001", "source_type": source_type, "event_type": "position",
            "location": {"segment_id": "S00", "rsu_id": rsu}}


def test_one_connection_per_rsu_with_its_own_address():
    t, clients = make()
    assert clients["sim-rsu-1"].connected_to == ("h1", 1883)
    assert clients["sim-rsu-2"].connected_to == ("h2", 1884)
    assert all(c.looping for c in clients.values())


def test_subscribes_to_the_rsu_alert_topic_on_connect():
    t, clients = make()
    c = clients["sim-rsu-2"]
    c.on_connect(c, None, {}, 0, None)
    assert c.subscribed == [("v2x/rsu-2/alerts", 1)]


def test_events_are_routed_by_rsu_id_with_qos1():
    t, clients = make()
    t.send([event(rsu="rsu-1", event_id="a"), event(rsu="rsu-2", event_id="b")])
    (topic1, payload1, qos1), = clients["sim-rsu-1"].published
    (topic2, payload2, qos2), = clients["sim-rsu-2"].published
    assert (topic1, qos1) == ("v2x/rsu-1/up", 1) and json.loads(payload1)["event_id"] == "a"
    assert (topic2, qos2) == ("v2x/rsu-2/up", 1) and json.loads(payload2)["event_id"] == "b"
    assert t.stats["published"] == 2


def test_rsu_sourced_events_are_skipped():
    t, clients = make()
    t.send([event(source_type="rsu"), event()])
    assert t.stats["skipped_rsu_events"] == 1 and t.stats["published"] == 1
    assert len(clients["sim-rsu-1"].published) == 1


def test_unknown_rsu_is_counted_not_crashed():
    t, _ = make()
    t.send([event(rsu="rsu-9")])
    assert t.stats["unrouted"] == 1 and t.stats["published"] == 0


def test_alerts_received_are_stored_and_counted():
    t, clients = make()
    alert = {"alert_id": "x", "alert_type": "sudden_braking", "origin": "rsu", "message": "m"}
    msg = SimpleNamespace(topic="v2x/rsu-1/alerts", payload=json.dumps(alert).encode())
    clients["sim-rsu-1"].on_message(clients["sim-rsu-1"], None, msg)
    clients["sim-rsu-1"].on_message(clients["sim-rsu-1"], None, SimpleNamespace(topic="t", payload=b"not json"))
    assert t.stats["alerts_received"] == 1 and t.stats["bad_alerts"] == 1
    assert t.alerts[0]["alert"] == alert and "received_wall_time" in t.alerts[0]


def test_close_waits_for_unacknowledged_publishes_and_disconnects():
    t, clients = make(acked=False)
    t.send([event()])
    t.close()
    assert all(i.waited for i in clients["sim-rsu-1"].infos)
    assert all(c.disconnected and not c.looping for c in clients.values())


def test_acknowledged_publishes_are_not_kept_in_memory():
    t, _ = make(acked=True)
    t.send([event(event_id=str(n)) for n in range(100)])
    assert t.pending == []


def test_full_accident_run_reaches_the_right_rsus():
    t, clients = make()
    cfg = scenario.load(ACCIDENT)
    stats = Simulator(cfg, t, seed=42, realtime=False, start_wall=1_790_000_000.0).run()
    sent = {rsu: [json.loads(p) for _, p, _ in clients[f"sim-{rsu}"].published] for rsu in ("rsu-1", "rsu-2")}
    all_sent = sent["rsu-1"] + sent["rsu-2"]
    assert all_sent and all(e["source_type"] != "rsu" for e in all_sent)
    for rsu, evs in sent.items():
        assert all(e["location"]["rsu_id"] == rsu for e in evs)
    assert len(all_sent) + t.stats["skipped_rsu_events"] == stats["generated"]
    assert {"position", "brake", "crash"} <= {e["event_type"] for e in all_sent}

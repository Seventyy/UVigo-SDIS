"""Where generated events go: SQS (D1), MQTT to the RSUs (D2), stdout (dry runs), a list (tests)."""
import json
import sys
import time


class ListTransport:
    def __init__(self):
        self.events = []

    def send(self, events):
        self.events.extend(events)

    def close(self):
        pass


class StdoutTransport:
    def send(self, events):
        for ev in events:
            sys.stdout.write(json.dumps(ev, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    def close(self):
        pass


class SqsTransport:
    """Sends events to an SQS queue with SendMessageBatch (10 per call), retrying failed entries."""

    def __init__(self, queue_name="v2x-events", region="us-east-1", max_retries=3):
        import boto3  # imported here so tests and dry runs do not need AWS
        self.sqs = boto3.client("sqs", region_name=region)
        self.url = self.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        self.max_retries = max_retries
        self.stats = {"sent": 0, "failed": 0, "batches": 0}

    def send(self, events):
        for i in range(0, len(events), 10):
            self._send_batch(events[i:i + 10])

    def _send_batch(self, batch):
        entries = {str(n): json.dumps(ev, separators=(",", ":")) for n, ev in enumerate(batch)}
        for attempt in range(self.max_retries + 1):
            resp = self.sqs.send_message_batch(
                QueueUrl=self.url,
                Entries=[{"Id": k, "MessageBody": body} for k, body in entries.items()])
            self.stats["batches"] += 1
            self.stats["sent"] += len(resp.get("Successful", []))
            failed = {f["Id"] for f in resp.get("Failed", [])}
            entries = {k: v for k, v in entries.items() if k in failed}
            if not entries:
                return
            time.sleep(0.2 * 2 ** attempt)
        self.stats["failed"] += len(entries)

    def close(self):
        pass


class MqttTransport:
    """Publishes vehicle events to their RSU's MQTT broker and listens for the alerts the RSU sends back.

    One connection per RSU. Routing uses the event's location.rsu_id:
      v2x/<rsu_id>/up      simulator -> RSU   (QoS 1)
      v2x/<rsu_id>/alerts  RSU -> vehicles    (the simulator subscribes and logs them)
    Events whose source_type is "rsu" are skipped: the RSU computes its own density.

    rsus: {"rsu-1": ("host", 1883), "rsu-2": ("host", 1883)}
    client_factory: function(client_id) -> MQTT client; tests pass a fake, the default builds a paho 2.x client.
    """

    def __init__(self, rsus, client_factory=None, qos=1, linger_s=1.0):
        self.rsus = dict(rsus)
        self.qos = qos
        self.linger_s = linger_s       # time kept after the last event to receive late alerts
        self.clients = {}
        self.pending = []              # publish handles not yet acknowledged by the broker
        self.alerts = []               # every alert received, with the wall-clock reception time
        self.stats = {"published": 0, "skipped_rsu_events": 0, "unrouted": 0,
                      "alerts_received": 0, "bad_alerts": 0}
        factory = client_factory or self._paho_client
        for rsu_id, (host, port) in self.rsus.items():
            client = factory(f"sim-{rsu_id}")
            client.on_connect = self._make_on_connect(rsu_id)
            client.on_message = self._on_message
            client.connect(host, port, 60)
            client.loop_start()
            self.clients[rsu_id] = client

    @staticmethod
    def _paho_client(client_id):
        import paho.mqtt.client as mqtt  # imported here so tests and dry runs do not need paho
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)  # paho 2.x needs the API version
        client.max_inflight_messages_set(1000)
        return client

    def _make_on_connect(self, rsu_id):
        def on_connect(client, userdata, flags, reason_code, properties=None):
            # subscribing here (not once) also restores the subscription after an automatic reconnect
            client.subscribe(f"v2x/{rsu_id}/alerts", qos=self.qos)
        return on_connect

    def _on_message(self, client, userdata, msg):
        try:
            alert = json.loads(msg.payload)
        except ValueError:
            self.stats["bad_alerts"] += 1
            return
        self.stats["alerts_received"] += 1
        record = {"received_wall_time": time.time(), "topic": msg.topic, "alert": alert}
        self.alerts.append(record)
        sys.stderr.write(f"[alert] {msg.topic} {alert.get('origin')} {alert.get('alert_type')} "
                         f"{alert.get('message')}\n")

    def send(self, events):
        for ev in events:
            if ev.get("source_type") == "rsu":
                self.stats["skipped_rsu_events"] += 1
                continue
            rsu_id = ev["location"]["rsu_id"]
            client = self.clients.get(rsu_id)
            if client is None:
                self.stats["unrouted"] += 1
                continue
            info = client.publish(f"v2x/{rsu_id}/up", json.dumps(ev, separators=(",", ":")), qos=self.qos)
            self.stats["published"] += 1
            self.pending.append(info)
        self.pending = [i for i in self.pending if not i.is_published()]   # keep only unacknowledged ones

    def close(self):
        for info in self.pending:
            try:
                info.wait_for_publish(timeout=5)
            except (ValueError, RuntimeError):
                pass                   # not connected or queue full: the event stays unacknowledged
        time.sleep(self.linger_s)
        for client in self.clients.values():
            client.loop_stop()
            client.disconnect()

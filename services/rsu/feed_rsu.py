"""Feed simulator events to an RSU over MQTT and print the alerts it sends back (local testing / demo).

  python -m simulator --scenario simulator/scenarios/accident.yaml --transport stdout --fast \
      | python services/rsu/feed_rsu.py --port 1883 --rsu rsu-1
"""
import argparse
import json
import sys
import time

import paho.mqtt.client as mqtt

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="localhost")
ap.add_argument("--port", type=int, default=1883)
ap.add_argument("--rsu", default="rsu-1")
ap.add_argument("--wait", type=float, default=4.0, help="seconds to keep listening for alerts at the end")
args = ap.parse_args()

alerts = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="feed-alerts")
alerts.on_connect = lambda c, u, f, rc, p: c.subscribe(f"v2x/{args.rsu}/alerts", qos=1)
alerts.on_message = lambda c, u, m: print("ALERT FROM RSU:", m.payload.decode())
alerts.connect(args.host, args.port)
alerts.loop_start()

pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="feed-events")
pub.connect(args.host, args.port)
pub.loop_start()

sent = 0
last = None
for line in sys.stdin:
    if not line.startswith('{"event_id"'):            # skip the simulator's run summary
        continue
    if json.loads(line)["source_type"] == "rsu":      # the RSU produces its own density events
        continue
    last = pub.publish(f"v2x/{args.rsu}/up", line.strip(), qos=1)
    sent += 1
if last is not None:
    last.wait_for_publish(timeout=60)                 # do not exit before every message really left
print(f"sent {sent} events to {args.host}:{args.port} ({args.rsu}), listening {args.wait}s for alerts ...")
time.sleep(args.wait)

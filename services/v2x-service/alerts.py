"""Alert logic kept free of AWS calls so it can be tested: building the alert and publishing it to SNS."""
import json
import sys
import time

BRAKE_THRESHOLD = 4        # m/s^2, same threshold the simulator uses for a brake event
SEVERE_THRESHOLD = 7


def build_alert(ev, created_time):
    """Alert for a hard-braking event, or None when the event does not trigger one."""
    p = ev["payload"]
    if ev["event_type"] != "brake" or p["decel_mps2"] < BRAKE_THRESHOLD:
        return None
    seg = ev["location"]["segment_id"]
    return {
        "alert_id": f"brake:{ev['event_id']}",       # derived from the event, so a duplicate never makes a second alert
        "alert_type": "sudden_braking",
        "origin": "cloud",
        "severity": 2 if p["decel_mps2"] >= SEVERE_THRESHOLD else 1,
        "created_time": created_time,
        "location": {"segment_id": seg, "rsu_id": ev["location"]["rsu_id"]},
        "cause_event_ids": [ev["event_id"]],
        "schema_version": "1.0",
        "message": f"Sudden braking by {ev['source_id']} on {seg}",
    }


def publish_alert(sns, topic_arn, alert, retries=3, sleep=time.sleep):
    """Publish the alert to SNS, retrying with exponential backoff. Returns True on success.

    It never raises: the alert is already stored in DynamoDB, and raising would make SQS redeliver the
    event, which would then be treated as a duplicate and the alert would never be retried anyway.
    """
    body = json.dumps(alert)
    for attempt in range(retries + 1):
        try:
            sns.publish(TopicArn=topic_arn, Message=body,
                        MessageAttributes={"alert_type": {"DataType": "String", "StringValue": alert["alert_type"]},
                                           "origin": {"DataType": "String", "StringValue": alert["origin"]}})
            return True
        except Exception as e:  # network, throttling, permissions...
            print(json.dumps({"sns_error": str(e), "alert_id": alert["alert_id"], "attempt": attempt + 1}),
                  file=sys.stderr, flush=True)
            if attempt < retries:
                sleep(0.2 * 2 ** attempt)
    return False

"""Helpers for the Deliverable 2 experiments: metrics of one simulator run, SQS depth sampling and
remote commands on the V2X-SERVICE instance. Everything that talks to AWS accepts an injected client
so the logic can be tested without AWS."""
import csv
import json
import os
import sys
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone

REGION = os.getenv("AWS_REGION", "us-east-1")


def _ts(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def percentile(sorted_values, p):
    if not sorted_values:
        return None
    return sorted_values[min(len(sorted_values) - 1, int(len(sorted_values) * p))]


def _scan(ddb, **kwargs):
    for page in ddb.get_paginator("scan").paginate(**kwargs):
        yield from page["Items"]


def run_metrics(run_dir, ddb=None, grace_s=10):
    """Observations for one simulator run, read from its run folder and from DynamoDB.

    Events are matched to the run by their event_time: the simulator stamps events with the wall clock,
    so a run owns the interval [start of run, start + duration]. Runs must not overlap in time.
    """
    if ddb is None:
        import boto3  # imported here so the helpers can be tested without AWS
        ddb = boto3.client("dynamodb", region_name=REGION)
    run = os.path.basename(os.path.normpath(run_dir))
    start = datetime.strptime(run[:15], "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
    with open(os.path.join(run_dir, "summary.json"), encoding="utf-8") as f:
        summary = json.load(f)
    with open(os.path.join(run_dir, "config.json"), encoding="utf-8") as f:
        config = json.load(f)
    lo = start - timedelta(seconds=2)
    hi = start + timedelta(seconds=float(summary.get("wall_s", 0)) + grace_s)
    window = {":a": {"S": _iso(lo)}, ":b": {"S": _iso(hi)}}

    latencies, stored, event_ids = [], 0, set()
    for it in _scan(ddb, TableName="v2x-events", ProjectionExpression="event_id, event_time, #t",
                    ExpressionAttributeNames={"#t": "trace"},
                    FilterExpression="event_time BETWEEN :a AND :b", ExpressionAttributeValues=window):
        stored += 1
        event_ids.add(it.get("event_id", {}).get("S"))
        cloud = it.get("trace", {}).get("M", {}).get("cloud_received_time", {}).get("S")
        if cloud:
            latencies.append((_ts(cloud) - _ts(it["event_time"]["S"])).total_seconds() * 1000)
    latencies.sort()

    # An alert is created some time after its event (queue wait), so it is matched to the run through its cause:
    # alert_id is "brake:<event_id>". Matching by creation time would miss alerts created after the window.
    alerts = sum(1 for it in _scan(ddb, TableName="v2x-alerts", ProjectionExpression="alert_id")
                 if it["alert_id"]["S"].split(":", 1)[-1] in event_ids)

    generated = summary["generated"]
    dropped = summary.get("dropped", 0)
    expected = generated - dropped                      # distinct events that were really sent
    sent = (summary.get("sqs") or {}).get("sent") or (summary.get("mqtt") or {}).get("published")
    return {
        "run_id": run, "seed": config.get("seed"), "faults": config.get("faults"),
        "generated": generated, "dropped_injected": dropped, "duplicated_injected": summary.get("duplicated", 0),
        "sent": sent, "accepted_stored": stored, "expected_distinct": expected,
        "lost": max(0, expected - stored), "duplicate_processed": max(0, stored - expected),
        "consistent": stored == expected,
        "p50_ms": round(percentile(latencies, 0.50)) if latencies else None,
        "p95_ms": round(percentile(latencies, 0.95)) if latencies else None,
        "max_ms": round(latencies[-1]) if latencies else None,
        "alerts": alerts,
    }


class QueueSampler:
    """Polls the depth of v2x-events (waiting + being processed) and of its dead-letter queue."""

    def __init__(self, sqs=None, interval=2.0, queue="v2x-events", dlq="v2x-events-dlq"):
        if sqs is None:
            import boto3
            sqs = boto3.client("sqs", region_name=REGION)
        self.sqs, self.interval = sqs, interval
        self.main_url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
        self.dlq_url = sqs.get_queue_url(QueueName=dlq)["QueueUrl"]
        self.samples = []            # (wall_time, waiting, in_flight, dead_letter)
        self._stop = threading.Event()
        self._thread = None

    def _counts(self, url):
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
        )["Attributes"]
        return int(attrs["ApproximateNumberOfMessages"]), int(attrs["ApproximateNumberOfMessagesNotVisible"])

    def sample_once(self):
        waiting, in_flight = self._counts(self.main_url)
        dead, _ = self._counts(self.dlq_url)
        self.samples.append((time.time(), waiting, in_flight, dead))

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.sample_once()
            except Exception as e:   # a failed poll must not kill the experiment
                print(f"[queue sampler] {e}", file=sys.stderr)
            self._stop.wait(self.interval)

    def start(self):
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def max_depth(self, since=0.0):
        return max([w + f for t, w, f, _ in self.samples if t >= since], default=0)

    def depth_at(self, when):
        """Depth of the last sample taken at or before `when`."""
        before = [w + f for t, w, f, _ in self.samples if t <= when]
        return before[-1] if before else None

    def dead_letters(self):
        return self.samples[-1][3] if self.samples else 0

    def wait_drained(self, since=0.0, timeout=180, quiet_samples=3):
        """Waits until the last `quiet_samples` samples show an empty queue. Returns the wall time of the
        first sample of that empty streak (>= since), or None on timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            recent = [s for s in self.samples if s[0] >= since]
            tail = recent[-quiet_samples:]
            if len(tail) == quiet_samples and all(w + f == 0 for _, w, f, _ in tail):
                return tail[0][0]
            time.sleep(min(self.interval, 1.0))
        return None

    def write_csv(self, path, since=0.0):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["time_utc", "waiting", "in_flight", "dead_letter"])
            for t, waiting, in_flight, dead in self.samples:
                if t >= since:
                    w.writerow([datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="seconds"),
                                waiting, in_flight, dead])


def service_instance(ec2=None):
    """(instance id, public ip) of the running v2x-service instance."""
    import boto3
    ec2 = ec2 or boto3.client("ec2", region_name=REGION)
    r = ec2.describe_instances(Filters=[{"Name": "tag:Role", "Values": ["v2x-service"]},
                                        {"Name": "instance-state-name", "Values": ["running"]}])
    for res in r["Reservations"]:
        for inst in res["Instances"]:
            return inst["InstanceId"], inst.get("PublicIpAddress")
    raise RuntimeError("no running instance tagged Role=v2x-service (run infra/aws/bootstrap.sh)")


def run_on_instance(instance_id, command, ssm=None, timeout=90):
    """Runs a shell command on an instance through SSM. Returns (status, stdout)."""
    import boto3
    ssm = ssm or boto3.client("ssm", region_name=REGION)
    cid = ssm.send_command(InstanceIds=[instance_id], DocumentName="AWS-RunShellScript",
                           Parameters={"commands": [command]})["Command"]["CommandId"]
    for _ in range(timeout):
        time.sleep(1)
        try:
            inv = ssm.get_command_invocation(CommandId=cid, InstanceId=instance_id)
        except Exception:        # the invocation is not visible yet
            continue
        if inv["Status"] in ("Success", "Failed", "Cancelled", "TimedOut"):
            return inv["Status"], inv.get("StandardOutputContent", "")
    return "Timeout", ""


def service_report(base_url, timeout=5):
    """GET /report of the service as a dict, or None when it does not answer."""
    try:
        with urllib.request.urlopen(f"{base_url}/report", timeout=timeout) as r:
            return json.load(r)
    except Exception:
        return None

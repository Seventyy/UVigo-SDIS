import json
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "infra" / "scripts"))
import v2xlab  # noqa: E402


class FakeDdb:
    """Serves canned items per table through the paginator interface used by run_metrics."""

    def __init__(self, events, alerts=()):
        self.tables = {"v2x-events": list(events), "v2x-alerts": list(alerts)}
        self.filters = []

    def get_paginator(self, name):
        assert name == "scan"
        ddb = self

        class P:
            def paginate(self, **kw):
                ddb.filters.append(kw.get("ExpressionAttributeValues"))
                return [{"Items": ddb.tables[kw["TableName"]]}]
        return P()


def ev(event_time, cloud_time, event_id="e"):
    return {"event_id": {"S": event_id}, "event_time": {"S": event_time},
            "trace": {"M": {"cloud_received_time": {"S": cloud_time}}}}


def make_run(summary, faults=None):
    root = tempfile.mkdtemp()
    run_dir = os.path.join(root, "20261114-100000-accident-s7")
    os.makedirs(run_dir)
    json.dump(summary, open(os.path.join(run_dir, "summary.json"), "w"))
    json.dump({"seed": 7, "faults": faults or {}}, open(os.path.join(run_dir, "config.json"), "w"))
    return run_dir


def test_percentile():
    assert v2xlab.percentile([], 0.95) is None
    assert v2xlab.percentile(list(range(100)), 0.95) == 95
    assert v2xlab.percentile([5], 0.99) == 5


def test_metrics_of_a_clean_run():
    run = make_run({"generated": 3, "dropped": 0, "duplicated": 0, "sqs": {"sent": 3}, "wall_s": 5})
    ddb = FakeDdb([ev("2026-11-14T10:00:01.000Z", "2026-11-14T10:00:01.100Z", "e1"),
                   ev("2026-11-14T10:00:02.000Z", "2026-11-14T10:00:02.200Z", "e2"),
                   ev("2026-11-14T10:00:03.000Z", "2026-11-14T10:00:03.300Z", "e3")],
                  alerts=[{"alert_id": {"S": "brake:e2"}}, {"alert_id": {"S": "brake:from-another-run"}}])
    m = v2xlab.run_metrics(run, ddb=ddb)
    assert (m["accepted_stored"], m["lost"], m["duplicate_processed"], m["consistent"]) == (3, 0, 0, True)
    assert m["p50_ms"] == 200 and m["p95_ms"] == 300 and m["alerts"] == 1 and m["sent"] == 3


def test_loss_and_duplicates_are_told_apart():
    # 10 generated, 2 dropped by the injector, 3 duplicate copies sent, only 6 distinct events stored
    run = make_run({"generated": 10, "dropped": 2, "duplicated": 3, "sqs": {"sent": 11}, "wall_s": 5})
    ddb = FakeDdb([ev(f"2026-11-14T10:00:0{i}.000Z", f"2026-11-14T10:00:0{i}.050Z") for i in range(6)])
    m = v2xlab.run_metrics(run, ddb=ddb)
    assert m["expected_distinct"] == 8 and m["accepted_stored"] == 6 and m["lost"] == 2
    assert m["duplicate_processed"] == 0 and m["consistent"] is False
    assert m["dropped_injected"] == 2 and m["duplicated_injected"] == 3


def test_more_rows_than_distinct_events_flags_duplicate_processing():
    run = make_run({"generated": 2, "dropped": 0, "duplicated": 0, "wall_s": 1})
    ddb = FakeDdb([ev("2026-11-14T10:00:01.000Z", "2026-11-14T10:00:01.010Z")] * 3)
    assert v2xlab.run_metrics(run, ddb=ddb)["duplicate_processed"] == 1


def test_query_window_covers_the_run_only():
    run = make_run({"generated": 1, "wall_s": 20})
    ddb = FakeDdb([])
    v2xlab.run_metrics(run, ddb=ddb, grace_s=10)
    window = ddb.filters[0]
    assert window[":a"]["S"] == "2026-11-14T09:59:58.000Z" and window[":b"]["S"] == "2026-11-14T10:00:30.000Z"


class FakeSqs:
    def __init__(self, depths):
        self.depths = list(depths)   # (waiting, in_flight) for each main-queue poll
        self.last = (0, 0)

    def get_queue_url(self, QueueName):
        return {"QueueUrl": QueueName}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        if QueueUrl == "v2x-events-dlq":
            w, f = 4, 0
        else:
            self.last = self.depths.pop(0) if self.depths else self.last
            w, f = self.last
        return {"Attributes": {"ApproximateNumberOfMessages": str(w), "ApproximateNumberOfMessagesNotVisible": str(f)}}


def test_sampler_tracks_max_depth_and_dead_letters():
    s = v2xlab.QueueSampler(sqs=FakeSqs([(5, 1), (20, 2), (3, 0)]), interval=0.01)
    for _ in range(3):
        s.sample_once()
    assert s.max_depth() == 22 and s.dead_letters() == 4
    assert s.depth_at(time.time()) == 3


def test_wait_drained_returns_start_of_the_empty_streak():
    s = v2xlab.QueueSampler(sqs=FakeSqs([(9, 0), (0, 0), (0, 0), (0, 0)]), interval=0.01)
    for _ in range(4):
        s.sample_once()
        time.sleep(0.002)
    t = s.wait_drained(timeout=1, quiet_samples=3)
    assert t == s.samples[1][0]


def test_wait_drained_times_out_when_the_queue_never_empties():
    s = v2xlab.QueueSampler(sqs=FakeSqs([(5, 0)]), interval=0.01)
    s.sample_once()
    assert s.wait_drained(timeout=0.2, quiet_samples=1) is None


def test_background_sampling_and_csv():
    s = v2xlab.QueueSampler(sqs=FakeSqs([(1, 0)] * 50), interval=0.01).start()
    time.sleep(0.15)
    s.stop()
    path = os.path.join(tempfile.mkdtemp(), "d.csv")
    s.write_csv(path)
    lines = open(path).read().strip().splitlines()
    assert lines[0] == "time_utc,waiting,in_flight,dead_letter" and len(lines) > 3


def test_alert_created_after_the_window_still_counts_for_its_run():
    run = make_run({"generated": 1, "dropped": 0, "duplicated": 0, "wall_s": 1})
    ddb = FakeDdb([ev("2026-11-14T10:00:01.000Z", "2026-11-14T10:00:30.000Z", "late")],
                  alerts=[{"alert_id": {"S": "brake:late"}}])
    assert v2xlab.run_metrics(run, ddb=ddb)["alerts"] == 1

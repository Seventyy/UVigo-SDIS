#!/usr/bin/env python3
"""Runs the two Deliverable 2 experiments and writes their result tables to results/.

  python infra/scripts/experiments.py delivery --factor dup        # baseline + two duplication levels
  python infra/scripts/experiments.py delivery --factor drop       # same with message loss
  python infra/scripts/experiments.py failure                      # stop V2X-SERVICE for 60 s, then restart it

Needs live AWS credentials and the infrastructure from infra/aws/bootstrap.sh. Each run uses a fresh random
seed, because repeating a seed repeats the event ids and the service would discard them as duplicates.
To go through the RSUs instead of straight to SQS, add:  --sim-args "--transport mqtt --rsu1 <ip1> --rsu2 <ip2>"
"""
import argparse
import csv
import json
import os
import random
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import v2xlab  # noqa: E402

REPO = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
FLAG = {"dup": "--dup", "drop": "--drop", "delay": "--delay-ms", "reorder": "--reorder-window"}
LEVELS = {"dup": ["0.1", "0.3"], "drop": ["0.1", "0.3"], "delay": ["100:300", "500:1500"], "reorder": ["5", "20"]}


def simulator_cmd(args, extra):
    cmd = [sys.executable, "-m", "simulator", "--time-scale", str(args.time_scale),
           "--seed", str(random.randint(1000, 99999))]
    cmd += shlex.split(args.sim_args) if args.sim_args else ["--transport", "sqs"]
    return cmd + extra


def parse_summary(stdout):
    return json.loads(stdout[stdout.index("{"):])


def run_dir_of(summary):
    return os.path.join(REPO, "runs", summary["run_id"])


def table(rows, columns):
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(str(r.get(c, "")) for c in columns) + " |" for r in rows]
    return "\n".join(lines)


def save(rows, columns, name):
    os.makedirs(os.path.join(REPO, "results"), exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    path = os.path.join(REPO, "results", f"{name}-{stamp}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"\nsaved {path}\n\n" + table(rows, columns))


def finish(sampler, t_sim_end, t_start, run_dir, args):
    """Waits for the queue to drain, then collects the metrics of the run."""
    os.makedirs(os.path.join(REPO, "results"), exist_ok=True)
    drained_at = sampler.wait_drained(since=t_sim_end, timeout=args.drain_timeout)
    time.sleep(args.settle)     # let the last writes reach DynamoDB
    metrics = v2xlab.run_metrics(run_dir)
    metrics["max_queue_depth"] = sampler.max_depth(since=t_start)
    metrics["drain_s"] = round(max(0.0, drained_at - t_sim_end), 1) if drained_at else "not drained"
    metrics["dead_letter"] = sampler.dead_letters()
    sampler.write_csv(os.path.join(REPO, "results", f"{metrics['run_id']}-queue-depth.csv"), since=t_start)
    return metrics


def delivery(args):
    levels = LEVELS[args.factor]
    conditions = [("baseline", [])] + [(f"{args.factor}={lv}", [FLAG[args.factor], lv]) for lv in levels]
    os.makedirs(os.path.join(REPO, "results"), exist_ok=True)
    sampler = v2xlab.QueueSampler(interval=2.0).start()
    rows = []
    try:
        for rep in range(args.repeats):
            for label, extra in conditions:
                print(f"\n=== {label} (repetition {rep + 1}/{args.repeats}) ===", flush=True)
                t_start = time.time()
                out = subprocess.run(simulator_cmd(args, extra), cwd=REPO, capture_output=True, text=True)
                if out.returncode != 0:
                    sys.exit(f"simulator failed:\n{out.stderr}")
                t_end = time.time()
                summary = parse_summary(out.stdout)
                m = finish(sampler, t_end, t_start, run_dir_of(summary), args)
                m["condition"] = label
                rows.append(m)
                print(json.dumps({k: m[k] for k in ("accepted_stored", "lost", "p95_ms", "max_queue_depth")}))
    finally:
        sampler.stop()
    save(rows, ["condition", "run_id", "generated", "dropped_injected", "duplicated_injected", "sent",
                "accepted_stored", "lost", "duplicate_processed", "consistent", "p50_ms", "p95_ms",
                "max_queue_depth", "drain_s", "dead_letter", "alerts"], f"delivery-{args.factor}")


def failure(args):
    instance_id, ip = v2xlab.service_instance()
    base_url = args.service_url or f"http://{ip}:8080"
    print(f"service instance {instance_id}, report at {base_url}/report")
    sampler = v2xlab.QueueSampler(interval=2.0).start()
    try:
        t_start = time.time()
        sim = subprocess.Popen(simulator_cmd(args, []), cwd=REPO, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
        time.sleep(args.down_after)
        print(f"[{args.down_after}s] stopping v2x-service", flush=True)
        t_stop = time.time()
        status, _ = v2xlab.run_on_instance(instance_id, "docker stop -t 2 v2x-service")
        print(f"   stop: {status}", flush=True)
        time.sleep(max(0, args.down_for - (time.time() - t_stop)))
        depth_at_restart = sampler.depth_at(time.time())
        t_restart = time.time()
        status, _ = v2xlab.run_on_instance(instance_id, "docker start v2x-service")
        print(f"[+{round(t_restart - t_stop)}s] restarted v2x-service: {status}", flush=True)
        stdout, stderr = sim.communicate()
        t_sim_end = time.time()
        if sim.returncode != 0:
            sys.exit(f"simulator failed:\n{stderr}")
        summary = parse_summary(stdout)
        m = finish(sampler, max(t_sim_end, t_restart), t_start, run_dir_of(summary), args)
    finally:
        sampler.stop()
    after = v2xlab.service_report(base_url) or {}
    drained = m["drain_s"]
    m.update(condition=f"service down {args.down_for}s", downtime_s=round(t_restart - t_stop),
             unprocessed_at_restart=depth_at_restart,
             recovery_s=round(sampler_recovery(sampler, t_restart), 1) if drained != "not drained" else drained,
             service_received_since_restart=after.get("received"), service_duplicate_since_restart=after.get("duplicate"))
    save([m], ["condition", "run_id", "generated", "downtime_s", "max_queue_depth", "unprocessed_at_restart",
               "recovery_s", "service_received_since_restart", "service_duplicate_since_restart",
               "accepted_stored", "lost", "duplicate_processed", "consistent", "p95_ms", "dead_letter"], "failure")


def sampler_recovery(sampler, t_restart):
    """Seconds from the restart until the queue is empty (first empty sample after the restart)."""
    empty = [t for t, w, f, _ in sampler.samples if t >= t_restart and w + f == 0]
    return (empty[0] - t_restart) if empty else float("nan")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("experiment", choices=["delivery", "failure"])
    p.add_argument("--factor", choices=list(FLAG), default="dup", help="delivery: anomaly to inject")
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--time-scale", type=float, default=10)
    p.add_argument("--sim-args", default="", help="replaces the default '--transport sqs'")
    p.add_argument("--down-after", type=float, default=15, help="failure: seconds before stopping the service")
    p.add_argument("--down-for", type=float, default=60, help="failure: seconds the service stays down")
    p.add_argument("--service-url", default="", help="failure: base URL of the service (default: looked up)")
    p.add_argument("--drain-timeout", type=float, default=240)
    p.add_argument("--settle", type=float, default=5)
    args = p.parse_args()
    {"delivery": delivery, "failure": failure}[args.experiment](args)


if __name__ == "__main__":
    main()

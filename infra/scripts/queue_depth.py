#!/usr/bin/env python3
"""Logs the SQS queue depth to a CSV file until Ctrl+C (or --duration seconds).
   python infra/scripts/queue_depth.py --out results/depth.csv"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import v2xlab  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--out", default="results/queue_depth.csv")
p.add_argument("--interval", type=float, default=2.0)
p.add_argument("--duration", type=float, default=0, help="seconds; 0 = until Ctrl+C")
a = p.parse_args()

os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
sampler = v2xlab.QueueSampler(interval=a.interval)
t0 = time.time()
try:
    while a.duration == 0 or time.time() - t0 < a.duration:
        sampler.sample_once()
        _, waiting, in_flight, dead = sampler.samples[-1]
        print(f"waiting={waiting} in_flight={in_flight} dead_letter={dead}", flush=True)
        time.sleep(a.interval)
except KeyboardInterrupt:
    pass
sampler.write_csv(a.out)
print(f"saved {len(sampler.samples)} samples to {a.out}; max depth {sampler.max_depth()}")

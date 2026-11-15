#!/usr/bin/env python3
"""Prints the observations of one simulator run:  python infra/scripts/metrics.py runs/<run_id> [--json]"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import v2xlab  # noqa: E402

if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--json"]
    m = v2xlab.run_metrics(args[0])
    if "--json" in sys.argv:
        print(json.dumps(m, indent=2))
    else:
        for k, v in m.items():
            print(f"{k:22} {v}")

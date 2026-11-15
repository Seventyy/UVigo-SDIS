# Experiment results

Runs of 15 November 2026 (UTC) on AWS Learner Lab, one `t3.small` instance for V2X-SERVICE, the accident scenario
(5 vehicles, 5 minutes of simulated time). Tools: `infra/scripts/experiments.py`; method in `docs/EXPERIMENTS.md`.
These runs go **straight from the simulator to SQS** (the cloud path); the repetition through the RSUs is pending
(see the end of this document). Raw tables: `results/*.csv`; queue depth over time: `results/*-queue-depth.csv`.

## Experiment 1: delivery anomalies (duplication, two levels)

Simulator at `--time-scale 10` (ten times real-time rate), one run per condition, a new random seed each.

| Condition | Sent | Duplicate copies | Stored (distinct) | Lost | Processed twice | p50 ms | p95 ms | Max queue | Drain s |
|---|---|---|---|---|---|---|---|---|---|
| baseline | 2541 | 0 | 2541 | 0 | 0 | 9708 | 20790 | 463 | 28.2 |
| dup 10 % | 2789 | 253 | 2536 | 0 | 0 | 2248 | 9556 | 206 | 23.9 |
| dup 30 % | 3304 | 763 | 2541 | 0 | 0 | 1638 | 8395 | 149 | 19.9 |

**Reading.** Final-state consistency holds at every level: the number of stored events equals the number of distinct
events generated, even with 763 duplicate copies in flight. Duplicates are detected by `event_id` and never stored,
counted or alerted twice (idempotent write).

Two cautions. (1) The latency is queueing delay, not network delay: at ten times the real rate the single consumer
falls behind and the queue grows to hundreds of messages. (2) The baseline ran first and has the highest latency,
while the duplication runs are faster; with one run per condition this order effect (warm-up of the freshly created
tables) cannot be told apart from the effect of the factor, so no conclusion is drawn about latency versus
duplication. The alert count (1, 0 and 2) varies with the random seed of each run (the braking vehicle does not always
exceed the 4 m/s² threshold), not with duplication.

### Reference: real-time rate

Run `20261115-011703-accident-s4072`, `--time-scale 1`, no faults, 2545 events:

| Condition | Stored | Lost | p50 ms | p95 ms | max ms | Alerts |
|---|---|---|---|---|---|---|
| real time (`--time-scale 1`), no faults | 2545 | 0 | 176 | 947 | 4026 | 1 |

At the real event rate every event is stored, nothing is lost, and the median latency from generation to arrival at
the service is 176 ms. Compared with the baseline above (p50 9708 ms, p95 20790 ms), a ten-fold increase of the event
rate multiplies the p95 latency by about 22 and makes the queue grow to hundreds of messages: the single consumer is
the bottleneck, not the network. The tail at real time (p95 947 ms, maximum 4 s) is probably caused by the bursts of the
simulator (the density records of all twelve segments are produced at the same instant) and by batching; this was not isolated.

## Experiment 2: component failure (V2X-SERVICE stopped for 60 s)

Container stopped 15 s after the start of the run, stopped for 60 s, restarted. Run `20261114-201545-accident-s28908`.

| Observation | Value |
|---|---|
| Downtime | 60 s |
| Maximum queue depth | 2145 messages |
| Unprocessed messages at restart | 1760 |
| Recovery time (restart until queue empty) | 88.4 s |
| Events received by the service after restart | 2172 |
| Redelivered duplicates detected after restart | 1 |
| Final state | 2542 stored of 2542 generated, 0 lost, 0 processed twice, consistent |
| Dead-letter queue | 0 |
| p95 latency | 84977 ms (dominated by the downtime) |

**Reading.** While the consumer is away the producer is not affected: SQS buffers the events (queue growth), and
nothing is lost. After the restart the service drains the backlog in about 88 s while new events keep arriving, which
is why the maximum depth (2145) is above the backlog at restart (1760). The single duplicate is a message that was
being processed when the container stopped: SQS returned it after the visibility timeout and the idempotent write
discarded it. This shows the at-least-once delivery plus idempotency design working under failure.

## Pending

- Repeat both experiments through the RSUs (`--sim-args "--transport mqtt --rsu1 <ip> --rsu2 <ip>"`) once they are
  deployed, and add the edge-cloud disconnection run (`POST /admin/uplink?up=false` on one RSU).
- Local alert latency (RSU, over MQTT) against cloud alert latency (SNS): `alerts_received.jsonl` of each simulator
  run records every alert with its reception time.

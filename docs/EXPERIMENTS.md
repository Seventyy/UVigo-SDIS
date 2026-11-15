# Experiments (Deliverable 2)

Two controlled experiments, as required: one on **message delivery** (delivery anomalies) and one on
**failure recovery** (component failure). Both are driven by `infra/scripts/experiments.py`, which launches the
simulator, samples the SQS queue depth every 2 s, waits for the queue to drain and reads the results from
DynamoDB. Tables are written to `results/*.csv` and printed as Markdown, ready to paste into the report.

## Before you start

1. Fresh Learner Lab credentials in `~/.aws/credentials` and `bash infra/aws/bootstrap.sh` finished.
2. The service container is running (`/report` of the service answers).
3. `python -m pip install -r simulator/requirements.txt` (boto3, paho-mqtt, PyYAML).
4. Do not run anything else against the queue during an experiment. Runs must not overlap in time.

## Experiment 1: delivery anomalies (duplication, two levels)

    python infra/scripts/experiments.py delivery --factor dup

Runs a baseline and two levels of duplication (10 % and 30 % of events sent twice), one condition at a time.
Use `--factor drop` for message loss (10 % and 30 %), `delay` or `reorder` for the other anomalies.
Each run takes about a minute at `--time-scale 10`. Add `--repeats 3` for three repetitions per condition.

| Column | Meaning |
|---|---|
| generated / dropped_injected / duplicated_injected | what the simulator produced and injected |
| sent | messages that really reached the transport (distinct events + duplicate copies) |
| accepted_stored | distinct events stored by the service (idempotent write keyed by `event_id`) |
| lost | distinct events sent but never stored (should be 0 with duplication) |
| duplicate_processed | rows stored beyond the distinct events sent (should be 0: idempotency) |
| consistent | `accepted_stored` equals the distinct events sent |
| p50_ms / p95_ms | latency from `event_time` (generation) to `cloud_received_time` |
| max_queue_depth / drain_s | peak SQS depth and seconds until the queue was empty after the run |

What to expect: duplicates are injected, but `accepted_stored` stays equal to the distinct events and
`duplicate_processed` stays 0, because the service refuses an `event_id` it already stored. With `drop`, the
loss is visible in `dropped_injected` and `lost` stays 0: the system cannot lose what never left the vehicle.

## Experiment 2: component failure (V2X-SERVICE stopped for 60 s)

    python infra/scripts/experiments.py failure

Starts the simulator, stops the `v2x-service` container 15 s later through SSM, keeps it down for 60 s and
starts it again. Events keep arriving in SQS meanwhile, so the queue grows and then drains.

| Column | Meaning |
|---|---|
| downtime_s | measured time the service was down |
| max_queue_depth / unprocessed_at_restart | queue growth while the consumer was away |
| recovery_s | seconds from the restart until the queue was empty again |
| service_received_since_restart / service_duplicate_since_restart | the service counters restart at 0 with the container; a non-zero duplicate count means messages were redelivered after the restart (retries) |
| accepted_stored / lost / duplicate_processed / consistent | final state; expected: nothing lost, nothing processed twice |
| dead_letter | messages that failed 5 times and moved to the dead-letter queue (expected 0) |

The queue depth over time of every run is saved as `results/<run_id>-queue-depth.csv` (for a chart).

## Going through the RSUs

Once the RSUs are deployed, repeat both experiments through the edge by adding:

    --sim-args "--transport mqtt --rsu1 <rsu-1 ip> --rsu2 <rsu-2 ip>"

The RSU aggregates position events, so `accepted_stored` will be lower than `expected_distinct`. In that case
read `lost` and `consistent` together with the RSU `/stats` counters, not on their own.

## Limits worth stating in the report

- Latency uses the simulator clock and the service clock (laptop vs EC2); a clock offset of tens of ms is possible.
- `lost` and `consistent` are exact only for the direct simulator-to-SQS path.
- One repetition per condition shows the mechanism; repeat for variance.

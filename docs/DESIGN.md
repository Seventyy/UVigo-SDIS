# Design

## 1. Schemas

**Event** (`schema/v2x-event.schema.json`, validated by the service and by the tests):

| Field | Meaning |
|---|---|
| `event_id` | UUID, globally unique; the idempotency key |
| `source_id`, `source_type` | `veh-0001` or `rsu-1`; `vehicle` or `rsu` |
| `seq` | per-source counter, used to recover order |
| `event_type` | `position`, `density`, `brake`, `crash`, `hazard`, `emergency` |
| `event_time` | generation time at the source, ISO 8601 |
| `location` | `segment_id` (S00 to S11), `rsu_id`, `lane`, `x`, `y` |
| `payload` | type-specific: speed and acceleration, deceleration, vehicle count and occupancy, hazard type... |
| `schema_version`, `sim_time_s` | `1.0`; simulation clock |
| `trace` | added on the way: RSU reception and forwarding times, then `cloud_received_time` at the service |

**Alert** (built in `services/v2x-service/alerts.py`): `alert_id`, `alert_type` (`sudden_braking`), `origin`
(`cloud` or `rsu`), `severity`, `created_time`, `location` (`segment_id`, `rsu_id`), `cause_event_ids`, `message`,
`schema_version`. The MQTT topics are `v2x/<rsu_id>/up` (vehicle to RSU) and `v2x/<rsu_id>/alerts` (RSU to vehicles),
both QoS 1. Segments S00 to S05 belong to `rsu-1`, S06 to S11 to `rsu-2`; a vehicle publishes to the RSU named in
the `location.rsu_id` of its event.

## 2. Messaging configuration and why

**SQS `v2x-events`** (standard queue, created by `infra/aws/bootstrap.sh`):

| Setting | Value | Reason |
|---|---|---|
| Queue type | standard | Duplicates are already handled by idempotency and order is recovered from `seq` and `event_time`, so FIFO's ordering buys nothing and costs throughput. |
| Visibility timeout | 30 s | Far above the processing time of one message (one DynamoDB transaction), so a message is not redelivered while it is being processed; short enough that a message held by a crashed consumer is retried quickly. Observed: one redelivery after the service restart experiment. |
| Long polling | 20 s | Fewer empty receives and lower latency than short polling. |
| Retention | 4 days | The queue survives a long consumer outage without losing events. |
| Dead-letter queue | `v2x-events-dlq`, `maxReceiveCount` 5, retention 14 days | A message that fails five times stops blocking the queue and is kept for inspection. |

**SNS `v2x-alerts`**: the service publishes each alert once; both RSUs are subscribed, so SNS fans it out. SNS
was chosen for alerts instead of a second queue because alerts must reach every RSU (fan-out) and must not wait
behind telemetry. Each RSU subscribes itself over **HTTP** on startup (the instructor confirmed HTTP is accepted:
HTTPS would need a certificate from a public CA, not available in Learner Lab) and confirms the subscription.
The subscription is renewed at every start because the instance's public IP changes after a restart.

## 3. Retry and acknowledgement behaviour

- **Event path (at-least-once).** The service deletes an SQS message only after its DynamoDB writes succeed. If it
  crashes or fails first, the message becomes visible again after the visibility timeout and is retried; after five
  receives it goes to the dead-letter queue.
- **Idempotent processing.** The event is stored only if its `event_id` is new, in the same DynamoDB transaction as the
  aggregate update. A redelivered or duplicated event is recognised, counted as `duplicate` and never counted or
  alerted twice. The alert id is derived from the event (`brake:<event_id>`) so even a repeated alert write is a no-op.
- **Alert path.** The service publishes to SNS with up to 3 retries (0.2, 0.4, 0.8 s backoff) and counts successes
  (`sns_published`) and failures (`sns_errors`) in `/report`. The alert is stored in DynamoDB before publishing.
  SNS then retries failed HTTP deliveries to the RSUs according to its delivery policy.

## 4. Edge processing and buffering in the RSU

> **Partner: verify this section against `services/rsu/` before submitting.** It records the design agreed for the RSU.

- **Validation** with the same JSON Schema as the service; invalid events are dropped and counted.
- **Deduplication** by `event_id` before anything is forwarded.
- **Aggregation:** position events are summarised into one density record per segment every 5 s; `brake`, `crash`,
  `hazard` and `emergency` events are forwarded immediately. In the accident scenario position events are 65 % of
  all events (1661 of 2546), so roughly one third of the traffic reaches the cloud.
- **Local alert:** a `crash` or a brake of 4 m/s² or more raises an alert with `origin: rsu` sent straight to the
  vehicles over MQTT, without the cloud round trip.
- **Cloud alerts:** received over HTTP from SNS, relayed to the vehicles of that RSU over MQTT.
- **Buffering when the cloud is unreachable:** in-memory queues, critical events separate from routine ones;
  uplink in batches of 10; retry with exponential backoff from 0.5 s to 30 s; capacity 5000 events; when full, the
  oldest routine events are discarded first, critical events last. `POST /admin/uplink?up=false` simulates an outage.

## 5. Known limitations

- If DynamoDB fails after an event is stored but before its alert is written, the redelivered message is a
  duplicate and the alert is not retried. Alerts that fail to publish to SNS are stored and counted, not re-sent.
- The queue and instance IPs are public and open to the Internet (security groups), as Learner Lab offers no
  fixed address for the vehicles' laptops.
- Latency is measured between the simulator clock (a laptop) and the service clock (EC2); clock offset is not corrected.
- Learner Lab credentials expire every few hours; CI/CD secrets must be refreshed each session.

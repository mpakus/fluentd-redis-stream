# Architecture and delivery contract

The [system diagram](../README.md#how-a-log-reaches-new-relic) traces a log from its application host to New Relic. Within that pipeline, one `redis_stream` input instance reads one stream using an existing consumer group. A background shipper reads files or process output on each producer host. It can attach origin metadata and call `XADD` outside the application's HTTP request path.

## Record format

One Redis Stream entry produces one Fluentd event. The input uses the tag configured on the collector for every entry.

| Field | Behavior |
| --- | --- |
| `message` | Preserves the original text, including JSON supplied as a string. A missing key sets `redis_input_error` to `missing message`. An empty string is a present key. |
| `hostname`, `source`, `environment`, `app` | Optional fields supplied by the producer. The input passes through these and other application fields as strings. |
| `timestamp` | Supply an ISO 8601 timestamp with an explicit zone, for example `2026-01-01T01:02:03.123456789+01:00`. A valid value sets Fluentd event time with nanosecond precision. Otherwise the input uses the milliseconds in the Redis ID. The original field stays in the record. |
| `redis_stream`, `redis_entry_id` | The input sets these to the actual stream name and entry ID, overwriting any producer values. Together they identify the original entry within a Redis instance. |
| `redis_input_error` | Reserved for the input. The input removes any producer value. A malformed timestamp sets `invalid timestamp`. If the message is also missing, `missing message` takes precedence. The record still goes through the route. |

## Reading and acknowledging

1. Create the consumer group at `0-0` to include existing entries. Keep its position across deployments. Redis tracks delivered, unacknowledged IDs in the group's pending entries list (PEL).
2. The input reads new entries with `XREADGROUP … STREAMS key >`. On startup and periodically thereafter it also reclaims idle pending entries with `XAUTOCLAIM`. It follows the returned cursor across pages, interleaving fresh reads even when a scan page is empty.
3. The input submits a batch to `router.emit_stream`. If the route raises, it rebuilds the batch from the Redis fields and retries after a second. This matters when a filter has mutated a previous attempt. When the route returns successfully, the input sends `XACK` for that batch.
4. Redis removes acknowledged IDs from the group's PEL. The stream retains the entries until its own retention policy removes them. If a Redis command fails, the reader logs a warning and retries after a second. A lost reply to `XACK` leaves the client uncertain whether Redis applied it. Subsequent recovery follows the group's actual state.

Use distinct consumer names for concurrently running collectors in a group. Set the reclaim idle time comfortably above normal batch processing time so a healthy reader can finish before another reader claims its entries. [Redis XREADGROUP](https://redis.io/docs/latest/commands/xreadgroup/) · [XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/) · [XACK](https://redis.io/docs/latest/commands/xack/)

## Failure boundaries

| Failure point | What to expect |
| --- | --- |
| Collector exits after reading, before the route accepts the batch | The IDs are pending. A replacement consumer can reclaim them after the idle threshold. |
| Persistent buffer accepts the batch, then the collector exits before ACK | The buffered copy survives a process restart on the same disk. Redis can also redeliver the same IDs. |
| Collector exits after ACK, before the output flushes | The group has finished with the entries. Recovery depends on reopening the output's persistent buffer on the same volume. |
| Output raises because its buffer is full | The input retries the current batch and withholds ACK. New entries can accumulate in Redis. |
| A filter intentionally drops an event or an error route absorbs a failure | Fluentd can return successfully and the input can acknowledge the batch. Route configuration determines the outcome. |
| Redis trims or evicts an entry before recovery | The consumer cannot recover the payload. The input logs the trimmed count when a claim response reports missing records. |

Choose a matching output for the tag and configure its failure behavior. The tested restart path uses one file-buffered output that propagates write errors. The input has no receipt from the downstream service when it acknowledges Redis. With multiple destinations, a partially successful route can replay records to a destination that already accepted them. [Fluentd router](https://github.com/fluent/fluentd/blob/v1.20.1/lib/fluent/event_router.rb) · [Fluentd error routing](https://github.com/fluent/fluentd/blob/v1.20.1/lib/fluent/root_agent.rb)

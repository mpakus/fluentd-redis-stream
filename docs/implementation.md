# Implementation and verification

The plugin registers `redis_stream` as a Fluentd input. It uses one Fluentd-managed reader thread per input instance and the `redis-client` gem with RESP2, so stream field/value pairs have the same representation on Redis 6.2 and 7.x. See the [input implementation](../lib/fluent/plugin/in_redis_stream.rb) and [delivery contract](architecture.md).

## How the reader works

- **Configure and start.** Validate required strings and positive limits. Check that Fluentd's router matches the tag, then start the reader thread.
- **Read and reclaim.** Read fresh entries in bounded batches. Resume each pending scan at the cursor Redis returned. Between unfinished pages, read fresh entries without blocking. A completed scan allows the next read to block for up to `block_ms`. The socket wait includes one additional second.
- **Route and acknowledge.** Build a `Fluent::MultiEventStream`, copy the source fields, assign event times and send the batch to the router. A route error retries the batch after one second. On success, acknowledge its IDs together.
- **Recover and stop.** Redis command errors are logged with the stream, group and error class. The loop waits one second before trying again. The thread checks Fluentd's running state between operations and closes the Redis client on exit.

The input API defines the router handoff, and Fluentd's thread helper manages the reader lifecycle. [Fluentd input API](https://docs.fluentd.org/plugin-development/api-plugin-input) · [Thread helper](https://docs.fluentd.org/plugin-helper-overview/api-plugin-helper-thread)

## Reproducible RSpec scenarios

Run `rtk proxy bundle exec rspec` from a checkout with a running test Redis. The suite launches Fluentd as a child process with a real file output and buffer. A test-only synchronous output pauses the worker on a known side of a buffer write. The spec checks the exact Redis IDs at that point before sending `SIGKILL`. At the end, `SIGUSR1` flushes the staged chunks and the spec reads the output file. [Running the suite](../README.md#run-the-specs)

| Scenario | What the spec verifies |
| --- | --- |
| Three abrupt stops | Stop before the buffer write, after the write but before ACK, and after ACK but before flush. All 120 original IDs reach the file. The eight entries buffered before the second crash appear twice, producing 128 rows in total and zero pending entries. Each row retains its expected fields and event time. |
| Full file buffer | Eight of 16 entries remain pending after a real buffer overflow. Restart with more space and verify that each of the 16 IDs reaches the file once. |
| Event fields | Preserve nanosecond precision when converting a timestamp with a UTC offset. Verify the ID fields assigned by the plugin and the diagnostic markers for missing messages and malformed timestamps. The crash test also checks event time derived from the Redis ID. |
| Graceful restart | Send `SIGTERM`, wait for a successful exit, and resume with the same consumer and buffer. All ten IDs arrive once, including the five published while the worker was down. |

These four examples were run on Redis 6.2.24 and 7.4.12 with Ruby 4.0.10 and Fluentd 1.20.1. The abrupt-stop test checks replay explicitly, so a run with missing **or unexpected duplicate** IDs fails.

## Staging delivery evidence

On November 22, 2026, five synthetic events passed from Redis through Fluentd 1.20.1 and `fluent-plugin-newrelic` 1.2.3 to New Relic Staging. The original test stream had expired, so the five entries were recreated with their original IDs before sending. The Log API returned HTTP 202. A separate NRQL query counted indexed events in the Staging account.

```sql
FROM Log SELECT count(*)
WHERE smoke_run_id = '5af38f0c-cad4-4153-9358-0c77c4ce01e6'
  AND redis_stream = 'fluentd-nr-smoke:5af38f0c-cad4-4153-9358-0c77c4ce01e6'
FACET redis_entry_id
SINCE '2026-11-20 23:50:19' UNTIL '2026-11-22 17:54:48' WITH TIMEZONE 'UTC'
```

All five IDs, `1795219219917-0` through `1795219219917-4`, had a count of one. The Redis group had zero pending entries at cleanup. This was a one-time check of successful delivery. The repeatable RSpec suite uses a local file destination.

## Remaining operational checks

Before a production rollout, exercise Redis connection loss during reads and acknowledgements, multi-page reclaim with trimmed entries and continuous new arrivals, and error routing via `@ERROR`. Verify how the chosen output handles HTTP 401, 429 and 503, loss of the buffer volume, and the measured peak traffic of the intended hosts. Record the supported outage duration and the time needed to drain the backlog.

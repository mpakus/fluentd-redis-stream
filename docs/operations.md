# Configure and operate the collector

Use the [local walkthrough](../README.md#try-it-locally) to check the input and tag with a terminal output. For staging, route the tag to a New Relic output backed by a persistent file buffer.

## Input settings

| Setting | Required or default | Purpose |
| --- | --- | --- |
| `redis_url` | Required | Redis connection URL. Use `rediss://` with certificate verification for TLS. Fluentd masks this configuration field. |
| `stream` | Required | Redis Stream key that the shippers write to. |
| `group` | Required | Existing consumer group whose position should survive restarts. |
| `consumer` | Required | Consumer name. Choose a unique value per concurrently running reader in the group. |
| `tag` | Required | Fluentd tag used for all emitted records. Configure a matching output. |
| `batch_size` | `100` | Maximum number of entries requested in each read or claim. |
| `block_ms` | `1000` | Maximum wait for fresh entries when a claim scan has finished. |
| `claim_idle_ms` | `60000` | Minimum idle time before an entry can be reclaimed. Also sets the delay before starting the next complete pending scan. |

The three integer settings take positive values. Create the group once on the target Redis. For an existing stream, the starting ID `0-0` includes its backlog. `MKSTREAM` creates an empty stream when needed. If the input logs `NOGROUP`, check the Redis database, stream and group names.

```sh
rtk proxy redis-cli -u "$LOG_REDIS_URL" XGROUP CREATE logs:staging fluentd 0-0 MKSTREAM
```

## Route staging logs to New Relic

Install the output and its JSON encoder in the same collector Ruby environment as the input. Version 1.2.3 of the New Relic plugin loads `yajl` at startup. Set `LOG_REDIS_URL`, `COLLECTOR_ID` and `NEW_RELIC_LICENSE_KEY` in the collector's service environment. The New Relic output reads the license key directly from that environment. Select the endpoint for the account's region. The example uses the US endpoint. [New Relic output configuration](https://docs.newrelic.com/docs/logs/forward-logs/fluentd-plugin-log-forwarding/)

```sh
rtk proxy fluent-gem install fluent-plugin-newrelic -v 1.2.3
rtk proxy fluent-gem install yajl-ruby
```

```conf
<source>
  @type redis_stream
  @id redis_staging
  redis_url "#{ENV.fetch('LOG_REDIS_URL')}"
  stream logs:staging
  group fluentd
  consumer "#{ENV.fetch('COLLECTOR_ID')}"
  tag strangler.logs
</source>

<match strangler.logs>
  @type newrelic
  @id newrelic_staging
  base_uri https://log-api.newrelic.com/log/v1
  <buffer time>
    @type file
    path /var/lib/fluent/buffer/newrelic
    timekey 5s
    timekey_wait 0s
    overflow_action throw_exception
    retry_forever true
  </buffer>
</match>
```

Give the Fluentd service write access to the buffer path on a volume retained across restarts. Set `total_limit_size` to fit that volume. With `throw_exception`, a full buffer can propagate an error to the reader and leave the batch pending. [Fluentd buffer settings](https://docs.fluentd.org/configuration/buffer-section)

**Check output errors as well as buffer depth.** In New Relic output 1.2.3, the HTTP response handler logs a non-2xx response and returns. A 429 or 503 can therefore finish a chunk without triggering Fluentd's retry logic. A successful staging upload and `retry_forever true` do not establish reliable delivery under HTTP rejection. [New Relic response handler](https://github.com/newrelic/newrelic-fluentd-output/blob/574ca6db02484073161c0af6ad30a140bdbc926e/lib/fluent/plugin/out_newrelic.rb#L101-L126)

## Operating checks

- **Access.** Scope producer, consumer and operator credentials to their roles and stream keys. Producers use `XADD`. The collector uses `XREADGROUP`, `XAUTOCLAIM` and `XACK`. Operators also need group creation and diagnostic access.
- **Filters.** Filter on producer fields such as `source` and `environment`. To filter on HTTP status, first parse representative Rails or NGINX messages. Test plain text and Rails request IDs or APM prefixes. When redacting a parsed field, check whether its original value is still in `message`. [Fluentd grep filter](https://docs.fluentd.org/filter/grep)
- **Retention and capacity.** Estimate queue capacity from peak incoming bytes per second multiplied by the longest expected outage. Allow for Redis overhead and time to catch up. A shared Redis also carries the application's workloads. A dedicated Redis isolates log growth. Set persistence, trimming and eviction policies for the loss tolerance of the deployment.
- **Monitoring.** Watch group lag where available, the oldest pending entry, pending count, output errors and disk usage. `XLEN` includes acknowledged history. For a delivery check, record the IDs returned by `XADD` and compare their counts in the destination.
- **Cutover and rollback.** Start with a dedicated staging stream on one host. Check origin fields, service and environment tags, and APM correlation. Switch each file's forwarding once to avoid overlap with an existing agent. For rollback, restore the original shipper and retain the stream, group and output buffer until the backlog has been reconciled.

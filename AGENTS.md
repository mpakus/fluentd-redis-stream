# fluentd-redis-stream

This Ruby gem reads logs from Redis Streams into a central Fluentd collector. The [README](README.md) covers installation and running the specs. The restart specs launch real Fluentd processes and create a temporary Redis key for each example.

- Always use the `rtk` and `ponytail` skills when writing, changing or improving code. Run commands through `rtk`. Use `rtk proxy` for full output. Choose the smallest change that verifies the required behavior.
- Use `codebase-design` when helpful for interface design and `rubypowers:ruby` for Ruby implementation and review. Check the API and source of the selected Fluentd version for lifecycle and routing details.
- Before changing the event format, Redis reader or acknowledgement timing, read the [architecture and delivery contract](docs/architecture.md).
- Follow the [implementation and verification guide](docs/implementation.md). Test recovery with real Redis and Fluentd and reconcile the destination against the original Redis entry IDs.
- When configuring the collector or switching shippers, use the [operations guide](docs/operations.md). New Relic credentials belong to the output on the collector.
- When a task is complete, merge its changes into `main`, delete the merged task branch locally and on GitHub, and push `main` to GitHub.

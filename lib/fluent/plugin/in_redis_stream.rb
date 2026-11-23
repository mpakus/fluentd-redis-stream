# frozen_string_literal: true

require "fluent/plugin/input"
require "fluent/event"
require "redis-client"
require "time"

module Fluent
  module Plugin
    class RedisStreamInput < Input
      Fluent::Plugin.register_input("redis_stream", self)
      helpers :thread

      config_param :redis_url, :string, secret: true
      config_param :stream, :string
      config_param :group, :string
      config_param :consumer, :string
      config_param :tag, :string
      config_param :batch_size, :integer, default: 100
      config_param :block_ms, :integer, default: 1000
      config_param :claim_idle_ms, :integer, default: 60_000

      def configure(conf)
        super
        %i[redis_url stream group consumer tag].each do |key|
          raise ConfigError, "#{key} must be nonempty" if public_send(key).strip.empty?
        end
        %i[batch_size block_ms claim_idle_ms].each do |key|
          raise ConfigError, "#{key} must be positive" unless public_send(key).positive?
        end
        # RESP2 gives both supported Redis versions the same array of field/value pairs.
        @connection = RedisClient.config(url: @redis_url, protocol: 2, reconnect_attempts: false)
      end

      def start
        super
        raise ConfigError, "no Fluentd route for #{@tag}" unless router.match?(@tag)

        thread_create(:redis_stream_reader) { consume }
      end

      private

      def consume
        redis = @connection.new_client
        cursor = "0-0"
        next_scan = 0
        while thread_current_running?
          begin
            if cursor != "0-0" || Fluent::Clock.now >= next_scan
              cursor, entries, deleted = redis.call("XAUTOCLAIM", @stream, @group, @consumer,
                                                    @claim_idle_ms, cursor, "COUNT", @batch_size)
              trimmed = deleted ? deleted.size : entries.count(nil)
              log.warn("pending records trimmed from stream", stream: @stream, count: trimmed) if trimmed.positive?
              deliver(redis, entries.compact)
              next_scan = Fluent::Clock.now + @claim_idle_ms / 1000.0
            end
            break unless thread_current_running?

            args = ["XREADGROUP", "GROUP", @group, @consumer, "COUNT", @batch_size]
            args.concat(["BLOCK", @block_ms]) if cursor == "0-0"
            reply = redis.blocking_call(@block_ms / 1000.0 + 1, *args, "STREAMS", @stream, ">")
            deliver(redis, reply.first.last) if reply
          rescue RedisClient::Error => e
            reason = e.is_a?(RedisClient::CommandError) ? e.message[/\A[A-Z]+\b/] : nil
            log.warn("Redis Streams command failed", stream: @stream, group: @group, error: e.class.name, reason: reason)
            sleep 1 if thread_current_running?
          end
        end
      ensure
        redis&.close
      end

      def deliver(redis, entries)
        return if entries.empty?

        while thread_current_running?
          begin
            # Filters may have modified the previous attempt before an output raised.
            router.emit_stream(@tag, build_events(entries))
          rescue StandardError => e
            log.warn("Fluentd route rejected batch, retrying", stream: @stream, count: entries.size, error: e.class.name)
            sleep 1 if thread_current_running?
            next
          end
          redis.call("XACK", @stream, @group, *entries.map(&:first))
          break
        end
      end

      def build_events(entries)
        events = MultiEventStream.new
        entries.each do |id, fields|
          record = {}
          fields.each_slice(2) { |key, value| record[key] = value.dup }
          record.delete("redis_input_error")
          record["redis_stream"] = @stream
          record["redis_entry_id"] = id
          time = event_time(id, record)
          record["redis_input_error"] = "missing message" unless record.key?("message")
          events.add(time, record)
        end
        events
      end

      def event_time(id, record)
        if (timestamp = record["timestamp"])
          begin
            raise ArgumentError unless timestamp.match?(/(?:Z|[+-]\d\d:\d\d)\z/i)

            return EventTime.from_time(Time.xmlschema(timestamp))
          rescue ArgumentError
            record["redis_input_error"] = "invalid timestamp"
          end
        end
        seconds, millis = id.to_i.divmod(1000)
        EventTime.new(seconds, millis * 1_000_000)
      end
    end
  end
end

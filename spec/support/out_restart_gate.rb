# frozen_string_literal: true

require "fluent/plugin/output"

# Test-only synchronous output. Its position in out_copy selects which side of
# the real file-buffer write the test can observe before killing the worker.
class RestartGate < Fluent::Plugin::Output
  Fluent::Plugin.register_output("restart_gate", self)
  config_param :marker, :string

  def process(_tag, events)
    ids = events.map { |_time, record| record.fetch("redis_entry_id") }
    File.write("#{@marker}.tmp", ids.join("\n"))
    File.rename("#{@marker}.tmp", @marker)
    sleep
  end
end

# frozen_string_literal: true

require "rspec"
require "json"
require "fileutils"
require "redis-client"
require "securerandom"
require "tmpdir"
require "time"
require "timeout"
require "rbconfig"

RSpec.describe "Fluentd redis_stream input" do
  before do
    @dir = Dir.mktmpdir("fluentd-redis-stream")
    @stream = "fluentd-restart-test:#{SecureRandom.uuid}"
    @url = ENV.fetch("TEST_REDIS_URL", "redis://127.0.0.1:6379/0")
    @redis = RedisClient.config(url: @url, protocol: 2).new_client
    @group = "restart-test"
    @redis.call("XGROUP", "CREATE", @stream, @group, "0-0", "MKSTREAM")
    @expected = {}
  end

  after do
    kill_worker if @pid
    @redis&.call("DEL", @stream) if @expected
    @redis&.close
    FileUtils.remove_entry(@dir) if @dir
  end

  def publish(count)
    count.times do
      number = @expected.size
      fields = { "hostname" => "web-#{number % 20}", "message" => "message #{number}" }
      id = @redis.call("XADD", @stream, "*", *fields.to_a.flatten)
      @expected[id] = fields
    end
  end

  def pending
    @redis.call("XPENDING", @stream, @group).first
  end

  def wait_for(description)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
    until yield
      if @pid && Process.waitpid(@pid, Process::WNOHANG)
        @pid = nil
        raise("Fluentd exited during #{description}\n#{File.read("#{@dir}/worker.log")}")
      end
      raise("Timeout during #{description}\n#{File.read("#{@dir}/worker.log")}") if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
      sleep 0.02
    end
  end

  def start_worker(name, gate: nil, buffer_limit: "64m")
    buffer = <<~CONF
      @type file
      path #{@dir}/received
      append true
      <inject>
        time_key fluent_event_time
        time_type string
        time_format %Y-%m-%dT%H:%M:%S.%NZ
        utc true
      </inject>
      <format>
        @type json
      </format>
      <buffer []>
        @type file
        path #{@dir}/buffer
        flush_mode interval
        flush_interval 1h
        flush_at_shutdown false
        total_limit_size #{buffer_limit}
        overflow_action throw_exception
      </buffer>
    CONF
    output = buffer
    if gate
      marker = "#{@dir}/#{name}.ready"
      pause = "@type restart_gate\nmarker #{marker}"
      stores = gate == :before_buffer ? [pause, buffer] : [buffer, pause]
      output = "@type copy\n" + stores.map { |store| "<store>\n#{store}\n</store>\n" }.join
    end
    File.write("#{@dir}/fluent.conf", <<~CONF)
      <source>
        @type redis_stream
        redis_url "\#{ENV.fetch('TEST_REDIS_URL')}"
        stream #{@stream}
        group #{@group}
        consumer #{name}
        tag test.logs
        batch_size 8
        block_ms 50
        claim_idle_ms 150
      </source>
      <match test.logs>
        #{output}
      </match>
    CONF
    command = [RbConfig.ruby, Gem.bin_path("fluentd", "fluentd"), "--no-supervisor",
               "-p", File.join(__dir__, "support"), "-c", "#{@dir}/fluent.conf"]
    @pid = Process.spawn({ "TEST_REDIS_URL" => @url }, *command, out: ["#{@dir}/worker.log", "a"], err: [:child, :out])
    return unless gate

    wait_for("worker #{name} to reach #{gate}") { File.exist?(marker) }
    File.readlines(marker, chomp: true)
  end

  def kill_worker
    Process.kill("KILL", @pid)
    Process.waitpid(@pid)
    @pid = nil
  end

  def caught_up?
    info = @redis.call("XINFO", "GROUPS", @stream).map { |fields| Hash[*fields] }
    group = info.find { |g| g["name"] == @group }
    group["pending"].zero? && group["last-delivered-id"] == @expected.keys.last
  end

  def received
    Dir.glob("#{@dir}/received*.log").flat_map do |file|
      File.readlines(file).select { |line| line.end_with?("\n") }.map { |line| JSON.parse(line) }
    end
  end

  it "recovers pending and buffered entries after SIGKILL, including the expected replay before XACK" do
    publish(8)
    before_buffer = start_worker("first", gate: :before_buffer)
    expect(before_buffer).to eq(@expected.keys)
    expect(pending).to eq(8)
    kill_worker

    # Let these entries become eligible before the replacement's initial scan.
    sleep 0.2
    after_buffer = start_worker("second", gate: :after_buffer)
    expect(after_buffer).to eq(before_buffer)
    expect(pending).to eq(8)
    kill_worker

    publish(92)
    start_worker("third")
    wait_for("all IDs acknowledged to the on-disk buffer") { caught_up? }
    expect(received).to be_empty
    kill_worker

    publish(20)
    start_worker("fourth")
    wait_for("post-restart catch-up") { caught_up? }
    Process.kill("USR1", @pid) # Force the real file output to flush its staged chunks.
    wait_for("file output after forced flush") { received.size >= @expected.size + after_buffer.size }
    records = received
    ids = records.map { |record| record.fetch("redis_entry_id") }
    expect(ids.uniq.sort).to eq(@expected.keys.sort)
    expect(ids.tally).to eq((@expected.keys + after_buffer).tally)
    records.each do |record|
      id = record.fetch("redis_entry_id")
      fields = @expected.fetch(id)
      expected_time = Time.at(Rational(id.to_i, 1000)).utc.iso8601(9)
      expect(record).to eq(fields.merge("redis_stream" => @stream, "redis_entry_id" => id, "fluent_event_time" => expected_time))
    end
    expect(pending).to eq(0)
  end

  it "keeps a rejected batch pending until the output has buffer space" do
    publish(16)
    start_worker("small-buffer", buffer_limit: "1")
    wait_for("file buffer overflow") { File.read("#{@dir}/worker.log").include?("BufferOverflowError") }
    expect(pending).to eq(8)
    kill_worker
    start_worker("larger-buffer")
    wait_for("recovery after overflow") { caught_up? }
    Process.kill("USR1", @pid)
    wait_for("all 16 records to reach output") { received.size >= 16 }
    expect(received.map { |r| r["redis_entry_id"] }.sort).to eq(@expected.keys.sort)
  end

  it "preserves source fields and event times and marks malformed entries" do
    inputs = [
      { "message" => "original", "timestamp" => "2026-01-01T01:02:03.123456789+01:00",
        "redis_entry_id" => "spoof", "redis_stream" => "other", "redis_input_error" => "spoof" },
      { "message" => "bad timestamp", "timestamp" => "yesterday" },
      { "hostname" => "missing-message" }
    ]
    inputs.each do |fields|
      id = @redis.call("XADD", @stream, "*", *fields.to_a.flatten)
      @expected[id] = fields
    end
    start_worker("fields")
    wait_for("field test ACK") { caught_up? }
    Process.kill("USR1", @pid)
    wait_for("field test output") { received.size == inputs.size }
    valid, invalid, missing = received
    expect(received.map { |r| r.fetch("redis_entry_id") }).to eq(@expected.keys)
    expect(valid.fetch("redis_stream")).to eq(@stream)
    expect(valid.fetch("fluent_event_time")).to eq("2026-01-01T00:02:03.123456789Z")
    expect(valid).not_to have_key("redis_input_error")
    expect(invalid.fetch("redis_input_error")).to eq("invalid timestamp")
    expect(invalid.fetch("timestamp")).to eq("yesterday")
    expect(missing.fetch("redis_input_error")).to eq("missing message")
    expect(missing.fetch("hostname")).to eq("missing-message")
  end

  it "resumes after SIGTERM with the same consumer name and persistent buffer" do
    publish(5)
    start_worker("stable")
    wait_for("buffer before normal shutdown") { caught_up? }
    Process.kill("TERM", @pid)
    status = Timeout.timeout(15) { Process.waitpid2(@pid).last }
    @pid = nil
    expect(status).to be_success
    expect(received).to be_empty
    publish(5)
    start_worker("stable")
    wait_for("catch-up after normal restart") { caught_up? }
    Process.kill("USR1", @pid)
    wait_for("flushed records after normal restart") { received.size >= 10 }
    expect(received.map { |r| r.fetch("redis_entry_id") }.sort).to eq(@expected.keys.sort)
  end
end

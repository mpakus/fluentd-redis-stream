Gem::Specification.new do |spec|
  spec.name = "fluent-plugin-redis-stream"
  spec.version = "0.1.0"
  spec.authors = ["fluentd-redis-stream contributors"]
  spec.summary = "Read and recover Redis Streams consumer group entries in Fluentd"
  spec.required_ruby_version = ">= 3.3"
  spec.files = Dir["lib/**/*.rb", "README.md", "docs/*.md"]
  spec.add_dependency "fluentd", "~> 1.20.0"
  spec.add_dependency "redis-client", ">= 0.26", "< 1"
end

require_relative "lib/seaduck/version"

Gem::Specification.new do |spec|
  spec.name          = "seaduck"
  spec.version       = SeaDuck::VERSION
  spec.summary       = "Apache Iceberg for Ruby, powered by libduckdb"
  spec.homepage      = "https://github.com/ankane/seaduck"
  spec.license       = "MIT"

  spec.author        = "Andrew Kane"
  spec.email         = "andrew@ankane.org"

  spec.files         = Dir["*.{md,txt}", "{lib}/**/*"]
  spec.require_path  = "lib"

  spec.required_ruby_version = ">= 3.2"

  spec.add_dependency "duckdb"
end

# dependencies
require "duckdb"

# stdlib
require "uri"

# modules
require_relative "seaduck/catalog"
require_relative "seaduck/result"
require_relative "seaduck/version"

# catalogs
require_relative "seaduck/glue_catalog"
require_relative "seaduck/rest_catalog"
require_relative "seaduck/s3_tables_catalog"

module SeaDuck
  class Error < StandardError; end
  class BinderError < Error; end
  class CatalogError < Error; end
  class ConversionError < Error; end
  class InvalidConfigurationError < Error; end
  class InvalidInputError < Error; end
  class IOError < Error; end
  class NotImplementedError < Error; end
  class PermissionError < Error; end
  class Rollback < Error; end
  class TransactionContextError < Error; end
end

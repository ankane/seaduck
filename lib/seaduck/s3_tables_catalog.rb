module SeaDuck
  class S3TablesCatalog < Catalog
    # https://duckdb.org/docs/stable/core_extensions/iceberg/amazon_s3_tables
    def initialize(arn:, default_namespace: "main")
      attach_options = {
        endpoint_type: "s3_tables"
      }
      secret_options = {
        type: "s3",
        provider: "credential_chain"
      }
      _initialize(
        arn,
        default_namespace:,
        attach_options:,
        secret_options:,
        extensions: ["aws", "httpfs"]
      )
    end
  end
end

module SeaDuck
  class GlueCatalog < Catalog
    # https://duckdb.org/docs/stable/core_extensions/iceberg/amazon_sagemaker_lakehouse
    def initialize(warehouse:, default_namespace: "main")
      attach_options = {
        endpoint_type: "glue"
      }
      secret_options = {
        type: "s3",
        provider: "credential_chain"
      }
      _initialize(
        warehouse,
        default_namespace:,
        attach_options:,
        secret_options:
      )
    end
  end
end

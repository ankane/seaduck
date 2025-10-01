require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"

$catalog = ENV["CATALOG"] || "rest"
puts "Using #{$catalog}"

class Minitest::Test
  def setup
    catalog.list_tables("seaduck_test").each do |t|
      catalog.drop_table(t)
    end
  end

  def catalog
    @catalog ||= begin
      case $catalog
      when "glue"
        SeaDuck::GlueCatalog.new(
          warehouse: ENV.fetch("GLUE_WAREHOUSE"),
          **catalog_options
        )
      when "rest"
        SeaDuck::RestCatalog.new(
          uri: "http://localhost:8181",
          _secret_options: {
            type: "s3",
            key_id: "admin",
            secret: "password",
            endpoint: "127.0.0.1:9000",
            url_style: "path",
            use_ssl: 0
          },
          **catalog_options
        )
      when "s3tables"
        SeaDuck::S3TablesCatalog.new(
          arn: ENV.fetch("S3_TABLES_ARN"),
          **catalog_options
        )
      else
        raise "Unsupported catalog"
      end
    end
  end

  def catalog_options
    {
      default_namespace: "seaduck_test"
    }
  end

  def s3tables?
    $catalog == "s3tables"
  end

  def create_events
    catalog.sql("CREATE TABLE events (a bigint, b text)")
    load_events
  end

  def load_events
    catalog.sql("COPY events FROM 'test/support/data.csv'")
  end
end

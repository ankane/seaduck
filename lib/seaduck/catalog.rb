module SeaDuck
  class Catalog
    def _initialize(url, default_namespace:, attach_options:, secret_options: nil, extensions: [])
      @catalog = "iceberg"
      @default_namespace = default_namespace

      @db = DuckDB::Database.open
      @conn = @db.connect

      install_extension("iceberg")
      extensions.each do |extension|
        install_extension(extension)
      end
      create_secret(secret_options) if secret_options
      attach_with_options(@catalog, url, {type: "iceberg"}.merge(attach_options))

      begin
        use_namespace(@default_namespace)
      rescue Error
        create_namespace(@default_namespace, if_not_exists: true)
        use_namespace(@default_namespace)
      end
      execute("DETACH memory")
    end

    def list_namespaces
      execute("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?", [@catalog]).rows
    end

    def create_namespace(namespace, if_not_exists: nil)
      execute("CREATE SCHEMA#{" IF NOT EXISTS" if if_not_exists} #{quote_namespace(namespace)}")
      nil
    end

    def namespace_exists?(namespace)
      execute("SELECT 1 FROM information_schema.schemata WHERE catalog_name = ? AND schema_name = ?", [@catalog, namespace]).any?
    end

    # CASCADE not implemented for Iceberg yet
    def drop_namespace(namespace, if_exists: nil)
      execute("DROP SCHEMA#{" IF EXISTS" if if_exists} #{quote_namespace(namespace)}")
      nil
    end

    def list_tables(namespace = nil)
      sql = +"SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = ?"
      params = [@catalog]

      if namespace
        sql << " AND table_schema = ?"
        params << namespace
      end

      execute(sql, params).rows
    end

    def table_exists?(table_name)
      namespace, table_name = split_table(table_name)
      execute("SELECT 1 FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ? AND table_name = ?", [@catalog, namespace, table_name]).any?
    end

    def drop_table(table_name, if_exists: nil)
      execute("DROP TABLE#{" IF EXISTS" if if_exists} #{quote_table(table_name)}")
    end

    def snapshots(table_name)
      symbolize_keys execute("SELECT * FROM iceberg_snapshots(#{quote_table(table_name)})")
    end

    def sql(sql, params = [])
      execute(sql, params)
    end

    def transaction
      execute("BEGIN")
      begin
        yield
        execute("COMMIT")
      rescue => e
        execute("ROLLBACK")
        raise e unless e.is_a?(Rollback)
      end
    end

    def attach(alias_, url)
      type = nil
      extension = nil

      uri = URI.parse(url)
      case uri.scheme
      when "postgres", "postgresql"
        type = "postgres"
        extension = "postgres"
      else
        raise ArgumentError, "Unsupported data source type: #{uri.scheme}"
      end

      install_extension(extension) if extension

      options = {
        type: type,
        read_only: true
      }
      attach_with_options(alias_, url, options)
    end

    def detach(alias_)
      execute("DETACH #{quote_identifier(alias_)}")
      nil
    end

    # experimental
    def extension_version
      execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = ?", ["iceberg"]).first["extension_version"]
    end

    # experimental
    def duckdb_version
      execute("SELECT VERSION() AS version").first["version"]
    end

    # libduckdb does not provide function
    # https://duckdb.org/docs/stable/sql/dialect/keywords_and_identifiers.html
    def quote_identifier(value)
      "\"#{encoded(value).gsub('"', '""')}\""
    end

    # libduckdb does not provide function
    # TODO support more types
    def quote(value)
      if value.nil?
        "NULL"
      elsif value == true
        "true"
      elsif value == false
        "false"
      elsif defined?(BigDecimal) && value.is_a?(BigDecimal)
        value.to_s("F")
      elsif value.is_a?(Numeric)
        value.to_s
      else
        if value.is_a?(Time)
          value = value.utc.iso8601(9)
        elsif value.is_a?(DateTime)
          value = value.iso8601(9)
        elsif value.is_a?(Date)
          value = value.strftime("%Y-%m-%d")
        end
        "'#{encoded(value).gsub("'", "''")}'"
      end
    end

    # hide internal state
    def inspect
      to_s
    end

    private

    def execute(sql, params = [])
      # use prepare instead of query to prevent multiple statements at once
      result =
        @conn.prepare(sql) do |stmt|
          params.each_with_index do |v, i|
            stmt.bind(i + 1, v)
          end
          stmt.execute
        end

      # TODO add column types
      Result.new(result.columns.map(&:name), result.to_a)
    rescue DuckDB::Error => e
      raise map_error(e), cause: nil
    end

    def error_mapping
      @error_mapping ||= {
        "Binder Error: " => BinderError,
        "Catalog Error: " => CatalogError,
        "Conversion Error: " => ConversionError,
        "Invalid Configuration Error: " => InvalidConfigurationError,
        "Invalid Input Error: " => InvalidInputError,
        "IO Error: " => IOError,
        "Not implemented Error: " => NotImplementedError,
        "Permission Error: " => PermissionError,
        "TransactionContext Error: " => TransactionContextError
      }
    end

    # not ideal to base on prefix, but do not see a better way at the moment
    def map_error(e)
      error_mapping.each do |prefix, cls|
        if e.message&.start_with?(prefix)
          return cls.new(e.message.delete_prefix(prefix))
        end
      end
      Error.new(e.message)
    end

    def install_extension(extension)
      execute("INSTALL #{quote_identifier(extension)}")
    end

    def create_secret(options)
      execute("CREATE SECRET (#{options_args(options)})")
    end

    def attach_with_options(alias_, url, options)
      execute("ATTACH #{quote(url)} AS #{quote_identifier(alias_)} (#{options_args(options)})")
    end

    def options_args(options)
      options.map { |k, v| "#{option_name(k)} #{quote(v)}" }.join(", ")
    end

    def option_name(k)
      name = k.to_s.upcase
      # should never contain user input, but just to be safe
      unless name.match?(/\A[A-Z_]+\z/)
        raise "Invalid option name"
      end
      name
    end

    def symbolize_keys(result)
      result.map { |v| v.transform_keys(&:to_sym) }
    end

    def use_namespace(namespace)
      execute("USE #{quote_namespace(namespace)}")
    end

    def quote_namespace(value)
      "#{quote_identifier(@catalog)}.#{quote_identifier(value)}"
    end

    def split_table(value)
      if value.is_a?(Array)
        if value.size == 2
          value
        else
          raise ArgumentError, "Invalid table identifier"
        end
      else
        [@default_namespace, value]
      end
    end

    def quote_table(value)
      namespace, table_name = split_table(value)
      "#{quote_namespace(namespace)}.#{quote_identifier(table_name)}"
    end

    def encoded(value)
      value = value.to_s if value.is_a?(Symbol)
      if !value.respond_to?(:to_str)
        raise TypeError, "no implicit conversion of #{value.class.name} into String"
      end
      if ![Encoding::UTF_8, Encoding::US_ASCII].include?(value.encoding) || !value.valid_encoding?
        raise ArgumentError, "Unsupported encoding"
      end
      value
    end
  end
end

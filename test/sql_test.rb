require_relative "test_helper"

class SqlTest < Minitest::Test
  def test_result
    create_events
    result = catalog.sql("SELECT * FROM events")
    assert_equal ["a", "b"], result.columns
    assert_equal [[1, "one"], [2, "two"], [3, "three"]], result.rows
    assert_equal [{"a" => 1, "b" => "one"}, {"a" => 2, "b" => "two"}, {"a" => 3, "b" => "three"}], result.to_a
    assert_equal ({"a" => 1, "b" => "one"}), result.first
    assert_equal false, result.empty?
  end

  def test_types
    assert_kind_of Integer, catalog.sql("SELECT 1").rows[0][0]
    assert_kind_of BigDecimal, catalog.sql("SELECT 1.0").rows[0][0]
    assert_kind_of Date, catalog.sql("SELECT current_date").rows[0][0]
    assert_kind_of Time, catalog.sql("SELECT current_time").rows[0][0]
    assert_kind_of TrueClass, catalog.sql("SELECT true").rows[0][0]
    assert_kind_of FalseClass, catalog.sql("SELECT false").rows[0][0]
    assert_kind_of NilClass, catalog.sql("SELECT NULL").rows[0][0]
  end

  def test_params
    assert_kind_of Integer, catalog.sql("SELECT ?", [1]).rows[0][0]
    assert_kind_of Float, catalog.sql("SELECT ?", [1.0]).rows[0][0]
    assert_kind_of BigDecimal, catalog.sql("SELECT ?", [BigDecimal("1")]).rows[0][0]
    assert_kind_of TrueClass, catalog.sql("SELECT ?", [true]).rows[0][0]
    assert_kind_of FalseClass, catalog.sql("SELECT ?", [false]).rows[0][0]
    assert_kind_of NilClass, catalog.sql("SELECT ?", [nil]).rows[0][0]
    # TODO try to fix
    assert_kind_of String, catalog.sql("SELECT ?", [Date.today]).rows[0][0]
    assert_kind_of String, catalog.sql("SELECT ?", [Time.now]).rows[0][0]
  end

  def test_extra_params
    error = assert_raises(SeaDuck::Error) do
      catalog.sql("SELECT ?", [1, 2])
    end
    assert_equal "fail to bind 2 parameter", error.message
  end

  def test_update
    create_events
    assert_raises(SeaDuck::NotImplementedError) do
      catalog.sql("UPDATE events SET b = ? WHERE a = ?", ["two!", 2])
    end
  end

  def test_delete
    create_events
    assert_raises(SeaDuck::NotImplementedError) do
      catalog.sql("DELETE FROM events WHERE a = ?", [2])
    end
  end

  def test_view
    create_events
    assert_raises(SeaDuck::NotImplementedError) do
      catalog.sql("CREATE VIEW events_view AS SELECT a AS c, b AS d FROM events")
    end
  end

  def test_transaction
    catalog.transaction do
      create_events
      load_events
    end
    assert_equal 6, catalog.sql("SELECT * FROM events").count
  end

  def test_transaction_rollback
    create_events
    catalog.transaction do
      load_events
      raise SeaDuck::Rollback
    end
    assert_equal 3, catalog.sql("SELECT * FROM events").count
  end

  def test_transaction_error
    create_events
    error = assert_raises do
      catalog.transaction do
        load_events
        raise "Error"
      end
    end
    assert_equal "Error", error.message
    assert_equal 3, catalog.sql("SELECT * FROM events").count
  end

  def test_transaction_nested
    error = assert_raises(SeaDuck::TransactionContextError) do
      catalog.transaction do
        catalog.transaction do
        end
      end
    end
    assert_equal "cannot start a transaction within a transaction", error.message
  end

  def test_multiple_statements
    error = assert_raises(SeaDuck::InvalidInputError) do
      catalog.sql("SELECT 1; SELECT 2").to_a
    end
    assert_equal "Cannot prepare multiple statements at once!", error.message
  end

  def test_quote_identifier
    assert_equal %{"events"}, catalog.quote_identifier("events")
    assert_equal %{"events"}, catalog.quote_identifier(:events)
    assert_equal %{"""events"""}, catalog.quote_identifier(%{"events"})

    error = assert_raises(TypeError) do
      catalog.quote_identifier(nil)
    end
    assert_equal "no implicit conversion of NilClass into String", error.message

    error = assert_raises(TypeError) do
      catalog.quote_identifier(Object.new)
    end
    assert_equal "no implicit conversion of Object into String", error.message
  end

  def test_quote_identifier_statement
    skip if s3tables? # The specified table name is not valid.

    table = 19.times.map { ["a", "'", '"', "\\"].sample }.join
    begin
      catalog.sql("CREATE TABLE #{catalog.quote_identifier(table)} (a integer, b varchar)")
    ensure
      catalog.drop_table(table, if_exists: true)
    end
  end

  def test_quote_identifier_schema
    create_events
    error = assert_raises(SeaDuck::CatalogError) do
      catalog.sql("COPY #{catalog.quote_identifier("seaduck_test.events")} FROM 'test/support/data.csv'")
    end
    assert_match "Table with name seaduck_test.events does not exist!", error.message
    assert_equal 3, catalog.sql("SELECT * FROM seaduck_test.events").count
  end

  def test_quote
    assert_equal "NULL", catalog.quote(nil)
    assert_equal "true", catalog.quote(true)
    assert_equal "false", catalog.quote(false)
    assert_equal "1", catalog.quote(1)
    assert_equal "0.5", catalog.quote(0.5)
    assert_equal "0.5", catalog.quote(BigDecimal("0.5"))
    assert_equal "'2025-01-02T03:04:05.123456000Z'", catalog.quote(Time.utc(2025, 1, 2, 3, 4, 5, 123456))
    assert_equal "'2025-01-02'", catalog.quote(Date.new(2025, 1, 2))
    assert_equal "'hello'", catalog.quote("hello")
    error = assert_raises(TypeError) do
      catalog.quote(Object.new)
    end
    assert_equal "no implicit conversion of Object into String", error.message
  end

  def test_quote_statement
    value = 19.times.map { ["a", "'", '"', "\\"].sample }.join
    assert_equal value, catalog.sql("SELECT #{catalog.quote(value)} AS value").rows[0][0]
  end
end

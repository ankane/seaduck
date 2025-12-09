require_relative "test_helper"

class CatalogTest < Minitest::Test
  def test_snapshots
    create_events

    snapshots = catalog.snapshots("events")
    assert_equal 1, snapshots.size
    assert_equal [1], snapshots.map { |v| v[:sequence_number] }

    load_events

    snapshots = catalog.snapshots("events")
    assert_equal 2, snapshots.size
    assert_equal [2, 1], snapshots.map { |v| v[:sequence_number] }
  end

  def test_schema_evolution
    create_events
    assert_raises(SeaDuck::NotImplementedError) do
      catalog.sql("ALTER TABLE events ADD COLUMN c VARCHAR DEFAULT 'hello'")
    end
  end

  def test_time_travel
    create_events
    snapshot_version = catalog.snapshots("events").last[:snapshot_id]
    load_events

    assert_equal 6, catalog.sql("SELECT * FROM events").count
    assert_equal 3, catalog.sql("SELECT * FROM events AT (VERSION => ?)", [snapshot_version]).count
  end

  def test_list_tables
    create_events
    assert_includes catalog.list_tables, ["seaduck_test", "events"]
  end

  def test_list_tables_namespace
    create_events
    assert_includes catalog.list_tables("seaduck_test"), ["seaduck_test", "events"]
  end

  def test_list_tables_namespace_missing
    assert_empty catalog.list_tables("missing")
  end

  def test_table_exists
    assert_equal false, catalog.table_exists?("events")
    create_events
    assert_equal true, catalog.table_exists?("events")
  end

  def test_drop_table
    create_events
    catalog.drop_table("events")
  end

  def test_drop_table_missing
    error = assert_raises(SeaDuck::CatalogError) do
      catalog.drop_table("events")
    end
    assert_match "Table with name events does not exist!", error.message
  end

  def test_drop_table_if_exists
    create_events
    catalog.drop_table("events", if_exists: true)
    catalog.drop_table("events", if_exists: true)
  end

  def test_attach_postgres
    skip unless ENV["TEST_POSTGRES"]

    require "pg"

    pg = PG.connect(dbname: "seaduck_test")
    pg.exec("DROP TABLE IF EXISTS postgres_events")
    pg.exec("CREATE TABLE postgres_events (id bigint, name text)")
    pg.exec_params("INSERT INTO postgres_events VALUES ($1, $2)", [1, "Test"])

    catalog.attach("pg", "postgres://localhost/seaduck_test")

    catalog.sql("CREATE TABLE events (id bigint, name text)")
    catalog.sql("INSERT INTO events SELECT * FROM pg.postgres_events")

    expected = [{"id" => 1, "name" => "Test"}]
    assert_equal expected, catalog.sql("SELECT * FROM events").to_a

    error = assert_raises(SeaDuck::InvalidInputError) do
      catalog.sql("INSERT INTO pg.postgres_events VALUES (?, ?)", [2, "Test 2"])
    end
    assert_match "attached in read-only mode!", error.message

    catalog.detach("pg")
    error = assert_raises(SeaDuck::CatalogError) do
      catalog.sql("INSERT INTO events SELECT * FROM pg.postgres_events")
    end
    assert_match "Table with name postgres_events does not exist!", error.message
  end

  def test_attach_unsupported_type
    error = assert_raises(ArgumentError) do
      catalog.attach("hello", "pg://")
    end
    assert_equal "Unsupported data source type: pg", error.message
  end

  def test_extension_version
    assert_equal "1c0c4c60", catalog.extension_version
  end

  def test_duckdb_version
    assert_equal "v1.4.3", catalog.duckdb_version
  end

  def test_inspect
    assert_equal catalog.inspect, catalog.to_s
    refute_match "@db", catalog.inspect
  end
end

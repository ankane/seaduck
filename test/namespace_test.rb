require_relative "test_helper"

class NamespaceTest < Minitest::Test
  def test_list_namespaces
    assert_includes catalog.list_namespaces, ["seaduck_test"]
  end

  def test_create_namespace
    catalog.drop_namespace("seaduck_test2", if_exists: true)

    assert_nil catalog.create_namespace("seaduck_test2")
    assert_equal true, catalog.namespace_exists?("seaduck_test2")
  end

  def test_create_namespace_if_not_exists
    catalog.drop_namespace("seaduck_test2", if_exists: true)

    assert_nil catalog.create_namespace("seaduck_test2")
    assert_nil catalog.create_namespace("seaduck_test2", if_not_exists: true)
  end

  def test_create_namespace_already_exists
    error = assert_raises(SeaDuck::Error) do
      catalog.create_namespace("seaduck_test")
    end
    assert_match "already exists", error.message
  end

  def test_namespace_exists
    assert_equal true, catalog.namespace_exists?("seaduck_test")
    assert_equal false, catalog.namespace_exists?("missing")
  end

  def test_drop_namespace
    catalog.drop_namespace("seaduck_test2", if_exists: true)

    assert_nil catalog.create_namespace("seaduck_test2")
    assert_equal true, catalog.namespace_exists?("seaduck_test2")

    assert_nil catalog.drop_namespace("seaduck_test2")
    assert_equal false, catalog.namespace_exists?("seaduck_test2")
  end

  def test_drop_namespace_if_exists
    assert_nil catalog.drop_namespace("missing", if_exists: true)
  end

  def test_drop_namespace_missing
    error = assert_raises(SeaDuck::Error) do
      catalog.drop_namespace("missing")
    end
    assert_match "does not exist", error.message
  end

  def test_drop_namespace_not_empty
    catalog.sql("CREATE TABLE events (id bigint)")
    error = assert_raises(SeaDuck::Error) do
      catalog.drop_namespace("seaduck_test")
    end
    assert_match "is not empty", error.message
  end
end

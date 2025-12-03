defmodule Milvex.Integration.CollectionTest do
  use Milvex.IntegrationCase, async: false

  @moduletag :integration

  describe "create_collection/4" do
    test "creates a new collection with valid schema", %{conn: conn} do
      name = unique_collection_name("create")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      assert :ok = Milvex.create_collection(conn, name, schema)
      assert {:ok, true} = Milvex.has_collection(conn, name)
    end

    test "creates collection with custom options", %{conn: conn} do
      name = unique_collection_name("create_opts")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      assert :ok = Milvex.create_collection(conn, name, schema, shards_num: 2)
      assert {:ok, true} = Milvex.has_collection(conn, name)

      {:ok, info} = Milvex.describe_collection(conn, name)
      assert info.shards_num == 2
    end

    test "creating collection with same name is idempotent", %{conn: conn} do
      name = unique_collection_name("duplicate")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      assert :ok = Milvex.create_collection(conn, name, schema)
      assert :ok = Milvex.create_collection(conn, name, schema)
    end
  end

  describe "drop_collection/3" do
    test "drops an existing collection", %{conn: conn} do
      name = unique_collection_name("drop")
      schema = standard_schema(name)

      :ok = Milvex.create_collection(conn, name, schema)
      assert {:ok, true} = Milvex.has_collection(conn, name)

      assert :ok = Milvex.drop_collection(conn, name)
      assert {:ok, false} = Milvex.has_collection(conn, name)
    end

    test "dropping non-existent collection is idempotent", %{conn: conn} do
      name = unique_collection_name("drop_nonexistent")

      assert :ok = Milvex.drop_collection(conn, name)
    end
  end

  describe "has_collection/3" do
    test "returns true for existing collection", %{conn: conn} do
      name = unique_collection_name("has_exists")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)
      assert {:ok, true} = Milvex.has_collection(conn, name)
    end

    test "returns false for non-existent collection", %{conn: conn} do
      name = unique_collection_name("has_nonexistent")
      assert {:ok, false} = Milvex.has_collection(conn, name)
    end
  end

  describe "describe_collection/3" do
    test "returns collection metadata", %{conn: conn} do
      name = unique_collection_name("describe")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)
      assert {:ok, info} = Milvex.describe_collection(conn, name)

      assert info.schema.name == name
      assert is_integer(info.collection_id)
      assert is_list(info.schema.fields)
      assert length(info.schema.fields) == 3
    end
  end

  describe "list_collections/2" do
    test "includes created collection in list", %{conn: conn} do
      name = unique_collection_name("list")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)
      assert {:ok, collections} = Milvex.list_collections(conn)

      assert is_list(collections)
      assert name in collections
    end
  end

  describe "load_collection/3" do
    test "loads collection into memory", %{conn: conn} do
      name = unique_collection_name("load")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      assert :ok = Milvex.load_collection(conn, name)
    end

    test "fails when loading collection without index", %{conn: conn} do
      name = unique_collection_name("load_no_index")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      assert {:error, error} = Milvex.load_collection(conn, name)
      assert %Milvex.Errors.Grpc{} = error
    end
  end

  describe "release_collection/3" do
    test "releases collection from memory", %{conn: conn} do
      name = unique_collection_name("release")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      :ok = Milvex.load_collection(conn, name)

      assert :ok = Milvex.release_collection(conn, name)
    end

    test "fails when releasing non-existent collection", %{conn: conn} do
      name = unique_collection_name("release_nonexistent")

      assert {:error, error} = Milvex.release_collection(conn, name)
      assert %Milvex.Errors.Grpc{} = error
    end
  end
end

defmodule Milvex.Integration.DataTest do
  use Milvex.IntegrationCase, async: false

  @moduletag :integration

  describe "insert/4" do
    test "inserts data with Data struct", %{conn: conn} do
      name = unique_collection_name("insert_data")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      data = sample_data(schema, 3)
      assert {:ok, result} = Milvex.insert(conn, name, data)
      assert result.insert_count == 3
      assert length(result.ids) == 3
    end

    test "inserts data with row maps", %{conn: conn} do
      name = unique_collection_name("insert_rows")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = sample_rows(3)
      assert {:ok, result} = Milvex.insert(conn, name, rows)
      assert result.insert_count == 3
      assert length(result.ids) == 3
    end

    test "generates IDs when auto_id is true", %{conn: conn} do
      name = unique_collection_name("insert_autoid")
      schema = standard_schema(name, auto_id: true)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      data = sample_data(schema, 3)
      assert {:ok, result} = Milvex.insert(conn, name, data)

      assert length(result.ids) == 3
      assert Enum.all?(result.ids, &is_integer/1)
    end

    test "accepts manual IDs when auto_id is false", %{conn: conn} do
      name = unique_collection_name("insert_manual")
      schema = manual_id_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = sample_rows_with_ids(3, start_id: 100)
      data = Data.from_rows!(rows, schema)

      assert {:ok, result} = Milvex.insert(conn, name, data)
      assert result.insert_count == 3
      assert 100 in result.ids
      assert 101 in result.ids
      assert 102 in result.ids
    end

    test "inserts into specific partition", %{conn: conn} do
      name = unique_collection_name("insert_partition")
      schema = standard_schema(name)
      partition = "test_partition"

      on_exit(fn ->
        cleanup_partition(conn, name, partition)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition)

      data = sample_data(schema, 3)
      assert {:ok, result} = Milvex.insert(conn, name, data, partition_name: partition)
      assert result.insert_count == 3
    end

    test "fails for non-existent collection", %{conn: conn} do
      name = unique_collection_name("insert_nonexistent")
      schema = standard_schema(name)

      data = sample_data(schema, 3)
      assert {:error, error} = Milvex.insert(conn, name, data)
      assert %Milvex.Errors.Grpc{} = error
    end

    test "fails with wrong dimension vectors", %{conn: conn} do
      name = unique_collection_name("insert_wrong_dim")
      schema = standard_schema(name, dimension: 4)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      wrong_dim_rows = [
        %{title: "Test", embedding: [0.1, 0.2]}
      ]

      assert {:error, _error} = Milvex.insert(conn, name, wrong_dim_rows)
    end
  end

  describe "delete/4" do
    test "deletes by id expression", %{conn: conn} do
      name = unique_collection_name("delete_by_id")
      schema = manual_id_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      rows = sample_rows_with_ids(5, start_id: 1)
      data = Data.from_rows!(rows, schema)
      {:ok, _} = Milvex.insert(conn, name, data)

      :ok = Milvex.load_collection(conn, name)

      assert {:ok, result} = Milvex.delete(conn, name, "id in [1, 2, 3]")
      assert result.delete_count == 3
    end

    test "deletes with filter expression", %{conn: conn} do
      name = unique_collection_name("delete_filter")
      schema = manual_id_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      rows = sample_rows_with_ids(5, start_id: 1)
      data = Data.from_rows!(rows, schema)
      {:ok, _} = Milvex.insert(conn, name, data)

      :ok = Milvex.load_collection(conn, name)

      assert {:ok, result} = Milvex.delete(conn, name, "id >= 3")
      assert result.delete_count == 3
    end

    test "fails for non-existent collection", %{conn: conn} do
      name = unique_collection_name("delete_nonexistent")

      assert {:error, error} = Milvex.delete(conn, name, "id > 0")
      assert %Milvex.Errors.Grpc{} = error
    end

    test "fails with invalid expression", %{conn: conn} do
      name = unique_collection_name("delete_invalid")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      :ok = Milvex.load_collection(conn, name)

      assert {:error, error} = Milvex.delete(conn, name, "invalid syntax !!!")
      assert %Milvex.Errors.Grpc{} = error
    end
  end

  describe "upsert/4" do
    test "inserts new records", %{conn: conn} do
      name = unique_collection_name("upsert_insert")
      schema = manual_id_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = sample_rows_with_ids(3, start_id: 1)
      data = Data.from_rows!(rows, schema)

      assert {:ok, result} = Milvex.upsert(conn, name, data)
      assert result.upsert_count == 3
      assert length(result.ids) == 3
    end

    test "updates existing records", %{conn: conn} do
      name = unique_collection_name("upsert_update")
      schema = manual_id_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      initial_rows = sample_rows_with_ids(3, start_id: 1)
      initial_data = Data.from_rows!(initial_rows, schema)
      {:ok, _} = Milvex.insert(conn, name, initial_data)

      :ok = Milvex.load_collection(conn, name)

      updated_rows = [
        %{id: 1, title: "Updated Item 1", embedding: random_vector(4)},
        %{id: 2, title: "Updated Item 2", embedding: random_vector(4)},
        %{id: 4, title: "New Item 4", embedding: random_vector(4)}
      ]

      updated_data = Data.from_rows!(updated_rows, schema)

      assert {:ok, result} = Milvex.upsert(conn, name, updated_data)
      assert result.upsert_count == 3

      assert_eventually(
        match?(
          {:ok, %{rows: rows}} when length(rows) == 4,
          Milvex.query(conn, name, "id >= 0", output_fields: ["id", "title"], limit: 10)
        )
      )
    end

    test "fails for non-existent collection", %{conn: conn} do
      name = unique_collection_name("upsert_nonexistent")
      schema = manual_id_schema(name)

      rows = sample_rows_with_ids(3)
      data = Data.from_rows!(rows, schema)

      assert {:error, error} = Milvex.upsert(conn, name, data)
      assert %Milvex.Errors.Grpc{} = error
    end
  end
end

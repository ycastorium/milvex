defmodule Milvex.Integration.PartitionTest do
  use Milvex.IntegrationCase, async: false

  @moduletag :integration

  describe "create_partition/4" do
    test "creates a new partition", %{conn: conn} do
      name = unique_collection_name("partition_create")
      schema = standard_schema(name)
      partition = "test_partition"

      on_exit(fn ->
        cleanup_partition(conn, name, partition)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      assert :ok = Milvex.create_partition(conn, name, partition)
      assert {:ok, true} = Milvex.has_partition(conn, name, partition)
    end

    test "creating duplicate partition is idempotent", %{conn: conn} do
      name = unique_collection_name("partition_dup")
      schema = standard_schema(name)
      partition = "duplicate_partition"

      on_exit(fn ->
        cleanup_partition(conn, name, partition)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition)

      assert :ok = Milvex.create_partition(conn, name, partition)
    end

    test "fails for non-existent collection", %{conn: conn} do
      name = unique_collection_name("partition_no_coll")

      assert {:error, error} = Milvex.create_partition(conn, name, "test_partition")
      assert %Milvex.Errors.Grpc{} = error
    end
  end

  describe "has_partition/4" do
    test "returns true for existing partition", %{conn: conn} do
      name = unique_collection_name("has_partition_exists")
      schema = standard_schema(name)
      partition = "existing_partition"

      on_exit(fn ->
        cleanup_partition(conn, name, partition)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition)

      assert {:ok, true} = Milvex.has_partition(conn, name, partition)
    end

    test "returns false for non-existent partition", %{conn: conn} do
      name = unique_collection_name("has_partition_no")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      assert {:ok, false} = Milvex.has_partition(conn, name, "nonexistent_partition")
    end
  end

  describe "list_partitions/3" do
    test "includes _default partition", %{conn: conn} do
      name = unique_collection_name("list_default")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      assert {:ok, partitions} = Milvex.list_partitions(conn, name)
      assert is_list(partitions)
      assert "_default" in partitions
    end

    test "includes created partitions", %{conn: conn} do
      name = unique_collection_name("list_custom")
      schema = standard_schema(name)
      partition1 = "partition_alpha"
      partition2 = "partition_beta"

      on_exit(fn ->
        cleanup_partition(conn, name, partition1)
        cleanup_partition(conn, name, partition2)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition1)
      :ok = Milvex.create_partition(conn, name, partition2)

      assert {:ok, partitions} = Milvex.list_partitions(conn, name)
      assert partition1 in partitions
      assert partition2 in partitions
    end
  end

  describe "drop_partition/4" do
    test "drops existing partition", %{conn: conn} do
      name = unique_collection_name("drop_partition")
      schema = standard_schema(name)
      partition = "to_drop"

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition)
      assert {:ok, true} = Milvex.has_partition(conn, name, partition)

      assert :ok = Milvex.drop_partition(conn, name, partition)
      assert {:ok, false} = Milvex.has_partition(conn, name, partition)
    end

    test "fails when dropping _default partition", %{conn: conn} do
      name = unique_collection_name("drop_default")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      assert {:error, error} = Milvex.drop_partition(conn, name, "_default")
      assert %Milvex.Errors.Grpc{} = error
    end

    test "dropping non-existent partition is idempotent", %{conn: conn} do
      name = unique_collection_name("drop_no_part")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      assert :ok = Milvex.drop_partition(conn, name, "nonexistent")
    end
  end

  describe "load_partitions/4" do
    test "loads specific partitions", %{conn: conn} do
      name = unique_collection_name("load_partitions")
      schema = standard_schema(name)
      partition = "to_load"

      on_exit(fn ->
        cleanup_partition(conn, name, partition)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      assert :ok = Milvex.load_partitions(conn, name, [partition])
    end

    test "fails without index", %{conn: conn} do
      name = unique_collection_name("load_no_idx")
      schema = standard_schema(name)
      partition = "no_index_partition"

      on_exit(fn ->
        cleanup_partition(conn, name, partition)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition)

      assert {:error, error} = Milvex.load_partitions(conn, name, [partition])
      assert %Milvex.Errors.Grpc{} = error
    end
  end

  describe "release_partitions/4" do
    test "releases specific partitions", %{conn: conn} do
      name = unique_collection_name("release_partitions")
      schema = standard_schema(name)
      partition = "to_release"

      on_exit(fn ->
        cleanup_partition(conn, name, partition)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      :ok = Milvex.load_partitions(conn, name, [partition])

      assert :ok = Milvex.release_partitions(conn, name, [partition])
    end
  end

  describe "partition data operations" do
    test "inserts data into specific partition", %{conn: conn} do
      name = unique_collection_name("part_insert")
      schema = standard_schema(name)
      partition = "data_partition"

      on_exit(fn ->
        cleanup_partition(conn, name, partition)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition)

      data = sample_data(schema, 5)
      assert {:ok, result} = Milvex.insert(conn, name, data, partition_name: partition)
      assert result.insert_count == 5
    end

    test "searches in specific partitions", %{conn: conn} do
      name = unique_collection_name("part_search")
      schema = standard_schema(name)
      partition1 = "search_partition_1"
      partition2 = "search_partition_2"

      on_exit(fn ->
        cleanup_partition(conn, name, partition1)
        cleanup_partition(conn, name, partition2)
        cleanup_collection(conn, name)
      end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_partition(conn, name, partition1)
      :ok = Milvex.create_partition(conn, name, partition2)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      data1 = sample_data(schema, 5)
      {:ok, _} = Milvex.insert(conn, name, data1, partition_name: partition1)

      data2 = sample_data(schema, 10)
      {:ok, _} = Milvex.insert(conn, name, data2, partition_name: partition2)

      :ok = Milvex.load_collection(conn, name)

      query_vector = random_vector(4)

      assert_eventually(fn ->
        with {:ok, result} <-
               Milvex.search(conn, name, [query_vector],
                 vector_field: "embedding",
                 top_k: 20,
                 partition_names: [partition1]
               ),
             [results | _] <- result.hits do
          length(results) <= 5
        else
          _ -> false
        end
      end)
    end
  end
end

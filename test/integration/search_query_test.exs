defmodule Milvex.Integration.SearchQueryTest do
  use Milvex.IntegrationCase, async: false

  @moduletag :integration

  describe "search/4" do
    setup %{conn: conn} do
      name = unique_collection_name("search")
      schema = standard_schema(name)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      rows = [
        %{title: "The Matrix", embedding: [1.0, 0.0, 0.0, 0.0]},
        %{title: "Inception", embedding: [0.0, 1.0, 0.0, 0.0]},
        %{title: "Interstellar", embedding: [0.0, 0.0, 1.0, 0.0]},
        %{title: "Avatar", embedding: [0.0, 0.0, 0.0, 1.0]},
        %{title: "Dune", embedding: [0.5, 0.5, 0.0, 0.0]}
      ]

      data = Data.from_rows!(rows, schema)
      {:ok, insert_result} = Milvex.insert(conn, name, data)
      :ok = Milvex.load_collection(conn, name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      %{collection_name: name, schema: schema, ids: insert_result.ids}
    end

    test "returns similar vectors", %{conn: conn, collection_name: name} do
      query_vector = [1.0, 0.0, 0.0, 0.0]

      assert_eventually(
        match?(
          {:ok, %{num_queries: 1, hits: [_ | _]}},
          Milvex.search(conn, name, [query_vector], vector_field: "embedding", top_k: 3)
        )
      )
    end

    test "respects top_k limit", %{conn: conn, collection_name: name} do
      query_vector = [0.5, 0.5, 0.0, 0.0]

      assert_eventually(fn ->
        case Milvex.search(conn, name, [query_vector], vector_field: "embedding", top_k: 2) do
          {:ok, result} -> length(hd(result.hits)) == 2
          _ -> false
        end
      end)
    end

    test "returns specified output_fields", %{conn: conn, collection_name: name} do
      query_vector = [1.0, 0.0, 0.0, 0.0]

      assert_eventually(fn ->
        with {:ok, result} <-
               Milvex.search(conn, name, [query_vector],
                 vector_field: "embedding",
                 top_k: 1,
                 output_fields: ["title"]
               ),
             [hit | _] <- result.hits,
             [first | _] <- hit do
          Map.has_key?(first, :title) or Map.has_key?(first, "title")
        else
          _ -> false
        end
      end)
    end

    test "applies filter expression", %{conn: conn, collection_name: name} do
      query_vector = [0.5, 0.5, 0.5, 0.5]

      assert_eventually(fn ->
        with {:ok, result} <-
               Milvex.search(conn, name, [query_vector],
                 vector_field: "embedding",
                 top_k: 10,
                 filter: "title like \"The%\"",
                 output_fields: ["title"]
               ),
             [results | _] <- result.hits do
          length(results) == 1
        else
          _ -> false
        end
      end)
    end

    test "handles multiple query vectors", %{conn: conn, collection_name: name} do
      query_vectors = [
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0]
      ]

      assert_eventually(fn ->
        case Milvex.search(conn, name, query_vectors,
               vector_field: "embedding",
               top_k: 2
             ) do
          {:ok, result} -> result.num_queries == 2 and length(result.hits) == 2
          _ -> false
        end
      end)
    end

    test "fails without vector_field option", %{conn: conn, collection_name: name} do
      query_vector = [1.0, 0.0, 0.0, 0.0]

      assert {:error, error} = Milvex.search(conn, name, [query_vector], top_k: 3)
      assert %Milvex.Errors.Invalid{} = error
    end

    test "fails on unloaded collection", %{conn: conn} do
      name = unique_collection_name("search_unloaded")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      query_vector = [1.0, 0.0, 0.0, 0.0]

      assert {:error, error} =
               Milvex.search(conn, name, [query_vector],
                 vector_field: "embedding",
                 top_k: 3
               )

      assert %Milvex.Errors.Grpc{} = error
    end
  end

  describe "search with different metrics" do
    test "search with L2 metric", %{conn: conn} do
      name = unique_collection_name("search_l2")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding", index_type: "AUTOINDEX", metric_type: "L2")

      rows = sample_rows(5)
      data = Data.from_rows!(rows, schema)
      {:ok, _} = Milvex.insert(conn, name, data)
      :ok = Milvex.load_collection(conn, name)

      query_vector = random_vector(4)

      assert_eventually(
        match?(
          {:ok, %{num_queries: 1}},
          Milvex.search(conn, name, [query_vector], vector_field: "embedding", top_k: 3)
        )
      )
    end

    test "search with IP metric", %{conn: conn} do
      name = unique_collection_name("search_ip")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding", index_type: "AUTOINDEX", metric_type: "IP")

      rows = sample_rows(5)
      data = Data.from_rows!(rows, schema)
      {:ok, _} = Milvex.insert(conn, name, data)
      :ok = Milvex.load_collection(conn, name)

      query_vector = random_vector(4)

      assert_eventually(
        match?(
          {:ok, %{num_queries: 1}},
          Milvex.search(conn, name, [query_vector], vector_field: "embedding", top_k: 3)
        )
      )
    end
  end

  describe "query/4" do
    setup %{conn: conn} do
      name = unique_collection_name("query")
      schema = manual_id_schema(name)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      rows = sample_rows_with_ids(10, start_id: 1)
      data = Data.from_rows!(rows, schema)
      {:ok, _} = Milvex.insert(conn, name, data)
      :ok = Milvex.load_collection(conn, name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      %{collection_name: name, schema: schema}
    end

    test "queries by expression", %{conn: conn, collection_name: name} do
      assert_eventually(fn ->
        case Milvex.query(conn, name, "id > 0", limit: 20) do
          {:ok, result} -> length(result.rows) == 10
          _ -> false
        end
      end)
    end

    test "returns specified output_fields", %{conn: conn, collection_name: name} do
      assert_eventually(fn ->
        with {:ok, result} <-
               Milvex.query(conn, name, "id > 0",
                 output_fields: ["id", "title"],
                 limit: 5
               ),
             [row | _] <- result.rows do
          Map.has_key?(row, :id) and Map.has_key?(row, :title)
        else
          _ -> false
        end
      end)
    end

    test "respects limit option", %{conn: conn, collection_name: name} do
      assert_eventually(fn ->
        case Milvex.query(conn, name, "id > 0", limit: 3) do
          {:ok, result} -> length(result.rows) == 3
          _ -> false
        end
      end)
    end

    test "respects offset option", %{conn: conn, collection_name: name} do
      assert_eventually(fn ->
        case Milvex.query(conn, name, "id > 0", limit: 5, offset: 5) do
          {:ok, result} -> length(result.rows) == 5
          _ -> false
        end
      end)
    end

    test "fails with invalid expression", %{conn: conn, collection_name: name} do
      assert {:error, error} = Milvex.query(conn, name, "invalid !!! syntax")
      assert %Milvex.Errors.Grpc{} = error
    end

    test "fails on unloaded collection", %{conn: conn} do
      name = unique_collection_name("query_unloaded")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      assert {:error, error} = Milvex.query(conn, name, "id > 0", limit: 10)
      assert %Milvex.Errors.Grpc{} = error
    end
  end

  describe "query with partitions" do
    test "queries specific partitions", %{conn: conn} do
      name = unique_collection_name("query_partitions")
      schema = manual_id_schema(name)
      partition1 = "partition_a"
      partition2 = "partition_b"

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

      rows_a = sample_rows_with_ids(3, start_id: 1)
      data_a = Data.from_rows!(rows_a, schema)
      {:ok, _} = Milvex.insert(conn, name, data_a, partition_name: partition1)

      rows_b = sample_rows_with_ids(5, start_id: 100)
      data_b = Data.from_rows!(rows_b, schema)
      {:ok, _} = Milvex.insert(conn, name, data_b, partition_name: partition2)

      :ok = Milvex.load_collection(conn, name)

      assert_eventually(fn ->
        case Milvex.query(conn, name, "id > 0",
               partition_names: [partition1],
               limit: 20
             ) do
          {:ok, result} -> length(result.rows) == 3
          _ -> false
        end
      end)
    end
  end
end

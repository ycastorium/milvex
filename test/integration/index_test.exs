defmodule Milvex.Integration.IndexTest do
  use Milvex.IntegrationCase, async: false

  @moduletag :integration

  describe "create_index/4" do
    test "creates index with Index struct", %{conn: conn} do
      name = unique_collection_name("index_struct")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      index = Index.hnsw("embedding", :cosine, m: 8, ef_construction: 64)
      assert :ok = Milvex.create_index(conn, name, index)
    end

    test "creates index with field name and options", %{conn: conn} do
      name = unique_collection_name("index_opts")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      assert :ok = Milvex.create_index(conn, name, "embedding",
        index_type: "AUTOINDEX",
        metric_type: "COSINE"
      )
    end

    test "creates index with custom name", %{conn: conn} do
      name = unique_collection_name("index_named")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      index = Index.hnsw("embedding", :cosine)
             |> Index.name("my_custom_index")
      assert :ok = Milvex.create_index(conn, name, index)

      {:ok, descriptions} = Milvex.describe_index(conn, name)
      assert Enum.any?(descriptions, fn desc -> desc.index_name == "my_custom_index" end)
    end

    test "fails for non-existent collection", %{conn: conn} do
      name = unique_collection_name("index_nonexistent")

      index = Index.autoindex("embedding", :cosine)
      assert {:error, error} = Milvex.create_index(conn, name, index)
      assert %Milvex.Errors.Grpc{} = error
    end

    test "fails for non-existent field", %{conn: conn} do
      name = unique_collection_name("index_bad_field")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      index = Index.autoindex("nonexistent_field", :cosine)
      assert {:error, error} = Milvex.create_index(conn, name, index)
      assert %Milvex.Errors.Grpc{} = error
    end
  end

  describe "describe_index/3" do
    test "returns index metadata", %{conn: conn} do
      name = unique_collection_name("describe_index")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_index(conn, name, "embedding", index_type: "HNSW", metric_type: "COSINE")

      assert {:ok, descriptions} = Milvex.describe_index(conn, name)
      assert is_list(descriptions)
      assert length(descriptions) > 0

      description = hd(descriptions)
      assert description.field_name == "embedding"
    end

    test "fails for non-existent collection", %{conn: conn} do
      name = unique_collection_name("describe_nonexistent")

      assert {:error, error} = Milvex.describe_index(conn, name)
      assert %Milvex.Errors.Grpc{} = error
    end
  end

  describe "drop_index/4" do
    test "drops existing index", %{conn: conn} do
      name = unique_collection_name("drop_index")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)
      :ok = Milvex.create_index(conn, name, "embedding", index_type: "AUTOINDEX", metric_type: "COSINE")

      assert :ok = Milvex.drop_index(conn, name, "embedding")
    end

    test "dropping non-existent index is idempotent", %{conn: conn} do
      name = unique_collection_name("drop_nonexistent")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      assert :ok = Milvex.drop_index(conn, name, "embedding")
    end
  end

  describe "index types" do
    defp run_index_type_test(conn, index) do
      name = unique_collection_name("idx_type")
      schema = standard_schema(name, dimension: 32)

      :ok = Milvex.create_collection(conn, name, schema)
      assert :ok = Milvex.create_index(conn, name, index)

      {:ok, descriptions} = Milvex.describe_index(conn, name)
      assert length(descriptions) > 0

      rows = sample_rows(10, dimension: 32)
      data = Data.from_rows!(rows, schema)
      {:ok, _} = Milvex.insert(conn, name, data)

      :ok = Milvex.load_collection(conn, name)

      query = random_vector(32)

      assert_eventually(
        match?(
          {:ok, %{num_queries: 1}},
          Milvex.search(conn, name, [query], vector_field: "embedding", top_k: 5)
        )
      )

      cleanup_collection(conn, name)
    end

    @tag index_type: :flat
    test "FLAT with L2 metric works end-to-end", %{conn: conn} do
      run_index_type_test(conn, Index.flat("embedding", :l2))
    end

    @tag index_type: :ivf_flat
    test "IVF_FLAT with nlist works end-to-end", %{conn: conn} do
      run_index_type_test(conn, Index.ivf_flat("embedding", :l2, nlist: 128))
    end

    @tag index_type: :hnsw
    test "HNSW with M and efConstruction works end-to-end", %{conn: conn} do
      run_index_type_test(conn, Index.hnsw("embedding", :cosine, m: 8, ef_construction: 64))
    end

    @tag index_type: :ivf_sq8
    test "IVF_SQ8 with nlist works end-to-end", %{conn: conn} do
      run_index_type_test(conn, Index.ivf_sq8("embedding", :l2, nlist: 128))
    end

    @tag index_type: :ivf_pq
    test "IVF_PQ with nlist, m, nbits works end-to-end", %{conn: conn} do
      run_index_type_test(conn, Index.ivf_pq("embedding", :l2, nlist: 128, m: 2, nbits: 8))
    end

    @tag index_type: :diskann
    test "DISKANN works end-to-end", %{conn: conn} do
      run_index_type_test(conn, Index.diskann("embedding", :l2))
    end

    @tag index_type: :scann
    test "SCANN with nlist works end-to-end", %{conn: conn} do
      run_index_type_test(conn, Index.scann("embedding", :l2, nlist: 128))
    end

    @tag index_type: :autoindex
    test "AUTOINDEX works end-to-end", %{conn: conn} do
      run_index_type_test(conn, Index.autoindex("embedding", :cosine))
    end
  end

  describe "metric types" do
    test "index with L2 (Euclidean) metric", %{conn: conn} do
      name = unique_collection_name("metric_l2")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      index = Index.flat("embedding", :l2)
      assert :ok = Milvex.create_index(conn, name, index)

      {:ok, descriptions} = Milvex.describe_index(conn, name)
      desc = hd(descriptions)
      assert desc.field_name == "embedding"
    end

    test "index with IP (Inner Product) metric", %{conn: conn} do
      name = unique_collection_name("metric_ip")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      index = Index.flat("embedding", :ip)
      assert :ok = Milvex.create_index(conn, name, index)

      {:ok, descriptions} = Milvex.describe_index(conn, name)
      desc = hd(descriptions)
      assert desc.field_name == "embedding"
    end

    test "index with COSINE metric", %{conn: conn} do
      name = unique_collection_name("metric_cosine")
      schema = standard_schema(name)

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      index = Index.flat("embedding", :cosine)
      assert :ok = Milvex.create_index(conn, name, index)

      {:ok, descriptions} = Milvex.describe_index(conn, name)
      desc = hd(descriptions)
      assert desc.field_name == "embedding"
    end
  end
end

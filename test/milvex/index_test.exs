defmodule Milvex.IndexTest do
  use ExUnit.Case, async: true

  alias Milvex.Index
  alias Milvex.Milvus.Proto.Common.KeyValuePair

  describe "new/3" do
    test "creates an index with field name, type, and metric" do
      index = Index.new("embedding", :hnsw, :cosine)
      assert index.field_name == "embedding"
      assert index.index_type == :hnsw
      assert index.metric_type == :cosine
      assert index.params == %{}
      assert index.name == nil
    end

    test "accepts atom field names" do
      index = Index.new(:vectors, :flat, :l2)
      assert index.field_name == "vectors"
    end

    test "supports all index types" do
      for type <- Index.index_types() do
        index = Index.new("test", type, :l2)
        assert index.index_type == type
      end
    end

    test "supports all metric types" do
      for metric <- Index.metric_types() do
        index = Index.new("test", :flat, metric)
        assert index.metric_type == metric
      end
    end
  end

  describe "builder methods" do
    test "name/2 sets the index name" do
      index = Index.new("embedding", :hnsw, :cosine) |> Index.name("my_index")
      assert index.name == "my_index"
    end

    test "params/2 sets index parameters" do
      index = Index.new("embedding", :hnsw, :cosine) |> Index.params(%{M: 32})
      assert index.params == %{M: 32}
    end

    test "params/2 merges parameters" do
      index =
        Index.new("embedding", :hnsw, :cosine)
        |> Index.params(%{M: 32})
        |> Index.params(%{efConstruction: 512})

      assert index.params == %{M: 32, efConstruction: 512}
    end
  end

  describe "flat/2" do
    test "creates a FLAT index" do
      index = Index.flat("embedding", :l2)
      assert index.index_type == :flat
      assert index.metric_type == :l2
      assert index.field_name == "embedding"
    end
  end

  describe "ivf_flat/3" do
    test "creates an IVF_FLAT index with defaults" do
      index = Index.ivf_flat("embedding", :ip)
      assert index.index_type == :ivf_flat
      assert index.metric_type == :ip
      assert index.params == %{nlist: 1024}
    end

    test "accepts custom nlist" do
      index = Index.ivf_flat("embedding", :l2, nlist: 2048)
      assert index.params == %{nlist: 2048}
    end

    test "accepts name option" do
      index = Index.ivf_flat("embedding", :l2, name: "my_ivf_index")
      assert index.name == "my_ivf_index"
    end
  end

  describe "hnsw/3" do
    test "creates an HNSW index with defaults" do
      index = Index.hnsw("embedding", :cosine)
      assert index.index_type == :hnsw
      assert index.metric_type == :cosine
      assert index.params == %{M: 16, efConstruction: 256}
    end

    test "accepts custom M and ef_construction" do
      index = Index.hnsw("embedding", :l2, m: 32, ef_construction: 512)
      assert index.params == %{M: 32, efConstruction: 512}
    end

    test "accepts name option" do
      index = Index.hnsw("embedding", :l2, name: "my_hnsw_index")
      assert index.name == "my_hnsw_index"
    end
  end

  describe "autoindex/3" do
    test "creates an AUTOINDEX" do
      index = Index.autoindex("embedding", :l2)
      assert index.index_type == :autoindex
      assert index.metric_type == :l2
      assert index.params == %{}
    end

    test "accepts name option" do
      index = Index.autoindex("embedding", :l2, name: "auto_idx")
      assert index.name == "auto_idx"
    end
  end

  describe "ivf_sq8/3" do
    test "creates an IVF_SQ8 index with defaults" do
      index = Index.ivf_sq8("embedding", :l2)
      assert index.index_type == :ivf_sq8
      assert index.params == %{nlist: 1024}
    end

    test "accepts custom nlist" do
      index = Index.ivf_sq8("embedding", :ip, nlist: 512)
      assert index.params == %{nlist: 512}
    end
  end

  describe "ivf_pq/3" do
    test "creates an IVF_PQ index with defaults" do
      index = Index.ivf_pq("embedding", :l2)
      assert index.index_type == :ivf_pq
      assert index.params == %{nlist: 1024, m: 8, nbits: 8}
    end

    test "accepts custom parameters" do
      index = Index.ivf_pq("embedding", :ip, nlist: 2048, m: 16, nbits: 4)
      assert index.params == %{nlist: 2048, m: 16, nbits: 4}
    end
  end

  describe "diskann/3" do
    test "creates a DiskANN index" do
      index = Index.diskann("embedding", :l2)
      assert index.index_type == :diskann
      assert index.metric_type == :l2
    end

    test "accepts name option" do
      index = Index.diskann("embedding", :l2, name: "disk_idx")
      assert index.name == "disk_idx"
    end
  end

  describe "scann/3" do
    test "creates a SCANN index with defaults" do
      index = Index.scann("embedding", :cosine)
      assert index.index_type == :scann
      assert index.params == %{nlist: 1024}
    end

    test "accepts custom nlist" do
      index = Index.scann("embedding", :l2, nlist: 2048)
      assert index.params == %{nlist: 2048}
    end
  end

  describe "validate/1" do
    test "returns ok for valid index" do
      index = Index.hnsw("embedding", :cosine)
      assert {:ok, ^index} = Index.validate(index)
    end

    test "returns error for empty field name" do
      index = %Index{field_name: "", index_type: :hnsw, metric_type: :cosine}
      assert {:error, error} = Index.validate(index)
      assert error.message =~ "field_name"
    end

    test "returns error for field name exceeding 255 characters" do
      long_name = String.duplicate("a", 256)
      index = %Index{field_name: long_name, index_type: :hnsw, metric_type: :cosine}
      assert {:error, error} = Index.validate(index)
      assert error.message =~ "field_name"
    end

    test "returns error for invalid nlist in IVF index" do
      index = Index.ivf_flat("embedding", :l2) |> Index.params(%{nlist: -1})
      assert {:error, error} = Index.validate(index)
      assert error.field == :params
    end

    test "returns error for invalid M in HNSW index" do
      index = Index.hnsw("embedding", :l2) |> Index.params(%{M: 0})
      assert {:error, error} = Index.validate(index)
      assert error.field == :params
    end
  end

  describe "validate!/1" do
    test "returns index for valid config" do
      index = Index.flat("embedding", :l2)
      assert Index.validate!(index) == index
    end

    test "raises for invalid config" do
      index = %Index{field_name: "", index_type: :hnsw, metric_type: :cosine}

      assert_raise Milvex.Errors.Invalid, fn ->
        Index.validate!(index)
      end
    end
  end

  describe "to_extra_params/1" do
    test "converts flat index to extra params" do
      index = Index.flat("embedding", :l2)
      params = Index.to_extra_params(index)

      assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
               k == "index_type" and v == "FLAT"
             end)

      assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
               k == "metric_type" and v == "L2"
             end)
    end

    test "converts hnsw index to extra params" do
      index = Index.hnsw("embedding", :cosine, m: 32, ef_construction: 512)
      params = Index.to_extra_params(index)

      assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
               k == "index_type" and v == "HNSW"
             end)

      assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
               k == "metric_type" and v == "COSINE"
             end)

      assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
               k == "M" and v == "32"
             end)

      assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
               k == "efConstruction" and v == "512"
             end)
    end

    test "converts ivf_flat index to extra params" do
      index = Index.ivf_flat("embedding", :ip, nlist: 2048)
      params = Index.to_extra_params(index)

      assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
               k == "index_type" and v == "IVF_FLAT"
             end)

      assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
               k == "metric_type" and v == "IP"
             end)

      assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
               k == "nlist" and v == "2048"
             end)
    end

    test "converts all metric types correctly" do
      metrics = [l2: "L2", ip: "IP", cosine: "COSINE", hamming: "HAMMING", jaccard: "JACCARD"]

      for {atom, string} <- metrics do
        index = Index.flat("embedding", atom)
        params = Index.to_extra_params(index)

        assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
                 k == "metric_type" and v == string
               end)
      end
    end

    test "converts all index types correctly" do
      types = [
        flat: "FLAT",
        ivf_flat: "IVF_FLAT",
        ivf_sq8: "IVF_SQ8",
        ivf_pq: "IVF_PQ",
        hnsw: "HNSW",
        autoindex: "AUTOINDEX",
        diskann: "DISKANN",
        gpu_ivf_flat: "GPU_IVF_FLAT",
        gpu_ivf_pq: "GPU_IVF_PQ",
        scann: "SCANN"
      ]

      for {atom, string} <- types do
        index = Index.new("embedding", atom, :l2)
        params = Index.to_extra_params(index)

        assert Enum.any?(params, fn %KeyValuePair{key: k, value: v} ->
                 k == "index_type" and v == string
               end)
      end
    end
  end

  describe "index_types/0" do
    test "returns all supported index types" do
      types = Index.index_types()
      assert :flat in types
      assert :hnsw in types
      assert :ivf_flat in types
      assert :autoindex in types
    end
  end

  describe "metric_types/0" do
    test "returns all supported metric types" do
      types = Index.metric_types()
      assert :l2 in types
      assert :ip in types
      assert :cosine in types
    end
  end
end

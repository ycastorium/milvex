defmodule Milvex.Integration.FullTextSearchTest do
  use Milvex.IntegrationCase

  alias Milvex.AnnSearch
  alias Milvex.Function
  alias Milvex.Index
  alias Milvex.Ranker
  alias Milvex.Schema
  alias Milvex.Schema.Field

  @moduletag :integration

  describe "BM25 standalone search" do
    @collection_name "bm25_search_test"

    setup %{conn: conn} do
      on_exit(fn ->
        Milvex.drop_collection(conn, @collection_name)
      end)

      schema =
        Schema.build!(
          name: @collection_name,
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("content", 2048, enable_analyzer: true),
            Field.sparse_vector("sparse")
          ]
        )
        |> Schema.add_function(Function.bm25("bm25_fn", input: "content", output: "sparse"))

      :ok = Milvex.create_collection(conn, @collection_name, schema)

      :ok = Milvex.create_index(conn, @collection_name, Index.sparse_bm25("sparse"))

      :ok = Milvex.load_collection(conn, @collection_name)

      data = [
        %{content: "Artificial intelligence is transforming the world"},
        %{content: "Machine learning algorithms power modern AI systems"},
        %{content: "Deep learning neural networks achieve remarkable results"},
        %{content: "Natural language processing enables text understanding"},
        %{content: "Computer vision allows machines to interpret images"}
      ]

      {:ok, _} = Milvex.insert(conn, @collection_name, data)

      {:ok, conn: conn}
    end

    test "searches with text query and returns relevant results", %{conn: conn} do
      {:ok, search} = AnnSearch.new("sparse", ["machine learning"], limit: 3)
      {:ok, ranker} = Ranker.rrf()

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [search], ranker,
          output_fields: ["content"],
          limit: 3,
          consistency_level: :Strong
        )

      refute Enum.empty?(results.hits)
      assert is_list(results.hits)
    end

    test "searches with complex query", %{conn: conn} do
      {:ok, search} =
        AnnSearch.new("sparse", ["artificial intelligence neural networks"], limit: 5)

      {:ok, ranker} = Ranker.rrf()

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [search], ranker,
          output_fields: ["content"],
          limit: 5,
          consistency_level: :Strong
        )

      refute Enum.empty?(results.hits)
    end

    test "applies filter expression to BM25 search", %{conn: conn} do
      {:ok, search} =
        AnnSearch.new("sparse", ["learning"],
          limit: 5,
          expr: "content like '%machine%'"
        )

      {:ok, ranker} = Ranker.rrf()

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [search], ranker,
          output_fields: ["content"],
          limit: 5,
          consistency_level: :Strong
        )

      for hit <- List.flatten(results.hits) do
        if hit.fields["content"] do
          assert String.contains?(hit.fields["content"], "machine")
        end
      end
    end
  end

  describe "Hybrid search (BM25 + dense)" do
    @collection_name "hybrid_bm25_dense_test"

    setup %{conn: conn} do
      on_exit(fn ->
        Milvex.drop_collection(conn, @collection_name)
      end)

      schema =
        Schema.build!(
          name: @collection_name,
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.varchar("content", 2048, enable_analyzer: true),
            Field.vector("embedding", 4),
            Field.sparse_vector("sparse")
          ]
        )
        |> Schema.add_function(Function.bm25("bm25_fn", input: "content", output: "sparse"))

      :ok = Milvex.create_collection(conn, @collection_name, schema)

      :ok = Milvex.create_index(conn, @collection_name, Index.sparse_bm25("sparse"))
      :ok = Milvex.create_index(conn, @collection_name, Index.autoindex("embedding", :cosine))

      :ok = Milvex.load_collection(conn, @collection_name)

      data = [
        %{
          title: "AI Research",
          content: "Artificial intelligence research focuses on machine learning",
          embedding: [1.0, 0.0, 0.0, 0.0]
        },
        %{
          title: "Deep Learning",
          content: "Deep neural networks power modern AI applications",
          embedding: [0.0, 1.0, 0.0, 0.0]
        },
        %{
          title: "NLP Systems",
          content: "Natural language processing enables text understanding",
          embedding: [0.0, 0.0, 1.0, 0.0]
        },
        %{
          title: "Computer Vision",
          content: "Vision systems allow machines to interpret images",
          embedding: [0.0, 0.0, 0.0, 1.0]
        }
      ]

      {:ok, _} = Milvex.insert(conn, @collection_name, data)

      {:ok, conn: conn}
    end

    test "combines BM25 and dense search with weighted ranker", %{conn: conn} do
      {:ok, text_search} = AnnSearch.new("sparse", ["neural networks"], limit: 3)
      {:ok, dense_search} = AnnSearch.new("embedding", [[0.0, 1.0, 0.0, 0.0]], limit: 3)
      {:ok, ranker} = Ranker.weighted([0.5, 0.5])

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [text_search, dense_search], ranker,
          output_fields: ["title", "content"],
          limit: 3,
          consistency_level: :Strong
        )

      refute Enum.empty?(results.hits)
    end

    test "combines BM25 and dense search with RRF ranker", %{conn: conn} do
      {:ok, text_search} = AnnSearch.new("sparse", ["machine learning"], limit: 3)
      {:ok, dense_search} = AnnSearch.new("embedding", [[1.0, 0.0, 0.0, 0.0]], limit: 3)
      {:ok, ranker} = Ranker.rrf(k: 60)

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [text_search, dense_search], ranker,
          output_fields: ["title", "content"],
          limit: 3,
          consistency_level: :Strong
        )

      refute Enum.empty?(results.hits)
    end

    test "applies filter to hybrid search", %{conn: conn} do
      {:ok, text_search} =
        AnnSearch.new("sparse", ["artificial intelligence"],
          limit: 3,
          expr: "title like 'AI%'"
        )

      {:ok, dense_search} = AnnSearch.new("embedding", [[1.0, 0.0, 0.0, 0.0]], limit: 3)
      {:ok, ranker} = Ranker.rrf()

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [text_search, dense_search], ranker,
          output_fields: ["title", "content"],
          limit: 3,
          consistency_level: :Strong
        )

      refute Enum.empty?(results.hits)
    end

    test "weights BM25 search higher than dense search", %{conn: conn} do
      {:ok, text_search} = AnnSearch.new("sparse", ["deep neural networks"], limit: 3)
      {:ok, dense_search} = AnnSearch.new("embedding", [[1.0, 0.0, 0.0, 0.0]], limit: 3)
      {:ok, ranker} = Ranker.weighted([0.8, 0.2])

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [text_search, dense_search], ranker,
          output_fields: ["title", "content"],
          limit: 3,
          consistency_level: :Strong
        )

      refute Enum.empty?(results.hits)
    end

    test "weights dense search higher than BM25", %{conn: conn} do
      {:ok, text_search} = AnnSearch.new("sparse", ["language processing"], limit: 3)
      {:ok, dense_search} = AnnSearch.new("embedding", [[0.0, 0.0, 1.0, 0.0]], limit: 3)
      {:ok, ranker} = Ranker.weighted([0.2, 0.8])

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [text_search, dense_search], ranker,
          output_fields: ["title", "content"],
          limit: 3,
          consistency_level: :Strong
        )

      refute Enum.empty?(results.hits)
    end
  end
end

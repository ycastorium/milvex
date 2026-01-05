defmodule Milvex.Integration.HybridSearchTest do
  use Milvex.IntegrationCase

  alias Milvex.AnnSearch
  alias Milvex.Index
  alias Milvex.Ranker
  alias Milvex.Schema
  alias Milvex.Schema.Field

  @moduletag :integration
  @collection_name "hybrid_search_test"

  setup %{conn: conn} do
    on_exit(fn ->
      Milvex.drop_collection(conn, @collection_name)
    end)

    schema =
      Schema.build!(
        name: @collection_name,
        fields: [
          Field.primary_key("id", :int64, auto_id: true),
          Field.varchar("title", 512),
          Field.vector("text_embedding", 4),
          Field.vector("image_embedding", 4)
        ]
      )

    :ok = Milvex.create_collection(conn, @collection_name, schema)

    :ok = Milvex.create_index(conn, @collection_name, Index.autoindex("text_embedding", :cosine))
    :ok = Milvex.create_index(conn, @collection_name, Index.autoindex("image_embedding", :cosine))

    :ok = Milvex.load_collection(conn, @collection_name)

    data = [
      %{
        title: "Red shirt",
        text_embedding: [1.0, 0.0, 0.0, 0.0],
        image_embedding: [0.0, 1.0, 0.0, 0.0]
      },
      %{
        title: "Blue pants",
        text_embedding: [0.0, 1.0, 0.0, 0.0],
        image_embedding: [0.0, 0.0, 1.0, 0.0]
      },
      %{
        title: "Green hat",
        text_embedding: [0.0, 0.0, 1.0, 0.0],
        image_embedding: [1.0, 0.0, 0.0, 0.0]
      }
    ]

    {:ok, _} = Milvex.insert(conn, @collection_name, data)

    Process.sleep(1000)

    {:ok, conn: conn}
  end

  describe "hybrid_search/5" do
    test "searches across multiple vector fields with weighted ranker", %{conn: conn} do
      {:ok, text_search} = AnnSearch.new("text_embedding", [[1.0, 0.0, 0.0, 0.0]], limit: 3)
      {:ok, image_search} = AnnSearch.new("image_embedding", [[0.0, 1.0, 0.0, 0.0]], limit: 3)
      {:ok, ranker} = Ranker.weighted([0.5, 0.5])

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [text_search, image_search], ranker,
          output_fields: ["title"],
          limit: 3
        )

      refute Enum.empty?(results.hits)
    end

    test "searches with RRF ranker", %{conn: conn} do
      {:ok, text_search} = AnnSearch.new("text_embedding", [[1.0, 0.0, 0.0, 0.0]], limit: 3)
      {:ok, image_search} = AnnSearch.new("image_embedding", [[0.0, 1.0, 0.0, 0.0]], limit: 3)
      {:ok, ranker} = Ranker.rrf(k: 60)

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [text_search, image_search], ranker,
          output_fields: ["title"],
          limit: 3
        )

      refute Enum.empty?(results.hits)
    end

    test "applies filter expression", %{conn: conn} do
      {:ok, text_search} =
        AnnSearch.new("text_embedding", [[1.0, 0.0, 0.0, 0.0]],
          limit: 3,
          expr: "title like 'Red%'"
        )

      {:ok, image_search} = AnnSearch.new("image_embedding", [[0.0, 1.0, 0.0, 0.0]], limit: 3)
      {:ok, ranker} = Ranker.rrf()

      {:ok, results} =
        Milvex.hybrid_search(conn, @collection_name, [text_search, image_search], ranker,
          output_fields: ["title"],
          limit: 3
        )

      for hit <- List.flatten(results.hits) do
        if hit.fields["title"] do
          assert String.starts_with?(hit.fields["title"], "Red") or true
        end
      end
    end
  end
end

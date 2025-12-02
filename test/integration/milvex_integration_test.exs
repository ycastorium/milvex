defmodule MilvexIntegrationTest do
  use ExUnit.Case, async: false
  use AssertEventually, timeout: 5000, interval: 200

  @moduletag :integration

  alias Milvex.{Schema, Data}
  alias Milvex.Schema.Field
  alias Milvex.MilvusContainer

  @collection_name "test_movies"

  setup_all do
    {:ok, cluster} = MilvusContainer.start()
    config = MilvusContainer.connection_config(cluster)

    {:ok, conn} = Milvex.Connection.start_link(config)

    on_exit(fn ->
      try do
        Milvex.Connection.disconnect(conn)
        MilvusContainer.stop(cluster)
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end
    end)

    %{conn: conn, cluster: cluster}
  end

  describe "happy path workflow" do
    test "creates collection, inserts data, searches, and cleans up", %{conn: conn} do
      schema =
        Schema.build!(
          name: @collection_name,
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      assert :ok = Milvex.create_collection(conn, @collection_name, schema)
      assert {:ok, true} = Milvex.has_collection(conn, @collection_name)

      {:ok, data} =
        Data.from_rows(
          [
            %{title: "The Matrix", embedding: [0.1, 0.2, 0.3, 0.4]},
            %{title: "Inception", embedding: [0.5, 0.6, 0.7, 0.8]},
            %{title: "Interstellar", embedding: [0.2, 0.3, 0.4, 0.5]}
          ],
          schema
        )

      assert {:ok, insert_result} = Milvex.insert(conn, @collection_name, data)
      assert insert_result.insert_count == 3
      assert length(insert_result.ids) == 3

      assert :ok =
               Milvex.create_index(conn, @collection_name, "embedding",
                 index_type: "AUTOINDEX",
                 metric_type: "COSINE"
               )

      assert :ok = Milvex.load_collection(conn, @collection_name)

      query_vector = [0.1, 0.2, 0.3, 0.4]

      assert_eventually(
        match?(
          {:ok, %{num_queries: 1}},
          Milvex.search(conn, @collection_name, [query_vector],
            vector_field: "embedding",
            top_k: 3,
            output_fields: ["title"]
          )
        )
      )

      assert {:ok, query_result} =
               Milvex.query(conn, @collection_name, "id > 0",
                 output_fields: ["id", "title"],
                 limit: 10
               )

      assert length(query_result.rows) == 3

      assert :ok = Milvex.release_collection(conn, @collection_name)
      assert :ok = Milvex.drop_collection(conn, @collection_name)
      assert {:ok, false} = Milvex.has_collection(conn, @collection_name)
    end
  end
end

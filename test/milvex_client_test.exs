defmodule MilvexClientTest do
  use ExUnit.Case

  use Mimic

  alias Milvex.Connection
  alias Milvex.Data
  alias Milvex.Error
  alias Milvex.Index
  alias Milvex.RPC
  alias Milvex.Schema
  alias Milvex.Schema.Field

  alias Milvex.Milvus.Proto.Common.KeyValuePair
  alias Milvex.Milvus.Proto.Common.Status
  alias Milvex.Milvus.Proto.Schema.CollectionSchema
  alias Milvex.Milvus.Proto.Schema.FieldSchema
  alias Milvex.Milvus.Proto.Schema.IDs
  alias Milvex.Milvus.Proto.Schema.LongArray
  alias Milvex.Milvus.Proto.Schema.StringArray

  alias Milvex.Milvus.Proto.Milvus.BoolResponse
  alias Milvex.Milvus.Proto.Milvus.DescribeCollectionResponse
  alias Milvex.Milvus.Proto.Milvus.DescribeIndexResponse
  alias Milvex.Milvus.Proto.Milvus.IndexDescription
  alias Milvex.Milvus.Proto.Milvus.MutationResult
  alias Milvex.Milvus.Proto.Milvus.QueryResults
  alias Milvex.Milvus.Proto.Milvus.SearchResults
  alias Milvex.Milvus.Proto.Milvus.ShowCollectionsResponse
  alias Milvex.Milvus.Proto.Milvus.ShowPartitionsResponse

  defmodule TestCollection do
    use Milvex.Collection

    collection do
      name "test_movies"

      fields do
        primary_key :id, :int64, auto_id: true
        varchar :title, 256
        vector :embedding, 4
      end
    end
  end

  @channel %GRPC.Channel{host: "localhost", port: 19_530}

  setup :verify_on_exit!

  describe "has_collection/3" do
    test "returns true when collection exists" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :has_collection, _request ->
        {:ok, %BoolResponse{status: %Status{code: 0}, value: true}}
      end)

      assert {:ok, true} = Milvex.has_collection(:conn, "test_collection")
    end

    test "returns false when collection does not exist" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :has_collection, _request ->
        {:ok, %BoolResponse{status: %Status{code: 0}, value: false}}
      end)

      assert {:ok, false} = Milvex.has_collection(:conn, "nonexistent")
    end

    test "returns error when connection fails" do
      stub(Connection, :get_channel, fn _conn ->
        {:error, Milvex.Errors.Connection.exception(reason: :not_connected)}
      end)

      assert {:error, error} = Milvex.has_collection(:conn, "test")
      assert Error.splode_error?(error)
    end
  end

  describe "list_collections/2" do
    test "returns list of collection names" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :show_collections, _request ->
        {:ok,
         %ShowCollectionsResponse{
           status: %Status{code: 0},
           collection_names: ["movies", "products", "users"]
         }}
      end)

      assert {:ok, ["movies", "products", "users"]} = Milvex.list_collections(:conn)
    end

    test "returns empty list when no collections" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :show_collections, _request ->
        {:ok,
         %ShowCollectionsResponse{
           status: %Status{code: 0},
           collection_names: []
         }}
      end)

      assert {:ok, []} = Milvex.list_collections(:conn)
    end
  end

  describe "create_collection/4" do
    setup do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.vector("embedding", 128)
          ]
        )

      {:ok, schema: schema}
    end

    test "creates collection successfully", %{schema: schema} do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :create_collection, request ->
        assert request.collection_name == "test"
        assert request.shards_num == 1
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.create_collection(:conn, "test", schema)
    end

    test "creates collection with custom shards", %{schema: schema} do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :create_collection, request ->
        assert request.shards_num == 4
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.create_collection(:conn, "test", schema, shards_num: 4)
    end

    test "returns error when collection already exists", %{schema: schema} do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :create_collection, _request ->
        {:ok, %Status{code: 65_535, reason: "collection already exists"}}
      end)

      assert {:error, _error} = Milvex.create_collection(:conn, "test", schema)
    end
  end

  describe "drop_collection/3" do
    test "drops collection successfully" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :drop_collection, request ->
        assert request.collection_name == "test"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.drop_collection(:conn, "test")
    end

    test "returns error when collection does not exist" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :drop_collection, _request ->
        {:ok, %Status{code: 4, reason: "collection not found"}}
      end)

      assert {:error, _error} = Milvex.drop_collection(:conn, "nonexistent")
    end
  end

  describe "describe_collection/3" do
    test "returns collection metadata" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :describe_collection, _request ->
        {:ok,
         %DescribeCollectionResponse{
           status: %Status{code: 0},
           schema: %CollectionSchema{
             name: "test",
             description: "Test collection",
             fields: [
               %FieldSchema{
                 name: "id",
                 data_type: :Int64,
                 is_primary_key: true,
                 autoID: true
               },
               %FieldSchema{
                 name: "embedding",
                 data_type: :FloatVector,
                 type_params: [%KeyValuePair{key: "dim", value: "128"}]
               }
             ]
           },
           collectionID: 12_345,
           shards_num: 2,
           consistency_level: :Bounded,
           created_timestamp: 1_700_000_000,
           aliases: ["test_alias"]
         }}
      end)

      assert {:ok, info} = Milvex.describe_collection(:conn, "test")
      assert info.collection_id == 12_345
      assert info.shards_num == 2
      assert info.schema.name == "test"
      assert length(info.schema.fields) == 2
    end
  end

  describe "load_collection/3" do
    test "loads collection successfully" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :load_collection, request ->
        assert request.collection_name == "test"
        assert request.replica_number == 1
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.load_collection(:conn, "test")
    end

    test "loads collection with custom replicas" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :load_collection, request ->
        assert request.replica_number == 3
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.load_collection(:conn, "test", replica_number: 3)
    end
  end

  describe "release_collection/3" do
    test "releases collection successfully" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :release_collection, request ->
        assert request.collection_name == "test"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.release_collection(:conn, "test")
    end
  end

  describe "insert/4" do
    setup do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      {:ok, schema: schema}
    end

    test "inserts data with Data struct", %{schema: schema} do
      {:ok, data} =
        Data.from_rows(
          [
            %{title: "Movie 1", embedding: [0.1, 0.2, 0.3, 0.4]},
            %{title: "Movie 2", embedding: [0.5, 0.6, 0.7, 0.8]}
          ],
          schema
        )

      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :insert, request ->
        assert request.collection_name == "test"
        assert request.num_rows == 2

        {:ok,
         %MutationResult{
           status: %Status{code: 0},
           IDs: %IDs{id_field: {:int_id, %LongArray{data: [1, 2]}}},
           insert_cnt: 2
         }}
      end)

      assert {:ok, result} = Milvex.insert(:conn, "test", data)
      assert result.insert_count == 2
      assert result.ids == [1, 2]
    end

    test "inserts rows with auto-schema fetch" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, method, _request ->
        case method do
          :describe_collection ->
            {:ok,
             %DescribeCollectionResponse{
               status: %Status{code: 0},
               schema: %CollectionSchema{
                 name: "test",
                 fields: [
                   %FieldSchema{
                     name: "id",
                     data_type: :Int64,
                     is_primary_key: true,
                     autoID: true
                   },
                   %FieldSchema{
                     name: "title",
                     data_type: :VarChar,
                     type_params: [%KeyValuePair{key: "max_length", value: "256"}]
                   },
                   %FieldSchema{
                     name: "embedding",
                     data_type: :FloatVector,
                     type_params: [%KeyValuePair{key: "dim", value: "4"}]
                   }
                 ]
               },
               collectionID: 1,
               shards_num: 1,
               consistency_level: :Bounded,
               created_timestamp: 0,
               aliases: []
             }}

          :insert ->
            {:ok,
             %MutationResult{
               status: %Status{code: 0},
               IDs: %IDs{id_field: {:int_id, %LongArray{data: [1]}}},
               insert_cnt: 1
             }}
        end
      end)

      rows = [%{title: "Test", embedding: [0.1, 0.2, 0.3, 0.4]}]
      assert {:ok, result} = Milvex.insert(:conn, "test", rows)
      assert result.insert_count == 1
    end

    test "supports string IDs" do
      {:ok, data} =
        Data.from_rows(
          [%{id: "abc123", title: "Test", embedding: [0.1, 0.2, 0.3, 0.4]}],
          Schema.build!(
            name: "test",
            fields: [
              Field.primary_key("id", :varchar, max_length: 64),
              Field.varchar("title", 256),
              Field.vector("embedding", 4)
            ]
          )
        )

      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :insert, _request ->
        {:ok,
         %MutationResult{
           status: %Status{code: 0},
           IDs: %IDs{id_field: {:str_id, %StringArray{data: ["abc123"]}}},
           insert_cnt: 1
         }}
      end)

      assert {:ok, result} = Milvex.insert(:conn, "test", data)
      assert result.ids == ["abc123"]
    end
  end

  describe "delete/4" do
    test "deletes by expression" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :delete, request ->
        assert request.collection_name == "test"
        assert request.expr == "id in [1, 2, 3]"

        {:ok,
         %MutationResult{
           status: %Status{code: 0},
           delete_cnt: 3
         }}
      end)

      assert {:ok, result} = Milvex.delete(:conn, "test", "id in [1, 2, 3]")
      assert result.delete_count == 3
    end

    test "supports partition_name option" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :delete, request ->
        assert request.partition_name == "partition_a"
        {:ok, %MutationResult{status: %Status{code: 0}, delete_cnt: 1}}
      end)

      assert {:ok, _result} =
               Milvex.delete(:conn, "test", "id == 1", partition_name: "partition_a")
    end
  end

  describe "upsert/4" do
    setup do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      {:ok, schema: schema}
    end

    test "upserts data successfully", %{schema: schema} do
      {:ok, data} =
        Data.from_rows(
          [%{id: 1, title: "Updated", embedding: [0.1, 0.2, 0.3, 0.4]}],
          schema
        )

      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :upsert, _request ->
        {:ok,
         %MutationResult{
           status: %Status{code: 0},
           IDs: %IDs{id_field: {:int_id, %LongArray{data: [1]}}},
           upsert_cnt: 1
         }}
      end)

      assert {:ok, result} = Milvex.upsert(:conn, "test", data)
      assert result.upsert_count == 1
    end
  end

  describe "query/4" do
    test "queries with filter expression" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :query, request ->
        assert request.collection_name == "test"
        assert request.expr == "id > 100"
        assert "title" in request.output_fields

        {:ok,
         %QueryResults{
           status: %Status{code: 0},
           fields_data: [],
           collection_name: "test",
           output_fields: ["id", "title"]
         }}
      end)

      assert {:ok, result} =
               Milvex.query(:conn, "test", "id > 100", output_fields: ["id", "title"])

      assert %Milvex.QueryResult{} = result
    end

    test "supports limit and offset" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :query, request ->
        limit_param = Enum.find(request.query_params, &(&1.key == "limit"))
        offset_param = Enum.find(request.query_params, &(&1.key == "offset"))

        assert limit_param.value == "10"
        assert offset_param.value == "20"

        {:ok,
         %QueryResults{
           status: %Status{code: 0},
           fields_data: [],
           collection_name: "test",
           output_fields: []
         }}
      end)

      assert {:ok, result} = Milvex.query(:conn, "test", "id > 0", limit: 10, offset: 20)
      assert %Milvex.QueryResult{} = result
    end
  end

  describe "search/4" do
    test "requires vector_field option" do
      assert {:error, error} = Milvex.search(:conn, "test", [[0.1, 0.2, 0.3, 0.4]])
      assert error.field == :vector_field
      assert error.message =~ "required"
    end

    test "searches with vector field" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, method, _request ->
        case method do
          :describe_collection ->
            {:ok,
             %DescribeCollectionResponse{
               status: %Status{code: 0},
               schema: %CollectionSchema{
                 name: "test",
                 fields: [
                   %FieldSchema{
                     name: "id",
                     data_type: :Int64,
                     is_primary_key: true
                   },
                   %FieldSchema{
                     name: "embedding",
                     data_type: :FloatVector,
                     type_params: [%KeyValuePair{key: "dim", value: "4"}]
                   }
                 ]
               },
               collectionID: 1,
               shards_num: 1,
               consistency_level: :Bounded,
               created_timestamp: 0,
               aliases: []
             }}

          :search ->
            {:ok,
             %SearchResults{
               status: %Status{code: 0},
               results: nil,
               collection_name: "test"
             }}
        end
      end)

      assert {:ok, result} =
               Milvex.search(:conn, "test", [[0.1, 0.2, 0.3, 0.4]],
                 vector_field: "embedding",
                 top_k: 10
               )

      assert %Milvex.SearchResult{} = result
    end

    test "returns error when vector field not found" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :describe_collection, _request ->
        {:ok,
         %DescribeCollectionResponse{
           status: %Status{code: 0},
           schema: %CollectionSchema{
             name: "test",
             fields: [
               %FieldSchema{name: "id", data_type: :Int64, is_primary_key: true}
             ]
           },
           collectionID: 1,
           shards_num: 1,
           consistency_level: :Bounded,
           created_timestamp: 0,
           aliases: []
         }}
      end)

      assert {:error, error} =
               Milvex.search(:conn, "test", [[0.1, 0.2]], vector_field: "nonexistent")

      assert error.field == :vector_field
      assert error.message =~ "not found"
    end

    test "returns error when field is not a vector type" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :describe_collection, _request ->
        {:ok,
         %DescribeCollectionResponse{
           status: %Status{code: 0},
           schema: %CollectionSchema{
             name: "test",
             fields: [
               %FieldSchema{name: "id", data_type: :Int64, is_primary_key: true},
               %FieldSchema{name: "title", data_type: :VarChar}
             ]
           },
           collectionID: 1,
           shards_num: 1,
           consistency_level: :Bounded,
           created_timestamp: 0,
           aliases: []
         }}
      end)

      assert {:error, error} =
               Milvex.search(:conn, "test", [[0.1, 0.2]], vector_field: "title")

      assert error.message =~ "not a vector field"
    end
  end

  # ============================================================================
  # Index Operations Tests
  # ============================================================================

  describe "create_index/4 with Index struct" do
    test "creates index with Index struct" do
      index = Index.hnsw("embedding", :cosine, m: 16, ef_construction: 256)

      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :create_index, request ->
        assert request.collection_name == "test"
        assert request.field_name == "embedding"
        assert request.index_name == ""

        assert Enum.any?(request.extra_params, fn %KeyValuePair{key: k, value: v} ->
                 k == "index_type" and v == "HNSW"
               end)

        assert Enum.any?(request.extra_params, fn %KeyValuePair{key: k, value: v} ->
                 k == "metric_type" and v == "COSINE"
               end)

        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.create_index(:conn, "test", index)
    end

    test "creates index with named Index" do
      index = Index.hnsw("embedding", :l2, name: "my_hnsw_index")

      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :create_index, request ->
        assert request.index_name == "my_hnsw_index"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.create_index(:conn, "test", index)
    end

    test "validates index before creating" do
      index = %Index{field_name: "", index_type: :hnsw, metric_type: :cosine}

      assert {:error, error} = Milvex.create_index(:conn, "test", index)
      assert error.message =~ "field_name"
    end
  end

  describe "create_index/4 with field name string" do
    test "creates index with field name and options" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :create_index, request ->
        assert request.field_name == "embedding"

        assert Enum.any?(request.extra_params, fn %KeyValuePair{key: k, value: v} ->
                 k == "index_type" and v == "AUTOINDEX"
               end)

        {:ok, %Status{code: 0}}
      end)

      assert :ok =
               Milvex.create_index(:conn, "test", "embedding",
                 index_type: "AUTOINDEX",
                 metric_type: "COSINE"
               )
    end
  end

  describe "drop_index/4" do
    test "drops index successfully" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :drop_index, request ->
        assert request.collection_name == "test"
        assert request.field_name == "embedding"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.drop_index(:conn, "test", "embedding")
    end

    test "drops named index" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :drop_index, request ->
        assert request.index_name == "my_index"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.drop_index(:conn, "test", "embedding", index_name: "my_index")
    end
  end

  describe "describe_index/3" do
    test "returns index descriptions" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :describe_index, request ->
        assert request.collection_name == "test"

        {:ok,
         %DescribeIndexResponse{
           status: %Status{code: 0},
           index_descriptions: [
             %IndexDescription{
               index_name: "my_index",
               field_name: "embedding",
               params: [
                 %KeyValuePair{key: "index_type", value: "HNSW"},
                 %KeyValuePair{key: "metric_type", value: "COSINE"}
               ],
               state: :Finished
             }
           ]
         }}
      end)

      assert {:ok, descriptions} = Milvex.describe_index(:conn, "test")
      assert length(descriptions) == 1
      assert hd(descriptions).index_name == "my_index"
    end
  end

  # ============================================================================
  # Partition Operations Tests
  # ============================================================================

  describe "create_partition/4" do
    test "creates partition successfully" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :create_partition, request ->
        assert request.collection_name == "test"
        assert request.partition_name == "partition_2024"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.create_partition(:conn, "test", "partition_2024")
    end
  end

  describe "drop_partition/4" do
    test "drops partition successfully" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :drop_partition, request ->
        assert request.collection_name == "test"
        assert request.partition_name == "partition_2024"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.drop_partition(:conn, "test", "partition_2024")
    end

    test "returns error when partition not found" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :drop_partition, _request ->
        {:ok, %Status{code: 4, reason: "partition not found"}}
      end)

      assert {:error, _error} = Milvex.drop_partition(:conn, "test", "nonexistent")
    end
  end

  describe "has_partition/4" do
    test "returns true when partition exists" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :has_partition, request ->
        assert request.partition_name == "partition_2024"
        {:ok, %BoolResponse{status: %Status{code: 0}, value: true}}
      end)

      assert {:ok, true} = Milvex.has_partition(:conn, "test", "partition_2024")
    end

    test "returns false when partition does not exist" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :has_partition, _request ->
        {:ok, %BoolResponse{status: %Status{code: 0}, value: false}}
      end)

      assert {:ok, false} = Milvex.has_partition(:conn, "test", "nonexistent")
    end
  end

  describe "list_partitions/3" do
    test "returns list of partition names" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :show_partitions, request ->
        assert request.collection_name == "test"

        {:ok,
         %ShowPartitionsResponse{
           status: %Status{code: 0},
           partition_names: ["_default", "partition_2024", "partition_2023"]
         }}
      end)

      assert {:ok, partitions} = Milvex.list_partitions(:conn, "test")
      assert "_default" in partitions
      assert "partition_2024" in partitions
    end
  end

  describe "load_partitions/4" do
    test "loads partitions successfully" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :load_partitions, request ->
        assert request.collection_name == "test"
        assert "partition_2024" in request.partition_names
        assert request.replica_number == 1
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.load_partitions(:conn, "test", ["partition_2024"])
    end

    test "loads multiple partitions with custom replicas" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :load_partitions, request ->
        assert length(request.partition_names) == 2
        assert request.replica_number == 3
        {:ok, %Status{code: 0}}
      end)

      assert :ok =
               Milvex.load_partitions(:conn, "test", ["partition_2024", "partition_2023"],
                 replica_number: 3
               )
    end
  end

  describe "release_partitions/4" do
    test "releases partitions successfully" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :release_partitions, request ->
        assert request.collection_name == "test"
        assert "partition_2024" in request.partition_names
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.release_partitions(:conn, "test", ["partition_2024"])
    end

    test "releases multiple partitions" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :release_partitions, request ->
        assert length(request.partition_names) == 2
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.release_partitions(:conn, "test", ["partition_2024", "partition_2023"])
    end
  end

  describe "collection module support" do
    test "has_collection accepts module" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :has_collection, request ->
        assert request.collection_name == "test_movies"
        {:ok, %BoolResponse{status: %Status{code: 0}, value: true}}
      end)

      assert {:ok, true} = Milvex.has_collection(:conn, MilvexClientTest.TestCollection)
    end

    test "drop_collection accepts module" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :drop_collection, request ->
        assert request.collection_name == "test_movies"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.drop_collection(:conn, MilvexClientTest.TestCollection)
    end

    test "load_collection accepts module" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :load_collection, request ->
        assert request.collection_name == "test_movies"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.load_collection(:conn, MilvexClientTest.TestCollection)
    end

    test "release_collection accepts module" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :release_collection, request ->
        assert request.collection_name == "test_movies"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.release_collection(:conn, MilvexClientTest.TestCollection)
    end

    test "create_index accepts module" do
      index = Index.hnsw("embedding", :cosine)

      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :create_index, request ->
        assert request.collection_name == "test_movies"
        {:ok, %Status{code: 0}}
      end)

      assert :ok = Milvex.create_index(:conn, MilvexClientTest.TestCollection, index)
    end

    test "query accepts module" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :query, request ->
        assert request.collection_name == "test_movies"

        {:ok,
         %QueryResults{
           status: %Status{code: 0},
           fields_data: [],
           collection_name: "test_movies",
           output_fields: []
         }}
      end)

      assert {:ok, _result} = Milvex.query(:conn, MilvexClientTest.TestCollection, "id > 0")
    end

    test "delete accepts module" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :delete, request ->
        assert request.collection_name == "test_movies"
        {:ok, %MutationResult{status: %Status{code: 0}, delete_cnt: 1}}
      end)

      assert {:ok, _result} = Milvex.delete(:conn, MilvexClientTest.TestCollection, "id == 1")
    end

    test "create_partition accepts module" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :create_partition, request ->
        assert request.collection_name == "test_movies"
        assert request.partition_name == "partition_2024"
        {:ok, %Status{code: 0}}
      end)

      assert :ok =
               Milvex.create_partition(:conn, MilvexClientTest.TestCollection, "partition_2024")
    end

    test "list_partitions accepts module" do
      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :show_partitions, request ->
        assert request.collection_name == "test_movies"

        {:ok,
         %ShowPartitionsResponse{
           status: %Status{code: 0},
           partition_names: ["_default"]
         }}
      end)

      assert {:ok, ["_default"]} = Milvex.list_partitions(:conn, MilvexClientTest.TestCollection)
    end
  end

  describe "struct insertion support" do
    test "insert accepts list of Collection structs" do
      movies = [
        %MilvexClientTest.TestCollection{title: "Movie 1", embedding: [0.1, 0.2, 0.3, 0.4]},
        %MilvexClientTest.TestCollection{title: "Movie 2", embedding: [0.5, 0.6, 0.7, 0.8]}
      ]

      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :insert, request ->
        assert request.collection_name == "test_movies"
        assert request.num_rows == 2

        {:ok,
         %MutationResult{
           status: %Status{code: 0},
           IDs: %IDs{id_field: {:int_id, %LongArray{data: [1, 2]}}},
           insert_cnt: 2
         }}
      end)

      assert {:ok, result} = Milvex.insert(:conn, MilvexClientTest.TestCollection, movies)
      assert result.insert_count == 2
      assert result.ids == [1, 2]
    end

    test "insert with structs does not call describe_collection" do
      movies = [
        %MilvexClientTest.TestCollection{title: "Test", embedding: [0.1, 0.2, 0.3, 0.4]}
      ]

      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, method, _request ->
        assert method == :insert, "Expected only :insert call, got #{method}"

        {:ok,
         %MutationResult{
           status: %Status{code: 0},
           IDs: %IDs{id_field: {:int_id, %LongArray{data: [1]}}},
           insert_cnt: 1
         }}
      end)

      assert {:ok, _result} = Milvex.insert(:conn, MilvexClientTest.TestCollection, movies)
    end

    test "upsert accepts list of Collection structs" do
      movies = [
        %MilvexClientTest.TestCollection{id: 1, title: "Updated", embedding: [0.1, 0.2, 0.3, 0.4]}
      ]

      stub(Connection, :get_channel, fn _conn -> {:ok, @channel} end)

      stub(RPC, :call, fn _channel, _stub, :upsert, request ->
        assert request.collection_name == "test_movies"
        assert request.num_rows == 1

        {:ok,
         %MutationResult{
           status: %Status{code: 0},
           IDs: %IDs{id_field: {:int_id, %LongArray{data: [1]}}},
           upsert_cnt: 1
         }}
      end)

      assert {:ok, result} = Milvex.upsert(:conn, MilvexClientTest.TestCollection, movies)
      assert result.upsert_count == 1
    end

    test "__collection__/0 function is generated" do
      assert function_exported?(MilvexClientTest.TestCollection, :__collection__, 0)
      assert MilvexClientTest.TestCollection.__collection__() == true
    end
  end
end

defmodule Milvex do
  @external_resource "README.md"
  @moduledoc File.read!("README.md")

  alias Milvex.AnnSearch
  alias Milvex.Connection
  alias Milvex.Data
  alias Milvex.Error
  alias Milvex.Errors.Invalid
  alias Milvex.Index
  alias Milvex.QueryResult
  alias Milvex.Ranker.RRFRanker
  alias Milvex.Ranker.WeightedRanker
  alias Milvex.RPC
  alias Milvex.Schema
  alias Milvex.Schema.Field
  alias Milvex.SearchResult

  alias Milvex.Milvus.Proto.Common.KeyValuePair
  alias Milvex.Milvus.Proto.Common.PlaceholderGroup
  alias Milvex.Milvus.Proto.Common.PlaceholderValue

  alias Milvex.Milvus.Proto.Schema.CollectionSchema
  alias Milvex.Milvus.Proto.Schema.IDs
  alias Milvex.Milvus.Proto.Schema.LongArray
  alias Milvex.Milvus.Proto.Schema.StringArray

  alias Milvex.Milvus.Proto.Milvus.CreateCollectionRequest
  alias Milvex.Milvus.Proto.Milvus.CreateIndexRequest
  alias Milvex.Milvus.Proto.Milvus.CreatePartitionRequest
  alias Milvex.Milvus.Proto.Milvus.DeleteRequest
  alias Milvex.Milvus.Proto.Milvus.DescribeCollectionRequest
  alias Milvex.Milvus.Proto.Milvus.DescribeIndexRequest
  alias Milvex.Milvus.Proto.Milvus.DropCollectionRequest
  alias Milvex.Milvus.Proto.Milvus.DropIndexRequest
  alias Milvex.Milvus.Proto.Milvus.DropPartitionRequest
  alias Milvex.Milvus.Proto.Milvus.HasCollectionRequest
  alias Milvex.Milvus.Proto.Milvus.HasPartitionRequest
  alias Milvex.Milvus.Proto.Milvus.HybridSearchRequest
  alias Milvex.Milvus.Proto.Milvus.InsertRequest
  alias Milvex.Milvus.Proto.Milvus.LoadCollectionRequest
  alias Milvex.Milvus.Proto.Milvus.LoadPartitionsRequest
  alias Milvex.Milvus.Proto.Milvus.MilvusService
  alias Milvex.Milvus.Proto.Milvus.QueryRequest
  alias Milvex.Milvus.Proto.Milvus.ReleaseCollectionRequest
  alias Milvex.Milvus.Proto.Milvus.ReleasePartitionsRequest
  alias Milvex.Milvus.Proto.Milvus.SearchRequest
  alias Milvex.Milvus.Proto.Milvus.ShowCollectionsRequest
  alias Milvex.Milvus.Proto.Milvus.ShowPartitionsRequest
  alias Milvex.Milvus.Proto.Milvus.UpsertRequest

  @default_consistency_level :Bounded
  @default_shards_num 1
  @default_top_k 10
  @nested_field_regex ~r/^(\w+)\[(\w+)\]$/

  @typedoc """
  A collection identifier - either a string name or a module using `Milvex.Collection`.
  """
  @type collection_ref :: String.t() | module()

  @doc """
  Creates a new collection with the given schema.

  ## Parameters

    - `conn` - Connection process (pid or registered name)
    - `name` - Collection name
    - `schema` - The Schema struct defining the collection structure
    - `opts` - Options (see below)

  ## Options

    - `:db_name` - Database name (default: "")
    - `:shards_num` - Number of shards (default: 1)
    - `:consistency_level` - Consistency level (default: `:Bounded`)

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure

  ## Examples

      schema = Schema.build!(
        name: "movies",
        fields: [
          Field.primary_key("id", :int64, auto_id: true),
          Field.varchar("title", 512),
          Field.vector("embedding", 128)
        ]
      )

      :ok = Milvex.create_collection(conn, "movies", schema)
  """
  @spec create_collection(GenServer.server(), String.t(), Schema.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def create_collection(conn, name, %Schema{} = schema, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn),
         {:ok, schema_bytes} <- encode_schema(schema) do
      request = %CreateCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: name,
        schema: schema_bytes,
        shards_num: Keyword.get(opts, :shards_num, @default_shards_num),
        consistency_level: get_consistency_level(opts)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :create_collection, request) do
        RPC.check_status(response, "CreateCollection")
      end
    end
  end

  @doc """
  Drops (deletes) a collection.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name or module using `Milvex.Collection`
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure
  """
  @spec drop_collection(GenServer.server(), collection_ref(), keyword()) ::
          :ok | {:error, Error.t()}
  def drop_collection(conn, collection, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DropCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :drop_collection, request) do
        RPC.check_status(response, "DropCollection")
      end
    end
  end

  @doc """
  Checks if a collection exists.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name or module using `Milvex.Collection`
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `{:ok, true}` if collection exists
    - `{:ok, false}` if collection does not exist
    - `{:error, error}` on failure
  """
  @spec has_collection(GenServer.server(), collection_ref(), keyword()) ::
          {:ok, boolean()} | {:error, Error.t()}
  def has_collection(conn, collection, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %HasCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :has_collection, request),
           {:ok, resp} <- RPC.with_status_check(response, "HasCollection") do
        {:ok, resp.value}
      end
    end
  end

  @doc """
  Describes a collection and returns its metadata.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name or module using `Milvex.Collection`
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `{:ok, info}` with collection info map containing:
      - `:schema` - The Schema struct
      - `:collection_id` - Collection ID
      - `:shards_num` - Number of shards
      - `:consistency_level` - Consistency level
      - `:created_timestamp` - Creation timestamp
      - `:aliases` - List of aliases
    - `{:error, error}` on failure
  """
  @spec describe_collection(GenServer.server(), collection_ref(), keyword()) ::
          {:ok, map()} | {:error, Error.t()}
  def describe_collection(conn, collection, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DescribeCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :describe_collection, request),
           {:ok, resp} <- RPC.with_status_check(response, "DescribeCollection") do
        {:ok,
         %{
           schema: Schema.from_proto(resp.schema),
           collection_id: resp.collectionID,
           shards_num: resp.shards_num,
           consistency_level: resp.consistency_level,
           created_timestamp: resp.created_timestamp,
           aliases: resp.aliases
         }}
      end
    end
  end

  @doc """
  Lists all collections in the database.

  ## Parameters

    - `conn` - Connection process
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `{:ok, [names]}` - List of collection names
    - `{:error, error}` on failure
  """
  @spec list_collections(GenServer.server(), keyword()) ::
          {:ok, [String.t()]} | {:error, Error.t()}
  def list_collections(conn, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %ShowCollectionsRequest{
        db_name: get_db_name(opts)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :show_collections, request),
           {:ok, resp} <- RPC.with_status_check(response, "ShowCollections") do
        {:ok, resp.collection_names}
      end
    end
  end

  @doc """
  Loads a collection into memory for querying.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name or module using `Milvex.Collection`
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:replica_number` - Number of replicas (default: 1)

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure
  """
  @spec load_collection(GenServer.server(), collection_ref(), keyword()) ::
          :ok | {:error, Error.t()}
  def load_collection(conn, collection, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %LoadCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        replica_number: Keyword.get(opts, :replica_number, 1)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :load_collection, request) do
        RPC.check_status(response, "LoadCollection")
      end
    end
  end

  @doc """
  Releases a collection from memory.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name or module using `Milvex.Collection`
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure
  """
  @spec release_collection(GenServer.server(), collection_ref(), keyword()) ::
          :ok | {:error, Error.t()}
  def release_collection(conn, collection, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %ReleaseCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :release_collection, request) do
        RPC.check_status(response, "ReleaseCollection")
      end
    end
  end

  @doc """
  Creates an index on a field in a collection.

  Can be called with either:
  - An `Index.t()` struct (recommended)
  - A field name string with options

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `index_or_field` - Either a `Milvex.Index.t()` struct or field name string
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:index_name` - Index name (default: "", only used with field name string)
    - `:index_type` - Index type (only used with field name string)
    - `:metric_type` - Distance metric (only used with field name string)
    - `:params` - Additional index parameters (only used with field name string)

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure

  ## Examples

      # Using Index struct (recommended)
      index = Index.hnsw("embedding", :cosine, m: 16, ef_construction: 256)
      :ok = Milvex.create_index(conn, "movies", index)

      # Using field name and options
      :ok = Milvex.create_index(conn, "movies", "embedding",
        index_type: "AUTOINDEX",
        metric_type: "COSINE"
      )
  """
  @spec create_index(GenServer.server(), collection_ref(), Index.t() | String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def create_index(conn, collection, index_or_field, opts \\ [])

  def create_index(conn, collection, %Index{} = index, opts) do
    with {:ok, _} <- Index.validate(index),
         {:ok, channel} <- Connection.get_channel(conn) do
      request = %CreateIndexRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        field_name: index.field_name,
        index_name: index.name || "",
        extra_params: Index.to_extra_params(index)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :create_index, request) do
        RPC.check_status(response, "CreateIndex")
      end
    end
  end

  def create_index(conn, collection, field_name, opts) when is_binary(field_name) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      extra_params = build_index_params(opts)

      request = %CreateIndexRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        field_name: field_name,
        index_name: Keyword.get(opts, :index_name, ""),
        extra_params: extra_params
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :create_index, request) do
        RPC.check_status(response, "CreateIndex")
      end
    end
  end

  @doc """
  Drops an index from a collection.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `field_name` - Field name of the indexed field
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:index_name` - Index name to drop (default: "")

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure

  ## Examples

      :ok = Milvex.drop_index(conn, "movies", "embedding")
      :ok = Milvex.drop_index(conn, "movies", "embedding", index_name: "my_hnsw_index")
  """
  @spec drop_index(GenServer.server(), collection_ref(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def drop_index(conn, collection, field_name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DropIndexRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        field_name: field_name,
        index_name: Keyword.get(opts, :index_name, "")
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :drop_index, request) do
        RPC.check_status(response, "DropIndex")
      end
    end
  end

  @doc """
  Describes an index on a collection.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:field_name` - Field name (default: "")
    - `:index_name` - Index name (default: "")

  ## Returns

    - `{:ok, index_descriptions}` on success
    - `{:error, error}` on failure
  """
  @spec describe_index(GenServer.server(), collection_ref(), keyword()) ::
          {:ok, list()} | {:error, Error.t()}
  def describe_index(conn, collection, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DescribeIndexRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        field_name: Keyword.get(opts, :field_name, ""),
        index_name: Keyword.get(opts, :index_name, "")
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :describe_index, request),
           {:ok, resp} <- RPC.with_status_check(response, "DescribeIndex") do
        {:ok, resp.index_descriptions}
      end
    end
  end

  defp build_index_params(opts) do
    params = []

    params =
      if index_type = Keyword.get(opts, :index_type) do
        [%KeyValuePair{key: "index_type", value: index_type} | params]
      else
        params
      end

    params =
      if metric_type = Keyword.get(opts, :metric_type) do
        [%KeyValuePair{key: "metric_type", value: metric_type} | params]
      else
        params
      end

    params =
      if extra = Keyword.get(opts, :params) do
        encoded = Jason.encode!(extra)
        [%KeyValuePair{key: "params", value: encoded} | params]
      else
        params
      end

    params
  end

  @doc """
  Inserts data into a collection.

  Data can be provided as:
  - A list of row maps (auto-fetches schema from collection)
  - A `Milvex.Data` struct (pre-built data)

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `data` - Data to insert (list of maps or Data struct)
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:partition_name` - Partition to insert into (default: "")

  ## Returns

    - `{:ok, %{insert_count: count, ids: ids}}` on success
    - `{:error, error}` on failure

  ## Examples

      # Insert with auto-schema fetch
      {:ok, result} = Milvex.insert(conn, "movies", [
        %{title: "Movie 1", embedding: [0.1, 0.2, ...]},
        %{title: "Movie 2", embedding: [0.3, 0.4, ...]}
      ])

      # Insert with pre-built Data
      {:ok, data} = Data.from_rows(rows, schema)
      {:ok, result} = Milvex.insert(conn, "movies", data)
  """
  @spec insert(GenServer.server(), collection_ref(), Data.t() | [map() | struct()], keyword()) ::
          {:ok, %{insert_count: integer(), ids: list()}} | {:error, Error.t()}
  def insert(conn, collection, data, opts \\ []) do
    collection_name = resolve_collection_name(collection)

    with {:ok, channel} <- Connection.get_channel(conn),
         {:ok, prepared_data} <- ensure_data(data, collection_name, conn) do
      request = %InsertRequest{
        db_name: get_db_name(opts),
        collection_name: collection_name,
        partition_name: Keyword.get(opts, :partition_name, ""),
        fields_data: Data.to_proto(prepared_data),
        num_rows: Data.num_rows(prepared_data)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :insert, request),
           {:ok, resp} <- RPC.with_status_check(response, "Insert") do
        {:ok,
         %{
           insert_count: resp.insert_cnt,
           ids: extract_ids(resp."IDs")
         }}
      end
    end
  end

  @doc """
  Deletes entities from a collection by filter expression.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `expr` - Filter expression (e.g., "id in [1, 2, 3]" or "age > 25")
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:partition_name` - Partition to delete from (default: "")
    - `:consistency_level` - Consistency level (default: `:Bounded`)

  ## Returns

    - `{:ok, %{delete_count: count}}` on success
    - `{:error, error}` on failure

  ## Examples

      {:ok, result} = Milvex.delete(conn, "movies", "id in [1, 2, 3]")
  """
  @spec delete(GenServer.server(), collection_ref(), String.t(), keyword()) ::
          {:ok, %{delete_count: integer()}} | {:error, Error.t()}
  def delete(conn, collection, expr, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DeleteRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        partition_name: Keyword.get(opts, :partition_name, ""),
        expr: expr,
        consistency_level: get_consistency_level(opts)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :delete, request),
           {:ok, resp} <- RPC.with_status_check(response, "Delete") do
        {:ok, %{delete_count: resp.delete_cnt}}
      end
    end
  end

  @doc """
  Upserts data into a collection.

  Works the same as `insert/4` but updates existing entities with matching primary keys.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `data` - Data to upsert (list of maps or Data struct)
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:partition_name` - Partition to upsert into (default: "")

  ## Returns

    - `{:ok, %{upsert_count: count, ids: ids}}` on success
    - `{:error, error}` on failure
  """
  @spec upsert(GenServer.server(), collection_ref(), Data.t() | [map() | struct()], keyword()) ::
          {:ok, %{upsert_count: integer(), ids: list()}} | {:error, Error.t()}
  def upsert(conn, collection, data, opts \\ []) do
    collection_name = resolve_collection_name(collection)

    with {:ok, channel} <- Connection.get_channel(conn),
         {:ok, prepared_data} <- ensure_data(data, collection_name, conn) do
      request = %UpsertRequest{
        db_name: get_db_name(opts),
        collection_name: collection_name,
        partition_name: Keyword.get(opts, :partition_name, ""),
        fields_data: Data.to_proto(prepared_data),
        num_rows: Data.num_rows(prepared_data)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :upsert, request),
           {:ok, resp} <- RPC.with_status_check(response, "Upsert") do
        {:ok,
         %{
           upsert_count: resp.upsert_cnt,
           ids: extract_ids(resp."IDs")
         }}
      end
    end
  end

  @doc """
  Queries entities from a collection using a filter expression.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `expr` - Filter expression (e.g., "id > 100", "status == 'active'")
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:output_fields` - List of field names to return (default: all)
    - `:partition_names` - List of partitions to query (default: all)
    - `:limit` - Maximum number of results
    - `:offset` - Number of results to skip
    - `:consistency_level` - Consistency level (default: `:Bounded`)

  ## Returns

    - `{:ok, QueryResult.t()}` on success
    - `{:error, error}` on failure

  ## Examples

      {:ok, result} = Milvex.query(conn, "movies", "year > 2020",
        output_fields: ["id", "title", "year"],
        limit: 100
      )
  """
  @spec query(GenServer.server(), collection_ref(), String.t(), keyword()) ::
          {:ok, QueryResult.t()} | {:error, Error.t()}
  def query(conn, collection, expr, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %QueryRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        expr: expr,
        output_fields: Keyword.get(opts, :output_fields, []),
        partition_names: Keyword.get(opts, :partition_names, []),
        query_params: build_query_params(opts),
        consistency_level: get_consistency_level(opts)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :query, request),
           {:ok, resp} <- RPC.with_status_check(response, "Query") do
        {:ok, QueryResult.from_proto(resp)}
      end
    end
  end

  @typedoc """
  Query vectors for search. Either a list of vectors (positional) or a map with atom keys (keyed).
  """
  @type vector_queries :: [[number()]] | %{atom() => [number()]}

  @doc """
  Searches for similar vectors in a collection.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `vectors` - Query vectors: list of vectors or map with atom keys
    - `opts` - Options (`:vector_field` is required)

  ## Options

    - `:vector_field` - (required) Name of the vector field to search
    - `:top_k` - Number of results per query (default: 10)
    - `:output_fields` - List of field names to include in results
    - `:filter` - Filter expression string (e.g., "year > 2020")
    - `:metric_type` - Similarity metric (`:L2`, `:IP`, `:COSINE`)
    - `:search_params` - Map of search parameters (e.g., `%{"nprobe" => 10}`)
    - `:partition_names` - List of partition names to search
    - `:db_name` - Database name (default: "")
    - `:consistency_level` - Consistency level (default: `:Bounded`)

  ## Returns

    - `{:ok, SearchResult.t()}` on success
    - `{:error, error}` on failure

  ## Examples

      {:ok, result} = Milvex.search(conn, "movies", [[0.1, 0.2, 0.3, ...]],
        vector_field: "embedding",
        top_k: 10,
        output_fields: ["title", "year"]
      )

      # Multiple queries with filter
      {:ok, result} = Milvex.search(conn, "movies", [query1, query2],
        vector_field: "embedding",
        top_k: 5,
        filter: "year > 2020"
      )

      # Named queries - results keyed by same atoms
      {:ok, result} = Milvex.search(conn, "movies",
        %{matrix_like: embedding1, inception_like: embedding2},
        vector_field: "embedding",
        top_k: 5
      )
      result.hits[:matrix_like]     # => [%Hit{}, ...]
      result.hits[:inception_like]  # => [%Hit{}, ...]
  """
  @spec search(GenServer.server(), collection_ref(), vector_queries(), keyword()) ::
          {:ok, SearchResult.t()} | {:error, Error.t()}
  def search(conn, collection, vectors, opts \\ [])

  def search(conn, collection, vectors, opts) when is_map(vectors) and not is_struct(vectors) do
    keys = Map.keys(vectors)
    vectors_list = Enum.map(keys, &Map.get(vectors, &1))

    with {:ok, result} <- search(conn, collection, vectors_list, opts) do
      keyed_hits = Enum.zip(keys, result.hits) |> Map.new()
      {:ok, %{result | hits: keyed_hits}}
    end
  end

  def search(conn, collection, vectors, opts) when is_list(vectors) do
    collection_name = resolve_collection_name(collection)

    with {:ok, vector_field} <- require_option(opts, :vector_field),
         {:ok, channel} <- Connection.get_channel(conn),
         {:ok, info} <- describe_collection(conn, collection_name, opts),
         {:ok, field, is_nested} <- find_vector_field(info.schema, vector_field),
         {:ok, placeholder_bytes} <- build_placeholder_group(vectors, field, is_nested) do
      request = %SearchRequest{
        db_name: get_db_name(opts),
        collection_name: collection_name,
        partition_names: Keyword.get(opts, :partition_names, []),
        dsl: Keyword.get(opts, :filter, ""),
        dsl_type: :BoolExprV1,
        search_input: {:placeholder_group, placeholder_bytes},
        output_fields: Keyword.get(opts, :output_fields, []),
        search_params: build_search_params(opts, vector_field),
        nq: length(vectors),
        consistency_level: get_consistency_level(opts)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :search, request),
           {:ok, resp} <- RPC.with_status_check(response, "Search") do
        {:ok, SearchResult.from_proto(resp)}
      end
    end
  end

  @doc """
  Performs a hybrid search combining multiple ANN searches with reranking.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name or module
    - `searches` - List of `AnnSearch.t()` structs
    - `ranker` - `WeightedRanker.t()` or `RRFRanker.t()`
    - `opts` - Options (see below)

  ## Options

    - `:output_fields` - List of field names to return
    - `:partition_names` - Partitions to search
    - `:consistency_level` - Consistency level (default: `:Bounded`)
    - `:db_name` - Database name (default: "")
    - `:limit` - Maximum number of final results

  ## Examples

      {:ok, search1} = AnnSearch.new("text_dense", [text_vec], limit: 10)
      {:ok, search2} = AnnSearch.new("image_dense", [image_vec], limit: 10)
      {:ok, ranker} = Ranker.weighted([0.7, 0.3])

      {:ok, results} = Milvex.hybrid_search(conn, "products", [search1, search2], ranker,
        output_fields: ["title", "price"]
      )
  """
  @spec hybrid_search(
          GenServer.server(),
          collection_ref(),
          [AnnSearch.t()],
          WeightedRanker.t() | RRFRanker.t(),
          keyword()
        ) :: {:ok, SearchResult.t()} | {:error, Error.t()}
  def hybrid_search(conn, collection, searches, ranker, opts \\ [])

  def hybrid_search(_conn, _collection, [], _ranker, _opts) do
    {:error, Invalid.exception(field: :searches, message: "must be a non-empty list")}
  end

  def hybrid_search(conn, collection, searches, %WeightedRanker{weights: weights} = ranker, opts) do
    if length(weights) != length(searches) do
      {:error, Invalid.exception(field: :weights, message: "count must match number of searches")}
    else
      do_hybrid_search(conn, collection, searches, ranker, opts)
    end
  end

  def hybrid_search(conn, collection, searches, %RRFRanker{} = ranker, opts) do
    do_hybrid_search(conn, collection, searches, ranker, opts)
  end

  defp do_hybrid_search(conn, collection, searches, ranker, opts) do
    collection_name = resolve_collection_name(collection)

    with {:ok, channel} <- Connection.get_channel(conn),
         {:ok, info} <- describe_collection(conn, collection_name, opts),
         {:ok, search_requests} <- build_search_requests(searches, info.schema) do
      request = %HybridSearchRequest{
        db_name: get_db_name(opts),
        collection_name: collection_name,
        requests: search_requests,
        rank_params: build_rank_params(ranker, opts),
        output_fields: Keyword.get(opts, :output_fields, []),
        partition_names: Keyword.get(opts, :partition_names, []),
        consistency_level: get_consistency_level(opts)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :hybrid_search, request),
           {:ok, resp} <- RPC.with_status_check(response, "HybridSearch") do
        {:ok, SearchResult.from_proto(resp)}
      end
    end
  end

  defp build_search_requests(searches, schema) do
    results =
      Enum.map(searches, fn search ->
        ann_search_to_search_request(search, schema)
      end)

    case Enum.find(results, &match?({:error, _}, &1)) do
      nil -> {:ok, Enum.map(results, fn {:ok, req} -> req end)}
      error -> error
    end
  end

  defp ann_search_to_search_request(%AnnSearch{} = search, schema) do
    with {:ok, field, is_nested} <- find_vector_field(schema, search.anns_field),
         {:ok, placeholder_bytes} <- build_ann_placeholder_group(search.data, field, is_nested) do
      request = %SearchRequest{
        dsl: search.expr || "",
        dsl_type: :BoolExprV1,
        search_input: {:placeholder_group, placeholder_bytes},
        search_params: build_ann_search_params(search),
        nq: length(search.data)
      }

      {:ok, request}
    end
  end

  defp build_ann_placeholder_group(data, field, is_nested) do
    cond do
      all_vectors_data?(data) -> build_placeholder_group(data, field, is_nested)
      all_strings_data?(data) -> build_text_placeholder_group(data)
      true -> {:error, Invalid.exception(field: :data, message: "invalid data format")}
    end
  end

  defp all_vectors_data?(data), do: Enum.all?(data, &is_list/1)
  defp all_strings_data?(data), do: Enum.all?(data, &is_binary/1)

  defp build_text_placeholder_group(texts) do
    placeholder = %PlaceholderValue{
      tag: "$0",
      type: :VarChar,
      values: texts
    }

    group = %PlaceholderGroup{placeholders: [placeholder]}
    {:ok, PlaceholderGroup.encode(group)}
  rescue
    e ->
      {:error, Invalid.exception(field: :data, message: "Failed to encode text: #{inspect(e)}")}
  end

  defp build_ann_search_params(%AnnSearch{anns_field: field, limit: limit, params: params}) do
    base_params = [
      %KeyValuePair{key: "anns_field", value: field},
      %KeyValuePair{key: "topk", value: to_string(limit)}
    ]

    case params do
      nil ->
        [%KeyValuePair{key: "params", value: "{}"} | base_params]

      extra when is_map(extra) ->
        encoded = Jason.encode!(extra)
        [%KeyValuePair{key: "params", value: encoded} | base_params]
    end
  end

  defp build_rank_params(%WeightedRanker{weights: weights}, opts) do
    params = Jason.encode!(%{weights: weights})

    [
      %KeyValuePair{key: "strategy", value: "weighted"},
      %KeyValuePair{key: "params", value: params}
    ] ++ build_limit_params(opts)
  end

  defp build_rank_params(%RRFRanker{k: k}, opts) do
    params = Jason.encode!(%{k: k})

    [
      %KeyValuePair{key: "strategy", value: "rrf"},
      %KeyValuePair{key: "params", value: params}
    ] ++ build_limit_params(opts)
  end

  defp build_limit_params(opts) do
    case Keyword.get(opts, :limit) do
      nil -> []
      limit -> [%KeyValuePair{key: "limit", value: to_string(limit)}]
    end
  end

  # ============================================================================
  # Partition Operations
  # ============================================================================

  @doc """
  Creates a partition in a collection.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `partition_name` - Name for the new partition
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure

  ## Examples

      :ok = Milvex.create_partition(conn, "movies", "movies_2024")
  """
  @spec create_partition(GenServer.server(), collection_ref(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def create_partition(conn, collection, partition_name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %CreatePartitionRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        partition_name: partition_name
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :create_partition, request) do
        RPC.check_status(response, "CreatePartition")
      end
    end
  end

  @doc """
  Drops a partition from a collection.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `partition_name` - Name of the partition to drop
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure

  ## Examples

      :ok = Milvex.drop_partition(conn, "movies", "movies_2024")
  """
  @spec drop_partition(GenServer.server(), collection_ref(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def drop_partition(conn, collection, partition_name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DropPartitionRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        partition_name: partition_name
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :drop_partition, request) do
        RPC.check_status(response, "DropPartition")
      end
    end
  end

  @doc """
  Checks if a partition exists in a collection.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `partition_name` - Partition name to check
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `{:ok, true}` if partition exists
    - `{:ok, false}` if partition does not exist
    - `{:error, error}` on failure

  ## Examples

      {:ok, true} = Milvex.has_partition(conn, "movies", "movies_2024")
  """
  @spec has_partition(GenServer.server(), collection_ref(), String.t(), keyword()) ::
          {:ok, boolean()} | {:error, Error.t()}
  def has_partition(conn, collection, partition_name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %HasPartitionRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        partition_name: partition_name
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :has_partition, request),
           {:ok, resp} <- RPC.with_status_check(response, "HasPartition") do
        {:ok, resp.value}
      end
    end
  end

  @doc """
  Lists all partitions in a collection.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `{:ok, partition_names}` - List of partition names
    - `{:error, error}` on failure

  ## Examples

      {:ok, ["_default", "movies_2024"]} = Milvex.list_partitions(conn, "movies")
  """
  @spec list_partitions(GenServer.server(), collection_ref(), keyword()) ::
          {:ok, [String.t()]} | {:error, Error.t()}
  def list_partitions(conn, collection, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %ShowPartitionsRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :show_partitions, request),
           {:ok, resp} <- RPC.with_status_check(response, "ShowPartitions") do
        {:ok, resp.partition_names}
      end
    end
  end

  @doc """
  Loads partitions into memory for querying.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `partition_names` - List of partition names to load
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:replica_number` - Number of replicas (default: 1)

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure

  ## Examples

      :ok = Milvex.load_partitions(conn, "movies", ["movies_2024", "movies_2023"])
  """
  @spec load_partitions(GenServer.server(), collection_ref(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def load_partitions(conn, collection, partition_names, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %LoadPartitionsRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        partition_names: partition_names,
        replica_number: Keyword.get(opts, :replica_number, 1)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :load_partitions, request) do
        RPC.check_status(response, "LoadPartitions")
      end
    end
  end

  @doc """
  Releases partitions from memory.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `partition_names` - List of partition names to release
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure

  ## Examples

      :ok = Milvex.release_partitions(conn, "movies", ["movies_2024"])
  """
  @spec release_partitions(GenServer.server(), collection_ref(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def release_partitions(conn, collection, partition_names, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %ReleasePartitionsRequest{
        db_name: get_db_name(opts),
        collection_name: resolve_collection_name(collection),
        partition_names: partition_names
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :release_partitions, request) do
        RPC.check_status(response, "ReleasePartitions")
      end
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

  defp resolve_collection_name(name) when is_binary(name), do: name

  defp resolve_collection_name(module) when is_atom(module) do
    Milvex.Collection.collection_name(module)
  end

  defp get_db_name(opts), do: Keyword.get(opts, :db_name, "")

  defp get_consistency_level(opts) do
    Keyword.get(opts, :consistency_level, @default_consistency_level)
  end

  defp encode_schema(schema) do
    schema_proto = Schema.to_proto(schema)
    {:ok, CollectionSchema.encode(schema_proto)}
  rescue
    e -> {:error, Invalid.exception(field: :schema, message: "Failed to encode: #{inspect(e)}")}
  end

  defp ensure_data(%Data{} = data, _collection, _conn), do: {:ok, data}

  defp ensure_data(rows, collection, conn) when is_list(rows) do
    case detect_row_type(rows) do
      {:collection_struct, module} ->
        schema = Milvex.Collection.to_schema(module)
        maps = Enum.map(rows, &struct_to_map/1)
        Data.from_rows(maps, schema)

      :map ->
        with {:ok, info} <- describe_collection(conn, collection) do
          Data.from_rows(rows, info.schema)
        end
    end
  end

  defp detect_row_type([]), do: :map

  defp detect_row_type([%{__struct__: module} | _]) when is_atom(module) do
    if function_exported?(module, :__collection__, 0) do
      {:collection_struct, module}
    else
      :map
    end
  end

  defp detect_row_type(_), do: :map

  defp struct_to_map(%{__struct__: _} = struct), do: Map.from_struct(struct)

  defp extract_ids(nil), do: []
  defp extract_ids(%IDs{id_field: {:int_id, %LongArray{data: ids}}}), do: ids
  defp extract_ids(%IDs{id_field: {:str_id, %StringArray{data: ids}}}), do: ids
  defp extract_ids(_), do: []

  defp build_query_params(opts) do
    []
    |> maybe_add_param("limit", opts[:limit])
    |> maybe_add_param("offset", opts[:offset])
  end

  defp maybe_add_param(params, _key, nil), do: params

  defp maybe_add_param(params, key, value) do
    [%KeyValuePair{key: key, value: to_string(value)} | params]
  end

  defp require_option(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} ->
        {:ok, value}

      :error ->
        {:error, Invalid.exception(field: key, message: "#{key} is required")}
    end
  end

  defp find_vector_field(schema, field_name) do
    case parse_nested_field_name(field_name) do
      {:nested, parent_name, child_name} ->
        find_nested_vector_field(schema, field_name, parent_name, child_name)

      :simple ->
        find_simple_vector_field(schema, field_name)
    end
  end

  defp parse_nested_field_name(field_name) do
    case Regex.run(@nested_field_regex, field_name) do
      [_, parent, child] -> {:nested, parent, child}
      nil -> :simple
    end
  end

  defp find_simple_vector_field(schema, field_name) do
    case Schema.get_field(schema, field_name) do
      nil ->
        {:error,
         Invalid.exception(field: :vector_field, message: "Field '#{field_name}' not found")}

      field ->
        if Field.vector_type?(field.data_type) do
          {:ok, field, false}
        else
          {:error,
           Invalid.exception(
             field: :vector_field,
             message: "Field '#{field_name}' is not a vector field"
           )}
        end
    end
  end

  defp find_nested_vector_field(schema, full_name, parent_name, child_name) do
    case Schema.get_field(schema, parent_name) do
      nil ->
        {:error,
         Invalid.exception(field: :vector_field, message: "Field '#{full_name}' not found")}

      %{data_type: :array_of_struct, struct_schema: struct_schema} when is_list(struct_schema) ->
        find_child_vector_field(struct_schema, full_name, child_name)

      _ ->
        {:error,
         Invalid.exception(
           field: :vector_field,
           message: "Field '#{parent_name}' is not an array_of_struct field"
         )}
    end
  end

  defp find_child_vector_field(struct_schema, full_name, child_name) do
    case Enum.find(struct_schema, &(&1.name == child_name)) do
      nil ->
        {:error,
         Invalid.exception(field: :vector_field, message: "Field '#{full_name}' not found")}

      field ->
        validate_nested_vector_field(field, full_name)
    end
  end

  defp validate_nested_vector_field(field, full_name) do
    if Field.vector_type?(field.data_type) do
      {:ok, field, true}
    else
      {:error,
       Invalid.exception(
         field: :vector_field,
         message: "Field '#{full_name}' is not a vector field"
       )}
    end
  end

  defp build_placeholder_group(vectors, field, is_nested) do
    placeholder_type = vector_type_to_placeholder_type(field.data_type, is_nested)
    dim = field.dimension

    encoded_values =
      Enum.map(vectors, fn vec ->
        encode_vector(vec, field.data_type, dim)
      end)

    placeholder = %PlaceholderValue{
      tag: "$0",
      type: placeholder_type,
      values: encoded_values
    }

    group = %PlaceholderGroup{placeholders: [placeholder]}
    {:ok, PlaceholderGroup.encode(group)}
  rescue
    e ->
      {:error,
       Invalid.exception(field: :vectors, message: "Failed to encode vectors: #{inspect(e)}")}
  end

  defp vector_type_to_placeholder_type(type, true) do
    case type do
      :float_vector -> :EmbListFloatVector
      :binary_vector -> :EmbListBinaryVector
      :float16_vector -> :EmbListFloat16Vector
      :bfloat16_vector -> :EmbListBFloat16Vector
      :sparse_float_vector -> :EmbListSparseFloatVector
      :int8_vector -> :EmbListInt8Vector
      _ -> :EmbListFloatVector
    end
  end

  defp vector_type_to_placeholder_type(type, false) do
    case type do
      :float_vector -> :FloatVector
      :binary_vector -> :BinaryVector
      :float16_vector -> :Float16Vector
      :bfloat16_vector -> :BFloat16Vector
      :sparse_float_vector -> :SparseFloatVector
      :int8_vector -> :Int8Vector
      _ -> :FloatVector
    end
  end

  defp encode_vector(vec, :float_vector, _dim) do
    vec
    |> Enum.map(&float_to_binary/1)
    |> IO.iodata_to_binary()
  end

  defp encode_vector(vec, :binary_vector, _dim) do
    IO.iodata_to_binary(vec)
  end

  defp encode_vector(vec, _type, _dim) do
    vec
    |> Enum.map(&float_to_binary/1)
    |> IO.iodata_to_binary()
  end

  defp float_to_binary(f) when is_float(f), do: <<f::little-float-32>>
  defp float_to_binary(i) when is_integer(i), do: <<i * 1.0::little-float-32>>

  defp build_search_params(opts, vector_field) do
    params = [%KeyValuePair{key: "anns_field", value: vector_field}]

    params =
      case Keyword.get(opts, :top_k, @default_top_k) do
        nil -> params
        top_k -> [%KeyValuePair{key: "topk", value: to_string(top_k)} | params]
      end

    params =
      case Keyword.get(opts, :metric_type) do
        nil -> params
        metric -> [%KeyValuePair{key: "metric_type", value: to_string(metric)} | params]
      end

    params =
      case Keyword.get(opts, :search_params) do
        nil ->
          [%KeyValuePair{key: "params", value: "{}"} | params]

        extra when is_map(extra) ->
          encoded = Jason.encode!(extra)
          [%KeyValuePair{key: "params", value: encoded} | params]
      end

    params
  end
end

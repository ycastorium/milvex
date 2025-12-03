defmodule Milvex do
  @moduledoc """
  High-level client API for Milvus vector database.
  """

  alias Milvex.Connection
  alias Milvex.Data
  alias Milvex.Error
  alias Milvex.Errors.Invalid
  alias Milvex.Index
  alias Milvex.QueryResult
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
  Creates a collection and raises on error.
  """
  @spec create_collection!(GenServer.server(), String.t(), Schema.t(), keyword()) :: :ok
  def create_collection!(conn, name, schema, opts \\ []) do
    case create_collection(conn, name, schema, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Drops (deletes) a collection.

  ## Parameters

    - `conn` - Connection process
    - `name` - Collection name to drop
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure
  """
  @spec drop_collection(GenServer.server(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def drop_collection(conn, name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DropCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: name
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :drop_collection, request) do
        RPC.check_status(response, "DropCollection")
      end
    end
  end

  @doc """
  Drops a collection and raises on error.
  """
  @spec drop_collection!(GenServer.server(), String.t(), keyword()) :: :ok
  def drop_collection!(conn, name, opts \\ []) do
    case drop_collection(conn, name, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Checks if a collection exists.

  ## Parameters

    - `conn` - Connection process
    - `name` - Collection name to check
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `{:ok, true}` if collection exists
    - `{:ok, false}` if collection does not exist
    - `{:error, error}` on failure
  """
  @spec has_collection(GenServer.server(), String.t(), keyword()) ::
          {:ok, boolean()} | {:error, Error.t()}
  def has_collection(conn, name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %HasCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: name
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :has_collection, request),
           {:ok, resp} <- RPC.with_status_check(response, "HasCollection") do
        {:ok, resp.value}
      end
    end
  end

  @doc """
  Checks if a collection exists and raises on error.
  """
  @spec has_collection!(GenServer.server(), String.t(), keyword()) :: boolean()
  def has_collection!(conn, name, opts \\ []) do
    case has_collection(conn, name, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @doc """
  Describes a collection and returns its metadata.

  ## Parameters

    - `conn` - Connection process
    - `name` - Collection name
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
  @spec describe_collection(GenServer.server(), String.t(), keyword()) ::
          {:ok, map()} | {:error, Error.t()}
  def describe_collection(conn, name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DescribeCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: name
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
  Describes a collection and raises on error.
  """
  @spec describe_collection!(GenServer.server(), String.t(), keyword()) :: map()
  def describe_collection!(conn, name, opts \\ []) do
    case describe_collection(conn, name, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
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
  Lists all collections and raises on error.
  """
  @spec list_collections!(GenServer.server(), keyword()) :: [String.t()]
  def list_collections!(conn, opts \\ []) do
    case list_collections(conn, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @doc """
  Loads a collection into memory for querying.

  ## Parameters

    - `conn` - Connection process
    - `name` - Collection name
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")
    - `:replica_number` - Number of replicas (default: 1)

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure
  """
  @spec load_collection(GenServer.server(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def load_collection(conn, name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %LoadCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: name,
        replica_number: Keyword.get(opts, :replica_number, 1)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :load_collection, request) do
        RPC.check_status(response, "LoadCollection")
      end
    end
  end

  @doc """
  Loads a collection and raises on error.
  """
  @spec load_collection!(GenServer.server(), String.t(), keyword()) :: :ok
  def load_collection!(conn, name, opts \\ []) do
    case load_collection(conn, name, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Releases a collection from memory.

  ## Parameters

    - `conn` - Connection process
    - `name` - Collection name
    - `opts` - Options

  ## Options

    - `:db_name` - Database name (default: "")

  ## Returns

    - `:ok` on success
    - `{:error, error}` on failure
  """
  @spec release_collection(GenServer.server(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def release_collection(conn, name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %ReleaseCollectionRequest{
        db_name: get_db_name(opts),
        collection_name: name
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :release_collection, request) do
        RPC.check_status(response, "ReleaseCollection")
      end
    end
  end

  @doc """
  Releases a collection and raises on error.
  """
  @spec release_collection!(GenServer.server(), String.t(), keyword()) :: :ok
  def release_collection!(conn, name, opts \\ []) do
    case release_collection(conn, name, opts) do
      :ok -> :ok
      {:error, error} -> raise error
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
  @spec create_index(GenServer.server(), String.t(), Index.t() | String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def create_index(conn, collection, index_or_field, opts \\ [])

  def create_index(conn, collection, %Index{} = index, opts) do
    with {:ok, _} <- Index.validate(index),
         {:ok, channel} <- Connection.get_channel(conn) do
      request = %CreateIndexRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
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
        collection_name: collection,
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
  Creates an index and raises on error.
  """
  @spec create_index!(GenServer.server(), String.t(), Index.t() | String.t(), keyword()) :: :ok
  def create_index!(conn, collection, index_or_field, opts \\ []) do
    case create_index(conn, collection, index_or_field, opts) do
      :ok -> :ok
      {:error, error} -> raise error
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
  @spec drop_index(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def drop_index(conn, collection, field_name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DropIndexRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
        field_name: field_name,
        index_name: Keyword.get(opts, :index_name, "")
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :drop_index, request) do
        RPC.check_status(response, "DropIndex")
      end
    end
  end

  @doc """
  Drops an index and raises on error.
  """
  @spec drop_index!(GenServer.server(), String.t(), String.t(), keyword()) :: :ok
  def drop_index!(conn, collection, field_name, opts \\ []) do
    case drop_index(conn, collection, field_name, opts) do
      :ok -> :ok
      {:error, error} -> raise error
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
  @spec describe_index(GenServer.server(), String.t(), keyword()) ::
          {:ok, list()} | {:error, Error.t()}
  def describe_index(conn, collection, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DescribeIndexRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
        field_name: Keyword.get(opts, :field_name, ""),
        index_name: Keyword.get(opts, :index_name, "")
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :describe_index, request),
           {:ok, resp} <- RPC.with_status_check(response, "DescribeIndex") do
        {:ok, resp.index_descriptions}
      end
    end
  end

  @doc """
  Describes an index and raises on error.
  """
  @spec describe_index!(GenServer.server(), String.t(), keyword()) :: list()
  def describe_index!(conn, collection, opts \\ []) do
    case describe_index(conn, collection, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
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
  @spec insert(GenServer.server(), String.t(), Data.t() | [map()], keyword()) ::
          {:ok, %{insert_count: integer(), ids: list()}} | {:error, Error.t()}
  def insert(conn, collection, data, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn),
         {:ok, prepared_data} <- ensure_data(data, collection, conn) do
      request = %InsertRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
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
  Inserts data and raises on error.
  """
  @spec insert!(GenServer.server(), String.t(), Data.t() | [map()], keyword()) :: %{
          insert_count: integer(),
          ids: list()
        }
  def insert!(conn, collection, data, opts \\ []) do
    case insert(conn, collection, data, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
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
  @spec delete(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, %{delete_count: integer()}} | {:error, Error.t()}
  def delete(conn, collection, expr, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DeleteRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
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
  Deletes entities and raises on error.
  """
  @spec delete!(GenServer.server(), String.t(), String.t(), keyword()) :: %{
          delete_count: integer()
        }
  def delete!(conn, collection, expr, opts \\ []) do
    case delete(conn, collection, expr, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
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
  @spec upsert(GenServer.server(), String.t(), Data.t() | [map()], keyword()) ::
          {:ok, %{upsert_count: integer(), ids: list()}} | {:error, Error.t()}
  def upsert(conn, collection, data, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn),
         {:ok, prepared_data} <- ensure_data(data, collection, conn) do
      request = %UpsertRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
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
  Upserts data and raises on error.
  """
  @spec upsert!(GenServer.server(), String.t(), Data.t() | [map()], keyword()) :: %{
          upsert_count: integer(),
          ids: list()
        }
  def upsert!(conn, collection, data, opts \\ []) do
    case upsert(conn, collection, data, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
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
  @spec query(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, QueryResult.t()} | {:error, Error.t()}
  def query(conn, collection, expr, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %QueryRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
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

  @doc """
  Queries entities and raises on error.
  """
  @spec query!(GenServer.server(), String.t(), String.t(), keyword()) :: QueryResult.t()
  def query!(conn, collection, expr, opts \\ []) do
    case query(conn, collection, expr, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @doc """
  Searches for similar vectors in a collection.

  ## Parameters

    - `conn` - Connection process
    - `collection` - Collection name
    - `vectors` - List of query vectors (2D list of floats)
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
  """
  @spec search(GenServer.server(), String.t(), [[number()]], keyword()) ::
          {:ok, SearchResult.t()} | {:error, Error.t()}
  def search(conn, collection, vectors, opts \\ []) do
    with {:ok, vector_field} <- require_option(opts, :vector_field),
         {:ok, channel} <- Connection.get_channel(conn),
         {:ok, info} <- describe_collection(conn, collection, opts),
         {:ok, field} <- find_vector_field(info.schema, vector_field),
         {:ok, placeholder_bytes} <- build_placeholder_group(vectors, field) do
      request = %SearchRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
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
  Searches for similar vectors and raises on error.
  """
  @spec search!(GenServer.server(), String.t(), [[number()]], keyword()) :: SearchResult.t()
  def search!(conn, collection, vectors, opts \\ []) do
    case search(conn, collection, vectors, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
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
  @spec create_partition(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def create_partition(conn, collection, partition_name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %CreatePartitionRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
        partition_name: partition_name
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :create_partition, request) do
        RPC.check_status(response, "CreatePartition")
      end
    end
  end

  @doc """
  Creates a partition and raises on error.
  """
  @spec create_partition!(GenServer.server(), String.t(), String.t(), keyword()) :: :ok
  def create_partition!(conn, collection, partition_name, opts \\ []) do
    case create_partition(conn, collection, partition_name, opts) do
      :ok -> :ok
      {:error, error} -> raise error
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
  @spec drop_partition(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def drop_partition(conn, collection, partition_name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %DropPartitionRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
        partition_name: partition_name
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :drop_partition, request) do
        RPC.check_status(response, "DropPartition")
      end
    end
  end

  @doc """
  Drops a partition and raises on error.
  """
  @spec drop_partition!(GenServer.server(), String.t(), String.t(), keyword()) :: :ok
  def drop_partition!(conn, collection, partition_name, opts \\ []) do
    case drop_partition(conn, collection, partition_name, opts) do
      :ok -> :ok
      {:error, error} -> raise error
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
  @spec has_partition(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, boolean()} | {:error, Error.t()}
  def has_partition(conn, collection, partition_name, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %HasPartitionRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
        partition_name: partition_name
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :has_partition, request),
           {:ok, resp} <- RPC.with_status_check(response, "HasPartition") do
        {:ok, resp.value}
      end
    end
  end

  @doc """
  Checks if a partition exists and raises on error.
  """
  @spec has_partition!(GenServer.server(), String.t(), String.t(), keyword()) :: boolean()
  def has_partition!(conn, collection, partition_name, opts \\ []) do
    case has_partition(conn, collection, partition_name, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
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
  @spec list_partitions(GenServer.server(), String.t(), keyword()) ::
          {:ok, [String.t()]} | {:error, Error.t()}
  def list_partitions(conn, collection, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %ShowPartitionsRequest{
        db_name: get_db_name(opts),
        collection_name: collection
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :show_partitions, request),
           {:ok, resp} <- RPC.with_status_check(response, "ShowPartitions") do
        {:ok, resp.partition_names}
      end
    end
  end

  @doc """
  Lists all partitions and raises on error.
  """
  @spec list_partitions!(GenServer.server(), String.t(), keyword()) :: [String.t()]
  def list_partitions!(conn, collection, opts \\ []) do
    case list_partitions(conn, collection, opts) do
      {:ok, result} -> result
      {:error, error} -> raise error
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
  @spec load_partitions(GenServer.server(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def load_partitions(conn, collection, partition_names, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %LoadPartitionsRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
        partition_names: partition_names,
        replica_number: Keyword.get(opts, :replica_number, 1)
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :load_partitions, request) do
        RPC.check_status(response, "LoadPartitions")
      end
    end
  end

  @doc """
  Loads partitions and raises on error.
  """
  @spec load_partitions!(GenServer.server(), String.t(), [String.t()], keyword()) :: :ok
  def load_partitions!(conn, collection, partition_names, opts \\ []) do
    case load_partitions(conn, collection, partition_names, opts) do
      :ok -> :ok
      {:error, error} -> raise error
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
  @spec release_partitions(GenServer.server(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def release_partitions(conn, collection, partition_names, opts \\ []) do
    with {:ok, channel} <- Connection.get_channel(conn) do
      request = %ReleasePartitionsRequest{
        db_name: get_db_name(opts),
        collection_name: collection,
        partition_names: partition_names
      }

      with {:ok, response} <- RPC.call(channel, MilvusService.Stub, :release_partitions, request) do
        RPC.check_status(response, "ReleasePartitions")
      end
    end
  end

  @doc """
  Releases partitions and raises on error.
  """
  @spec release_partitions!(GenServer.server(), String.t(), [String.t()], keyword()) :: :ok
  def release_partitions!(conn, collection, partition_names, opts \\ []) do
    case release_partitions(conn, collection, partition_names, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  # ============================================================================
  # Private Helpers
  # ============================================================================

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
    with {:ok, info} <- describe_collection(conn, collection) do
      Data.from_rows(rows, info.schema)
    end
  end

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
    case Schema.get_field(schema, field_name) do
      nil ->
        {:error,
         Invalid.exception(field: :vector_field, message: "Field '#{field_name}' not found")}

      field ->
        if Field.vector_type?(field.data_type) do
          {:ok, field}
        else
          {:error,
           Invalid.exception(
             field: :vector_field,
             message: "Field '#{field_name}' is not a vector field"
           )}
        end
    end
  end

  defp build_placeholder_group(vectors, field) do
    placeholder_type = vector_type_to_placeholder_type(field.data_type)
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

  defp vector_type_to_placeholder_type(:float_vector), do: :FloatVector
  defp vector_type_to_placeholder_type(:binary_vector), do: :BinaryVector
  defp vector_type_to_placeholder_type(:float16_vector), do: :Float16Vector
  defp vector_type_to_placeholder_type(:bfloat16_vector), do: :BFloat16Vector
  defp vector_type_to_placeholder_type(:sparse_float_vector), do: :SparseFloatVector
  defp vector_type_to_placeholder_type(:int8_vector), do: :Int8Vector
  defp vector_type_to_placeholder_type(_), do: :FloatVector

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

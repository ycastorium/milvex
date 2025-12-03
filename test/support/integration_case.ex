defmodule Milvex.IntegrationCase do
  @moduledoc """
  Shared test case for Milvex integration tests.

  Provides common setup, helpers, and aliases for testing against a real Milvus instance.
  """
  use ExUnit.CaseTemplate

  alias Milvex.MilvusContainer

  using do
    quote do
      use AssertEventually, timeout: 5000, interval: 200
      import Milvex.IntegrationCase.Helpers
      alias Milvex.Data
      alias Milvex.Index
      alias Milvex.Schema
      alias Milvex.Schema.Field
    end
  end

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
end

defmodule Milvex.IntegrationCase.Helpers do
  @moduledoc """
  Helper functions for integration tests.
  """

  alias Milvex.Data
  alias Milvex.Schema
  alias Milvex.Schema.Field

  @doc """
  Generates a unique collection name with the given prefix.
  """
  def unique_collection_name(prefix \\ "test") do
    "#{prefix}_#{:erlang.unique_integer([:positive])}"
  end

  @doc """
  Creates a standard schema for testing with id, title, and embedding fields.

  Options:
    - `:dimension` - Vector dimension (default: 4)
    - `:auto_id` - Whether to auto-generate IDs (default: true)
  """
  def standard_schema(collection_name, opts \\ []) do
    dim = Keyword.get(opts, :dimension, 4)
    auto_id = Keyword.get(opts, :auto_id, true)

    Schema.build!(
      name: collection_name,
      fields: [
        Field.primary_key("id", :int64, auto_id: auto_id),
        Field.varchar("title", 256),
        Field.vector("embedding", dim)
      ]
    )
  end

  @doc """
  Creates a schema without auto_id for testing manual ID insertion.
  """
  def manual_id_schema(collection_name, opts \\ []) do
    standard_schema(collection_name, Keyword.put(opts, :auto_id, false))
  end

  @doc """
  Generates sample data rows for testing.
  """
  def sample_rows(count \\ 3, opts \\ []) do
    dim = Keyword.get(opts, :dimension, 4)

    for i <- 1..count do
      %{
        title: "Item #{i}",
        embedding: random_vector(dim)
      }
    end
  end

  @doc """
  Generates sample data rows with manual IDs for testing.
  """
  def sample_rows_with_ids(count \\ 3, opts \\ []) do
    dim = Keyword.get(opts, :dimension, 4)
    start_id = Keyword.get(opts, :start_id, 1)

    for i <- start_id..(start_id + count - 1) do
      %{
        id: i,
        title: "Item #{i}",
        embedding: random_vector(dim)
      }
    end
  end

  @doc """
  Creates sample Data struct from rows and schema.
  """
  def sample_data(schema, count \\ 3, opts \\ []) do
    rows = sample_rows(count, opts)
    Data.from_rows!(rows, schema)
  end

  @doc """
  Generates a random vector of the given dimension.
  """
  def random_vector(dim) do
    for _ <- 1..dim, do: :rand.uniform()
  end

  @doc """
  Safely cleans up a collection (release + drop), ignoring errors.
  """
  def cleanup_collection(conn, name) do
    try do
      Milvex.release_collection(conn, name)
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    try do
      Milvex.drop_collection(conn, name)
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  @doc """
  Safely cleans up a partition, ignoring errors.
  """
  def cleanup_partition(conn, collection_name, partition_name) do
    try do
      Milvex.release_partitions(conn, collection_name, [partition_name])
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    try do
      Milvex.drop_partition(conn, collection_name, partition_name)
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  @doc """
  Sets up a collection with index and loads it for search/query tests.
  Returns the collection name and schema.
  """
  def setup_loaded_collection(conn, prefix \\ "loaded") do
    name = unique_collection_name(prefix)
    schema = standard_schema(name)

    :ok = Milvex.create_collection(conn, name, schema)

    :ok =
      Milvex.create_index(conn, name, "embedding", index_type: "AUTOINDEX", metric_type: "COSINE")

    :ok = Milvex.load_collection(conn, name)

    {name, schema}
  end

  @doc """
  Sets up a collection with index, data, and loads it for search/query tests.
  Returns the collection name, schema, and inserted IDs.
  """
  def setup_loaded_collection_with_data(conn, prefix \\ "loaded", count \\ 5) do
    {name, schema} = setup_loaded_collection(conn, prefix)

    data = sample_data(schema, count)
    {:ok, insert_result} = Milvex.insert(conn, name, data)

    {name, schema, insert_result.ids}
  end
end

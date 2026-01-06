defmodule Milvex.Schema.Migration do
  @moduledoc """
  Implements schema migration for Milvus collections.

  Provides functionality to:
  - Create collections from DSL definitions
  - Verify existing schemas against expected definitions
  - Ensure indexes are created and up-to-date

  ## Options

  - `:strict` - When `true`, raises on schema mismatches instead of logging warnings.
    Useful for CI/CD pipelines. Default: `false`

  ## Examples

      # Basic migration
      Milvex.Schema.Migration.migrate!(conn, MyApp.Movies, [])

      # Strict mode (fails on schema mismatch)
      Milvex.Schema.Migration.migrate!(conn, MyApp.Movies, strict: true)
  """
  require Logger

  alias Milvex.Errors.Grpc
  alias Milvex.Errors.Invalid

  @type schema_diff :: %{
          missing: [String.t()],
          extra: [String.t()],
          mismatches: [{String.t(), map(), map()}]
        }

  @doc """
  Migrates a collection module to Milvus.

  If the collection exists, verifies the schema matches. If not, creates it.
  Always ensures indexes are properly configured.

  ## Options

  - `:strict` - Raise on schema mismatch instead of warning. Default: `false`
  - Any additional options are passed to Milvus API calls.

  ## Returns

  - `:ok` on success
  - Raises `Milvex.Error` on failure
  """
  @spec migrate!(GenServer.server(), module(), keyword()) :: :ok
  def migrate!(connection, collection_module, opts) do
    collection_name = Milvex.Collection.collection_name(collection_module)

    case Milvex.has_collection(connection, collection_name, opts) do
      {:ok, true} ->
        verify_schema!(connection, collection_module, collection_name, opts)
        ensure_indexes!(connection, collection_module, collection_name, opts)

      {:ok, false} ->
        create_collection!(connection, collection_module, collection_name, opts)
        ensure_indexes!(connection, collection_module, collection_name, opts)

      {:error, reason} ->
        raise Grpc.exception(
                operation: :has_collection,
                code: :check_failed,
                message: "Failed to check collection existence: #{format_error(reason)}"
              )
    end

    :ok
  end

  @doc """
  Verifies that the existing collection schema matches the expected schema.

  ## Options

  - `:strict` - Raise on mismatch instead of warning. Default: `false`

  ## Returns

  - `{:ok, :match}` - Schemas match
  - `{:ok, {:mismatch, diff}}` - Schemas differ (only in non-strict mode)
  - Raises in strict mode when schemas don't match
  """
  @spec verify_schema!(GenServer.server(), module(), String.t(), keyword()) ::
          {:ok, :match} | {:ok, {:mismatch, schema_diff()}}
  def verify_schema!(connection, collection_module, collection_name, opts \\ []) do
    expected_schema = Milvex.Collection.to_schema(collection_module)
    strict? = Keyword.get(opts, :strict, false)

    case Milvex.describe_collection(connection, collection_name, opts) do
      {:ok, %{schema: current_schema}} ->
        compare_schemas(expected_schema, current_schema, collection_name, strict?)

      {:error, reason} ->
        raise Grpc.exception(
                operation: :describe_collection,
                code: :fetch_failed,
                message:
                  "Failed to fetch schema for '#{collection_name}': #{format_error(reason)}"
              )
    end
  end

  defp compare_schemas(expected, current, collection_name, strict?) do
    expected_fields = Map.new(expected.fields, &{&1.name, &1})
    current_fields = Map.new(current.fields, &{&1.name, &1})

    expected_names = MapSet.new(Map.keys(expected_fields))
    current_names = MapSet.new(Map.keys(current_fields))

    missing = MapSet.difference(expected_names, current_names) |> MapSet.to_list()
    extra = MapSet.difference(current_names, expected_names) |> MapSet.to_list()
    common = MapSet.intersection(expected_names, current_names) |> MapSet.to_list()

    mismatches =
      common
      |> Enum.reject(&fields_match?(expected_fields[&1], current_fields[&1]))
      |> Enum.map(&{&1, expected_fields[&1], current_fields[&1]})

    diff = %{missing: missing, extra: extra, mismatches: mismatches}

    if has_differences?(diff) do
      handle_schema_mismatch(collection_name, diff, strict?)
    else
      {:ok, :match}
    end
  end

  defp has_differences?(%{missing: [], extra: [], mismatches: []}), do: false
  defp has_differences?(_), do: true

  defp handle_schema_mismatch(collection_name, diff, true = _strict?) do
    raise Invalid.exception(
            field: :schema,
            message: format_schema_mismatch(collection_name, diff),
            context: diff
          )
  end

  defp handle_schema_mismatch(collection_name, diff, false = _strict?) do
    Logger.warning(format_schema_mismatch(collection_name, diff))
    {:ok, {:mismatch, diff}}
  end

  defp format_schema_mismatch(collection_name, %{
         missing: missing,
         extra: extra,
         mismatches: mismatches
       }) do
    warnings =
      []
      |> add_warning_if(missing != [], "Missing fields in Milvus: #{inspect(missing)}")
      |> add_warning_if(extra != [], "Extra fields in Milvus: #{inspect(extra)}")
      |> Enum.concat(Enum.map(mismatches, &format_mismatch/1))
      |> Enum.reverse()

    """
    Schema mismatch detected for collection '#{collection_name}'.
    Manual intervention may be required.
    #{Enum.join(warnings, "\n")}
    """
  end

  defp add_warning_if(warnings, false, _msg), do: warnings
  defp add_warning_if(warnings, true, msg), do: [msg | warnings]

  defp format_mismatch({name, expected, current}) do
    "Field '#{name}' mismatch: expected #{format_field(expected)}, got #{format_field(current)}"
  end

  defp fields_match?(expected, current) do
    expected.data_type == current.data_type and
      expected.dimension == current.dimension and
      expected.max_length == current.max_length and
      expected.nullable == current.nullable and
      expected.is_partition_key == current.is_partition_key and
      expected.is_clustering_key == current.is_clustering_key and
      expected.element_type == current.element_type and
      expected.max_capacity == current.max_capacity
  end

  defp format_field(field) do
    base = to_string(field.data_type)

    attrs =
      []
      |> add_attr_if(field.dimension, "dim=#{field.dimension}")
      |> add_attr_if(field.max_length, "max_length=#{field.max_length}")
      |> add_attr_if(field.nullable, "nullable")
      |> add_attr_if(field.is_partition_key, "partition_key")
      |> add_attr_if(field.is_clustering_key, "clustering_key")
      |> add_attr_if(field.element_type, "element=#{field.element_type}")
      |> add_attr_if(field.max_capacity, "max_capacity=#{field.max_capacity}")

    case attrs do
      [] -> base
      _ -> "#{base}(#{Enum.join(attrs, ", ")})"
    end
  end

  defp add_attr_if(attrs, nil, _), do: attrs
  defp add_attr_if(attrs, false, _), do: attrs
  defp add_attr_if(attrs, _, attr), do: [attr | attrs]

  defp ensure_indexes!(connection, collection_module, collection_name, opts) do
    if function_exported?(collection_module, :index_config, 0) do
      indexes = collection_module.index_config()
      Enum.each(indexes, &ensure_index!(connection, collection_name, &1, opts))
    end
  end

  defp ensure_index!(connection, collection_name, %Milvex.Index{} = desired_index, opts) do
    field_name = desired_index.field_name

    case get_current_index(connection, collection_name, field_name, opts) do
      {:ok, nil} ->
        create_index!(connection, collection_name, desired_index, opts)

      {:ok, current_index} ->
        if index_matches?(current_index, desired_index) do
          Logger.debug("Index on '#{field_name}' is up to date")
        else
          Logger.info("Index config changed on '#{field_name}', recreating...")
          recreate_index!(connection, collection_name, field_name, desired_index, opts)
        end

      {:error, reason} ->
        raise Grpc.exception(
                operation: :describe_index,
                code: :fetch_failed,
                message: "Failed to check index on '#{field_name}': #{format_error(reason)}"
              )
    end
  end

  defp get_current_index(connection, collection_name, field_name, opts) do
    case Milvex.describe_index(
           connection,
           collection_name,
           Keyword.put(opts, :field_name, field_name)
         ) do
      {:ok, index_descriptions} ->
        matching = Enum.find(index_descriptions, fn desc -> desc.field_name == field_name end)
        {:ok, matching}

      {:error, %{code: 700}} ->
        {:ok, nil}

      {:error, %{message: msg}} when is_binary(msg) ->
        if String.contains?(msg, "index not found") do
          {:ok, nil}
        else
          {:error, msg}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp index_matches?(current_desc, desired_index) do
    current_params = extract_index_params(current_desc.params)
    desired_type = normalize_index_type(desired_index.index_type)
    desired_metric = normalize_metric_type(desired_index.metric_type)

    type_matches? = current_params["index_type"] == desired_type
    metric_matches? = current_params["metric_type"] == desired_metric
    params_match? = index_params_match?(current_params, desired_index)

    type_matches? and metric_matches? and params_match?
  end

  defp index_params_match?(current_params, desired_index) do
    case desired_index.index_type do
      :hnsw ->
        matches_param?(current_params, "M", desired_index.params[:M]) and
          matches_param?(current_params, "efConstruction", desired_index.params[:efConstruction])

      type when type in [:ivf_flat, :ivf_sq8, :scann] ->
        matches_param?(current_params, "nlist", desired_index.params[:nlist])

      :ivf_pq ->
        matches_param?(current_params, "nlist", desired_index.params[:nlist]) and
          matches_param?(current_params, "m", desired_index.params[:m]) and
          matches_param?(current_params, "nbits", desired_index.params[:nbits])

      _ ->
        true
    end
  end

  defp matches_param?(_current, _key, nil), do: true

  defp matches_param?(current_params, key, expected) do
    case current_params[key] do
      nil -> true
      value -> to_string(value) == to_string(expected)
    end
  end

  defp extract_index_params(params) when is_list(params) do
    Map.new(params, fn kv -> {kv.key, kv.value} end)
  end

  defp normalize_index_type(type) when is_atom(type) do
    type |> Atom.to_string() |> String.upcase()
  end

  defp normalize_metric_type(type) when is_atom(type) do
    type |> Atom.to_string() |> String.upcase()
  end

  defp create_index!(connection, collection_name, %Milvex.Index{} = index, opts) do
    case Milvex.create_index(connection, collection_name, index, opts) do
      :ok ->
        Logger.info("Created index on '#{index.field_name}' for #{collection_name}")

      {:error, reason} ->
        raise Grpc.exception(
                operation: :create_index,
                code: :create_failed,
                message:
                  "Failed to create index on '#{index.field_name}': #{format_error(reason)}"
              )
    end
  end

  defp recreate_index!(connection, collection_name, field_name, new_index, opts) do
    case Milvex.drop_index(connection, collection_name, field_name, opts) do
      :ok ->
        create_index!(connection, collection_name, new_index, opts)

      {:error, reason} ->
        raise Grpc.exception(
                operation: :drop_index,
                code: :drop_failed,
                message: "Failed to drop index on '#{field_name}': #{format_error(reason)}"
              )
    end
  end

  defp create_collection!(connection, collection_module, collection_name, opts) do
    schema = Milvex.Collection.to_schema(collection_module)

    case Milvex.create_collection(connection, collection_name, schema, opts) do
      :ok ->
        Logger.info("Created Milvus collection: #{collection_name}")

      {:error, reason} ->
        raise Grpc.exception(
                operation: :create_collection,
                code: :create_failed,
                message:
                  "Failed to create collection '#{collection_name}': #{format_error(reason)}"
              )
    end
  end

  defp format_error(error) when is_exception(error), do: Exception.message(error)
  defp format_error(error), do: to_string(error)
end

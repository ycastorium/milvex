defmodule Milvex.SearchResult do
  @moduledoc """
  Parser for Milvus search results.

  Converts the flat SearchResultData format from Milvus into
  structured results grouped by query.

  ## Examples

      # Parse from proto
      {:ok, result} = SearchResult.from_proto(search_results)

      # Access results
      result.num_queries      # Number of queries
      result.top_k            # Top-K used
      result.collection_name  # Collection searched

      # Hits GROUPED BY QUERY - result.hits is list of lists
      result.hits             # [[query1_hits], [query2_hits], ...]
      query1_hits = Enum.at(result.hits, 0)

      # Each hit in a query group
      hit.id                  # Primary key
      hit.score               # Similarity score
      hit.distance            # Raw distance (optional)
      hit.fields              # Map of output field values
  """

  alias Milvex.Data.FieldData
  alias Milvex.Milvus.Proto.Milvus.SearchResults
  alias Milvex.Milvus.Proto.Schema.{IDs, LongArray, SearchResultData, StringArray}

  defmodule Hit do
    @moduledoc """
    Represents a single search hit.
    """
    @type t :: %__MODULE__{
            id: integer() | String.t(),
            score: float(),
            distance: float() | nil,
            fields: map()
          }

    defstruct [:id, :score, :distance, :fields]
  end

  @type t :: %__MODULE__{
          num_queries: non_neg_integer(),
          top_k: non_neg_integer(),
          hits: [[Hit.t()]],
          collection_name: String.t()
        }

  defstruct [:num_queries, :top_k, :hits, :collection_name]

  @doc """
  Parses a SearchResults proto into a SearchResult struct.

  Converts flat result arrays into hits grouped by query.

  ## Parameters
    - `proto` - The SearchResults protobuf struct
  """
  @spec from_proto(SearchResults.t()) :: t()
  def from_proto(%SearchResults{results: nil} = proto) do
    %__MODULE__{
      num_queries: 0,
      top_k: 0,
      hits: [],
      collection_name: proto.collection_name
    }
  end

  def from_proto(%SearchResults{results: results} = proto) do
    hits = build_hits(results)

    %__MODULE__{
      num_queries: results.num_queries,
      top_k: results.top_k,
      hits: hits,
      collection_name: proto.collection_name
    }
  end

  @doc """
  Returns the total number of hits across all queries.
  """
  @spec total_hits(t()) :: non_neg_integer()
  def total_hits(%__MODULE__{hits: hits}) do
    hits |> List.flatten() |> length()
  end

  @doc """
  Returns the hits for a specific query (0-indexed).
  """
  @spec get_query_hits(t(), non_neg_integer()) :: [Hit.t()]
  def get_query_hits(%__MODULE__{hits: hits}, query_index) when query_index >= 0 do
    Enum.at(hits, query_index, [])
  end

  @doc """
  Returns the top hit for each query.
  """
  @spec top_hits(t()) :: [Hit.t() | nil]
  def top_hits(%__MODULE__{hits: hits}) do
    Enum.map(hits, &List.first/1)
  end

  @doc """
  Checks if the result is empty.
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{hits: hits}), do: hits == [] or Enum.all?(hits, &(&1 == []))

  defp build_hits(%SearchResultData{} = data) do
    ids = extract_ids(data.ids)
    scores = data.scores || []
    distances = data.distances || []
    topks = data.topks || []
    output_columns = parse_output_columns(data.fields_data)

    hits = build_flat_hits(ids, scores, distances, output_columns)
    group_hits_by_query(hits, topks)
  end

  defp extract_ids(nil), do: []

  defp extract_ids(%IDs{id_field: {:int_id, %LongArray{data: ids}}}), do: ids
  defp extract_ids(%IDs{id_field: {:str_id, %StringArray{data: ids}}}), do: ids
  defp extract_ids(_), do: []

  defp parse_output_columns(fields_data) when is_list(fields_data) do
    Enum.reduce(fields_data, %{}, fn field_data, acc ->
      {name, values} = FieldData.from_proto(field_data)
      Map.put(acc, name, values)
    end)
  end

  defp parse_output_columns(_), do: %{}

  defp build_flat_hits(ids, scores, distances, output_columns) do
    field_rows = transpose_output_columns(output_columns, length(ids))
    distances_padded = pad_list(distances, length(ids), nil)
    scores_padded = pad_list(scores, length(ids), 0.0)

    [ids, scores_padded, distances_padded, field_rows]
    |> Enum.zip()
    |> Enum.map(fn {id, score, distance, fields} ->
      %Hit{id: id, score: score, distance: distance, fields: fields}
    end)
  end

  defp transpose_output_columns(columns, count) when map_size(columns) == 0 do
    List.duplicate(%{}, count)
  end

  defp transpose_output_columns(_columns, 0), do: []

  defp transpose_output_columns(columns, _count) do
    {field_names, value_lists} = columns |> Map.to_list() |> Enum.unzip()

    value_lists
    |> Enum.zip()
    |> Enum.map(fn tuple ->
      tuple
      |> Tuple.to_list()
      |> Enum.zip(field_names)
      |> Map.new(fn {val, name} -> {name, val} end)
    end)
  end

  defp pad_list(list, target_len, _default) when length(list) >= target_len, do: list

  defp pad_list(list, target_len, default),
    do: list ++ List.duplicate(default, target_len - length(list))

  defp group_hits_by_query(hits, []), do: [hits]

  defp group_hits_by_query(hits, topks) do
    {groups, _} =
      Enum.reduce(topks, {[], hits}, fn count, {groups, remaining} ->
        {group, rest} = Enum.split(remaining, count)
        {[group | groups], rest}
      end)

    Enum.reverse(groups)
  end
end

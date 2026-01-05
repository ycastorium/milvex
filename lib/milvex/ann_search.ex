defmodule Milvex.AnnSearch do
  @moduledoc """
  Represents a single ANN search request for hybrid search.

  Supports both vector data (for dense/sparse vector search) and
  text data (for BM25 full-text search).

  ## Examples

      # Vector search
      {:ok, search} = AnnSearch.new("embedding", [[0.1, 0.2, 0.3]],
        limit: 10,
        params: %{nprobe: 10}
      )

      # Text search (BM25)
      {:ok, search} = AnnSearch.new("text_sparse", ["search query"],
        limit: 10
      )
  """

  alias Milvex.Errors.Invalid

  @typedoc """
  Query data - either vectors for dense/sparse search or text strings for BM25.
  """
  @type query_data :: [[number()]] | [String.t()]

  @type t :: %__MODULE__{
          anns_field: String.t(),
          data: query_data(),
          limit: pos_integer(),
          params: map() | nil,
          expr: String.t() | nil
        }

  defstruct [:anns_field, :data, :limit, :params, :expr]

  @doc """
  Creates a new ANN search request.

  ## Parameters

    - `anns_field` - Name of the vector field to search
    - `data` - Query vectors or text strings
    - `opts` - Options (see below)

  ## Options

    - `:limit` - (required) Maximum results for this sub-search
    - `:params` - Search parameters map (e.g., `%{nprobe: 10}`)
    - `:expr` - Filter expression string

  ## Examples

      {:ok, search} = AnnSearch.new("embedding", [[0.1, 0.2]], limit: 10)
      {:ok, search} = AnnSearch.new("text_sparse", ["query"], limit: 5)
  """
  @spec new(String.t(), query_data(), keyword()) :: {:ok, t()} | {:error, Invalid.t()}
  def new(anns_field, data, opts \\ []) do
    with :ok <- validate_anns_field(anns_field),
         :ok <- validate_data(data),
         {:ok, limit} <- validate_limit(opts[:limit]) do
      {:ok,
       %__MODULE__{
         anns_field: anns_field,
         data: data,
         limit: limit,
         params: opts[:params],
         expr: opts[:expr]
       }}
    end
  end

  defp validate_anns_field(field) when is_binary(field) and byte_size(field) > 0, do: :ok

  defp validate_anns_field(_) do
    {:error, Invalid.exception(field: :anns_field, message: "must be a non-empty string")}
  end

  defp validate_data([_ | _] = data) do
    cond do
      all_vectors?(data) ->
        :ok

      all_strings?(data) ->
        :ok

      true ->
        {:error,
         Invalid.exception(
           field: :data,
           message: "must be list of vectors or list of text queries, not mixed"
         )}
    end
  end

  defp validate_data(_) do
    {:error, Invalid.exception(field: :data, message: "must be a non-empty list")}
  end

  defp validate_limit(nil) do
    {:error, Invalid.exception(field: :limit, message: "is required")}
  end

  defp validate_limit(limit) when is_integer(limit) and limit > 0 do
    {:ok, limit}
  end

  defp validate_limit(_) do
    {:error, Invalid.exception(field: :limit, message: "must be a positive integer")}
  end

  defp all_vectors?(data) do
    Enum.all?(data, fn
      [_ | _] = item -> Enum.all?(item, &is_number/1)
      _ -> false
    end)
  end

  defp all_strings?(data) do
    Enum.all?(data, &is_binary/1)
  end
end

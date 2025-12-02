defmodule Milvex.QueryResult do
  @moduledoc """
  Parser for Milvus query results.

  Converts the columnar FieldData format from Milvus QueryResults
  into row-oriented Elixir maps for easy consumption.

  ## Examples

      # Parse from proto
      {:ok, result} = QueryResult.from_proto(query_results)

      # Access results
      result.rows             # List of row maps
      result.collection_name  # Collection queried
      result.output_fields    # Field names returned

      # Each row is a map
      row = hd(result.rows)
      row.id     # Primary key value
      row.title  # Field value
  """

  alias Milvex.Data.FieldData
  alias Milvex.Milvus.Proto.Milvus.QueryResults

  @type t :: %__MODULE__{
          rows: [map()],
          collection_name: String.t(),
          output_fields: [String.t()],
          primary_field_name: String.t() | nil
        }

  defstruct [:rows, :collection_name, :output_fields, :primary_field_name]

  @doc """
  Parses a QueryResults proto into a QueryResult struct.

  Converts columnar FieldData to row-oriented maps.

  ## Parameters
    - `proto` - The QueryResults protobuf struct
  """
  @spec from_proto(QueryResults.t()) :: t()
  def from_proto(%QueryResults{} = proto) do
    columns = parse_columns(proto.fields_data)
    rows = transpose_columns_to_rows(columns)

    %__MODULE__{
      rows: rows,
      collection_name: proto.collection_name,
      output_fields: proto.output_fields,
      primary_field_name: empty_to_nil(proto.primary_field_name)
    }
  end

  @doc """
  Returns the number of rows in the result.
  """
  @spec num_rows(t()) :: non_neg_integer()
  def num_rows(%__MODULE__{rows: rows}), do: length(rows)

  @doc """
  Gets a specific row by index (0-based).
  """
  @spec get_row(t(), non_neg_integer()) :: map() | nil
  def get_row(%__MODULE__{rows: rows}, index) when index >= 0 do
    Enum.at(rows, index)
  end

  @doc """
  Gets all values for a specific field across all rows.
  """
  @spec get_column(t(), String.t() | atom()) :: [term()]
  def get_column(%__MODULE__{rows: rows}, field_name) do
    key = to_string(field_name)
    Enum.map(rows, &Map.get(&1, key))
  end

  @doc """
  Checks if the result is empty.
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{rows: rows}), do: rows == []

  defp parse_columns(fields_data) do
    Enum.reduce(fields_data, %{}, fn field_data, acc ->
      {name, values} = FieldData.from_proto(field_data)
      Map.put(acc, name, values)
    end)
  end

  defp transpose_columns_to_rows(columns) when map_size(columns) == 0, do: []

  defp transpose_columns_to_rows(columns) do
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

  defp empty_to_nil(""), do: nil
  defp empty_to_nil(str), do: str
end

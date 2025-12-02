defmodule Milvex.Data do
  @moduledoc """
  Builder for Milvus insert data.

  Converts row-oriented Elixir data to column-oriented FieldData format
  required by Milvus for insert operations.

  ## Examples

      # Create data from row format
      schema = Schema.build!(
        name: "movies",
        fields: [
          Field.primary_key("id", :int64),
          Field.varchar("title", 256),
          Field.vector("embedding", 128)
        ]
      )

      {:ok, data} = Data.from_rows([
        %{id: 1, title: "Movie 1", embedding: [0.1, 0.2, ...]},
        %{id: 2, title: "Movie 2", embedding: [0.3, 0.4, ...]}
      ], schema)

      # Or from column format
      {:ok, data} = Data.from_columns(%{
        id: [1, 2],
        title: ["Movie 1", "Movie 2"],
        embedding: [[0.1, 0.2, ...], [0.3, 0.4, ...]]
      }, schema)

      # Convert to proto for insert
      fields_data = Data.to_proto(data)
  """

  alias Milvex.Data.FieldData
  alias Milvex.Schema

  @type t :: %__MODULE__{
          fields: %{String.t() => list()},
          schema: Schema.t(),
          num_rows: non_neg_integer()
        }

  defstruct [:fields, :schema, :num_rows]

  @doc """
  Creates Data from a list of row maps.

  Each row should be a map with field names as keys. All rows must have
  consistent field names.

  ## Parameters
    - `rows` - List of maps representing rows
    - `schema` - The Schema for type information

  ## Returns
    - `{:ok, data}` on success
    - `{:error, error}` on validation failure
  """
  @spec from_rows(list(map()), Schema.t()) :: {:ok, t()} | {:error, Milvex.Error.t()}
  def from_rows(rows, %Schema{} = schema) when is_list(rows) do
    with :ok <- validate_rows(rows, schema) do
      columns = transpose_rows_to_columns(rows, schema)
      num_rows = length(rows)

      {:ok, %__MODULE__{fields: columns, schema: schema, num_rows: num_rows}}
    end
  end

  @doc """
  Creates Data from a list of row maps. Raises on error.
  """
  @spec from_rows!(list(map()), Schema.t()) :: t()
  def from_rows!(rows, schema) do
    case from_rows(rows, schema) do
      {:ok, data} -> data
      {:error, error} -> raise error
    end
  end

  @doc """
  Creates Data from column format.

  Columns should be a map where keys are field names and values are lists
  of values for that column. All columns must have the same length.

  ## Parameters
    - `columns` - Map of field_name => [values]
    - `schema` - The Schema for type information

  ## Returns
    - `{:ok, data}` on success
    - `{:error, error}` on validation failure
  """
  @spec from_columns(map(), Schema.t()) :: {:ok, t()} | {:error, Milvex.Error.t()}
  def from_columns(columns, %Schema{} = schema) when is_map(columns) do
    columns = normalize_column_keys(columns)

    with :ok <- validate_columns(columns, schema) do
      num_rows = get_column_length(columns)

      {:ok, %__MODULE__{fields: columns, schema: schema, num_rows: num_rows}}
    end
  end

  @doc """
  Creates Data from column format. Raises on error.
  """
  @spec from_columns!(map(), Schema.t()) :: t()
  def from_columns!(columns, schema) do
    case from_columns(columns, schema) do
      {:ok, data} -> data
      {:error, error} -> raise error
    end
  end

  @doc """
  Converts the Data to a list of FieldData proto structs.
  """
  @spec to_proto(t()) :: [Milvex.Milvus.Proto.Schema.FieldData.t()]
  def to_proto(%__MODULE__{fields: fields, schema: schema}) do
    schema.fields
    |> Enum.filter(fn field -> not field.auto_id end)
    |> Enum.map(fn field ->
      values = Map.get(fields, field.name, [])
      FieldData.to_proto(field.name, values, field)
    end)
  end

  @doc """
  Returns the number of rows in the data.
  """
  @spec num_rows(t()) :: non_neg_integer()
  def num_rows(%__MODULE__{num_rows: n}), do: n

  @doc """
  Returns the field names in the data.
  """
  @spec field_names(t()) :: [String.t()]
  def field_names(%__MODULE__{fields: fields}), do: Map.keys(fields)

  @doc """
  Gets the values for a specific field.
  """
  @spec get_field(t(), String.t() | atom()) :: list() | nil
  def get_field(%__MODULE__{fields: fields}, name) when is_atom(name) do
    Map.get(fields, Atom.to_string(name))
  end

  def get_field(%__MODULE__{fields: fields}, name) when is_binary(name) do
    Map.get(fields, name)
  end

  defp transpose_rows_to_columns(rows, schema) do
    field_names =
      schema.fields
      |> Enum.filter(fn f -> not f.auto_id end)
      |> Enum.map(& &1.name)

    init = Map.new(field_names, fn name -> {name, []} end)

    rows
    |> Enum.reduce(init, fn row, acc ->
      Enum.reduce(field_names, acc, fn name, inner_acc ->
        Map.update!(inner_acc, name, fn vals -> [get_row_value(row, name) | vals] end)
      end)
    end)
    |> Map.new(fn {k, v} -> {k, Enum.reverse(v)} end)
  end

  defp get_row_value(row, field_name) do
    cond do
      Map.has_key?(row, field_name) -> Map.get(row, field_name)
      Map.has_key?(row, String.to_atom(field_name)) -> Map.get(row, String.to_atom(field_name))
      true -> nil
    end
  end

  defp normalize_column_keys(columns) do
    Map.new(columns, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {k, v}
    end)
  end

  defp validate_rows([], _schema), do: :ok

  defp validate_rows(rows, schema) do
    required_fields = get_required_field_names(schema)

    with :ok <- validate_row_consistency(rows) do
      validate_required_fields_present(rows, required_fields)
    end
  end

  defp validate_row_consistency([]), do: :ok

  defp validate_row_consistency([first | rest]) do
    first_keys = first |> Map.keys() |> Enum.sort()

    case Enum.find_index(rest, fn row ->
           row |> Map.keys() |> Enum.sort() != first_keys
         end) do
      nil ->
        :ok

      idx ->
        {:error,
         invalid_error(:rows, "row at index #{idx + 1} has different fields than the first row")}
    end
  end

  defp validate_required_fields_present([], _required_fields), do: :ok

  defp validate_required_fields_present([first | _], required_fields) do
    row_keys =
      first
      |> Map.keys()
      |> Enum.map(&to_string/1)
      |> MapSet.new()

    missing = MapSet.difference(required_fields, row_keys)

    if MapSet.size(missing) == 0 do
      :ok
    else
      missing_str = missing |> MapSet.to_list() |> Enum.join(", ")
      {:error, invalid_error(:rows, "missing required fields: #{missing_str}")}
    end
  end

  defp validate_columns(columns, schema) do
    required_fields = get_required_field_names(schema)

    with :ok <- validate_column_lengths(columns) do
      validate_required_columns_present(columns, required_fields)
    end
  end

  defp validate_column_lengths(columns) when map_size(columns) == 0, do: :ok

  defp validate_column_lengths(columns) do
    lengths =
      columns
      |> Map.values()
      |> Enum.map(&length/1)
      |> Enum.uniq()

    case lengths do
      [_] -> :ok
      _ -> {:error, invalid_error(:columns, "all columns must have the same length")}
    end
  end

  defp validate_required_columns_present(columns, required_fields) do
    column_keys = columns |> Map.keys() |> MapSet.new()
    missing = MapSet.difference(required_fields, column_keys)

    if MapSet.size(missing) == 0 do
      :ok
    else
      missing_str = missing |> MapSet.to_list() |> Enum.join(", ")
      {:error, invalid_error(:columns, "missing required fields: #{missing_str}")}
    end
  end

  defp get_required_field_names(schema) do
    schema.fields
    |> Enum.filter(fn field ->
      not field.auto_id and not field.nullable
    end)
    |> Enum.map(& &1.name)
    |> MapSet.new()
  end

  defp get_column_length(columns) when map_size(columns) == 0, do: 0

  defp get_column_length(columns) do
    columns
    |> Map.values()
    |> List.first()
    |> length()
  end

  defp invalid_error(field, message) do
    Milvex.Errors.Invalid.exception(field: field, message: message)
  end
end

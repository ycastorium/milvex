defmodule Milvex.Data.FieldData do
  @moduledoc """
  Converts between Elixir values and Milvus FieldData protobuf structures.

  This module handles the conversion of column data to/from the FieldData proto
  format used by Milvus for insert operations and result parsing.
  """

  import Bitwise

  alias Milvex.Schema.Field

  alias Milvex.Milvus.Proto.Schema.ArrayArray
  alias Milvex.Milvus.Proto.Schema.BoolArray
  alias Milvex.Milvus.Proto.Schema.DoubleArray
  alias Milvex.Milvus.Proto.Schema.FieldData
  alias Milvex.Milvus.Proto.Schema.FloatArray
  alias Milvex.Milvus.Proto.Schema.IntArray
  alias Milvex.Milvus.Proto.Schema.JSONArray
  alias Milvex.Milvus.Proto.Schema.LongArray
  alias Milvex.Milvus.Proto.Schema.ScalarField
  alias Milvex.Milvus.Proto.Schema.SparseFloatArray
  alias Milvex.Milvus.Proto.Schema.StringArray
  alias Milvex.Milvus.Proto.Schema.StructArrayField
  alias Milvex.Milvus.Proto.Schema.TimestamptzArray
  alias Milvex.Milvus.Proto.Schema.VectorArray
  alias Milvex.Milvus.Proto.Schema.VectorField

  @doc """
  Converts a column of values to a FieldData proto struct.

  ## Parameters
    - `field_name` - Name of the field
    - `values` - List of values for this column
    - `field_schema` - The Field struct with type information

  ## Examples

      iex> field = Field.scalar("age", :int32)
      iex> FieldData.to_proto("age", [25, 30, 35], field)
      %FieldData{field_name: "age", type: :Int32, scalars: %ScalarField{...}}
  """
  @spec to_proto(String.t(), list(), Field.t()) :: FieldData.t()
  def to_proto(field_name, values, %Field{} = field_schema) do
    data_type = field_schema.data_type

    cond do
      data_type == :array_of_struct ->
        build_struct_array_field_data(field_name, values, field_schema)

      Field.vector_type?(data_type) ->
        build_vector_field_data(field_name, values, field_schema)

      true ->
        build_scalar_field_data(field_name, values, field_schema)
    end
  end

  @doc """
  Converts dynamic field values to a FieldData proto struct with is_dynamic flag.

  Used for dynamic fields when `enable_dynamic_field` is true on the schema.
  Each value should be a map representing the dynamic fields for that row.

  ## Parameters
    - `field_name` - Name of the dynamic field (typically "$meta")
    - `values` - List of maps, one per row, containing the dynamic field values
  """
  @spec to_proto_dynamic(String.t(), list(map())) :: FieldData.t()
  def to_proto_dynamic(field_name, values) do
    json_bytes = Enum.map(values, &encode_json/1)
    scalar_field = %ScalarField{data: {:json_data, %JSONArray{data: json_bytes}}}

    %FieldData{
      field_name: field_name,
      type: :JSON,
      field: {:scalars, scalar_field},
      is_dynamic: true
    }
  end

  @doc """
  Extracts values from a FieldData proto struct.

  Returns a tuple of `{field_name, values}` where values is a list.

  ## Examples

      iex> field_data = %FieldData{field_name: "age", scalars: %ScalarField{data: {:int_data, %IntArray{data: [25, 30]}}}}
      iex> FieldData.from_proto(field_data)
      {"age", [25, 30]}
  """
  @spec from_proto(FieldData.t()) :: {String.t(), list()}
  def from_proto(%FieldData{} = field_data) do
    values = extract_values(field_data)
    {field_data.field_name, values}
  end

  @doc """
  Builds a ScalarField proto for the given data type and values.
  """
  @spec build_scalar_field(Field.data_type(), list()) :: ScalarField.t()
  def build_scalar_field(data_type, values) do
    case data_type do
      :bool ->
        %ScalarField{data: {:bool_data, %BoolArray{data: values}}}

      type when type in [:int8, :int16, :int32] ->
        %ScalarField{data: {:int_data, %IntArray{data: values}}}

      :int64 ->
        %ScalarField{data: {:long_data, %LongArray{data: values}}}

      :float ->
        %ScalarField{data: {:float_data, %FloatArray{data: values}}}

      :double ->
        %ScalarField{data: {:double_data, %DoubleArray{data: values}}}

      type when type in [:varchar, :text] ->
        %ScalarField{data: {:string_data, %StringArray{data: values}}}

      :json ->
        json_bytes = Enum.map(values, &encode_json/1)
        %ScalarField{data: {:json_data, %JSONArray{data: json_bytes}}}

      :timestamp ->
        string_values = Enum.map(values, &encode_timestamp_string/1)
        %ScalarField{data: {:string_data, %StringArray{data: string_values}}}
    end
  end

  @doc """
  Builds a VectorField proto for the given data type and values.

  For dense vectors, values should be a list of lists (each inner list is a vector).
  For sparse vectors, values should be a list of tuple lists: `[[{idx, val}, ...], ...]`.
  """
  @spec build_vector_field(Field.data_type(), list(), pos_integer() | nil) :: VectorField.t()
  def build_vector_field(data_type, values, dimension) do
    case data_type do
      :float_vector ->
        flat_values = List.flatten(values)
        %VectorField{dim: dimension, data: {:float_vector, %FloatArray{data: flat_values}}}

      :binary_vector ->
        binary_data = encode_binary_vectors(values)
        %VectorField{dim: dimension, data: {:binary_vector, binary_data}}

      :float16_vector ->
        binary_data = encode_float16_vectors(values)
        %VectorField{dim: dimension, data: {:float16_vector, binary_data}}

      :bfloat16_vector ->
        binary_data = encode_bfloat16_vectors(values)
        %VectorField{dim: dimension, data: {:bfloat16_vector, binary_data}}

      :int8_vector ->
        binary_data = encode_int8_vectors(values)
        %VectorField{dim: dimension, data: {:int8_vector, binary_data}}

      :sparse_float_vector ->
        {contents, max_dim} = encode_sparse_vectors(values)
        sparse = %SparseFloatArray{contents: contents, dim: max_dim}
        %VectorField{data: {:sparse_float_vector, sparse}}
    end
  end

  @doc """
  Extracts values from a ScalarField proto.
  """
  @spec extract_scalar_values(ScalarField.t()) :: list()
  def extract_scalar_values(%ScalarField{data: data}), do: do_extract_scalar(data)

  defp do_extract_scalar({:bool_data, %BoolArray{data: values}}), do: values
  defp do_extract_scalar({:int_data, %IntArray{data: values}}), do: values
  defp do_extract_scalar({:long_data, %LongArray{data: values}}), do: values
  defp do_extract_scalar({:float_data, %FloatArray{data: values}}), do: values
  defp do_extract_scalar({:double_data, %DoubleArray{data: values}}), do: values
  defp do_extract_scalar({:string_data, %StringArray{data: values}}), do: values

  defp do_extract_scalar({:json_data, %JSONArray{data: values}}),
    do: Enum.map(values, &decode_json/1)

  defp do_extract_scalar({:timestamptz_data, %TimestamptzArray{data: values}}),
    do: Enum.map(values, &decode_timestamp/1)

  defp do_extract_scalar({:array_data, %ArrayArray{data: scalar_fields}}) do
    Enum.map(scalar_fields, &extract_scalar_values/1)
  end

  defp do_extract_scalar(_), do: []

  @doc """
  Extracts values from a VectorField proto.

  Returns vectors as lists for float vectors, or as-is for binary types.
  Sparse vectors are returned as tuple lists.
  """
  @spec extract_vector_values(VectorField.t()) :: list()
  def extract_vector_values(%VectorField{dim: dim, data: data}) do
    case data do
      {:float_vector, %FloatArray{data: values}} ->
        chunk_vector(values, dim)

      {:binary_vector, binary} ->
        decode_binary_vectors(binary, dim)

      {:float16_vector, binary} ->
        decode_float16_vectors(binary, dim)

      {:bfloat16_vector, binary} ->
        decode_bfloat16_vectors(binary, dim)

      {:int8_vector, binary} ->
        decode_int8_vectors(binary, dim)

      {:sparse_float_vector, %SparseFloatArray{contents: contents}} ->
        decode_sparse_vectors(contents)

      {:vector_array, %VectorArray{data: vector_fields}} ->
        Enum.map(vector_fields, &extract_vector_values/1)

      _ ->
        []
    end
  end

  defp build_scalar_field_data(field_name, values, %Field{
         data_type: data_type,
         nullable: nullable
       }) do
    has_nils = Enum.any?(values, &is_nil/1)

    {non_nil_values, valid_data} =
      if has_nils and nullable do
        valid = Enum.map(values, &(not is_nil(&1)))
        non_nils = Enum.reject(values, &is_nil/1)
        {non_nils, valid}
      else
        {values, []}
      end

    scalar_field = build_scalar_field(data_type, non_nil_values)

    %FieldData{
      field_name: field_name,
      type: data_type_to_proto(data_type),
      field: {:scalars, scalar_field},
      valid_data: valid_data
    }
  end

  defp build_vector_field_data(field_name, values, %Field{data_type: data_type, dimension: dim}) do
    vector_field = build_vector_field(data_type, values, dim)

    %FieldData{
      field_name: field_name,
      type: data_type_to_proto(data_type),
      field: {:vectors, vector_field}
    }
  end

  defp build_struct_array_field_data(field_name, values, %Field{struct_schema: struct_schema}) do
    fields_data = encode_struct_array_columns(values, struct_schema)

    struct_array = %StructArrayField{
      fields: fields_data
    }

    %FieldData{
      field_name: field_name,
      type: :ArrayOfStruct,
      field: {:struct_arrays, struct_array}
    }
  end

  defp encode_struct_array_columns(values, struct_schema) do
    Enum.map(struct_schema, fn field ->
      values_per_row = extract_nested_field_values_per_row(values, field.name)
      encode_nested_struct_field(field.name, values_per_row, field)
    end)
  end

  defp extract_nested_field_values_per_row(rows_of_struct_arrays, field_name) do
    Enum.map(rows_of_struct_arrays, fn
      nil ->
        []

      struct_array when is_list(struct_array) ->
        Enum.map(struct_array, fn struct_item ->
          get_struct_field_value(struct_item, field_name)
        end)
    end)
  end

  defp get_struct_field_value(struct_item, field_name) when is_map(struct_item) do
    field_name_str = to_string(field_name)

    Enum.find_value(struct_item, fn {k, v} ->
      if to_string(k) == field_name_str, do: v
    end)
  end

  defp get_struct_field_value(_, _), do: nil

  defp encode_nested_struct_field(
         field_name,
         values_per_row,
         %Field{data_type: data_type} = field
       )
       when data_type in [
              :float_vector,
              :binary_vector,
              :float16_vector,
              :bfloat16_vector,
              :sparse_float_vector,
              :int8_vector
            ] do
    row_vectors =
      Enum.map(values_per_row, fn row_values ->
        build_vector_field(data_type, row_values, field.dimension)
      end)

    vector_array = %VectorArray{
      dim: field.dimension,
      data: row_vectors,
      element_type: data_type_to_proto(data_type)
    }

    %FieldData{
      field_name: field_name,
      type: :ArrayOfVector,
      field: {:vectors, %VectorField{dim: field.dimension, data: {:vector_array, vector_array}}}
    }
  end

  defp encode_nested_struct_field(field_name, values_per_row, %Field{data_type: data_type}) do
    row_scalars =
      Enum.map(values_per_row, fn row_values ->
        build_scalar_field(data_type, row_values)
      end)

    array_array = %ArrayArray{
      data: row_scalars,
      element_type: data_type_to_proto(data_type)
    }

    %FieldData{
      field_name: field_name,
      type: :Array,
      field: {:scalars, %ScalarField{data: {:array_data, array_array}}}
    }
  end

  defp extract_values(%FieldData{field: {:scalars, scalars}}) do
    extract_scalar_values(scalars)
  end

  defp extract_values(%FieldData{field: {:vectors, vectors}}) do
    extract_vector_values(vectors)
  end

  defp extract_values(%FieldData{field: {:struct_arrays, struct_arrays}}) do
    extract_struct_array_values(struct_arrays)
  end

  defp extract_values(_), do: []

  defp extract_struct_array_values(%StructArrayField{fields: fields}) do
    columns =
      Enum.map(fields, fn field_data ->
        {field_data.field_name, extract_values(field_data)}
      end)
      |> Map.new()

    transpose_struct_columns_to_rows(columns)
  end

  defp transpose_struct_columns_to_rows(columns) when map_size(columns) == 0, do: []

  defp transpose_struct_columns_to_rows(columns) do
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

  defp chunk_vector(flat_list, dim) when is_integer(dim) and dim > 0 do
    Enum.chunk_every(flat_list, dim)
  end

  defp chunk_vector(flat_list, _), do: [flat_list]

  defp encode_json(value) when is_binary(value), do: value
  defp encode_json(value), do: Jason.encode!(value)

  defp decode_json(bytes) do
    case Jason.decode(bytes) do
      {:ok, value} -> value
      {:error, _} -> bytes
    end
  end

  defp encode_timestamp_string(nil), do: nil

  defp encode_timestamp_string(%DateTime{} = dt) do
    DateTime.to_iso8601(dt)
  end

  defp encode_timestamp_string(%NaiveDateTime{} = ndt) do
    ndt
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp encode_timestamp_string(unix) when is_integer(unix) do
    unix
    |> DateTime.from_unix!(:microsecond)
    |> DateTime.to_iso8601()
  end

  defp encode_timestamp_string(iso_string) when is_binary(iso_string) do
    case DateTime.from_iso8601(iso_string) do
      {:ok, dt, _offset} ->
        DateTime.to_iso8601(dt)

      {:error, _} ->
        case NaiveDateTime.from_iso8601(iso_string) do
          {:ok, ndt} ->
            ndt
            |> DateTime.from_naive!("Etc/UTC")
            |> DateTime.to_iso8601()

          {:error, reason} ->
            raise ArgumentError, "Invalid timestamp format: #{iso_string}, reason: #{reason}"
        end
    end
  end

  defp decode_timestamp(iso_string) when is_binary(iso_string) do
    case DateTime.from_iso8601(iso_string) do
      {:ok, dt, _offset} -> dt
      {:error, _} -> iso_string
    end
  end

  defp decode_timestamp(unix_microseconds) when is_integer(unix_microseconds) do
    DateTime.from_unix!(unix_microseconds, :microsecond)
  end

  defp encode_binary_vectors(vectors) do
    vectors
    |> Enum.map(&encode_binary_vector/1)
    |> IO.iodata_to_binary()
  end

  defp encode_binary_vector(vector) when is_list(vector) do
    vector
    |> Enum.chunk_every(8)
    |> Enum.map(&bits_to_byte/1)
    |> IO.iodata_to_binary()
  end

  defp encode_binary_vector(binary) when is_binary(binary), do: binary

  defp bits_to_byte(bits) do
    bits
    |> Enum.with_index()
    |> Enum.reduce(0, fn {bit, idx}, acc ->
      if bit == 1 or bit == true, do: acc ||| 1 <<< (7 - idx), else: acc
    end)
  end

  defp decode_binary_vectors(binary, dim) do
    bytes_per_vector = div(dim, 8)

    binary
    |> :binary.bin_to_list()
    |> Enum.chunk_every(bytes_per_vector)
    |> Enum.map(&decode_binary_vector(&1, dim))
  end

  defp decode_binary_vector(bytes, dim) do
    bytes
    |> Enum.flat_map(&byte_to_bits/1)
    |> Enum.take(dim)
  end

  defp byte_to_bits(byte) do
    for i <- 7..0//-1, do: byte >>> i &&& 1
  end

  defp encode_float16_vectors(vectors) do
    encode_typed_vectors(vectors, :f16)
  end

  defp decode_float16_vectors(binary, dim) do
    decode_typed_vectors(binary, dim, :f16)
  end

  defp encode_bfloat16_vectors(vectors) do
    encode_typed_vectors(vectors, :bf16)
  end

  defp decode_bfloat16_vectors(binary, dim) do
    decode_typed_vectors(binary, dim, :bf16)
  end

  defp encode_int8_vectors(vectors) do
    encode_typed_vectors(vectors, :s8)
  end

  defp encode_typed_vectors([], _type), do: <<>>

  defp encode_typed_vectors([first | _] = vectors, _type) when is_binary(first) do
    IO.iodata_to_binary(vectors)
  end

  defp encode_typed_vectors([first | _] = vectors, type) when is_struct(first, Nx.Tensor) do
    vectors
    |> Nx.stack()
    |> Nx.as_type(type)
    |> Nx.to_binary()
  end

  defp encode_typed_vectors([first | _] = vectors, type) when is_list(first) do
    require_nx!()

    vectors
    |> Nx.tensor(type: type)
    |> Nx.to_binary()
  end

  defp require_nx! do
    unless Code.ensure_loaded?(Nx) do
      raise ArgumentError, """
      Nx is required to convert lists of floats to float16/bfloat16/int8 vectors.

      Either:
      1. Add {:nx, "~> 0.9"} to your dependencies
      2. Pass raw binary data instead
      3. Pass Nx tensors directly
      """
    end
  end

  defp decode_int8_vectors(binary, dim) do
    decode_typed_vectors(binary, dim, :s8)
  end

  defp decode_typed_vectors(binary, dim, type) do
    require_nx!()

    binary
    |> Nx.from_binary(type)
    |> Nx.reshape({:auto, dim})
    |> Nx.to_list()
  end

  defp encode_sparse_vectors(vectors) do
    {contents, dims} =
      vectors
      |> Enum.map(&encode_sparse_vector/1)
      |> Enum.unzip()

    max_dim = if dims == [], do: 0, else: Enum.max(dims)
    {contents, max_dim}
  end

  defp encode_sparse_vector(sparse_tuples) do
    sorted = Enum.sort_by(sparse_tuples, fn {idx, _val} -> idx end)

    max_idx =
      case sorted do
        [] -> 0
        list -> elem(List.last(list), 0) + 1
      end

    binary =
      sorted
      |> Enum.flat_map(fn {idx, val} ->
        [<<idx::32-little-unsigned>>, <<val::32-little-float>>]
      end)
      |> IO.iodata_to_binary()

    {binary, max_idx}
  end

  defp decode_sparse_vectors(contents) do
    Enum.map(contents, &decode_sparse_vector/1)
  end

  defp decode_sparse_vector(binary) do
    decode_sparse_pairs(binary, [])
  end

  defp decode_sparse_pairs(<<>>, acc), do: Enum.reverse(acc)

  defp decode_sparse_pairs(<<idx::32-little-unsigned, val::32-little-float, rest::binary>>, acc) do
    decode_sparse_pairs(rest, [{idx, val} | acc])
  end

  defp data_type_to_proto(:bool), do: :Bool
  defp data_type_to_proto(:int8), do: :Int8
  defp data_type_to_proto(:int16), do: :Int16
  defp data_type_to_proto(:int32), do: :Int32
  defp data_type_to_proto(:int64), do: :Int64
  defp data_type_to_proto(:float), do: :Float
  defp data_type_to_proto(:double), do: :Double
  defp data_type_to_proto(:varchar), do: :VarChar
  defp data_type_to_proto(:json), do: :JSON
  defp data_type_to_proto(:text), do: :Text
  defp data_type_to_proto(:timestamp), do: :Timestamptz
  defp data_type_to_proto(:array), do: :Array
  defp data_type_to_proto(:struct), do: :Struct
  defp data_type_to_proto(:array_of_struct), do: :ArrayOfStruct
  defp data_type_to_proto(:binary_vector), do: :BinaryVector
  defp data_type_to_proto(:float_vector), do: :FloatVector
  defp data_type_to_proto(:float16_vector), do: :Float16Vector
  defp data_type_to_proto(:bfloat16_vector), do: :BFloat16Vector
  defp data_type_to_proto(:sparse_float_vector), do: :SparseFloatVector
  defp data_type_to_proto(:int8_vector), do: :Int8Vector
end

defmodule Milvex.Data.FieldData do
  @moduledoc """
  Converts between Elixir values and Milvus FieldData protobuf structures.

  This module handles the conversion of column data to/from the FieldData proto
  format used by Milvus for insert operations and result parsing.
  """

  import Bitwise

  alias Milvex.Schema.Field

  alias Milvex.Milvus.Proto.Schema.{
    BoolArray,
    DoubleArray,
    FieldData,
    FloatArray,
    IntArray,
    JSONArray,
    LongArray,
    ScalarField,
    SparseFloatArray,
    StringArray,
    VectorField
  }

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

    if Field.vector_type?(data_type) do
      build_vector_field_data(field_name, values, field_schema)
    else
      build_scalar_field_data(field_name, values, field_schema)
    end
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
  def extract_scalar_values(%ScalarField{data: data}) do
    case data do
      {:bool_data, %BoolArray{data: values}} -> values
      {:int_data, %IntArray{data: values}} -> values
      {:long_data, %LongArray{data: values}} -> values
      {:float_data, %FloatArray{data: values}} -> values
      {:double_data, %DoubleArray{data: values}} -> values
      {:string_data, %StringArray{data: values}} -> values
      {:json_data, %JSONArray{data: values}} -> Enum.map(values, &decode_json/1)
      _ -> []
    end
  end

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

      _ ->
        []
    end
  end

  defp build_scalar_field_data(field_name, values, %Field{data_type: data_type}) do
    scalar_field = build_scalar_field(data_type, values)

    %FieldData{
      field_name: field_name,
      type: data_type_to_proto(data_type),
      field: {:scalars, scalar_field}
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

  defp extract_values(%FieldData{field: {:scalars, scalars}}) do
    extract_scalar_values(scalars)
  end

  defp extract_values(%FieldData{field: {:vectors, vectors}}) do
    extract_vector_values(vectors)
  end

  defp extract_values(_), do: []

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
    vectors
    |> Enum.flat_map(&encode_float16_vector/1)
    |> IO.iodata_to_binary()
  end

  defp encode_float16_vector(vector) do
    Enum.map(vector, &float_to_float16/1)
  end

  defp decode_float16_vectors(binary, dim) do
    bytes_per_vector = dim * 2

    for <<chunk::binary-size(bytes_per_vector) <- binary>> do
      for <<f16::16-little <- chunk>>, do: float16_to_float(f16)
    end
  end

  defp encode_bfloat16_vectors(vectors) do
    vectors
    |> Enum.flat_map(&encode_bfloat16_vector/1)
    |> IO.iodata_to_binary()
  end

  defp encode_bfloat16_vector(vector) do
    Enum.map(vector, &float_to_bfloat16/1)
  end

  defp decode_bfloat16_vectors(binary, dim) do
    bytes_per_vector = dim * 2

    for <<chunk::binary-size(bytes_per_vector) <- binary>> do
      for <<bf16::16-little <- chunk>>, do: bfloat16_to_float(bf16)
    end
  end

  defp encode_int8_vectors(vectors) do
    vectors
    |> List.flatten()
    |> Enum.map(&unsigned_int8/1)
    |> :binary.list_to_bin()
  end

  defp unsigned_int8(byte) when byte < 0, do: byte + 256
  defp unsigned_int8(byte), do: byte

  defp decode_int8_vectors(binary, dim) do
    binary
    |> :binary.bin_to_list()
    |> Enum.map(&signed_int8/1)
    |> Enum.chunk_every(dim)
  end

  defp signed_int8(byte) when byte > 127, do: byte - 256
  defp signed_int8(byte), do: byte

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

  defp float_to_float16(f) when is_float(f) do
    <<sign::1, exp::8, mantissa::23>> = <<f::32-float>>

    {f16_exp, f16_mantissa} =
      cond do
        exp == 0 -> {0, 0}
        exp == 255 -> {31, mantissa >>> 13}
        exp < 113 -> {0, 0}
        exp > 142 -> {31, 0}
        true -> {exp - 112, mantissa >>> 13}
      end

    <<sign::1, f16_exp::5, f16_mantissa::10>>
  end

  defp float16_to_float(f16) do
    <<sign::1, exp::5, mantissa::10>> = <<f16::16>>

    {f32_exp, f32_mantissa} =
      cond do
        exp == 0 -> {0, 0}
        exp == 31 -> {255, mantissa <<< 13}
        true -> {exp + 112, mantissa <<< 13}
      end

    <<result::32-float>> = <<sign::1, f32_exp::8, f32_mantissa::23>>
    result
  end

  defp float_to_bfloat16(f) when is_float(f) do
    <<hi::16, _lo::16>> = <<f::32-float>>
    <<hi::16-little>>
  end

  defp bfloat16_to_float(bf16) do
    <<hi::16>> = <<bf16::16-little>>
    <<result::32-float>> = <<hi::16, 0::16>>
    result
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
  defp data_type_to_proto(:array), do: :Array
  defp data_type_to_proto(:binary_vector), do: :BinaryVector
  defp data_type_to_proto(:float_vector), do: :FloatVector
  defp data_type_to_proto(:float16_vector), do: :Float16Vector
  defp data_type_to_proto(:bfloat16_vector), do: :BFloat16Vector
  defp data_type_to_proto(:sparse_float_vector), do: :SparseFloatVector
  defp data_type_to_proto(:int8_vector), do: :Int8Vector
end

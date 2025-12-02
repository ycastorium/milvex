defmodule Bench.DataGenerators do
  @moduledoc false

  import Bitwise

  alias Milvex.Data.FieldData
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

  alias Milvex.Milvus.Proto.Schema.{IDs, LongArray, SearchResultData, StringArray}
  alias Milvex.Milvus.Proto.Milvus.{QueryResults, SearchResults}

  def generate_scalars(:bool, count) do
    for _ <- 1..count, do: :rand.uniform(2) == 1
  end

  def generate_scalars(:int8, count) do
    for _ <- 1..count, do: :rand.uniform(256) - 128
  end

  def generate_scalars(:int16, count) do
    for _ <- 1..count, do: :rand.uniform(65536) - 32768
  end

  def generate_scalars(:int32, count) do
    for _ <- 1..count, do: :rand.uniform(4_294_967_296) - 2_147_483_648
  end

  def generate_scalars(:int64, count) do
    for _ <- 1..count, do: :rand.uniform(1_000_000_000)
  end

  def generate_scalars(:float, count) do
    for _ <- 1..count, do: :rand.uniform() * 1000.0
  end

  def generate_scalars(:double, count) do
    for _ <- 1..count, do: :rand.uniform() * 1_000_000.0
  end

  def generate_scalars(:varchar, count) do
    for _ <- 1..count, do: random_string(50)
  end

  def generate_scalars(:json, count) do
    for _ <- 1..count do
      %{
        "key1" => random_string(10),
        "key2" => :rand.uniform(1000),
        "key3" => :rand.uniform() > 0.5
      }
    end
  end

  def generate_vectors(:float_vector, count, dimension) do
    for _ <- 1..count do
      for _ <- 1..dimension, do: :rand.uniform() * 2.0 - 1.0
    end
  end

  def generate_vectors(:binary_vector, count, dimension) do
    bytes_per_vector = div(dimension, 8)
    for _ <- 1..count do
      for _ <- 1..bytes_per_vector, do: :rand.uniform(256) - 1
    end
  end

  def generate_vectors(:float16_vector, count, dimension) do
    for _ <- 1..count do
      for _ <- 1..dimension, do: :rand.uniform() * 2.0 - 1.0
    end
  end

  def generate_vectors(:bfloat16_vector, count, dimension) do
    for _ <- 1..count do
      for _ <- 1..dimension, do: :rand.uniform() * 2.0 - 1.0
    end
  end

  def generate_vectors(:int8_vector, count, dimension) do
    for _ <- 1..count do
      for _ <- 1..dimension, do: :rand.uniform(256) - 128
    end
  end

  def generate_sparse_vectors(count, max_dimension, density) do
    num_elements = trunc(max_dimension * density)
    for _ <- 1..count do
      indices = Enum.take_random(0..(max_dimension - 1), num_elements) |> Enum.sort()
      for idx <- indices, do: {idx, :rand.uniform() * 2.0 - 1.0}
    end
  end

  def build_scalar_field_proto(:bool, values) do
    %FieldData{
      field_name: "bench_bool",
      type: :Bool,
      field: {:scalars, %ScalarField{data: {:bool_data, %BoolArray{data: values}}}}
    }
  end

  def build_scalar_field_proto(:int8, values) do
    %FieldData{
      field_name: "bench_int8",
      type: :Int8,
      field: {:scalars, %ScalarField{data: {:int_data, %IntArray{data: values}}}}
    }
  end

  def build_scalar_field_proto(:int16, values) do
    %FieldData{
      field_name: "bench_int16",
      type: :Int16,
      field: {:scalars, %ScalarField{data: {:int_data, %IntArray{data: values}}}}
    }
  end

  def build_scalar_field_proto(:int32, values) do
    %FieldData{
      field_name: "bench_int32",
      type: :Int32,
      field: {:scalars, %ScalarField{data: {:int_data, %IntArray{data: values}}}}
    }
  end

  def build_scalar_field_proto(:int64, values) do
    %FieldData{
      field_name: "bench_int64",
      type: :Int64,
      field: {:scalars, %ScalarField{data: {:long_data, %LongArray{data: values}}}}
    }
  end

  def build_scalar_field_proto(:float, values) do
    %FieldData{
      field_name: "bench_float",
      type: :Float,
      field: {:scalars, %ScalarField{data: {:float_data, %FloatArray{data: values}}}}
    }
  end

  def build_scalar_field_proto(:double, values) do
    %FieldData{
      field_name: "bench_double",
      type: :Double,
      field: {:scalars, %ScalarField{data: {:double_data, %DoubleArray{data: values}}}}
    }
  end

  def build_scalar_field_proto(:varchar, values) do
    %FieldData{
      field_name: "bench_varchar",
      type: :VarChar,
      field: {:scalars, %ScalarField{data: {:string_data, %StringArray{data: values}}}}
    }
  end

  def build_scalar_field_proto(:json, values) do
    json_bytes = Enum.map(values, &Jason.encode!/1)
    %FieldData{
      field_name: "bench_json",
      type: :JSON,
      field: {:scalars, %ScalarField{data: {:json_data, %JSONArray{data: json_bytes}}}}
    }
  end

  def build_vector_field_proto(:float_vector, values, dimension) do
    flat_values = List.flatten(values)
    %FieldData{
      field_name: "bench_float_vector",
      type: :FloatVector,
      field: {:vectors, %VectorField{dim: dimension, data: {:float_vector, %FloatArray{data: flat_values}}}}
    }
  end

  def build_vector_field_proto(:binary_vector, values, dimension) do
    binary_data = values |> List.flatten() |> :binary.list_to_bin()
    %FieldData{
      field_name: "bench_binary_vector",
      type: :BinaryVector,
      field: {:vectors, %VectorField{dim: dimension, data: {:binary_vector, binary_data}}}
    }
  end

  def build_vector_field_proto(:float16_vector, values, dimension) do
    binary_data = encode_float16_vectors(values)
    %FieldData{
      field_name: "bench_float16_vector",
      type: :Float16Vector,
      field: {:vectors, %VectorField{dim: dimension, data: {:float16_vector, binary_data}}}
    }
  end

  def build_vector_field_proto(:bfloat16_vector, values, dimension) do
    binary_data = encode_bfloat16_vectors(values)
    %FieldData{
      field_name: "bench_bfloat16_vector",
      type: :BFloat16Vector,
      field: {:vectors, %VectorField{dim: dimension, data: {:bfloat16_vector, binary_data}}}
    }
  end

  def build_vector_field_proto(:int8_vector, values, dimension) do
    binary_data =
      values
      |> List.flatten()
      |> Enum.map(fn byte -> if byte < 0, do: byte + 256, else: byte end)
      |> :binary.list_to_bin()

    %FieldData{
      field_name: "bench_int8_vector",
      type: :Int8Vector,
      field: {:vectors, %VectorField{dim: dimension, data: {:int8_vector, binary_data}}}
    }
  end

  def build_vector_field_proto(:sparse_float_vector, values, _dimension) do
    {contents, max_dim} = encode_sparse_vectors(values)
    sparse = %SparseFloatArray{contents: contents, dim: max_dim}
    %FieldData{
      field_name: "bench_sparse_vector",
      type: :SparseFloatVector,
      field: {:vectors, %VectorField{data: {:sparse_float_vector, sparse}}}
    }
  end

  def build_search_results_proto(num_queries, top_k, output_fields) do
    total_hits = num_queries * top_k
    ids = for i <- 1..total_hits, do: i
    scores = for _ <- 1..total_hits, do: :rand.uniform()
    topks = List.duplicate(top_k, num_queries)

    fields_data = Enum.map(output_fields, fn {name, type, values} ->
      build_output_field(name, type, values)
    end)

    result_data = %SearchResultData{
      num_queries: num_queries,
      top_k: top_k,
      ids: %IDs{id_field: {:int_id, %LongArray{data: ids}}},
      scores: scores,
      topks: topks,
      fields_data: fields_data
    }

    %SearchResults{
      results: result_data,
      collection_name: "bench_collection"
    }
  end

  def build_query_results_proto(_count, output_fields) do
    fields_data = Enum.map(output_fields, fn {name, type, values} ->
      build_output_field(name, type, values)
    end)

    %QueryResults{
      collection_name: "bench_collection",
      fields_data: fields_data,
      output_fields: Enum.map(output_fields, fn {name, _, _} -> name end)
    }
  end

  def build_field(name, :bool), do: Field.scalar(name, :bool)
  def build_field(name, :int8), do: Field.scalar(name, :int8)
  def build_field(name, :int16), do: Field.scalar(name, :int16)
  def build_field(name, :int32), do: Field.scalar(name, :int32)
  def build_field(name, :int64), do: Field.scalar(name, :int64)
  def build_field(name, :float), do: Field.scalar(name, :float)
  def build_field(name, :double), do: Field.scalar(name, :double)
  def build_field(name, :varchar), do: Field.varchar(name, 256)
  def build_field(name, :json), do: Field.scalar(name, :json)

  def build_field(name, :float_vector, dimension), do: Field.vector(name, dimension)
  def build_field(name, :binary_vector, dimension), do: Field.vector(name, dimension, type: :binary_vector)
  def build_field(name, :float16_vector, dimension), do: Field.vector(name, dimension, type: :float16_vector)
  def build_field(name, :bfloat16_vector, dimension), do: Field.vector(name, dimension, type: :bfloat16_vector)
  def build_field(name, :int8_vector, dimension), do: Field.vector(name, dimension, type: :int8_vector)
  def build_field(name, :sparse_float_vector, _dimension), do: Field.sparse_vector(name)

  defp build_output_field(name, :int64, values) do
    %FieldData{
      field_name: name,
      type: :Int64,
      field: {:scalars, %ScalarField{data: {:long_data, %LongArray{data: values}}}}
    }
  end

  defp build_output_field(name, :varchar, values) do
    %FieldData{
      field_name: name,
      type: :VarChar,
      field: {:scalars, %ScalarField{data: {:string_data, %StringArray{data: values}}}}
    }
  end

  defp build_output_field(name, :float, values) do
    %FieldData{
      field_name: name,
      type: :Float,
      field: {:scalars, %ScalarField{data: {:float_data, %FloatArray{data: values}}}}
    }
  end

  defp random_string(length) do
    :crypto.strong_rand_bytes(length)
    |> Base.encode64()
    |> binary_part(0, length)
  end

  defp encode_float16_vectors(vectors) do
    vectors
    |> Enum.flat_map(fn vector ->
      Enum.map(vector, &float_to_float16/1)
    end)
    |> IO.iodata_to_binary()
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

  defp encode_bfloat16_vectors(vectors) do
    vectors
    |> Enum.flat_map(fn vector ->
      Enum.map(vector, &float_to_bfloat16/1)
    end)
    |> IO.iodata_to_binary()
  end

  defp float_to_bfloat16(f) when is_float(f) do
    <<hi::16, _lo::16>> = <<f::32-float>>
    <<hi::16-little>>
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
end

Code.require_file("support/data_generators.ex", __DIR__)

alias Milvex.Data.FieldData
alias Bench.DataGenerators

dimension = 128
sparse_max_dim = 10_000
sparse_density = 0.02

inputs = %{
  "100 rows" => 100,
  "1K rows" => 1_000,
  "10K rows" => 10_000,
  "100K rows" => 100_000
}

scalar_types = [:bool, :int8, :int16, :int32, :int64, :float, :double, :varchar, :json]
vector_types = [:float_vector, :binary_vector, :float16_vector, :bfloat16_vector, :int8_vector]

IO.puts("\n=== Scalar Field Building Benchmarks ===\n")

scalar_build_benchmarks =
  for type <- scalar_types, into: %{} do
    {"build_scalar_field (#{type})",
     {
       fn {count, _proto} ->
         values = DataGenerators.generate_scalars(type, count)
         FieldData.build_scalar_field(type, values)
       end,
       before_scenario: fn count ->
         values = DataGenerators.generate_scalars(type, count)
         proto = DataGenerators.build_scalar_field_proto(type, values)
         {count, proto}
       end
     }}
  end

Benchee.run(
  scalar_build_benchmarks,
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Scalar Field Extraction Benchmarks ===\n")

scalar_extract_benchmarks =
  for type <- scalar_types, into: %{} do
    {"extract_scalar_values (#{type})",
     {
       fn {_count, proto} ->
         FieldData.from_proto(proto)
       end,
       before_scenario: fn count ->
         values = DataGenerators.generate_scalars(type, count)
         proto = DataGenerators.build_scalar_field_proto(type, values)
         {count, proto}
       end
     }}
  end

Benchee.run(
  scalar_extract_benchmarks,
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Vector Field Building Benchmarks (dim=#{dimension}) ===\n")

vector_build_benchmarks =
  for type <- vector_types, into: %{} do
    {"build_vector_field (#{type})",
     {
       fn {count, _proto} ->
         values = DataGenerators.generate_vectors(type, count, dimension)
         FieldData.build_vector_field(type, values, dimension)
       end,
       before_scenario: fn count ->
         values = DataGenerators.generate_vectors(type, count, dimension)
         proto = DataGenerators.build_vector_field_proto(type, values, dimension)
         {count, proto}
       end
     }}
  end

Benchee.run(
  vector_build_benchmarks,
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Sparse Vector Benchmarks (max_dim=#{sparse_max_dim}, density=#{sparse_density}) ===\n")

Benchee.run(
  %{
    "build_vector_field (sparse_float_vector)" =>
      {
        fn {count, _proto} ->
          values = DataGenerators.generate_sparse_vectors(count, sparse_max_dim, sparse_density)
          FieldData.build_vector_field(:sparse_float_vector, values, nil)
        end,
        before_scenario: fn count ->
          values = DataGenerators.generate_sparse_vectors(count, sparse_max_dim, sparse_density)
          proto = DataGenerators.build_vector_field_proto(:sparse_float_vector, values, nil)
          {count, proto}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Vector Field Extraction Benchmarks (dim=#{dimension}) ===\n")

vector_extract_benchmarks =
  for type <- vector_types, into: %{} do
    {"extract_vector_values (#{type})",
     {
       fn {_count, proto} ->
         FieldData.from_proto(proto)
       end,
       before_scenario: fn count ->
         values = DataGenerators.generate_vectors(type, count, dimension)
         proto = DataGenerators.build_vector_field_proto(type, values, dimension)
         {count, proto}
       end
     }}
  end

Benchee.run(
  vector_extract_benchmarks,
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Sparse Vector Extraction Benchmarks ===\n")

Benchee.run(
  %{
    "extract_vector_values (sparse_float_vector)" =>
      {
        fn {_count, proto} ->
          FieldData.from_proto(proto)
        end,
        before_scenario: fn count ->
          values = DataGenerators.generate_sparse_vectors(count, sparse_max_dim, sparse_density)
          proto = DataGenerators.build_vector_field_proto(:sparse_float_vector, values, nil)
          {count, proto}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Vector Dimension Comparison (float_vector) ===\n")

dimension_inputs = %{
  "dim=128, 1K rows" => {1_000, 128},
  "dim=768, 1K rows" => {1_000, 768},
  "dim=1536, 1K rows" => {1_000, 1536}
}

Benchee.run(
  %{
    "build_vector_field (float_vector)" =>
      {
        fn {{count, dim}, _proto} ->
          values = DataGenerators.generate_vectors(:float_vector, count, dim)
          FieldData.build_vector_field(:float_vector, values, dim)
        end,
        before_scenario: fn {count, dim} ->
          values = DataGenerators.generate_vectors(:float_vector, count, dim)
          proto = DataGenerators.build_vector_field_proto(:float_vector, values, dim)
          {{count, dim}, proto}
        end
      },
    "extract_vector_values (float_vector)" =>
      {
        fn {_input, proto} ->
          FieldData.from_proto(proto)
        end,
        before_scenario: fn {count, dim} ->
          values = DataGenerators.generate_vectors(:float_vector, count, dim)
          proto = DataGenerators.build_vector_field_proto(:float_vector, values, dim)
          {{count, dim}, proto}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: dimension_inputs,
  formatters: [Benchee.Formatters.Console]
)

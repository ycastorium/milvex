Code.require_file("support/data_generators.ex", __DIR__)

alias Milvex.Data
alias Milvex.Schema
alias Milvex.Schema.Field
alias Bench.DataGenerators

dimension = 128

inputs = %{
  "100 rows" => 100,
  "1K rows" => 1_000,
  "10K rows" => 10_000,
  "100K rows" => 100_000
}

simple_schema = Schema.build!(
  name: "bench_simple",
  fields: [
    Field.primary_key("id", :int64),
    Field.varchar("name", 256),
    Field.vector("embedding", dimension)
  ]
)

complex_schema = Schema.build!(
  name: "bench_complex",
  fields: [
    Field.primary_key("id", :int64),
    Field.scalar("bool_field", :bool),
    Field.scalar("int32_field", :int32),
    Field.scalar("int64_field", :int64),
    Field.scalar("float_field", :float),
    Field.scalar("double_field", :double),
    Field.varchar("varchar_field", 256),
    Field.scalar("json_field", :json),
    Field.vector("float_vector", dimension),
    Field.vector("float16_vector", dimension, type: :float16_vector)
  ]
)

generate_simple_rows = fn count ->
  ids = DataGenerators.generate_scalars(:int64, count)
  names = DataGenerators.generate_scalars(:varchar, count)
  embeddings = DataGenerators.generate_vectors(:float_vector, count, dimension)

  Enum.zip([ids, names, embeddings])
  |> Enum.map(fn {id, name, embedding} ->
    %{"id" => id, "name" => name, "embedding" => embedding}
  end)
end

generate_simple_columns = fn count ->
  %{
    "id" => DataGenerators.generate_scalars(:int64, count),
    "name" => DataGenerators.generate_scalars(:varchar, count),
    "embedding" => DataGenerators.generate_vectors(:float_vector, count, dimension)
  }
end

generate_complex_rows = fn count ->
  ids = DataGenerators.generate_scalars(:int64, count)
  bools = DataGenerators.generate_scalars(:bool, count)
  int32s = DataGenerators.generate_scalars(:int32, count)
  int64s = DataGenerators.generate_scalars(:int64, count)
  floats = DataGenerators.generate_scalars(:float, count)
  doubles = DataGenerators.generate_scalars(:double, count)
  varchars = DataGenerators.generate_scalars(:varchar, count)
  jsons = DataGenerators.generate_scalars(:json, count)
  float_vectors = DataGenerators.generate_vectors(:float_vector, count, dimension)
  float16_vectors = DataGenerators.generate_vectors(:float16_vector, count, dimension)

  [ids, bools, int32s, int64s, floats, doubles, varchars, jsons, float_vectors, float16_vectors]
  |> Enum.zip()
  |> Enum.map(fn {id, bool, int32, int64, float, double, varchar, json, fv, f16v} ->
    %{
      "id" => id,
      "bool_field" => bool,
      "int32_field" => int32,
      "int64_field" => int64,
      "float_field" => float,
      "double_field" => double,
      "varchar_field" => varchar,
      "json_field" => json,
      "float_vector" => fv,
      "float16_vector" => f16v
    }
  end)
end

generate_complex_columns = fn count ->
  %{
    "id" => DataGenerators.generate_scalars(:int64, count),
    "bool_field" => DataGenerators.generate_scalars(:bool, count),
    "int32_field" => DataGenerators.generate_scalars(:int32, count),
    "int64_field" => DataGenerators.generate_scalars(:int64, count),
    "float_field" => DataGenerators.generate_scalars(:float, count),
    "double_field" => DataGenerators.generate_scalars(:double, count),
    "varchar_field" => DataGenerators.generate_scalars(:varchar, count),
    "json_field" => DataGenerators.generate_scalars(:json, count),
    "float_vector" => DataGenerators.generate_vectors(:float_vector, count, dimension),
    "float16_vector" => DataGenerators.generate_vectors(:float16_vector, count, dimension)
  }
end

IO.puts("\n=== Data.from_rows Benchmarks (Simple Schema: id, name, embedding) ===\n")

Benchee.run(
  %{
    "Data.from_rows (simple)" =>
      {
        fn {rows, _columns, _data} ->
          Data.from_rows!(rows, simple_schema)
        end,
        before_scenario: fn count ->
          rows = generate_simple_rows.(count)
          columns = generate_simple_columns.(count)
          {:ok, data} = Data.from_columns(columns, simple_schema)
          {rows, columns, data}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Data.from_columns Benchmarks (Simple Schema) ===\n")

Benchee.run(
  %{
    "Data.from_columns (simple)" =>
      {
        fn {_rows, columns, _data} ->
          Data.from_columns!(columns, simple_schema)
        end,
        before_scenario: fn count ->
          rows = generate_simple_rows.(count)
          columns = generate_simple_columns.(count)
          {:ok, data} = Data.from_columns(columns, simple_schema)
          {rows, columns, data}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Data.to_proto Benchmarks (Simple Schema) ===\n")

Benchee.run(
  %{
    "Data.to_proto (simple)" =>
      {
        fn {_rows, _columns, data} ->
          Data.to_proto(data)
        end,
        before_scenario: fn count ->
          rows = generate_simple_rows.(count)
          columns = generate_simple_columns.(count)
          {:ok, data} = Data.from_columns(columns, simple_schema)
          {rows, columns, data}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Data.from_rows Benchmarks (Complex Schema: 10 fields) ===\n")

Benchee.run(
  %{
    "Data.from_rows (complex)" =>
      {
        fn {rows, _columns, _data} ->
          Data.from_rows!(rows, complex_schema)
        end,
        before_scenario: fn count ->
          rows = generate_complex_rows.(count)
          columns = generate_complex_columns.(count)
          {:ok, data} = Data.from_columns(columns, complex_schema)
          {rows, columns, data}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Data.from_columns Benchmarks (Complex Schema) ===\n")

Benchee.run(
  %{
    "Data.from_columns (complex)" =>
      {
        fn {_rows, columns, _data} ->
          Data.from_columns!(columns, complex_schema)
        end,
        before_scenario: fn count ->
          rows = generate_complex_rows.(count)
          columns = generate_complex_columns.(count)
          {:ok, data} = Data.from_columns(columns, complex_schema)
          {rows, columns, data}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Data.to_proto Benchmarks (Complex Schema) ===\n")

Benchee.run(
  %{
    "Data.to_proto (complex)" =>
      {
        fn {_rows, _columns, data} ->
          Data.to_proto(data)
        end,
        before_scenario: fn count ->
          rows = generate_complex_rows.(count)
          columns = generate_complex_columns.(count)
          {:ok, data} = Data.from_columns(columns, complex_schema)
          {rows, columns, data}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Full Pipeline: from_rows -> to_proto (Simple Schema) ===\n")

Benchee.run(
  %{
    "full pipeline (simple)" =>
      {
        fn {rows, _columns, _data} ->
          rows
          |> Data.from_rows!(simple_schema)
          |> Data.to_proto()
        end,
        before_scenario: fn count ->
          rows = generate_simple_rows.(count)
          columns = generate_simple_columns.(count)
          {:ok, data} = Data.from_columns(columns, simple_schema)
          {rows, columns, data}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Full Pipeline: from_rows -> to_proto (Complex Schema) ===\n")

Benchee.run(
  %{
    "full pipeline (complex)" =>
      {
        fn {rows, _columns, _data} ->
          rows
          |> Data.from_rows!(complex_schema)
          |> Data.to_proto()
        end,
        before_scenario: fn count ->
          rows = generate_complex_rows.(count)
          columns = generate_complex_columns.(count)
          {:ok, data} = Data.from_columns(columns, complex_schema)
          {rows, columns, data}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Row vs Column Input Comparison ===\n")

Benchee.run(
  %{
    "from_rows (simple)" =>
      {
        fn {rows, _columns, _data} ->
          Data.from_rows!(rows, simple_schema)
        end,
        before_scenario: fn count ->
          rows = generate_simple_rows.(count)
          columns = generate_simple_columns.(count)
          {:ok, data} = Data.from_columns(columns, simple_schema)
          {rows, columns, data}
        end
      },
    "from_columns (simple)" =>
      {
        fn {_rows, columns, _data} ->
          Data.from_columns!(columns, simple_schema)
        end,
        before_scenario: fn count ->
          rows = generate_simple_rows.(count)
          columns = generate_simple_columns.(count)
          {:ok, data} = Data.from_columns(columns, simple_schema)
          {rows, columns, data}
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

Code.require_file("support/data_generators.ex", __DIR__)

alias Milvex.SearchResult
alias Milvex.QueryResult
alias Bench.DataGenerators

inputs = %{
  "100 rows" => 100,
  "1K rows" => 1_000,
  "10K rows" => 10_000,
  "100K rows" => 100_000
}

IO.puts("\n=== SearchResult.from_proto Benchmarks (single query) ===\n")

Benchee.run(
  %{
    "SearchResult.from_proto (no output fields)" =>
      {
        fn proto ->
          SearchResult.from_proto(proto)
        end,
        before_scenario: fn count ->
          DataGenerators.build_search_results_proto(1, count, [])
        end
      },
    "SearchResult.from_proto (3 output fields)" =>
      {
        fn proto ->
          SearchResult.from_proto(proto)
        end,
        before_scenario: fn count ->
          output_fields = [
            {"title", :varchar, DataGenerators.generate_scalars(:varchar, count)},
            {"score", :float, DataGenerators.generate_scalars(:float, count)},
            {"category", :int64, DataGenerators.generate_scalars(:int64, count)}
          ]
          DataGenerators.build_search_results_proto(1, count, output_fields)
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== SearchResult.from_proto Benchmarks (multiple queries) ===\n")

multi_query_inputs = %{
  "10 queries x 100 results" => {10, 100},
  "10 queries x 1K results" => {10, 1_000},
  "100 queries x 100 results" => {100, 100},
  "100 queries x 1K results" => {100, 1_000}
}

Benchee.run(
  %{
    "SearchResult.from_proto (multi-query, no fields)" =>
      {
        fn proto ->
          SearchResult.from_proto(proto)
        end,
        before_scenario: fn {num_queries, top_k} ->
          DataGenerators.build_search_results_proto(num_queries, top_k, [])
        end
      },
    "SearchResult.from_proto (multi-query, 3 fields)" =>
      {
        fn proto ->
          SearchResult.from_proto(proto)
        end,
        before_scenario: fn {num_queries, top_k} ->
          total = num_queries * top_k
          output_fields = [
            {"title", :varchar, DataGenerators.generate_scalars(:varchar, total)},
            {"score", :float, DataGenerators.generate_scalars(:float, total)},
            {"category", :int64, DataGenerators.generate_scalars(:int64, total)}
          ]
          DataGenerators.build_search_results_proto(num_queries, top_k, output_fields)
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: multi_query_inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== QueryResult.from_proto Benchmarks ===\n")

Benchee.run(
  %{
    "QueryResult.from_proto (3 fields)" =>
      {
        fn proto ->
          QueryResult.from_proto(proto)
        end,
        before_scenario: fn count ->
          output_fields = [
            {"id", :int64, DataGenerators.generate_scalars(:int64, count)},
            {"title", :varchar, DataGenerators.generate_scalars(:varchar, count)},
            {"score", :float, DataGenerators.generate_scalars(:float, count)}
          ]
          DataGenerators.build_query_results_proto(count, output_fields)
        end
      },
    "QueryResult.from_proto (6 fields)" =>
      {
        fn proto ->
          QueryResult.from_proto(proto)
        end,
        before_scenario: fn count ->
          output_fields = [
            {"id", :int64, DataGenerators.generate_scalars(:int64, count)},
            {"title", :varchar, DataGenerators.generate_scalars(:varchar, count)},
            {"description", :varchar, DataGenerators.generate_scalars(:varchar, count)},
            {"score", :float, DataGenerators.generate_scalars(:float, count)},
            {"category", :int64, DataGenerators.generate_scalars(:int64, count)},
            {"rating", :float, DataGenerators.generate_scalars(:float, count)}
          ]
          DataGenerators.build_query_results_proto(count, output_fields)
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Search vs Query Result Parsing Comparison ===\n")

Benchee.run(
  %{
    "SearchResult.from_proto" =>
      {
        fn proto ->
          SearchResult.from_proto(proto)
        end,
        before_scenario: fn count ->
          output_fields = [
            {"title", :varchar, DataGenerators.generate_scalars(:varchar, count)},
            {"score", :float, DataGenerators.generate_scalars(:float, count)},
            {"category", :int64, DataGenerators.generate_scalars(:int64, count)}
          ]
          DataGenerators.build_search_results_proto(1, count, output_fields)
        end
      },
    "QueryResult.from_proto" =>
      {
        fn proto ->
          QueryResult.from_proto(proto)
        end,
        before_scenario: fn count ->
          output_fields = [
            {"id", :int64, DataGenerators.generate_scalars(:int64, count)},
            {"title", :varchar, DataGenerators.generate_scalars(:varchar, count)},
            {"score", :float, DataGenerators.generate_scalars(:float, count)}
          ]
          DataGenerators.build_query_results_proto(count, output_fields)
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

IO.puts("\n=== Result Access Patterns ===\n")

Benchee.run(
  %{
    "SearchResult.get_query_hits" =>
      {
        fn result ->
          SearchResult.get_query_hits(result, 0)
        end,
        before_scenario: fn count ->
          output_fields = [
            {"title", :varchar, DataGenerators.generate_scalars(:varchar, count)}
          ]
          proto = DataGenerators.build_search_results_proto(1, count, output_fields)
          SearchResult.from_proto(proto)
        end
      },
    "SearchResult.top_hits" =>
      {
        fn result ->
          SearchResult.top_hits(result)
        end,
        before_scenario: fn count ->
          num_queries = div(count, 10) + 1
          top_k = 10
          total = num_queries * top_k
          output_fields = [
            {"title", :varchar, DataGenerators.generate_scalars(:varchar, total)}
          ]
          proto = DataGenerators.build_search_results_proto(num_queries, top_k, output_fields)
          SearchResult.from_proto(proto)
        end
      },
    "QueryResult.get_column" =>
      {
        fn result ->
          QueryResult.get_column(result, "title")
        end,
        before_scenario: fn count ->
          output_fields = [
            {"id", :int64, DataGenerators.generate_scalars(:int64, count)},
            {"title", :varchar, DataGenerators.generate_scalars(:varchar, count)}
          ]
          proto = DataGenerators.build_query_results_proto(count, output_fields)
          QueryResult.from_proto(proto)
        end
      }
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  inputs: inputs,
  formatters: [Benchee.Formatters.Console]
)

# Used by "mix format"
spark_locals_without_parens = [
  collection: 1,
  fields: 1,
  primary_key: 2,
  primary_key: 3,
  vector: 2,
  vector: 3,
  sparse_vector: 1,
  sparse_vector: 2,
  varchar: 2,
  varchar: 3,
  scalar: 2,
  scalar: 3,
  array: 3,
  array: 4,
  name: 1,
  description: 1,
  enable_dynamic_field: 1
]

[
  import_deps: [:grpc, :protobuf, :spark],
  plugins: [Recode.FormatterPlugin],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  locals_without_parens: spark_locals_without_parens,
  export: [
    locals_without_parens: spark_locals_without_parens
  ]
]

# Used by "mix format"
[
  import_deps: [:grpc, :protobuf],
  plugins: [Recode.FormatterPlugin],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
]

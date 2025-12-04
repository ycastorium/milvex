# Getting Started

This guide walks you through setting up Milvex and performing basic vector operations.

## Prerequisites

- Elixir 1.19 or later
- A running Milvus instance (local or cloud)

## Installation

Add `milvex` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:milvex, "~> 0.1.0"}
  ]
end
```

Then fetch dependencies:

```bash
mix deps.get
```

## Connecting to Milvus

### Basic Connection

```elixir
{:ok, conn} = Milvex.Connection.start_link(host: "localhost", port: 19530)
```

### Named Connection

For use throughout your application, start a named connection:

```elixir
{:ok, _} = Milvex.Connection.start_link([host: "localhost"], name: :milvus)

# Use the named connection
Milvex.search(:milvus, "collection", vectors, vector_field: "embedding")
```

### Under a Supervisor

The recommended approach for production:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Milvex.Connection, [host: "localhost", port: 19530, name: MyApp.Milvus]}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

### Connection Options

```elixir
Milvex.Connection.start_link(
  host: "localhost",        # Milvus server hostname
  port: 19530,              # gRPC port (default: 19530, or 443 for SSL)
  database: "default",      # Database name
  user: "root",             # Username (optional)
  password: "milvus",       # Password (optional)
  token: "api_token",       # API token (alternative to user/password)
  ssl: true,                # Enable SSL/TLS
  ssl_options: [],          # SSL options for transport
  timeout: 30_000           # Connection timeout in ms
)
```

You can also use a URI:

```elixir
{:ok, config} = Milvex.Config.parse_uri("https://user:pass@milvus.example.com:443/mydb")
{:ok, conn} = Milvex.Connection.start_link(config)
```

## Creating a Collection

### Define the Schema

Use the fluent builder API to define your collection schema:

```elixir
alias Milvex.Schema
alias Milvex.Schema.Field

schema = Schema.build!(
  name: "movies",
  fields: [
    Field.primary_key("id", :int64, auto_id: true),
    Field.varchar("title", 512),
    Field.vector("embedding", 128)
  ],
  enable_dynamic_field: true
)
```

### Create Collection and Index

```elixir
alias Milvex.Index

# Create the collection
:ok = Milvex.create_collection(conn, "movies", schema)

# Create an HNSW index for vector search
index = Index.hnsw("embedding", :cosine, m: 16, ef_construction: 256)
:ok = Milvex.create_index(conn, "movies", index)

# Load collection into memory for searching
:ok = Milvex.load_collection(conn, "movies")
```

## Inserting Data

Insert data as a list of maps:

```elixir
{:ok, result} = Milvex.insert(conn, "movies", [
  %{title: "The Matrix", embedding: generate_embedding("The Matrix")},
  %{title: "Inception", embedding: generate_embedding("Inception")}
])

# result.ids contains the auto-generated IDs
IO.inspect(result.ids)
```

## Searching Vectors

Perform similarity search:

```elixir
query_vector = generate_embedding("science fiction action")

{:ok, results} = Milvex.search(conn, "movies", [query_vector],
  vector_field: "embedding",
  top_k: 10,
  output_fields: ["title"],
  filter: "title like \"The%\""
)

# Access results
for hit <- results.hits do
  IO.puts("#{hit.id}: #{hit.fields["title"]} (score: #{hit.score})")
end
```

## Querying by Expression

Query records using filter expressions:

```elixir
{:ok, results} = Milvex.query(conn, "movies", "id > 0",
  output_fields: ["id", "title"],
  limit: 100
)
```

## Index Types

Choose the right index for your use case:

```elixir
# HNSW - best for high recall with good performance
Index.hnsw("field", :cosine, m: 16, ef_construction: 256)

# IVF_FLAT - good balance for medium datasets
Index.ivf_flat("field", :l2, nlist: 1024)

# AUTOINDEX - let Milvus choose optimal settings
Index.autoindex("field", :ip)

# IVF_PQ - memory efficient for large datasets
Index.ivf_pq("field", :l2, nlist: 1024, m: 8, nbits: 8)

# DiskANN - for datasets that don't fit in memory
Index.diskann("field", :l2)
```

Supported metric types: `:l2`, `:ip`, `:cosine`, `:hamming`, `:jaccard`

## Using Partitions

Organize data into partitions for efficient querying:

```elixir
# Create partition
:ok = Milvex.create_partition(conn, "movies", "movies_2024")

# Insert into partition
{:ok, _} = Milvex.insert(conn, "movies", data, partition_name: "movies_2024")

# Search specific partitions
{:ok, _} = Milvex.search(conn, "movies", vectors,
  vector_field: "embedding",
  partition_names: ["movies_2024", "movies_2023"]
)

# Load/release partitions
:ok = Milvex.load_partitions(conn, "movies", ["movies_2024"])
:ok = Milvex.release_partitions(conn, "movies", ["movies_2024"])
```

## Next Steps

- Read the [Architecture](architecture.md) guide to understand how Milvex works internally
- Learn about [Error Handling](error-handling.md) patterns
- Explore the `Milvex` module documentation for all available operations

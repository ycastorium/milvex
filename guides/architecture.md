# Architecture

This guide explains how Milvex is structured and how data flows through the system.

## Module Overview

```
Milvex (High-level API)
    |
    v
Milvex.Connection (gRPC Channel Manager)
    |
    v
Milvex.RPC (Low-level gRPC Wrapper)
    |
    v
Generated Proto Modules (Milvex.Milvus.Proto.*)
```

## Core Modules

### Milvex

The main entry point for all operations. Provides high-level functions for:

- Collection management (`create_collection`, `drop_collection`, `load_collection`)
- Data operations (`insert`, `delete`, `upsert`)
- Search and query (`search`, `query`)
- Index management (`create_index`, `drop_index`)
- Partition management (`create_partition`, `drop_partition`)

All functions have two variants:
- Regular functions return `{:ok, result}` or `{:error, error}`
- Bang functions (e.g., `insert!`) raise on error

### Milvex.Connection

A `GenStateMachine` that manages the gRPC channel lifecycle. Handles:

- Initial connection establishment
- Health monitoring
- Automatic reconnection with exponential backoff

#### Connection States

```
:connecting -----> :connected <-----> :reconnecting
    |                  |                    |
    |                  v                    |
    |           (health checks)             |
    |                  |                    |
    +------------------+--------------------+
              (on connection failure)
```

- **`:connecting`** - Initial state, attempting to establish connection
- **`:connected`** - Channel active, periodic health checks running
- **`:reconnecting`** - Connection lost, attempting to restore with backoff

#### Reconnection Strategy

Uses exponential backoff with jitter to prevent thundering herd:

```elixir
delay = min(base_delay * (multiplier ^ retry_count), max_delay)
jittered_delay = delay * (1 + random * jitter_factor)
```

### Milvex.RPC

Low-level wrapper around gRPC calls. Responsibilities:

- Execute gRPC requests against the Milvus server
- Convert Milvus proto `Status` codes to Splode errors
- Handle gRPC transport errors

## Data Layer

### Schema Building

```
Milvex.Schema.build/1
    |
    +-- Milvex.Schema.Field (field definitions)
    |
    v
CollectionSchema proto
```

The `Schema` and `Schema.Field` modules provide a fluent builder API:

```elixir
Schema.build!(
  name: "collection",
  fields: [
    Field.primary_key("id", :int64),
    Field.vector("embedding", 128)
  ]
)
```

### Data Conversion

```
Row-oriented data (list of maps)
    |
    v
Milvex.Data.to_field_data/2
    |
    v
Column-oriented FieldData protos
    |
    v
gRPC InsertRequest
```

Milvex converts Elixir's natural row-oriented data (list of maps) to Milvus's required column-oriented format automatically.

### Index Configuration

```elixir
Index.hnsw("field", :cosine, m: 16, ef_construction: 256)
    |
    v
Validated index params
    |
    v
CreateIndexRequest proto
```

The `Index` module provides builders for all Milvus index types with validation.

## Result Parsing

```
gRPC Response
    |
    v
Milvex.SearchResult / Milvex.QueryResult
    |
    v
Structured Elixir data
```

- `SearchResult` - Parses similarity search responses with scores
- `QueryResult` - Parses query responses (no scores)

Both provide access to:
- Matched IDs
- Requested output fields
- Hit scores (search only)

## Error Handling

All errors flow through `Milvex.Error` using Splode:

```
gRPC Error / Milvus Status / Validation Error
    |
    v
Milvex.Errors.* (typed error structs)
    |
    v
{:error, error} or raised exception
```

See the [Error Handling](error-handling.md) guide for details.

## Generated Proto Files

The `lib/milvex/milvus/proto/` directory contains Elixir modules generated from Milvus protobuf definitions. These are internal and rarely used directly.

To regenerate after Milvus proto updates:

```bash
cd milvus-proto/proto
protoc --elixir_out=one_file_per_module=true,plugins=grpc:../../lib \
       --elixir_opt=package_prefix=milvex \
       --elixir_opt=include_docs=true *.proto
```

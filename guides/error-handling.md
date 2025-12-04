# Error Handling

Milvex uses [Splode](https://hexdocs.pm/splode) for structured error handling. All errors are typed and can be pattern matched for precise error handling.

## Error Types

### Milvex.Errors.Invalid

Validation and input errors. Raised when user-provided data fails validation.

**Fields:**
- `field` - The field that failed validation (optional)
- `message` - Human-readable error message
- `code` - Error code atom (optional)
- `context` - Additional context map (optional)

**Common causes:**
- Invalid configuration parameters
- Schema validation failures
- Missing required fields
- Type mismatches

```elixir
%Milvex.Errors.Invalid{
  field: :dimension,
  message: "must be a positive integer",
  code: :invalid_dimension
}
```

### Milvex.Errors.Connection

Network and connection errors. Raised when connection to Milvus fails.

**Fields:**
- `reason` - Error reason (string or atom)
- `host` - Target host (optional)
- `port` - Target port (optional)
- `retriable` - Whether the operation can be retried (optional)

**Common causes:**
- Unable to establish gRPC connection
- Connection timeout
- Network unreachable
- Connection lost during operation

```elixir
%Milvex.Errors.Connection{
  reason: :timeout,
  host: "localhost",
  port: 19530,
  retriable: true
}
```

### Milvex.Errors.Grpc

Server-side errors from Milvus or gRPC layer.

**Fields:**
- `code` - Error code (integer or atom)
- `message` - Error message from server
- `details` - Additional details map (optional)
- `operation` - The operation that failed (optional)

**Common causes:**
- Milvus returns an error status code
- gRPC call fails
- Server-side validation fails
- Operation not permitted

```elixir
%Milvex.Errors.Grpc{
  code: 1,
  message: "collection not found: movies",
  operation: :search
}
```

### Milvex.Errors.Unknown

Catch-all for unexpected errors.

**Fields:**
- `error` - The original error
- `stacktrace` - Stack trace if available

## Pattern Matching on Errors

Use pattern matching to handle specific error types:

```elixir
case Milvex.search(conn, "movies", vectors, vector_field: "embedding") do
  {:ok, results} ->
    process_results(results)

  {:error, %Milvex.Errors.Connection{retriable: true} = error} ->
    Logger.warning("Connection issue, retrying: #{Exception.message(error)}")
    retry_operation()

  {:error, %Milvex.Errors.Connection{} = error} ->
    Logger.error("Connection failed: #{Exception.message(error)}")
    {:error, :connection_failed}

  {:error, %Milvex.Errors.Grpc{code: code} = error} ->
    Logger.error("Milvus error (#{code}): #{Exception.message(error)}")
    handle_milvus_error(code)

  {:error, %Milvex.Errors.Invalid{field: field} = error} ->
    Logger.error("Validation error on #{field}: #{Exception.message(error)}")
    {:error, :invalid_input}

  {:error, error} ->
    Logger.error("Unexpected error: #{Exception.message(error)}")
    {:error, :unknown}
end
```

## Using Bang Functions

All Milvex operations have bang variants that raise on error:

```elixir
# Returns {:ok, result} or {:error, error}
{:ok, results} = Milvex.search(conn, "movies", vectors, vector_field: "embedding")

# Raises on error
results = Milvex.search!(conn, "movies", vectors, vector_field: "embedding")
```

Use bang functions when:
- Errors should halt execution
- In scripts or one-off operations
- When the calling code has a `try/rescue` block

Use regular functions when:
- You need to handle specific error cases
- Building resilient applications
- Implementing retry logic

## Error Messages

All error types implement `Exception.message/1`:

```elixir
case Milvex.insert(conn, "movies", data) do
  {:ok, result} ->
    {:ok, result}

  {:error, error} ->
    Logger.error(Exception.message(error))
    {:error, error}
end
```

## Common Error Scenarios

### Collection Not Found

```elixir
case Milvex.search(conn, "nonexistent", vectors, vector_field: "embedding") do
  {:error, %Milvex.Errors.Grpc{message: msg}} when msg =~ "not found" ->
    {:error, :collection_not_found}
  other ->
    other
end
```

### Connection Timeout

```elixir
case Milvex.Connection.start_link(host: "unreachable.host", timeout: 5000) do
  {:ok, conn} ->
    {:ok, conn}

  {:error, %Milvex.Errors.Connection{reason: :timeout}} ->
    {:error, :connection_timeout}

  {:error, %Milvex.Errors.Connection{reason: reason}} ->
    {:error, {:connection_failed, reason}}
end
```

### Invalid Schema

```elixir
case Milvex.Schema.build(name: "test", fields: []) do
  {:ok, schema} ->
    {:ok, schema}

  {:error, %Milvex.Errors.Invalid{field: :fields}} ->
    {:error, :no_fields_defined}
end
```

## Retry Strategies

For transient errors, implement retry logic:

```elixir
defmodule MyApp.Milvus do
  def search_with_retry(conn, collection, vectors, opts, retries \\ 3) do
    case Milvex.search(conn, collection, vectors, opts) do
      {:ok, results} ->
        {:ok, results}

      {:error, %Milvex.Errors.Connection{retriable: true}} when retries > 0 ->
        Process.sleep(1000)
        search_with_retry(conn, collection, vectors, opts, retries - 1)

      {:error, error} ->
        {:error, error}
    end
  end
end
```

The connection itself handles reconnection automatically with exponential backoff, but application-level retries may be needed for transient operation failures.

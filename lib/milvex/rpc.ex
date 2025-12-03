defmodule Milvex.RPC do
  @moduledoc """
  Low-level gRPC wrapper with consistent error handling.

  Provides helper functions for making gRPC calls and converting
  Milvus proto Status codes and gRPC errors to Splode errors.

  ## Usage

      alias Milvex.RPC
      alias Milvex.Milvus.Proto.Milvus.MilvusService

      # Make a gRPC call with automatic error conversion
      case RPC.call(channel, MilvusService.Stub, :show_collections, request) do
        {:ok, response} -> handle_response(response)
        {:error, error} -> handle_error(error)
      end

      # Check and convert a Milvus Status
      case RPC.check_status(status, "CreateCollection") do
        :ok -> :ok
        {:error, error} -> {:error, error}
      end
  """

  alias Milvex.Error
  alias Milvex.Errors.Connection
  alias Milvex.Errors.Grpc
  alias Milvex.Milvus.Proto.Common.Status

  @type rpc_result :: {:ok, struct()} | {:error, Error.t()}

  @doc """
  Call a gRPC method with automatic error conversion.

  ## Parameters

  - `channel` - The GRPC.Channel to use
  - `stub_module` - The generated gRPC stub module (e.g., `MilvusService.Stub`)
  - `method` - The RPC method name as an atom (e.g., `:show_collections`)
  - `request` - The request struct
  - `opts` - Options to pass to the gRPC call (e.g., `[timeout: 10_000]`)

  ## Returns

  - `{:ok, response}` on success
  - `{:error, error}` on failure (Connection or Grpc error)

  ## Examples

      RPC.call(channel, MilvusService.Stub, :show_collections, request)
      RPC.call(channel, MilvusService.Stub, :create_collection, request, timeout: 30_000)
  """
  @spec call(GRPC.Channel.t(), module(), atom(), struct(), keyword()) :: rpc_result()
  def call(channel, stub_module, method, request, opts \\ []) do
    case apply(stub_module, method, [channel, request, opts]) do
      {:ok, response} ->
        {:ok, response}

      {:error, %GRPC.RPCError{} = error} ->
        {:error, grpc_error_to_error(error, to_string(method))}

      {:error, reason} ->
        {:error, connection_error(reason)}
    end
  end

  @doc """
  Checks a Milvus Status and converts to an error if not successful.

  Many Milvus RPC calls return a Status struct. Use this function to check
  if the operation succeeded and convert to a proper error if not.

  ## Parameters

  - `status` - The `Milvex.Milvus.Proto.Common.Status` struct
  - `operation` - A string describing the operation (for error context)

  ## Returns

  - `:ok` if status indicates success (code 0)
  - `{:error, error}` if status indicates failure

  ## Examples

      case RPC.check_status(response.status, "CreateCollection") do
        :ok -> {:ok, :created}
        {:error, error} -> {:error, error}
      end
  """
  @spec check_status(Status.t() | nil, String.t()) :: :ok | {:error, Error.t()}
  def check_status(nil, operation) do
    {:error, status_to_error(nil, operation)}
  end

  def check_status(%Status{code: 0}, _operation) do
    :ok
  end

  def check_status(%Status{} = status, operation) do
    {:error, status_to_error(status, operation)}
  end

  @doc """
  Converts a Milvus proto Status to a Splode error.

  ## Parameters

  - `status` - The `Milvex.Milvus.Proto.Common.Status` struct (or nil)
  - `operation` - A string describing the operation (for error context)

  ## Returns

  A `Milvex.Error.t()` representing the status error.
  """
  @spec status_to_error(Status.t() | nil, String.t()) :: Grpc.t()
  def status_to_error(nil, operation) do
    Grpc.exception(
      operation: operation,
      code: :unknown,
      message: "Missing status in response"
    )
  end

  def status_to_error(%Status{code: code, reason: reason, detail: detail}, operation) do
    message = build_message(reason, detail)

    Grpc.exception(
      operation: operation,
      code: code,
      message: message,
      details: %{
        reason: reason,
        detail: detail
      }
    )
  end

  @doc """
  Converts a GRPC.RPCError to a Splode error.

  ## Parameters

  - `grpc_error` - The `GRPC.RPCError` struct
  - `operation` - A string describing the operation (for error context)

  ## Returns

  A `Milvex.Error.t()` representing the gRPC error.
  """
  @spec grpc_error_to_error(GRPC.RPCError.t(), String.t()) :: Grpc.t()
  def grpc_error_to_error(%GRPC.RPCError{status: status, message: message}, operation) do
    Grpc.exception(
      operation: operation,
      code: status,
      message: message || "gRPC error",
      details: %{grpc_status: status}
    )
  end

  @doc """
  Checks if a response has an embedded status field and validates it.

  Use this for responses that embed a Status struct rather than returning it directly.

  ## Examples

      response = %{status: %Status{code: 0}, collections: [...]}
      case RPC.check_response_status(response, "ShowCollections") do
        :ok -> {:ok, response}
        {:error, error} -> {:error, error}
      end
  """
  @spec check_response_status(map(), String.t()) :: :ok | {:error, Error.t()}
  def check_response_status(%{status: status}, operation) do
    check_status(status, operation)
  end

  def check_response_status(_response, _operation) do
    :ok
  end

  @doc """
  Extracts the response if status is successful, otherwise returns error.

  This is a convenience function that combines status checking with response extraction.

  ## Examples

      case RPC.with_status_check(response, "ShowCollections") do
        {:ok, response} -> {:ok, response.collection_names}
        {:error, error} -> {:error, error}
      end
  """
  @spec with_status_check(map(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  def with_status_check(%{status: %Status{code: 0}} = response, _operation) do
    {:ok, response}
  end

  def with_status_check(%{status: status} = _response, operation) do
    {:error, status_to_error(status, operation)}
  end

  def with_status_check(response, _operation) when is_map(response) do
    {:ok, response}
  end

  defp connection_error(reason) do
    Connection.exception(
      reason: reason,
      retriable: retriable_error?(reason)
    )
  end

  defp retriable_error?(:timeout), do: true
  defp retriable_error?(:closed), do: true
  defp retriable_error?(:econnrefused), do: true
  defp retriable_error?(:econnreset), do: true
  defp retriable_error?(:ehostunreach), do: true
  defp retriable_error?(:enetunreach), do: true
  defp retriable_error?(_), do: false

  defp build_message(reason, detail) when is_binary(reason) and reason != "" do
    if is_binary(detail) and detail != "" do
      "#{reason}: #{detail}"
    else
      reason
    end
  end

  defp build_message(_reason, detail) when is_binary(detail) and detail != "" do
    detail
  end

  defp build_message(_reason, _detail) do
    "Operation failed"
  end
end

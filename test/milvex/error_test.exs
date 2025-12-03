defmodule Milvex.ErrorTest do
  use ExUnit.Case, async: true

  alias Milvex.Error
  alias Milvex.Errors.Connection
  alias Milvex.Errors.Grpc
  alias Milvex.Errors.Invalid
  alias Milvex.Errors.Unknown

  describe "Invalid errors" do
    test "creates error with field and message" do
      error = %Invalid{field: "host", message: "cannot be blank"}
      assert Invalid.message(error) == "Invalid host: cannot be blank"
    end

    test "creates error with message only" do
      error = %Invalid{field: nil, message: "validation failed"}
      assert Invalid.message(error) == "Invalid input: validation failed"
    end

    test "can be created via exception/1" do
      error = Invalid.exception(field: "port", message: "must be positive")
      assert Error.splode_error?(error)
    end
  end

  describe "Connection errors" do
    test "creates error with host and port" do
      error = %Connection{reason: :timeout, host: "localhost", port: 19_530}
      assert Connection.message(error) == "Connection failed to localhost:19530: timeout"
    end

    test "creates error with reason only" do
      error = %Connection{reason: "network unreachable", host: nil, port: nil}
      assert Connection.message(error) == "Connection error: network unreachable"
    end

    test "handles atom reasons" do
      error = %Connection{reason: :econnrefused, host: "127.0.0.1", port: 19_530}
      assert Connection.message(error) =~ "econnrefused"
    end
  end

  describe "Grpc errors" do
    test "creates error with operation context" do
      error = %Grpc{operation: "CreateCollection", code: 1, message: "collection exists"}
      assert Grpc.message(error) == "gRPC error in CreateCollection (code: 1): collection exists"
    end

    test "creates error without operation" do
      error = %Grpc{operation: nil, code: 2, message: "server error"}
      assert Grpc.message(error) == "gRPC error (code: 2): server error"
    end

    test "handles atom codes" do
      error = %Grpc{operation: "Search", code: :not_found, message: "collection not found"}
      assert Grpc.message(error) =~ "not_found"
    end
  end

  describe "Unknown errors" do
    test "creates error from exception" do
      exception = %RuntimeError{message: "something went wrong"}
      error = %Unknown{error: exception}
      assert Unknown.message(error) == "Unknown error: something went wrong"
    end

    test "creates error with context" do
      error = %Unknown{error: "unexpected", context: %{step: "initialization"}}
      assert Unknown.message(error) =~ "unexpected"
      assert Unknown.message(error) =~ "initialization"
    end

    test "creates error from term" do
      error = %Unknown{error: {:error, :badarg}}
      assert Unknown.message(error) =~ "{:error, :badarg}"
    end
  end

  describe "Error aggregation" do
    test "splode_error? returns true for valid errors" do
      error = Invalid.exception(message: "test")
      assert Error.splode_error?(error)
    end
  end
end

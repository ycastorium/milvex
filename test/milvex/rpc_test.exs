defmodule Milvex.RPCTest do
  use ExUnit.Case, async: true

  alias Milvex.RPC
  alias Milvex.Error
  alias Milvex.Milvus.Proto.Common.Status

  describe "check_status/2" do
    test "returns :ok for success status (code 0)" do
      status = %Status{code: 0, reason: "", detail: ""}
      assert :ok = RPC.check_status(status, "TestOperation")
    end

    test "returns error for non-zero status code" do
      status = %Status{code: 1, reason: "collection not found", detail: ""}
      assert {:error, error} = RPC.check_status(status, "TestOperation")
      assert Error.splode_error?(error)
    end

    test "returns error for nil status" do
      assert {:error, error} = RPC.check_status(nil, "TestOperation")
      assert Error.splode_error?(error)
    end
  end

  describe "status_to_error/2" do
    test "creates error with operation context" do
      status = %Status{code: 4, reason: "collection not exists", detail: "movies"}
      error = RPC.status_to_error(status, "HasCollection")

      assert Error.splode_error?(error)
      assert error.operation == "HasCollection"
      assert error.code == 4
      assert error.message =~ "collection not exists"
    end

    test "builds message from reason only" do
      status = %Status{code: 1, reason: "error message", detail: ""}
      error = RPC.status_to_error(status, "Test")

      assert error.message == "error message"
    end

    test "builds message from detail when reason empty" do
      status = %Status{code: 1, reason: "", detail: "detailed error"}
      error = RPC.status_to_error(status, "Test")

      assert error.message == "detailed error"
    end

    test "builds message combining reason and detail" do
      status = %Status{code: 1, reason: "error", detail: "more details"}
      error = RPC.status_to_error(status, "Test")

      assert error.message == "error: more details"
    end

    test "uses fallback message when both empty" do
      status = %Status{code: 1, reason: "", detail: ""}
      error = RPC.status_to_error(status, "Test")

      assert error.message == "Operation failed"
    end

    test "handles nil status" do
      error = RPC.status_to_error(nil, "Test")

      assert error.code == :unknown
      assert error.message =~ "Missing status"
    end
  end

  describe "grpc_error_to_error/2" do
    test "converts GRPC.RPCError to Splode error" do
      grpc_error = %GRPC.RPCError{
        status: GRPC.Status.unavailable(),
        message: "server unavailable"
      }

      error = RPC.grpc_error_to_error(grpc_error, "Connect")

      assert Error.splode_error?(error)
      assert error.operation == "Connect"
      assert error.code == GRPC.Status.unavailable()
      assert error.message == "server unavailable"
    end

    test "handles nil message" do
      grpc_error = %GRPC.RPCError{
        status: GRPC.Status.internal(),
        message: nil
      }

      error = RPC.grpc_error_to_error(grpc_error, "Operation")

      assert error.message == "gRPC error"
    end
  end

  describe "check_response_status/2" do
    test "returns :ok when status code is 0" do
      response = %{status: %Status{code: 0}, data: "test"}
      assert :ok = RPC.check_response_status(response, "Operation")
    end

    test "returns error when status code is non-zero" do
      response = %{status: %Status{code: 1, reason: "failed"}, data: nil}
      assert {:error, _error} = RPC.check_response_status(response, "Operation")
    end

    test "returns :ok when response has no status field" do
      response = %{data: "test"}
      assert :ok = RPC.check_response_status(response, "Operation")
    end
  end

  describe "with_status_check/2" do
    test "returns {:ok, response} when status code is 0" do
      response = %{status: %Status{code: 0}, collections: ["test"]}
      assert {:ok, ^response} = RPC.with_status_check(response, "ShowCollections")
    end

    test "returns error when status code is non-zero" do
      response = %{status: %Status{code: 4, reason: "not found"}}
      assert {:error, error} = RPC.with_status_check(response, "ShowCollections")
      assert Error.splode_error?(error)
    end

    test "returns {:ok, response} when response has no status field" do
      response = %{data: "test"}
      assert {:ok, ^response} = RPC.with_status_check(response, "Operation")
    end
  end
end

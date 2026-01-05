defmodule Milvex.FunctionTest do
  use ExUnit.Case, async: true

  alias Milvex.Function
  alias Milvex.Milvus.Proto.Common.KeyValuePair
  alias Milvex.Milvus.Proto.Schema.FunctionSchema

  describe "new/2" do
    test "creates a function with name and type" do
      func = Function.new("bm25_fn", :BM25)
      assert func.name == "bm25_fn"
      assert func.type == :BM25
      assert func.input_field_names == []
      assert func.output_field_names == []
      assert func.params == %{}
    end

    test "accepts atom names" do
      func = Function.new(:bm25_fn, :BM25)
      assert func.name == "bm25_fn"
    end

    test "supports different function types" do
      for type <- [:BM25, :TextEmbedding, :Rerank] do
        func = Function.new("test_fn", type)
        assert func.type == type
      end
    end
  end

  describe "builder methods" do
    test "input_field_names/2 sets input field names" do
      func = Function.new("bm25_fn", :BM25) |> Function.input_field_names(["content"])
      assert func.input_field_names == ["content"]
    end

    test "input_field_names/2 accepts atom field names" do
      func = Function.new("bm25_fn", :BM25) |> Function.input_field_names([:content, :title])
      assert func.input_field_names == ["content", "title"]
    end

    test "output_field_names/2 sets output field names" do
      func = Function.new("bm25_fn", :BM25) |> Function.output_field_names(["sparse"])
      assert func.output_field_names == ["sparse"]
    end

    test "output_field_names/2 accepts atom field names" do
      func = Function.new("bm25_fn", :BM25) |> Function.output_field_names([:sparse])
      assert func.output_field_names == ["sparse"]
    end

    test "param/3 adds a parameter" do
      func = Function.new("bm25_fn", :BM25) |> Function.param("key", "value")
      assert func.params == %{"key" => "value"}
    end

    test "param/3 can add multiple parameters" do
      func =
        Function.new("bm25_fn", :BM25)
        |> Function.param("key1", "value1")
        |> Function.param("key2", "value2")

      assert func.params == %{"key1" => "value1", "key2" => "value2"}
    end
  end

  describe "bm25/2" do
    test "creates BM25 function with single input and output" do
      func = Function.bm25("bm25_fn", input: "content", output: "sparse")
      assert func.name == "bm25_fn"
      assert func.type == :BM25
      assert func.input_field_names == ["content"]
      assert func.output_field_names == ["sparse"]
    end

    test "accepts atom field names" do
      func = Function.bm25("bm25_fn", input: :content, output: :sparse)
      assert func.input_field_names == ["content"]
      assert func.output_field_names == ["sparse"]
    end

    test "accepts multiple input fields" do
      func = Function.bm25("bm25_fn", input: ["title", "content"], output: "sparse")
      assert func.input_field_names == ["title", "content"]
      assert func.output_field_names == ["sparse"]
    end

    test "accepts multiple output fields" do
      func = Function.bm25("bm25_fn", input: "content", output: ["sparse1", "sparse2"])
      assert func.input_field_names == ["content"]
      assert func.output_field_names == ["sparse1", "sparse2"]
    end

    test "raises when input is missing" do
      assert_raise KeyError, fn ->
        Function.bm25("bm25_fn", output: "sparse")
      end
    end

    test "raises when output is missing" do
      assert_raise KeyError, fn ->
        Function.bm25("bm25_fn", input: "content")
      end
    end
  end

  describe "to_proto/1" do
    test "converts basic function to proto" do
      func =
        Function.new("bm25_fn", :BM25)
        |> Function.input_field_names(["content"])
        |> Function.output_field_names(["sparse"])

      proto = Function.to_proto(func)

      assert %FunctionSchema{} = proto
      assert proto.name == "bm25_fn"
      assert proto.type == :BM25
      assert proto.input_field_names == ["content"]
      assert proto.output_field_names == ["sparse"]
    end

    test "includes params in proto" do
      func =
        Function.new("bm25_fn", :BM25)
        |> Function.input_field_names(["content"])
        |> Function.output_field_names(["sparse"])
        |> Function.param("key", "value")

      proto = Function.to_proto(func)

      assert Enum.any?(proto.params, fn %KeyValuePair{key: k, value: v} ->
               k == "key" and v == "value"
             end)
    end

    test "converts BM25 function to proto" do
      func = Function.bm25("bm25_fn", input: "content", output: "sparse")
      proto = Function.to_proto(func)

      assert proto.name == "bm25_fn"
      assert proto.type == :BM25
      assert proto.input_field_names == ["content"]
      assert proto.output_field_names == ["sparse"]
    end
  end

  describe "from_proto/1" do
    test "converts proto to function" do
      proto = %FunctionSchema{
        name: "bm25_fn",
        type: :BM25,
        input_field_names: ["content"],
        output_field_names: ["sparse"],
        params: []
      }

      func = Function.from_proto(proto)

      assert func.name == "bm25_fn"
      assert func.type == :BM25
      assert func.input_field_names == ["content"]
      assert func.output_field_names == ["sparse"]
    end

    test "handles nil field names" do
      proto = %FunctionSchema{
        name: "bm25_fn",
        type: :BM25,
        input_field_names: nil,
        output_field_names: nil,
        params: []
      }

      func = Function.from_proto(proto)
      assert func.input_field_names == []
      assert func.output_field_names == []
    end

    test "decodes params from proto" do
      proto = %FunctionSchema{
        name: "bm25_fn",
        type: :BM25,
        input_field_names: ["content"],
        output_field_names: ["sparse"],
        params: [%KeyValuePair{key: "key", value: "value"}]
      }

      func = Function.from_proto(proto)
      assert func.params == %{"key" => "value"}
    end

    test "roundtrip conversion preserves data" do
      original = Function.bm25("bm25_fn", input: ["title", "content"], output: "sparse")
      proto = Function.to_proto(original)
      restored = Function.from_proto(proto)

      assert restored.name == original.name
      assert restored.type == original.type
      assert restored.input_field_names == original.input_field_names
      assert restored.output_field_names == original.output_field_names
    end

    test "roundtrip conversion preserves params" do
      original =
        Function.bm25("bm25_fn", input: "content", output: "sparse")
        |> Function.param("key1", "value1")
        |> Function.param("key2", "value2")

      proto = Function.to_proto(original)
      restored = Function.from_proto(proto)

      assert restored.params == original.params
    end
  end
end

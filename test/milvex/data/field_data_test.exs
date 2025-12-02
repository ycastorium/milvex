defmodule Milvex.Data.FieldDataTest do
  use ExUnit.Case, async: true

  alias Milvex.Data.FieldData
  alias Milvex.Schema.Field

  alias Milvex.Milvus.Proto.Schema.{
    BoolArray,
    DoubleArray,
    FloatArray,
    IntArray,
    JSONArray,
    LongArray,
    ScalarField,
    SparseFloatArray,
    StringArray,
    VectorField
  }

  alias Milvex.Milvus.Proto.Schema.FieldData, as: FieldDataProto

  describe "build_scalar_field/2" do
    test "builds bool scalar field" do
      scalar = FieldData.build_scalar_field(:bool, [true, false, true])
      assert {:bool_data, %BoolArray{data: [true, false, true]}} = scalar.data
    end

    test "builds int8 scalar field" do
      scalar = FieldData.build_scalar_field(:int8, [1, 2, 3])
      assert {:int_data, %IntArray{data: [1, 2, 3]}} = scalar.data
    end

    test "builds int16 scalar field" do
      scalar = FieldData.build_scalar_field(:int16, [100, 200, 300])
      assert {:int_data, %IntArray{data: [100, 200, 300]}} = scalar.data
    end

    test "builds int32 scalar field" do
      scalar = FieldData.build_scalar_field(:int32, [1000, 2000, 3000])
      assert {:int_data, %IntArray{data: [1000, 2000, 3000]}} = scalar.data
    end

    test "builds int64 scalar field" do
      scalar = FieldData.build_scalar_field(:int64, [1_000_000, 2_000_000])
      assert {:long_data, %LongArray{data: [1_000_000, 2_000_000]}} = scalar.data
    end

    test "builds float scalar field" do
      scalar = FieldData.build_scalar_field(:float, [1.5, 2.5, 3.5])
      assert {:float_data, %FloatArray{data: [1.5, 2.5, 3.5]}} = scalar.data
    end

    test "builds double scalar field" do
      scalar = FieldData.build_scalar_field(:double, [1.123456789, 2.987654321])
      assert {:double_data, %DoubleArray{data: [1.123456789, 2.987654321]}} = scalar.data
    end

    test "builds varchar scalar field" do
      scalar = FieldData.build_scalar_field(:varchar, ["hello", "world"])
      assert {:string_data, %StringArray{data: ["hello", "world"]}} = scalar.data
    end

    test "builds text scalar field" do
      scalar = FieldData.build_scalar_field(:text, ["long text", "another text"])
      assert {:string_data, %StringArray{data: ["long text", "another text"]}} = scalar.data
    end

    test "builds json scalar field with maps" do
      scalar = FieldData.build_scalar_field(:json, [%{key: "value"}, %{num: 42}])
      assert {:json_data, %JSONArray{data: json_bytes}} = scalar.data
      assert [first, second] = json_bytes
      assert {:ok, %{"key" => "value"}} = Jason.decode(first)
      assert {:ok, %{"num" => 42}} = Jason.decode(second)
    end

    test "builds json scalar field with pre-encoded strings" do
      scalar = FieldData.build_scalar_field(:json, [~s({"a":1}), ~s({"b":2})])
      assert {:json_data, %JSONArray{data: [~s({"a":1}), ~s({"b":2})]}} = scalar.data
    end
  end

  describe "extract_scalar_values/1" do
    test "extracts bool values" do
      scalar = %ScalarField{data: {:bool_data, %BoolArray{data: [true, false]}}}
      assert [true, false] = FieldData.extract_scalar_values(scalar)
    end

    test "extracts int values" do
      scalar = %ScalarField{data: {:int_data, %IntArray{data: [1, 2, 3]}}}
      assert [1, 2, 3] = FieldData.extract_scalar_values(scalar)
    end

    test "extracts long values" do
      scalar = %ScalarField{data: {:long_data, %LongArray{data: [1_000_000, 2_000_000]}}}
      assert [1_000_000, 2_000_000] = FieldData.extract_scalar_values(scalar)
    end

    test "extracts float values" do
      scalar = %ScalarField{data: {:float_data, %FloatArray{data: [1.5, 2.5]}}}
      assert [1.5, 2.5] = FieldData.extract_scalar_values(scalar)
    end

    test "extracts double values" do
      scalar = %ScalarField{data: {:double_data, %DoubleArray{data: [1.123, 2.456]}}}
      assert [1.123, 2.456] = FieldData.extract_scalar_values(scalar)
    end

    test "extracts string values" do
      scalar = %ScalarField{data: {:string_data, %StringArray{data: ["a", "b"]}}}
      assert ["a", "b"] = FieldData.extract_scalar_values(scalar)
    end

    test "extracts json values and decodes them" do
      json_bytes = [Jason.encode!(%{key: "value"}), Jason.encode!(%{num: 42})]
      scalar = %ScalarField{data: {:json_data, %JSONArray{data: json_bytes}}}
      assert [%{"key" => "value"}, %{"num" => 42}] = FieldData.extract_scalar_values(scalar)
    end

    test "returns empty list for nil data" do
      scalar = %ScalarField{data: nil}
      assert [] = FieldData.extract_scalar_values(scalar)
    end
  end

  describe "build_vector_field/3" do
    test "builds float_vector field" do
      vectors = [[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]]
      vector = FieldData.build_vector_field(:float_vector, vectors, 4)

      assert vector.dim == 4
      assert {:float_vector, %FloatArray{data: flat}} = vector.data
      assert flat == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
    end

    test "builds sparse_float_vector field" do
      vectors = [[{0, 0.5}, {10, 0.3}, {100, 0.8}], [{5, 1.0}, {50, 0.5}]]
      vector = FieldData.build_vector_field(:sparse_float_vector, vectors, nil)

      assert {:sparse_float_vector, %SparseFloatArray{contents: contents, dim: dim}} = vector.data
      assert dim == 101
      assert length(contents) == 2
    end

    test "builds binary_vector field" do
      vectors = [[1, 0, 1, 0, 1, 0, 1, 0], [0, 1, 0, 1, 0, 1, 0, 1]]
      vector = FieldData.build_vector_field(:binary_vector, vectors, 8)

      assert vector.dim == 8
      assert {:binary_vector, binary} = vector.data
      assert is_binary(binary)
    end

    test "builds int8_vector field" do
      vectors = [[1, 2, 3, 4], [-1, -2, -3, -4]]
      vector = FieldData.build_vector_field(:int8_vector, vectors, 4)

      assert vector.dim == 4
      assert {:int8_vector, binary} = vector.data
      assert is_binary(binary)
    end
  end

  describe "extract_vector_values/1" do
    test "extracts float_vector values and chunks by dimension" do
      vector = %VectorField{
        dim: 4,
        data: {:float_vector, %FloatArray{data: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]}}
      }

      result = FieldData.extract_vector_values(vector)
      assert [[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]] = result
    end

    test "extracts sparse_float_vector values as tuple lists" do
      content1 =
        <<0::32-little-unsigned, 0.5::32-little-float, 10::32-little-unsigned,
          0.25::32-little-float>>

      content2 = <<5::32-little-unsigned, 1.0::32-little-float>>

      vector = %VectorField{
        dim: 11,
        data: {:sparse_float_vector, %SparseFloatArray{contents: [content1, content2], dim: 11}}
      }

      result = FieldData.extract_vector_values(vector)
      assert [[{0, 0.5}, {10, 0.25}], [{5, 1.0}]] = result
    end

    test "returns empty list for nil data" do
      vector = %VectorField{dim: 4, data: nil}
      assert [] = FieldData.extract_vector_values(vector)
    end
  end

  describe "to_proto/3 for scalar fields" do
    test "converts int64 column to FieldData" do
      field = Field.scalar("age", :int64)
      proto = FieldData.to_proto("age", [25, 30, 35], field)

      assert proto.field_name == "age"
      assert proto.type == :Int64
      assert {:scalars, scalar} = proto.field
      assert {:long_data, %LongArray{data: [25, 30, 35]}} = scalar.data
    end

    test "converts varchar column to FieldData" do
      field = Field.varchar("name", 256)
      proto = FieldData.to_proto("name", ["Alice", "Bob"], field)

      assert proto.field_name == "name"
      assert proto.type == :VarChar
      assert {:scalars, scalar} = proto.field
      assert {:string_data, %StringArray{data: ["Alice", "Bob"]}} = scalar.data
    end

    test "converts bool column to FieldData" do
      field = Field.scalar("active", :bool)
      proto = FieldData.to_proto("active", [true, false], field)

      assert proto.field_name == "active"
      assert proto.type == :Bool
      assert {:scalars, scalar} = proto.field
      assert {:bool_data, %BoolArray{data: [true, false]}} = scalar.data
    end
  end

  describe "to_proto/3 for vector fields" do
    test "converts float_vector column to FieldData" do
      field = Field.vector("embedding", 4)
      vectors = [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]]
      proto = FieldData.to_proto("embedding", vectors, field)

      assert proto.field_name == "embedding"
      assert proto.type == :FloatVector
      assert {:vectors, vector} = proto.field
      assert vector.dim == 4
      assert {:float_vector, %FloatArray{data: flat}} = vector.data
      assert length(flat) == 8
    end

    test "converts sparse_float_vector column to FieldData" do
      field = Field.sparse_vector("sparse_emb")
      vectors = [[{0, 0.5}, {10, 0.3}], [{5, 1.0}]]
      proto = FieldData.to_proto("sparse_emb", vectors, field)

      assert proto.field_name == "sparse_emb"
      assert proto.type == :SparseFloatVector
      assert {:vectors, vector} = proto.field
      assert {:sparse_float_vector, sparse} = vector.data
      assert length(sparse.contents) == 2
    end
  end

  describe "from_proto/1" do
    test "extracts field name and scalar values" do
      proto = %FieldDataProto{
        field_name: "count",
        type: :Int32,
        field: {:scalars, %ScalarField{data: {:int_data, %IntArray{data: [1, 2, 3]}}}}
      }

      {name, values} = FieldData.from_proto(proto)
      assert name == "count"
      assert values == [1, 2, 3]
    end

    test "extracts field name and vector values" do
      proto = %FieldDataProto{
        field_name: "vec",
        type: :FloatVector,
        field:
          {:vectors,
           %VectorField{dim: 2, data: {:float_vector, %FloatArray{data: [1.0, 2.0, 3.0, 4.0]}}}}
      }

      {name, values} = FieldData.from_proto(proto)
      assert name == "vec"
      assert values == [[1.0, 2.0], [3.0, 4.0]]
    end
  end

  describe "roundtrip conversions" do
    test "int64 roundtrip" do
      field = Field.scalar("id", :int64)
      original = [100, 200, 300]
      proto = FieldData.to_proto("id", original, field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == original
    end

    test "varchar roundtrip" do
      field = Field.varchar("title", 512)
      original = ["Hello", "World", "Test"]
      proto = FieldData.to_proto("title", original, field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == original
    end

    test "float roundtrip" do
      field = Field.scalar("score", :float)
      original = [1.5, 2.5, 3.5]
      proto = FieldData.to_proto("score", original, field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == original
    end

    test "bool roundtrip" do
      field = Field.scalar("active", :bool)
      original = [true, false, true, false]
      proto = FieldData.to_proto("active", original, field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == original
    end

    test "float_vector roundtrip" do
      field = Field.vector("embedding", 4)
      original = [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]]
      proto = FieldData.to_proto("embedding", original, field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == original
    end

    test "sparse_float_vector roundtrip" do
      field = Field.sparse_vector("sparse")
      original = [[{0, 0.5}, {10, 0.25}, {100, 0.75}], [{5, 1.0}, {50, 0.5}]]
      proto = FieldData.to_proto("sparse", original, field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == original
    end

    test "json roundtrip" do
      field = Field.scalar("metadata", :json)
      original = [%{"key" => "value"}, %{"num" => 42, "nested" => %{"a" => 1}}]
      proto = FieldData.to_proto("metadata", original, field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == original
    end
  end

  describe "edge cases" do
    test "empty values list" do
      field = Field.scalar("empty", :int64)
      proto = FieldData.to_proto("empty", [], field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == []
    end

    test "single value" do
      field = Field.scalar("single", :int64)
      proto = FieldData.to_proto("single", [42], field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == [42]
    end

    test "empty sparse vector" do
      field = Field.sparse_vector("empty_sparse")
      original = [[], []]
      proto = FieldData.to_proto("empty_sparse", original, field)
      {_name, extracted} = FieldData.from_proto(proto)
      assert extracted == original
    end
  end
end

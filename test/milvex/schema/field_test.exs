defmodule Milvex.Schema.FieldTest do
  use ExUnit.Case, async: true

  alias Milvex.Milvus.Proto.Common.KeyValuePair
  alias Milvex.Milvus.Proto.Schema.FieldSchema
  alias Milvex.Schema.Field

  describe "new/2" do
    test "creates a field with name and type" do
      field = Field.new("id", :int64)
      assert field.name == "id"
      assert field.data_type == :int64
      assert field.is_primary_key == false
      assert field.auto_id == false
    end

    test "accepts atom names" do
      field = Field.new(:embedding, :float_vector)
      assert field.name == "embedding"
    end

    test "supports all scalar types" do
      for type <- [:bool, :int8, :int16, :int32, :int64, :float, :double, :varchar, :json, :text] do
        field = Field.new("test", type)
        assert field.data_type == type
      end
    end

    test "supports all vector types" do
      for type <- [
            :binary_vector,
            :float_vector,
            :float16_vector,
            :bfloat16_vector,
            :sparse_float_vector,
            :int8_vector
          ] do
        field = Field.new("test", type)
        assert field.data_type == type
      end
    end
  end

  describe "builder methods" do
    test "description/2 sets the description" do
      field = Field.new("id", :int64) |> Field.description("Primary identifier")
      assert field.description == "Primary identifier"
    end

    test "set_primary_key/1 marks field as primary key" do
      field = Field.new("id", :int64) |> Field.set_primary_key()
      assert field.is_primary_key == true
    end

    test "set_primary_key/2 accepts boolean argument" do
      field = Field.new("id", :int64) |> Field.set_primary_key(false)
      assert field.is_primary_key == false
    end

    test "auto_id/2 enables auto ID generation" do
      field = Field.new("id", :int64) |> Field.auto_id()
      assert field.auto_id == true
    end

    test "dimension/2 sets vector dimension" do
      field = Field.new("embedding", :float_vector) |> Field.dimension(128)
      assert field.dimension == 128
    end

    test "max_length/2 sets varchar max length" do
      field = Field.new("title", :varchar) |> Field.max_length(512)
      assert field.max_length == 512
    end

    test "element_type/2 sets array element type" do
      field = Field.new("tags", :array) |> Field.element_type(:varchar)
      assert field.element_type == :varchar
    end

    test "max_capacity/2 sets array max capacity" do
      field = Field.new("tags", :array) |> Field.max_capacity(100)
      assert field.max_capacity == 100
    end

    test "nullable/2 marks field as nullable" do
      field = Field.new("title", :varchar) |> Field.nullable()
      assert field.nullable == true
    end

    test "partition_key/2 marks field as partition key" do
      field = Field.new("category", :int64) |> Field.partition_key()
      assert field.is_partition_key == true
    end

    test "clustering_key/2 marks field as clustering key" do
      field = Field.new("timestamp", :int64) |> Field.clustering_key()
      assert field.is_clustering_key == true
    end

    test "default/2 sets default value" do
      field = Field.new("status", :varchar) |> Field.default("active")
      assert field.default_value == "active"
    end

    test "enable_analyzer/2 enables text analyzer" do
      field = Field.new("content", :varchar) |> Field.enable_analyzer()
      assert field.enable_analyzer == true
    end

    test "enable_analyzer/2 accepts boolean argument" do
      field = Field.new("content", :varchar) |> Field.enable_analyzer(false)
      assert field.enable_analyzer == false
    end
  end

  describe "smart constructors" do
    test "primary_key/3 creates int64 primary key" do
      field = Field.primary_key("id", :int64)
      assert field.name == "id"
      assert field.data_type == :int64
      assert field.is_primary_key == true
      assert field.auto_id == false
    end

    test "primary_key/3 with auto_id option" do
      field = Field.primary_key("id", :int64, auto_id: true)
      assert field.auto_id == true
    end

    test "primary_key/3 creates varchar primary key with max_length" do
      field = Field.primary_key("pk", :varchar, max_length: 128)
      assert field.data_type == :varchar
      assert field.max_length == 128
    end

    test "primary_key/3 defaults varchar max_length to 64" do
      field = Field.primary_key("pk", :varchar)
      assert field.max_length == 64
    end

    test "vector/3 creates vector field with dimension" do
      field = Field.vector("embedding", 768)
      assert field.name == "embedding"
      assert field.data_type == :float_vector
      assert field.dimension == 768
    end

    test "vector/3 accepts type option" do
      field = Field.vector("embedding", 512, type: :float16_vector)
      assert field.data_type == :float16_vector
    end

    test "vector/3 raises for invalid type" do
      assert_raise ArgumentError, ~r/Invalid vector type/, fn ->
        Field.vector("embedding", 128, type: :int64)
      end
    end

    test "sparse_vector/2 creates sparse vector field" do
      field = Field.sparse_vector("sparse_emb")
      assert field.data_type == :sparse_float_vector
      assert field.dimension == nil
    end

    test "varchar/3 creates varchar field" do
      field = Field.varchar("title", 256)
      assert field.data_type == :varchar
      assert field.max_length == 256
      assert field.nullable == false
    end

    test "varchar/3 with nullable option" do
      field = Field.varchar("title", 256, nullable: true)
      assert field.nullable == true
    end

    test "varchar/3 with enable_analyzer option" do
      field = Field.varchar("content", 4096, enable_analyzer: true)
      assert field.enable_analyzer == true
    end

    test "scalar/3 creates scalar field" do
      field = Field.scalar("age", :int32)
      assert field.data_type == :int32
      assert field.nullable == false
    end

    test "array/3 creates array field" do
      field = Field.array("tags", :varchar, max_capacity: 50, max_length: 64)
      assert field.data_type == :array
      assert field.element_type == :varchar
      assert field.max_capacity == 50
      assert field.max_length == 64
    end

    test "array/3 requires max_capacity" do
      assert_raise KeyError, fn ->
        Field.array("tags", :varchar, [])
      end
    end

    test "array/3 with struct element type creates array_of_struct" do
      struct_fields = [
        Field.varchar("text", 1024),
        Field.vector("embedding", 128)
      ]

      field = Field.array("sentences", :struct, max_capacity: 50, struct_schema: struct_fields)
      assert field.data_type == :array_of_struct
      assert field.max_capacity == 50
      assert field.struct_schema == struct_fields
      assert field.element_type == :struct
    end

    test "struct/2 creates struct field" do
      struct_fields = [
        Field.varchar("text", 1024),
        Field.vector("embedding", 128)
      ]

      field = Field.struct("sentence", fields: struct_fields)
      assert field.data_type == :struct
      assert field.struct_schema == struct_fields
    end
  end

  describe "validate/1" do
    test "valid int64 primary key field" do
      field = Field.primary_key("id", :int64, auto_id: true)
      assert {:ok, ^field} = Field.validate(field)
    end

    test "valid vector field" do
      field = Field.vector("embedding", 128)
      assert {:ok, ^field} = Field.validate(field)
    end

    test "valid varchar field" do
      field = Field.varchar("title", 512)
      assert {:ok, ^field} = Field.validate(field)
    end

    test "rejects empty name" do
      field = Field.new("", :int64)
      assert {:error, error} = Field.validate(field)
      assert error.field == :name
      assert error.message =~ "cannot be empty"
    end

    test "rejects name exceeding 255 characters" do
      field = Field.new(String.duplicate("a", 256), :int64)
      assert {:error, error} = Field.validate(field)
      assert error.field == :name
      assert error.message =~ "cannot exceed 255 characters"
    end

    test "rejects invalid name characters" do
      field = Field.new("my-field", :int64)
      assert {:error, error} = Field.validate(field)
      assert error.field == :name
      assert error.message =~ "must start with a letter"
    end

    test "rejects name starting with number" do
      field = Field.new("1field", :int64)
      assert {:error, error} = Field.validate(field)
      assert error.field == :name
    end

    test "rejects vector field without dimension" do
      field = Field.new("embedding", :float_vector)
      assert {:error, error} = Field.validate(field)
      assert error.field == :dimension
      assert error.message =~ "is required"
    end

    test "sparse vectors don't require dimension" do
      field = Field.sparse_vector("sparse_emb")
      assert {:ok, _} = Field.validate(field)
    end

    test "rejects binary vector with non-multiple-of-8 dimension" do
      field = Field.new("binary_emb", :binary_vector) |> Field.dimension(100)
      assert {:error, error} = Field.validate(field)
      assert error.field == :dimension
      assert error.message =~ "multiple of 8"
    end

    test "accepts binary vector with multiple-of-8 dimension" do
      field = Field.new("binary_emb", :binary_vector) |> Field.dimension(128)
      assert {:ok, _} = Field.validate(field)
    end

    test "rejects varchar without max_length" do
      field = Field.new("title", :varchar)
      assert {:error, error} = Field.validate(field)
      assert error.field == :max_length
      assert error.message =~ "is required"
    end

    test "rejects array without element_type" do
      field = Field.new("tags", :array) |> Field.max_capacity(100)
      assert {:error, error} = Field.validate(field)
      assert error.field == :element_type
    end

    test "rejects array without max_capacity" do
      field = Field.new("tags", :array) |> Field.element_type(:varchar)
      assert {:error, error} = Field.validate(field)
      assert error.field == :max_capacity
    end

    test "rejects array_of_struct without struct_schema" do
      field = %Field{
        name: "sentences",
        data_type: :array_of_struct,
        max_capacity: 50,
        element_type: :struct
      }

      assert {:error, error} = Field.validate(field)
      assert error.field == :struct_schema
    end

    test "validates array_of_struct with struct_schema" do
      struct_fields = [Field.varchar("text", 1024)]
      field = Field.array("sentences", :struct, max_capacity: 50, struct_schema: struct_fields)
      assert {:ok, _} = Field.validate(field)
    end

    test "rejects non-int64/varchar primary key" do
      field = Field.new("id", :float) |> Field.set_primary_key()
      assert {:error, error} = Field.validate(field)
      assert error.field == :data_type
      assert error.message =~ "primary key must be int64 or varchar"
    end

    test "rejects auto_id on varchar primary key" do
      field =
        Field.new("id", :varchar)
        |> Field.set_primary_key()
        |> Field.auto_id()
        |> Field.max_length(64)

      assert {:error, error} = Field.validate(field)
      assert error.field == :auto_id
      assert error.message =~ "only supported for int64"
    end
  end

  describe "validate!/1" do
    test "returns field when valid" do
      field = Field.primary_key("id", :int64)
      assert Field.validate!(field) == field
    end

    test "raises on invalid field" do
      field = Field.new("", :int64)

      assert_raise Milvex.Errors.Invalid, fn ->
        Field.validate!(field)
      end
    end
  end

  describe "to_proto/1" do
    test "converts basic field to proto" do
      field = Field.new("id", :int64) |> Field.set_primary_key() |> Field.auto_id()
      proto = Field.to_proto(field)

      assert %FieldSchema{} = proto
      assert proto.name == "id"
      assert proto.data_type == :Int64
      assert proto.is_primary_key == true
      assert proto.autoID == true
    end

    test "includes dimension in type_params for vectors" do
      field = Field.vector("embedding", 128)
      proto = Field.to_proto(field)

      assert proto.data_type == :FloatVector

      assert Enum.any?(proto.type_params, fn %KeyValuePair{key: k, value: v} ->
               k == "dim" and v == "128"
             end)
    end

    test "includes max_length in type_params for varchar" do
      field = Field.varchar("title", 512)
      proto = Field.to_proto(field)

      assert proto.data_type == :VarChar

      assert Enum.any?(proto.type_params, fn %KeyValuePair{key: k, value: v} ->
               k == "max_length" and v == "512"
             end)
    end

    test "converts array field with element_type" do
      field = Field.array("tags", :varchar, max_capacity: 100, max_length: 64)
      proto = Field.to_proto(field)

      assert proto.data_type == :Array
      assert proto.element_type == :VarChar
    end
  end

  describe "from_proto/1" do
    test "converts proto to field" do
      proto = %FieldSchema{
        name: "embedding",
        data_type: :FloatVector,
        is_primary_key: false,
        autoID: false,
        type_params: [%KeyValuePair{key: "dim", value: "256"}],
        nullable: false
      }

      field = Field.from_proto(proto)

      assert field.name == "embedding"
      assert field.data_type == :float_vector
      assert field.dimension == 256
    end

    test "handles empty description" do
      proto = %FieldSchema{
        name: "id",
        data_type: :Int64,
        description: "",
        is_primary_key: true
      }

      field = Field.from_proto(proto)
      assert field.description == nil
    end

    test "roundtrip conversion preserves data" do
      original = Field.vector("embedding", 768, description: "Doc embeddings")
      proto = Field.to_proto(original)
      restored = Field.from_proto(proto)

      assert restored.name == original.name
      assert restored.data_type == original.data_type
      assert restored.dimension == original.dimension
      assert restored.description == original.description
    end

    test "to_proto includes enable_analyzer in type_params" do
      field = Field.varchar("content", 4096, enable_analyzer: true)
      proto = Field.to_proto(field)

      assert Enum.any?(proto.type_params, fn %KeyValuePair{key: k, value: v} ->
               k == "enable_analyzer" and v == "true"
             end)
    end

    test "from_proto decodes enable_analyzer from type_params" do
      proto = %FieldSchema{
        name: "content",
        data_type: :VarChar,
        is_primary_key: false,
        autoID: false,
        type_params: [
          %KeyValuePair{key: "max_length", value: "4096"},
          %KeyValuePair{key: "enable_analyzer", value: "true"}
        ],
        nullable: false
      }

      field = Field.from_proto(proto)
      assert field.enable_analyzer == true
    end

    test "roundtrip conversion preserves enable_analyzer" do
      original = Field.varchar("content", 4096, enable_analyzer: true)
      proto = Field.to_proto(original)
      restored = Field.from_proto(proto)

      assert restored.name == original.name
      assert restored.data_type == original.data_type
      assert restored.max_length == original.max_length
      assert restored.enable_analyzer == original.enable_analyzer
    end
  end

  describe "type helpers" do
    test "data_types/0 returns all types" do
      types = Field.data_types()
      assert :int64 in types
      assert :float_vector in types
      assert length(types) > 10
    end

    test "scalar_types/0 returns scalar types" do
      types = Field.scalar_types()
      assert :int64 in types
      assert :varchar in types
      refute :float_vector in types
    end

    test "vector_types/0 returns vector types" do
      types = Field.vector_types()
      assert :float_vector in types
      assert :binary_vector in types
      refute :int64 in types
    end

    test "vector_type?/1 checks if type is vector" do
      assert Field.vector_type?(:float_vector)
      assert Field.vector_type?(:sparse_float_vector)
      refute Field.vector_type?(:int64)
    end

    test "scalar_type?/1 checks if type is scalar" do
      assert Field.scalar_type?(:int64)
      assert Field.scalar_type?(:varchar)
      refute Field.scalar_type?(:float_vector)
    end

    test "array_of_struct?/1 checks if field is array of struct" do
      struct_fields = [Field.varchar("text", 1024)]

      array_of_struct =
        Field.array("sentences", :struct, max_capacity: 50, struct_schema: struct_fields)

      regular_array = Field.array("tags", :varchar, max_capacity: 100, max_length: 64)
      scalar = Field.scalar("count", :int32)

      assert Field.array_of_struct?(array_of_struct)
      refute Field.array_of_struct?(regular_array)
      refute Field.array_of_struct?(scalar)
    end

    test "struct?/1 checks if field is struct" do
      struct_fields = [Field.varchar("text", 1024)]
      struct_field = Field.struct("sentence", fields: struct_fields)
      scalar = Field.scalar("count", :int32)

      assert Field.struct?(struct_field)
      refute Field.struct?(scalar)
    end
  end
end

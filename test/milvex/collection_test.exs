defmodule Milvex.CollectionTest do
  use ExUnit.Case, async: true

  alias Milvex.Collection
  alias Milvex.Collection.Dsl.Field

  defmodule BasicCollection do
    use Milvex.Collection

    collection do
      name "test_collection"
      description "A test collection"

      fields do
        primary_key :id, :int64, auto_id: true
        varchar :title, 256
        vector :embedding, 128
      end
    end
  end

  defmodule FullCollection do
    use Milvex.Collection

    collection do
      name "full_collection"
      enable_dynamic_field true

      fields do
        primary_key :id, :int64
        varchar :name, 512, nullable: true
        scalar :count, :int32
        scalar :score, :float
        scalar :is_active, :bool
        scalar :metadata, :json
        vector :dense_embedding, 256
        vector :binary_emb, 64, type: :binary_vector
        sparse_vector :sparse_emb
        array :tags, :varchar, max_capacity: 50, max_length: 100
        array :scores, :float, max_capacity: 10
      end
    end
  end

  defmodule VarcharPKCollection do
    use Milvex.Collection

    collection do
      name "varchar_pk_collection"

      fields do
        primary_key :pk, :varchar, max_length: 64
        vector :embedding, 128
      end
    end
  end

  describe "basic collection definition" do
    test "extracts collection name" do
      assert Collection.collection_name(BasicCollection) == "test_collection"
    end

    test "extracts description" do
      assert Collection.description(BasicCollection) == "A test collection"
    end

    test "defaults enable_dynamic_field to false" do
      refute Collection.enable_dynamic_field?(BasicCollection)
    end

    test "extracts fields" do
      fields = Collection.fields(BasicCollection)
      assert length(fields) == 3

      names = Enum.map(fields, & &1.name)
      assert :id in names
      assert :title in names
      assert :embedding in names
    end

    test "identifies primary key" do
      pk = Collection.primary_key(BasicCollection)
      assert pk.name == :id
      assert pk.type == :int64
      assert pk.auto_id == true
    end

    test "identifies vector fields" do
      vectors = Collection.vector_fields(BasicCollection)
      assert length(vectors) == 1
      assert hd(vectors).name == :embedding
      assert hd(vectors).dimension == 128
    end
  end

  describe "collection with all field types" do
    test "enables dynamic field" do
      assert Collection.enable_dynamic_field?(FullCollection)
    end

    test "extracts all fields" do
      fields = Collection.fields(FullCollection)
      assert length(fields) == 11
    end

    test "identifies vector fields including sparse" do
      vectors = Collection.vector_fields(FullCollection)
      assert length(vectors) == 3

      types = Enum.map(vectors, & &1.type)
      assert :float_vector in types
      assert :binary_vector in types
      assert :sparse_float_vector in types
    end

    test "identifies scalar fields (excludes varchar and arrays)" do
      scalars = Collection.scalar_fields(FullCollection)
      names = Enum.map(scalars, & &1.name)

      assert :count in names
      assert :score in names
      assert :is_active in names
      assert :metadata in names

      refute :name in names
      refute :tags in names
    end
  end

  describe "varchar primary key" do
    test "supports varchar primary key" do
      pk = Collection.primary_key(VarcharPKCollection)
      assert pk.name == :pk
      assert pk.type == :varchar
      assert pk.max_length == 64
    end
  end

  describe "to_schema/1" do
    test "converts DSL to Milvex.Schema struct" do
      schema = Collection.to_schema(BasicCollection)

      assert %Milvex.Schema{} = schema
      assert schema.name == "test_collection"
      assert schema.description == "A test collection"
      refute schema.enable_dynamic_field
      assert length(schema.fields) == 3
    end

    test "converts fields correctly" do
      schema = Collection.to_schema(BasicCollection)

      pk = Enum.find(schema.fields, &(&1.name == "id"))
      assert pk.data_type == :int64
      assert pk.is_primary_key
      assert pk.auto_id

      varchar = Enum.find(schema.fields, &(&1.name == "title"))
      assert varchar.data_type == :varchar
      assert varchar.max_length == 256

      vector = Enum.find(schema.fields, &(&1.name == "embedding"))
      assert vector.data_type == :float_vector
      assert vector.dimension == 128
    end
  end

  describe "to_proto/1" do
    test "converts DSL to CollectionSchema proto" do
      proto = Collection.to_proto(BasicCollection)

      assert %Milvex.Milvus.Proto.Schema.CollectionSchema{} = proto
      assert proto.name == "test_collection"
      assert proto.description == "A test collection"
      assert length(proto.fields) == 3
    end

    test "converts field types to proto enums" do
      proto = Collection.to_proto(BasicCollection)

      pk = Enum.find(proto.fields, &(&1.name == "id"))
      assert pk.data_type == :Int64
      assert pk.is_primary_key
      assert pk.autoID

      vector = Enum.find(proto.fields, &(&1.name == "embedding"))
      assert vector.data_type == :FloatVector
    end
  end

  describe "struct generation" do
    test "generates struct from DSL fields" do
      movie = %BasicCollection{id: 1, title: "Test", embedding: [0.1, 0.2]}
      assert movie.id == 1
      assert movie.title == "Test"
      assert movie.embedding == [0.1, 0.2]
    end

    test "struct fields match DSL field names" do
      fields = BasicCollection.__struct__() |> Map.keys() |> Enum.reject(&(&1 == :__struct__))
      assert :id in fields
      assert :title in fields
      assert :embedding in fields
    end

    test "full collection struct has all fields" do
      full = %FullCollection{
        id: 1,
        name: "Test",
        count: 10,
        score: 0.5,
        is_active: true,
        metadata: %{"key" => "value"},
        dense_embedding: [0.1],
        binary_emb: <<1, 2, 3>>,
        sparse_emb: %{0 => 0.5},
        tags: ["tag1"],
        scores: [0.9]
      }

      assert full.id == 1
      assert full.name == "Test"
      assert full.count == 10
    end

    test "struct supports pattern matching" do
      movie = %BasicCollection{id: 1, title: "Inception", embedding: [0.1]}

      assert %BasicCollection{title: title} = movie
      assert title == "Inception"
    end

    test "struct supports update syntax" do
      movie = %BasicCollection{id: 1, title: "Old", embedding: [0.1]}
      updated = %{movie | title: "New"}

      assert updated.title == "New"
      assert updated.id == 1
    end
  end

  describe "Field helper functions" do
    test "vector?/1 identifies vector fields" do
      assert Field.vector?(%Field{type: :float_vector})
      assert Field.vector?(%Field{type: :binary_vector})
      assert Field.vector?(%Field{type: :sparse_float_vector})
      refute Field.vector?(%Field{type: :int64})
      refute Field.vector?(%Field{type: :varchar})
    end

    test "sparse_vector?/1 identifies sparse vectors" do
      assert Field.sparse_vector?(%Field{type: :sparse_float_vector})
      refute Field.sparse_vector?(%Field{type: :float_vector})
    end

    test "scalar?/1 identifies scalar fields" do
      assert Field.scalar?(%Field{type: :int32, element_type: nil})
      assert Field.scalar?(%Field{type: :varchar, element_type: nil})
      refute Field.scalar?(%Field{type: :float_vector})
      refute Field.scalar?(%Field{type: :int32, element_type: :int32})
    end

    test "array?/1 identifies array fields" do
      assert Field.array?(%Field{element_type: :varchar})
      refute Field.array?(%Field{element_type: nil})
    end
  end
end

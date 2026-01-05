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

  defmodule StructArrayCollection do
    use Milvex.Collection

    collection do
      name "struct_array_collection"

      fields do
        primary_key :id, :int64, auto_id: true

        array :sentences, :struct,
          max_capacity: 10,
          struct_schema: [
            Milvex.Schema.Field.varchar("text", 256),
            Milvex.Schema.Field.vector("embedding", 128)
          ]
      end
    end
  end

  defmodule BM25Collection do
    use Milvex.Collection

    collection do
      name "bm25_collection"

      fields do
        primary_key :id, :int64, auto_id: true
        varchar :text, 1000, enable_analyzer: true
        sparse_vector :text_sparse
      end

      functions do
        bm25 :text_bm25, input: :text, output: :text_sparse
      end
    end
  end

  defmodule MultiBM25Collection do
    use Milvex.Collection

    collection do
      name "multi_bm25_collection"

      fields do
        primary_key :id, :int64, auto_id: true
        varchar :title, 256, enable_analyzer: true
        varchar :content, 2000, enable_analyzer: true
        sparse_vector :sparse_emb
      end

      functions do
        bm25 :multi_bm25, input: [:title, :content], output: :sparse_emb
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

  defmodule StringPrefixCollection do
    use Milvex.Collection

    collection do
      name "items"
      prefix("prod_")

      fields do
        primary_key :id, :int64, auto_id: true
        vector :embedding, 128
      end
    end
  end

  defmodule FunctionPrefixCollection do
    use Milvex.Collection

    collection do
      name "items"
      prefix(fn -> "dynamic_" end)

      fields do
        primary_key :id, :int64, auto_id: true
        vector :embedding, 128
      end
    end
  end

  describe "collection prefix" do
    test "collection without prefix returns base name" do
      assert Collection.collection_name(BasicCollection) == "test_collection"
      assert Collection.prefix(BasicCollection) == nil
    end

    test "collection with string prefix returns prefixed name" do
      assert Collection.collection_name(StringPrefixCollection) == "prod_items"
    end

    test "prefix/1 returns raw string prefix" do
      assert Collection.prefix(StringPrefixCollection) == "prod_"
    end

    test "collection with function prefix returns prefixed name" do
      assert Collection.collection_name(FunctionPrefixCollection) == "dynamic_items"
    end

    test "prefix/1 returns raw function prefix" do
      prefix = Collection.prefix(FunctionPrefixCollection)
      assert is_function(prefix, 0)
      assert prefix.() == "dynamic_"
    end

    test "to_schema/1 uses prefixed collection name" do
      schema = Collection.to_schema(StringPrefixCollection)
      assert schema.name == "prod_items"
    end

    test "to_proto/1 uses prefixed collection name" do
      proto = Collection.to_proto(StringPrefixCollection)
      assert proto.name == "prod_items"
    end
  end

  describe "struct array fields" do
    test "extracts struct array field with struct_schema" do
      fields = Collection.fields(StructArrayCollection)
      array_field = Enum.find(fields, &(&1.element_type == :struct))

      assert array_field.name == :sentences
      assert array_field.max_capacity == 10
      assert length(array_field.struct_schema) == 2
    end

    test "struct_schema contains varchar and vector fields" do
      fields = Collection.fields(StructArrayCollection)
      array_field = Enum.find(fields, &(&1.element_type == :struct))

      struct_fields = array_field.struct_schema
      text_field = Enum.find(struct_fields, &(&1.name == "text"))
      vector_field = Enum.find(struct_fields, &(&1.name == "embedding"))

      assert text_field.data_type == :varchar
      assert text_field.max_length == 256
      assert vector_field.data_type == :float_vector
      assert vector_field.dimension == 128
    end

    test "to_schema/1 converts struct array correctly" do
      schema = Collection.to_schema(StructArrayCollection)
      array_field = Enum.find(schema.fields, &(&1.name == "sentences"))

      assert array_field.data_type == :array_of_struct
      assert length(array_field.struct_schema) == 2
    end

    test "struct generates field for struct array" do
      record = %StructArrayCollection{
        id: 1,
        sentences: [
          %{"text" => "Hello", "embedding" => List.duplicate(0.0, 128)}
        ]
      }

      assert record.id == 1
      assert length(record.sentences) == 1
    end
  end

  describe "functions section" do
    test "DSL collection with functions section works" do
      assert Collection.collection_name(BM25Collection) == "bm25_collection"
    end

    test "functions/1 returns function definitions" do
      funcs = Collection.functions(BM25Collection)
      assert length(funcs) == 1

      func = hd(funcs)
      assert func.name == :text_bm25
      assert func.input == :text
      assert func.output == :text_sparse
    end

    test "functions/1 returns multiple input fields" do
      funcs = Collection.functions(MultiBM25Collection)
      assert length(funcs) == 1

      func = hd(funcs)
      assert func.name == :multi_bm25
      assert func.input == [:title, :content]
      assert func.output == :sparse_emb
    end

    test "functions/1 returns empty list when no functions defined" do
      funcs = Collection.functions(BasicCollection)
      assert funcs == []
    end
  end

  describe "enable_analyzer field option" do
    test "varchar field with enable_analyzer" do
      fields = Collection.fields(BM25Collection)
      text_field = Enum.find(fields, &(&1.name == :text))

      assert text_field.enable_analyzer == true
    end

    test "varchar field without enable_analyzer defaults to false" do
      fields = Collection.fields(BasicCollection)
      title_field = Enum.find(fields, &(&1.name == :title))

      assert title_field.enable_analyzer == false
    end
  end

  describe "to_schema/1 with functions" do
    test "includes functions in schema" do
      schema = Collection.to_schema(BM25Collection)

      assert length(schema.functions) == 1
      func = hd(schema.functions)

      assert func.name == "text_bm25"
      assert func.type == :BM25
      assert func.input_field_names == ["text"]
      assert func.output_field_names == ["text_sparse"]
    end

    test "includes enable_analyzer in schema fields" do
      schema = Collection.to_schema(BM25Collection)
      text_field = Enum.find(schema.fields, &(&1.name == "text"))

      assert text_field.enable_analyzer == true
    end

    test "converts multiple input fields correctly" do
      schema = Collection.to_schema(MultiBM25Collection)

      assert length(schema.functions) == 1
      func = hd(schema.functions)

      assert func.name == "multi_bm25"
      assert func.input_field_names == ["title", "content"]
      assert func.output_field_names == ["sparse_emb"]
    end
  end
end

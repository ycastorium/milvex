defmodule Milvex.DataTest do
  use ExUnit.Case, async: true

  alias Milvex.Data
  alias Milvex.Schema
  alias Milvex.Schema.Field

  describe "from_rows/2" do
    setup do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("title", 256),
            Field.scalar("score", :float)
          ]
        )

      {:ok, schema: schema}
    end

    test "converts rows to column format", %{schema: schema} do
      rows = [
        %{id: 1, title: "Movie 1", score: 8.5},
        %{id: 2, title: "Movie 2", score: 9.0},
        %{id: 3, title: "Movie 3", score: 7.5}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      assert Data.num_rows(data) == 3
      assert Data.get_field(data, "id") == [1, 2, 3]
      assert Data.get_field(data, "title") == ["Movie 1", "Movie 2", "Movie 3"]
      assert Data.get_field(data, "score") == [8.5, 9.0, 7.5]
    end

    test "handles atom keys in rows", %{schema: schema} do
      rows = [
        %{id: 1, title: "Test", score: 5.0}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      assert Data.get_field(data, :id) == [1]
      assert Data.get_field(data, :title) == ["Test"]
    end

    test "handles string keys in rows", %{schema: schema} do
      rows = [
        %{"id" => 1, "title" => "Test", "score" => 5.0}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      assert Data.get_field(data, "id") == [1]
    end

    test "returns error for inconsistent row fields" do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("title", 256)
          ]
        )

      rows = [
        %{id: 1, title: "Test"},
        %{id: 2}
      ]

      assert {:error, error} = Data.from_rows(rows, schema)
      assert error.message =~ "different fields"
    end

    test "returns error for missing required fields", %{schema: schema} do
      rows = [
        %{id: 1, title: "Test"}
      ]

      assert {:error, error} = Data.from_rows(rows, schema)
      assert error.message =~ "missing required fields"
      assert error.message =~ "score"
    end

    test "handles empty rows", %{schema: schema} do
      {:ok, data} = Data.from_rows([], schema)

      assert Data.num_rows(data) == 0
      assert Data.get_field(data, "id") == []
    end

    test "from_rows! raises on error", %{schema: schema} do
      rows = [%{id: 1}]

      assert_raise Milvex.Errors.Invalid, fn ->
        Data.from_rows!(rows, schema)
      end
    end
  end

  describe "from_columns/2" do
    setup do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("name", 128)
          ]
        )

      {:ok, schema: schema}
    end

    test "creates data from column format", %{schema: schema} do
      columns = %{
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"]
      }

      {:ok, data} = Data.from_columns(columns, schema)

      assert Data.num_rows(data) == 3
      assert Data.get_field(data, "id") == [1, 2, 3]
      assert Data.get_field(data, "name") == ["Alice", "Bob", "Charlie"]
    end

    test "normalizes atom keys to strings", %{schema: schema} do
      columns = %{
        id: [1, 2],
        name: ["A", "B"]
      }

      {:ok, data} = Data.from_columns(columns, schema)

      assert Data.get_field(data, "id") == [1, 2]
    end

    test "returns error for mismatched column lengths", %{schema: schema} do
      columns = %{
        id: [1, 2, 3],
        name: ["A", "B"]
      }

      assert {:error, error} = Data.from_columns(columns, schema)
      assert error.message =~ "same length"
    end

    test "returns error for missing required columns", %{schema: schema} do
      columns = %{
        id: [1, 2]
      }

      assert {:error, error} = Data.from_columns(columns, schema)
      assert error.message =~ "missing required fields"
    end

    test "handles empty columns", %{schema: schema} do
      columns = %{
        id: [],
        name: []
      }

      {:ok, data} = Data.from_columns(columns, schema)

      assert Data.num_rows(data) == 0
    end

    test "from_columns! raises on error", %{schema: schema} do
      columns = %{id: [1]}

      assert_raise Milvex.Errors.Invalid, fn ->
        Data.from_columns!(columns, schema)
      end
    end
  end

  describe "with auto_id fields" do
    test "excludes auto_id fields from required validation" do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("name", 128)
          ]
        )

      rows = [
        %{name: "Alice"},
        %{name: "Bob"}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      assert Data.num_rows(data) == 2
      assert Data.get_field(data, "name") == ["Alice", "Bob"]
    end
  end

  describe "with nullable fields" do
    test "nullable fields are not required" do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("name", 128, nullable: true)
          ]
        )

      rows = [%{id: 1}]

      {:ok, data} = Data.from_rows(rows, schema)
      assert Data.num_rows(data) == 1
    end
  end

  describe "with vector fields" do
    test "handles float vectors" do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64),
            Field.vector("embedding", 4)
          ]
        )

      rows = [
        %{id: 1, embedding: [0.1, 0.2, 0.3, 0.4]},
        %{id: 2, embedding: [0.5, 0.6, 0.7, 0.8]}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      assert Data.get_field(data, "embedding") == [
               [0.1, 0.2, 0.3, 0.4],
               [0.5, 0.6, 0.7, 0.8]
             ]
    end

    test "handles sparse vectors" do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64),
            Field.sparse_vector("sparse")
          ]
        )

      rows = [
        %{id: 1, sparse: [{0, 0.5}, {10, 0.25}]},
        %{id: 2, sparse: [{5, 1.0}]}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      assert Data.get_field(data, "sparse") == [
               [{0, 0.5}, {10, 0.25}],
               [{5, 1.0}]
             ]
    end
  end

  describe "to_proto/1" do
    test "converts data to FieldData list" do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      rows = [
        %{id: 1, title: "Test", embedding: [0.1, 0.2, 0.3, 0.4]}
      ]

      {:ok, data} = Data.from_rows(rows, schema)
      proto = Data.to_proto(data)

      assert length(proto) == 3

      id_field = Enum.find(proto, &(&1.field_name == "id"))
      assert id_field.type == :Int64

      title_field = Enum.find(proto, &(&1.field_name == "title"))
      assert title_field.type == :VarChar

      embedding_field = Enum.find(proto, &(&1.field_name == "embedding"))
      assert embedding_field.type == :FloatVector
    end
  end

  describe "field_names/1" do
    test "returns all field names" do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("name", 128)
          ]
        )

      {:ok, data} = Data.from_rows([%{id: 1, name: "Test"}], schema)

      names = Data.field_names(data)
      assert "id" in names
      assert "name" in names
    end
  end

  describe "with dynamic fields enabled" do
    setup do
      schema =
        Schema.build!(
          name: "test_dynamic",
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("title", 256)
          ]
        )

      {:ok, schema: schema}
    end

    test "from_rows preserves extra fields as $meta", %{schema: schema} do
      rows = [
        %{id: 1, title: "Item 1", category: "books", rating: 4.5},
        %{id: 2, title: "Item 2", category: "movies", rating: 3.0}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      assert Data.get_field(data, "$meta") == [
               %{"category" => "books", "rating" => 4.5},
               %{"category" => "movies", "rating" => 3.0}
             ]
    end

    test "from_rows handles atom keys in dynamic fields", %{schema: schema} do
      rows = [
        %{id: 1, title: "Item 1", extra_field: "value1"},
        %{id: 2, title: "Item 2", extra_field: "value2"}
      ]

      {:ok, data} = Data.from_rows(rows, schema)
      meta = Data.get_field(data, "$meta")

      assert Enum.all?(meta, fn m -> Map.has_key?(m, "extra_field") end)
    end

    test "from_rows handles string keys in dynamic fields", %{schema: schema} do
      rows = [
        %{"id" => 1, "title" => "Item 1", "extra_field" => "value1"},
        %{"id" => 2, "title" => "Item 2", "extra_field" => "value2"}
      ]

      {:ok, data} = Data.from_rows(rows, schema)
      meta = Data.get_field(data, "$meta")

      assert Enum.all?(meta, fn m -> Map.has_key?(m, "extra_field") end)
    end

    test "from_rows omits $meta when no extra fields present", %{schema: schema} do
      rows = [
        %{id: 1, title: "Item 1"},
        %{id: 2, title: "Item 2"}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      refute Map.has_key?(data.fields, "$meta")
    end

    test "from_columns preserves extra columns as $meta", %{schema: schema} do
      columns = %{
        "id" => [1, 2],
        "title" => ["Item 1", "Item 2"],
        "category" => ["books", "movies"],
        "rating" => [4.5, 3.0]
      }

      {:ok, data} = Data.from_columns(columns, schema)

      assert Data.get_field(data, "$meta") == [
               %{"category" => "books", "rating" => 4.5},
               %{"category" => "movies", "rating" => 3.0}
             ]
    end

    test "from_columns omits $meta when no extra columns", %{schema: schema} do
      columns = %{
        "id" => [1, 2],
        "title" => ["Item 1", "Item 2"]
      }

      {:ok, data} = Data.from_columns(columns, schema)

      refute Map.has_key?(data.fields, "$meta")
    end

    test "to_proto includes dynamic field with is_dynamic flag", %{schema: schema} do
      rows = [
        %{id: 1, title: "Item 1", extra: "value"}
      ]

      {:ok, data} = Data.from_rows(rows, schema)
      proto = Data.to_proto(data)

      dynamic_field = Enum.find(proto, &(&1.field_name == "$meta"))
      assert dynamic_field != nil
      assert dynamic_field.is_dynamic == true
      assert dynamic_field.type == :JSON
    end

    test "handles nested data in dynamic fields", %{schema: schema} do
      rows = [
        %{id: 1, title: "Item 1", metadata: %{tags: ["a", "b"], count: 42}}
      ]

      {:ok, data} = Data.from_rows(rows, schema)
      meta = Data.get_field(data, "$meta")

      assert [%{"metadata" => nested}] = meta
      assert nested[:tags] == ["a", "b"]
      assert nested[:count] == 42
    end
  end

  describe "without dynamic fields" do
    test "extra fields in rows are ignored" do
      schema =
        Schema.build!(
          name: "test_no_dynamic",
          enable_dynamic_field: false,
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("title", 256)
          ]
        )

      rows = [
        %{id: 1, title: "Item 1", extra: "ignored"}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      refute Map.has_key?(data.fields, "$meta")
      refute Map.has_key?(data.fields, "extra")
    end

    test "to_proto does not include $meta even if manually present in fields" do
      schema =
        Schema.build!(
          name: "test_no_dynamic",
          enable_dynamic_field: false,
          fields: [
            Field.primary_key("id", :int64),
            Field.varchar("title", 256)
          ]
        )

      data = %Data{
        fields: %{
          "id" => [1, 2],
          "title" => ["Item 1", "Item 2"],
          "$meta" => [%{"extra" => "value1"}, %{"extra" => "value2"}]
        },
        schema: schema,
        num_rows: 2
      }

      proto = Data.to_proto(data)

      assert length(proto) == 2
      field_names = Enum.map(proto, & &1.field_name)
      assert "id" in field_names
      assert "title" in field_names
      refute "$meta" in field_names
    end
  end

  describe "with is_dynamic schema fields" do
    test "from_rows routes is_dynamic field values to $meta" do
      schema =
        Schema.build!(
          name: "test_is_dynamic",
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.varchar("metadata", 512, dynamic: true),
            Field.vector("embedding", 4)
          ]
        )

      rows = [
        %{title: "Item 1", metadata: "meta1", embedding: [0.1, 0.2, 0.3, 0.4]},
        %{title: "Item 2", metadata: "meta2", embedding: [0.5, 0.6, 0.7, 0.8]}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      assert Data.get_field(data, "title") == ["Item 1", "Item 2"]
      assert Data.get_field(data, "embedding") == [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]]
      assert Data.get_field(data, "$meta") == [%{"metadata" => "meta1"}, %{"metadata" => "meta2"}]
      assert Data.get_field(data, "metadata") == nil
    end

    test "from_columns routes is_dynamic field values to $meta" do
      schema =
        Schema.build!(
          name: "test_is_dynamic_cols",
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.scalar("extra", :json, dynamic: true),
            Field.vector("embedding", 4)
          ]
        )

      columns = %{
        title: ["A", "B"],
        extra: [%{"key" => "val1"}, %{"key" => "val2"}],
        embedding: [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]]
      }

      {:ok, data} = Data.from_columns(columns, schema)

      assert Data.get_field(data, "title") == ["A", "B"]

      assert Data.get_field(data, "$meta") == [
               %{"extra" => %{"key" => "val1"}},
               %{"extra" => %{"key" => "val2"}}
             ]

      assert Data.get_field(data, "extra") == nil
    end

    test "to_proto excludes is_dynamic fields from regular field data" do
      schema =
        Schema.build!(
          name: "test_is_dynamic_proto",
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.varchar("metadata", 512, dynamic: true),
            Field.vector("embedding", 4)
          ]
        )

      rows = [
        %{title: "Item 1", metadata: "meta1", embedding: [0.1, 0.2, 0.3, 0.4]}
      ]

      {:ok, data} = Data.from_rows(rows, schema)
      proto = Data.to_proto(data)

      field_names = Enum.map(proto, & &1.field_name)
      assert "title" in field_names
      assert "embedding" in field_names
      assert "$meta" in field_names
      refute "metadata" in field_names

      meta_field = Enum.find(proto, &(&1.field_name == "$meta"))
      assert meta_field.is_dynamic == true
    end

    test "is_dynamic fields are not required for validation" do
      schema =
        Schema.build!(
          name: "test_not_required",
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.varchar("optional_meta", 512, dynamic: true),
            Field.vector("embedding", 4)
          ]
        )

      # Rows without the is_dynamic field should be valid
      rows = [
        %{title: "Item 1", embedding: [0.1, 0.2, 0.3, 0.4]}
      ]

      {:ok, data} = Data.from_rows(rows, schema)
      assert Data.get_field(data, "title") == ["Item 1"]
    end

    test "multiple is_dynamic fields route to $meta" do
      schema =
        Schema.build!(
          name: "test_multi_dynamic",
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.varchar("meta1", 256, dynamic: true),
            Field.scalar("meta2", :int64, dynamic: true),
            Field.vector("embedding", 4)
          ]
        )

      rows = [
        %{title: "Item", meta1: "val1", meta2: 42, embedding: [0.1, 0.2, 0.3, 0.4]}
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      assert Data.get_field(data, "$meta") == [%{"meta1" => "val1", "meta2" => 42}]
    end

    test "combines is_dynamic fields with enable_dynamic_field undefined fields" do
      schema =
        Schema.build!(
          name: "test_combined",
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.varchar("schema_dynamic", 256, dynamic: true),
            Field.vector("embedding", 4)
          ]
        )

      rows = [
        %{
          title: "Item",
          schema_dynamic: "from_schema",
          undefined_field: "from_row",
          embedding: [0.1, 0.2, 0.3, 0.4]
        }
      ]

      {:ok, data} = Data.from_rows(rows, schema)

      meta = Data.get_field(data, "$meta")
      assert meta == [%{"schema_dynamic" => "from_schema", "undefined_field" => "from_row"}]
    end
  end
end

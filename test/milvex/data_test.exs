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

    test "to_proto excludes auto_id fields" do
      schema =
        Schema.build!(
          name: "test",
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("name", 128)
          ]
        )

      {:ok, data} = Data.from_rows([%{name: "Test"}], schema)
      proto = Data.to_proto(data)

      assert length(proto) == 1
      assert hd(proto).field_name == "name"
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
end

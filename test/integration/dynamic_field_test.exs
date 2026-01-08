defmodule Milvex.Integration.DynamicFieldTest do
  @moduledoc """
  Integration tests for dynamic field support in Milvex.

  Tests the collection-level `enable_dynamic_field` feature which allows inserting
  fields not defined in the schema. These undefined fields are stored in a special
  `$meta` JSON field with `is_dynamic: true` flag set by Milvus internally.

  Also tests the per-field `is_dynamic` option which routes schema field data
  to the `$meta` dynamic field, allowing declarative definition of fields that
  should be stored dynamically.
  """
  use Milvex.IntegrationCase, async: false

  @moduletag :integration

  describe "enable_dynamic_field at collection level" do
    test "creates collection with dynamic field support enabled", %{conn: conn} do
      name = unique_collection_name("dyn_enabled")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      assert :ok = Milvex.create_collection(conn, name, schema)
      assert {:ok, true} = Milvex.has_collection(conn, name)

      {:ok, info} = Milvex.describe_collection(conn, name)
      assert info.schema.enable_dynamic_field == true
    end

    test "describes collection returns is_dynamic flag on dynamic field", %{conn: conn} do
      name = unique_collection_name("dyn_describe")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      {:ok, info} = Milvex.describe_collection(conn, name)

      dynamic_field = Enum.find(info.schema.fields, & &1.is_dynamic)

      if dynamic_field != nil do
        assert dynamic_field.data_type == :json
        assert dynamic_field.is_dynamic == true
      end
    end
  end

  describe "insert with dynamic fields" do
    test "inserts rows with undefined fields when enable_dynamic_field is true", %{conn: conn} do
      name = unique_collection_name("ins_dyn")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = [
        %{title: "Item 1", embedding: random_vector(4), category: "books", rating: 4.5},
        %{title: "Item 2", embedding: random_vector(4), category: "movies", rating: 3.0}
      ]

      assert {:ok, result} = Milvex.insert(conn, name, rows)
      assert result.insert_count == 2
    end

    test "inserts rows with nested dynamic field values", %{conn: conn} do
      name = unique_collection_name("ins_nested_dyn")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: true),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = [
        %{
          title: "Item 1",
          embedding: random_vector(4),
          metadata: %{"author" => "John", "year" => 2024}
        },
        %{
          title: "Item 2",
          embedding: random_vector(4),
          metadata: %{"author" => "Jane", "year" => 2025}
        }
      ]

      assert {:ok, result} = Milvex.insert(conn, name, rows)
      assert result.insert_count == 2
    end

    test "inserts with mixed static and dynamic fields", %{conn: conn} do
      name = unique_collection_name("ins_mixed")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.scalar("count", :int32),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = [
        %{id: 1, title: "Static", count: 10, embedding: random_vector(4), extra: "dynamic value"},
        %{id: 2, title: "Mixed", count: 20, embedding: random_vector(4), extra: "another value"}
      ]

      assert {:ok, result} = Milvex.insert(conn, name, rows)
      assert result.insert_count == 2
    end
  end

  describe "query with dynamic fields" do
    test "queries dynamic fields after insert", %{conn: conn} do
      name = unique_collection_name("query_dyn")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      rows = [
        %{id: 1, title: "Item 1", embedding: random_vector(4), category: "books"},
        %{id: 2, title: "Item 2", embedding: random_vector(4), category: "movies"}
      ]

      {:ok, _} = Milvex.insert(conn, name, rows)
      :ok = Milvex.load_collection(conn, name)

      assert_eventually(
        match?(
          {:ok, %{rows: [%{"$meta" => %{"category" => "books"}} | _]}},
          Milvex.query(conn, name, "id == 1", output_fields: ["title", "category"])
        )
      )
    end

    test "filters by dynamic fields in query expression", %{conn: conn} do
      name = unique_collection_name("filter_dyn")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      rows = [
        %{id: 1, title: "Book 1", embedding: random_vector(4), category: "books", rating: 4.5},
        %{id: 2, title: "Movie 1", embedding: random_vector(4), category: "movies", rating: 3.0},
        %{id: 3, title: "Book 2", embedding: random_vector(4), category: "books", rating: 5.0}
      ]

      {:ok, _} = Milvex.insert(conn, name, rows)
      :ok = Milvex.load_collection(conn, name)

      assert_eventually(fn ->
        case Milvex.query(conn, name, "category == \"books\"",
               output_fields: ["title", "category", "rating"]
             ) do
          {:ok, %{rows: result_rows}} when length(result_rows) == 2 ->
            result_rows
            |> Enum.all?(fn row ->
              row["$meta"]["category"] == "books" and
                row["title"] in ["Book 1", "Book 2"] and
                row["$meta"]["rating"] in [4.5, 5.0]
            end)

          _ ->
            false
        end
      end)
    end
  end

  describe "search with dynamic fields" do
    test "searches with dynamic field filter", %{conn: conn} do
      name = unique_collection_name("search_dyn")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      rows = [
        %{id: 1, title: "Item 1", embedding: [1.0, 0.0, 0.0, 0.0], category: "A"},
        %{id: 2, title: "Item 2", embedding: [0.9, 0.1, 0.0, 0.0], category: "A"},
        %{id: 3, title: "Item 3", embedding: [0.8, 0.2, 0.0, 0.0], category: "B"}
      ]

      {:ok, _} = Milvex.insert(conn, name, rows)
      :ok = Milvex.load_collection(conn, name)

      query_vector = [1.0, 0.0, 0.0, 0.0]

      assert_eventually(fn ->
        case Milvex.search(conn, name, [query_vector],
               vector_field: "embedding",
               top_k: 10,
               filter: "category == \"A\"",
               output_fields: ["title", "category"]
             ) do
          {:ok, %{hits: [result_hits | _]}} when length(result_hits) == 2 ->
            result_hits
            |> Enum.all?(fn hit ->
              hit.fields["$meta"]["category"] == "A" and
                hit.fields["title"] in ["Item 1", "Item 2"] and
                is_float(hit.distance)
            end)

          _ ->
            false
        end
      end)
    end
  end

  describe "is_dynamic field routing" do
    test "inserts data with is_dynamic schema fields routed to $meta", %{conn: conn} do
      name = unique_collection_name("is_dyn_route")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.varchar("metadata", 512, dynamic: true),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      rows = [
        %{id: 1, title: "Item 1", metadata: "category=books", embedding: random_vector(4)},
        %{id: 2, title: "Item 2", metadata: "category=movies", embedding: random_vector(4)}
      ]

      assert {:ok, result} = Milvex.insert(conn, name, rows)
      assert result.insert_count == 2

      :ok = Milvex.load_collection(conn, name)

      assert_eventually(fn ->
        case Milvex.query(conn, name, "id == 1", output_fields: ["title", "metadata"]) do
          {:ok, %{rows: [row | _]}} ->
            row["title"] == "Item 1" and row["$meta"]["metadata"] == "category=books"

          _ ->
            false
        end
      end)
    end

    test "combines is_dynamic fields with undefined dynamic fields", %{conn: conn} do
      name = unique_collection_name("combined_dyn")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.scalar("schema_dynamic", :int64, dynamic: true),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      :ok =
        Milvex.create_index(conn, name, "embedding",
          index_type: "AUTOINDEX",
          metric_type: "COSINE"
        )

      rows = [
        %{
          id: 1,
          title: "Item",
          schema_dynamic: 42,
          undefined_field: "extra",
          embedding: random_vector(4)
        }
      ]

      {:ok, _} = Milvex.insert(conn, name, rows)
      :ok = Milvex.load_collection(conn, name)

      assert_eventually(fn ->
        case Milvex.query(conn, name, "id == 1",
               output_fields: ["title", "schema_dynamic", "undefined_field"]
             ) do
          {:ok, %{rows: [row | _]}} ->
            row["title"] == "Item" and
              row["$meta"]["schema_dynamic"] == 42 and
              row["$meta"]["undefined_field"] == "extra"

          _ ->
            false
        end
      end)
    end
  end

  describe "Data module dynamic field handling" do
    test "from_rows correctly separates dynamic fields", %{conn: conn} do
      name = unique_collection_name("data_sep")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = [
        %{id: 1, title: "Test", embedding: [0.1, 0.2, 0.3, 0.4], extra1: "val1", extra2: 123}
      ]

      data = Data.from_rows!(rows, schema)

      assert Data.get_field(data, "id") == [1]
      assert Data.get_field(data, "title") == ["Test"]
      assert Data.get_field(data, "embedding") == [[0.1, 0.2, 0.3, 0.4]]
      assert Data.get_field(data, "$meta") == [%{"extra1" => "val1", "extra2" => 123}]
    end

    test "from_columns correctly separates dynamic fields", %{conn: conn} do
      name = unique_collection_name("data_col_sep")

      schema =
        Schema.build!(
          name: name,
          enable_dynamic_field: true,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      columns = %{
        id: [1, 2],
        title: ["A", "B"],
        embedding: [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8]],
        extra: ["x", "y"]
      }

      data = Data.from_columns!(columns, schema)

      assert Data.get_field(data, "$meta") == [%{"extra" => "x"}, %{"extra" => "y"}]
    end
  end
end

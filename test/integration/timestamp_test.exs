defmodule Milvex.Integration.TimestampTest do
  use Milvex.IntegrationCase, async: false

  @moduletag :integration

  describe "timestamp field support" do
    test "creates collection with timestamp field", %{conn: conn} do
      name = unique_collection_name("timestamp_create")

      schema =
        Schema.build!(
          name: name,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.timestamp("created_at"),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      assert :ok = Milvex.create_collection(conn, name, schema)
      assert {:ok, info} = Milvex.describe_collection(conn, name)
      field_names = Enum.map(info.schema.fields, & &1.name)
      assert "created_at" in field_names
    end

    test "inserts data with DateTime values", %{conn: conn} do
      name = unique_collection_name("timestamp_datetime")

      schema =
        Schema.build!(
          name: name,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.timestamp("created_at"),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = [
        %{
          id: 1,
          title: "Item 1",
          created_at: ~U[2025-01-01 00:00:00Z],
          embedding: random_vector(4)
        },
        %{
          id: 2,
          title: "Item 2",
          created_at: ~U[2025-06-15 12:30:00Z],
          embedding: random_vector(4)
        }
      ]

      assert {:ok, result} = Milvex.insert(conn, name, rows)
      assert result.insert_count == 2
    end

    test "inserts data with ISO 8601 strings", %{conn: conn} do
      name = unique_collection_name("timestamp_iso")

      schema =
        Schema.build!(
          name: name,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.timestamp("created_at"),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = [
        %{
          id: 1,
          title: "Item 1",
          created_at: "2025-01-01T00:00:00Z",
          embedding: random_vector(4)
        },
        %{
          id: 2,
          title: "Item 2",
          created_at: "2025-05-01T23:59:59+08:00",
          embedding: random_vector(4)
        }
      ]

      assert {:ok, result} = Milvex.insert(conn, name, rows)
      assert result.insert_count == 2
    end

    test "queries and retrieves timestamp values", %{conn: conn} do
      name = unique_collection_name("timestamp_query")

      schema =
        Schema.build!(
          name: name,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.timestamp("created_at"),
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

      timestamp1 = ~U[2025-01-01 00:00:00.000000Z]
      timestamp2 = ~U[2025-06-15 12:30:45.000000Z]

      rows = [
        %{id: 1, title: "Item 1", created_at: timestamp1, embedding: random_vector(4)},
        %{id: 2, title: "Item 2", created_at: timestamp2, embedding: random_vector(4)}
      ]

      {:ok, _} = Milvex.insert(conn, name, rows)
      :ok = Milvex.load_collection(conn, name)

      assert_eventually(fn ->
        case Milvex.query(conn, name, "id == 1", output_fields: ["title", "created_at"]) do
          {:ok, %{rows: [row | _]}} ->
            row["created_at"] == timestamp1

          _ ->
            false
        end
      end)
    end

    test "filters by timestamp comparison", %{conn: conn} do
      name = unique_collection_name("timestamp_filter")

      schema =
        Schema.build!(
          name: name,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.timestamp("created_at"),
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
          title: "Old Item",
          created_at: ~U[2024-01-01 00:00:00Z],
          embedding: random_vector(4)
        },
        %{
          id: 2,
          title: "Recent Item",
          created_at: ~U[2025-06-01 00:00:00Z],
          embedding: random_vector(4)
        },
        %{
          id: 3,
          title: "New Item",
          created_at: ~U[2025-12-01 00:00:00Z],
          embedding: random_vector(4)
        }
      ]

      {:ok, _} = Milvex.insert(conn, name, rows)
      :ok = Milvex.load_collection(conn, name)

      assert_eventually(fn ->
        case Milvex.query(conn, name, "created_at > ISO '2025-01-01T00:00:00Z'",
               output_fields: ["id", "title", "created_at"]
             ) do
          {:ok, %{rows: result_rows}} when length(result_rows) == 2 ->
            Enum.all?([2, 3], fn id ->
              Enum.any?(result_rows, &(&1["id"] == id))
            end)

          _ ->
            false
        end
      end)
    end

    test "nullable timestamp field", %{conn: conn} do
      name = unique_collection_name("timestamp_nullable")

      schema =
        Schema.build!(
          name: name,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.timestamp("updated_at", nullable: true),
            Field.vector("embedding", 4)
          ]
        )

      on_exit(fn -> cleanup_collection(conn, name) end)

      :ok = Milvex.create_collection(conn, name, schema)

      rows = [
        %{
          id: 1,
          title: "With timestamp",
          updated_at: ~U[2025-01-01 00:00:00Z],
          embedding: random_vector(4)
        },
        %{id: 2, title: "Without timestamp", updated_at: nil, embedding: random_vector(4)}
      ]

      assert {:ok, result} = Milvex.insert(conn, name, rows)
      assert result.insert_count == 2
    end

    test "searches with timestamp filter", %{conn: conn} do
      name = unique_collection_name("timestamp_search")

      schema =
        Schema.build!(
          name: name,
          fields: [
            Field.primary_key("id", :int64, auto_id: false),
            Field.varchar("title", 256),
            Field.timestamp("created_at"),
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
          title: "Item 1",
          created_at: ~U[2024-01-01 00:00:00Z],
          embedding: [1.0, 0.0, 0.0, 0.0]
        },
        %{
          id: 2,
          title: "Item 2",
          created_at: ~U[2025-06-01 00:00:00Z],
          embedding: [0.9, 0.1, 0.0, 0.0]
        },
        %{
          id: 3,
          title: "Item 3",
          created_at: ~U[2025-12-01 00:00:00Z],
          embedding: [0.8, 0.2, 0.0, 0.0]
        }
      ]

      {:ok, _} = Milvex.insert(conn, name, rows)
      :ok = Milvex.load_collection(conn, name)

      query_vector = [1.0, 0.0, 0.0, 0.0]

      assert_eventually(fn ->
        case Milvex.search(conn, name, [query_vector],
               vector_field: "embedding",
               top_k: 10,
               filter: "created_at >= ISO '2025-01-01T00:00:00Z'",
               output_fields: ["title", "created_at"]
             ) do
          {:ok, %{hits: [result_hits | _]}} when length(result_hits) == 2 ->
            Enum.all?([2, 3], fn id ->
              Enum.any?(result_hits, &(&1.id == id))
            end)

          _ ->
            false
        end
      end)
    end
  end
end

defmodule Milvex.QueryResultTest do
  use ExUnit.Case, async: true

  alias Milvex.Milvus.Proto.Milvus.QueryResults
  alias Milvex.QueryResult

  alias Milvex.Milvus.Proto.Schema.FieldData
  alias Milvex.Milvus.Proto.Schema.FloatArray
  alias Milvex.Milvus.Proto.Schema.LongArray
  alias Milvex.Milvus.Proto.Schema.ScalarField
  alias Milvex.Milvus.Proto.Schema.StringArray
  alias Milvex.Milvus.Proto.Schema.VectorField

  describe "from_proto/1" do
    test "parses empty result" do
      proto = %QueryResults{
        fields_data: [],
        collection_name: "test_collection",
        output_fields: [],
        primary_field_name: ""
      }

      result = QueryResult.from_proto(proto)

      assert result.rows == []
      assert result.collection_name == "test_collection"
      assert result.output_fields == []
      assert result.primary_field_name == nil
    end

    test "parses scalar fields into rows" do
      proto = %QueryResults{
        fields_data: [
          %FieldData{
            field_name: "id",
            type: :Int64,
            field: {:scalars, %ScalarField{data: {:long_data, %LongArray{data: [1, 2, 3]}}}}
          },
          %FieldData{
            field_name: "name",
            type: :VarChar,
            field:
              {:scalars,
               %ScalarField{data: {:string_data, %StringArray{data: ["Alice", "Bob", "Charlie"]}}}}
          }
        ],
        collection_name: "users",
        output_fields: ["id", "name"],
        primary_field_name: "id"
      }

      result = QueryResult.from_proto(proto)

      assert length(result.rows) == 3
      assert result.collection_name == "users"
      assert result.output_fields == ["id", "name"]
      assert result.primary_field_name == "id"

      assert Enum.at(result.rows, 0) == %{"id" => 1, "name" => "Alice"}
      assert Enum.at(result.rows, 1) == %{"id" => 2, "name" => "Bob"}
      assert Enum.at(result.rows, 2) == %{"id" => 3, "name" => "Charlie"}
    end

    test "parses vector fields into rows" do
      proto = %QueryResults{
        fields_data: [
          %FieldData{
            field_name: "id",
            type: :Int64,
            field: {:scalars, %ScalarField{data: {:long_data, %LongArray{data: [1, 2]}}}}
          },
          %FieldData{
            field_name: "embedding",
            type: :FloatVector,
            field:
              {:vectors,
               %VectorField{
                 dim: 4,
                 data:
                   {:float_vector, %FloatArray{data: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]}}
               }}
          }
        ],
        collection_name: "vectors",
        output_fields: ["id", "embedding"],
        primary_field_name: "id"
      }

      result = QueryResult.from_proto(proto)

      assert length(result.rows) == 2
      assert Enum.at(result.rows, 0)["embedding"] == [0.1, 0.2, 0.3, 0.4]
      assert Enum.at(result.rows, 1)["embedding"] == [0.5, 0.6, 0.7, 0.8]
    end

    test "handles single row" do
      proto = %QueryResults{
        fields_data: [
          %FieldData{
            field_name: "count",
            type: :Int64,
            field: {:scalars, %ScalarField{data: {:long_data, %LongArray{data: [42]}}}}
          }
        ],
        collection_name: "stats",
        output_fields: ["count"],
        primary_field_name: ""
      }

      result = QueryResult.from_proto(proto)

      assert result.rows == [%{"count" => 42}]
    end
  end

  describe "num_rows/1" do
    test "returns count of rows" do
      result =
        QueryResult.from_proto(%QueryResults{
          fields_data: [
            %FieldData{
              field_name: "id",
              type: :Int64,
              field:
                {:scalars, %ScalarField{data: {:long_data, %LongArray{data: [1, 2, 3, 4, 5]}}}}
            }
          ],
          collection_name: "test",
          output_fields: ["id"],
          primary_field_name: "id"
        })

      assert QueryResult.num_rows(result) == 5
    end

    test "returns 0 for empty result" do
      result =
        QueryResult.from_proto(%QueryResults{
          fields_data: [],
          collection_name: "test",
          output_fields: [],
          primary_field_name: ""
        })

      assert QueryResult.num_rows(result) == 0
    end
  end

  describe "get_row/2" do
    setup do
      result =
        QueryResult.from_proto(%QueryResults{
          fields_data: [
            %FieldData{
              field_name: "id",
              type: :Int64,
              field: {:scalars, %ScalarField{data: {:long_data, %LongArray{data: [10, 20, 30]}}}}
            },
            %FieldData{
              field_name: "value",
              type: :VarChar,
              field:
                {:scalars,
                 %ScalarField{data: {:string_data, %StringArray{data: ["a", "b", "c"]}}}}
            }
          ],
          collection_name: "test",
          output_fields: ["id", "value"],
          primary_field_name: "id"
        })

      {:ok, result: result}
    end

    test "returns row at index", %{result: result} do
      assert QueryResult.get_row(result, 0) == %{"id" => 10, "value" => "a"}
      assert QueryResult.get_row(result, 1) == %{"id" => 20, "value" => "b"}
      assert QueryResult.get_row(result, 2) == %{"id" => 30, "value" => "c"}
    end

    test "returns nil for out of bounds index", %{result: result} do
      assert QueryResult.get_row(result, 10) == nil
    end
  end

  describe "get_column/2" do
    setup do
      result =
        QueryResult.from_proto(%QueryResults{
          fields_data: [
            %FieldData{
              field_name: "id",
              type: :Int64,
              field: {:scalars, %ScalarField{data: {:long_data, %LongArray{data: [1, 2, 3]}}}}
            },
            %FieldData{
              field_name: "score",
              type: :Float,
              field:
                {:scalars, %ScalarField{data: {:float_data, %FloatArray{data: [1.5, 2.5, 3.5]}}}}
            }
          ],
          collection_name: "test",
          output_fields: ["id", "score"],
          primary_field_name: "id"
        })

      {:ok, result: result}
    end

    test "returns all values for a field", %{result: result} do
      assert QueryResult.get_column(result, "id") == [1, 2, 3]
      assert QueryResult.get_column(result, "score") == [1.5, 2.5, 3.5]
    end

    test "accepts atom field names", %{result: result} do
      assert QueryResult.get_column(result, :id) == [1, 2, 3]
    end

    test "returns nils for unknown field", %{result: result} do
      assert QueryResult.get_column(result, "unknown") == [nil, nil, nil]
    end
  end

  describe "empty?/1" do
    test "returns true for empty result" do
      result =
        QueryResult.from_proto(%QueryResults{
          fields_data: [],
          collection_name: "test",
          output_fields: [],
          primary_field_name: ""
        })

      assert QueryResult.empty?(result)
    end

    test "returns false for non-empty result" do
      result =
        QueryResult.from_proto(%QueryResults{
          fields_data: [
            %FieldData{
              field_name: "id",
              type: :Int64,
              field: {:scalars, %ScalarField{data: {:long_data, %LongArray{data: [1]}}}}
            }
          ],
          collection_name: "test",
          output_fields: ["id"],
          primary_field_name: "id"
        })

      refute QueryResult.empty?(result)
    end
  end
end

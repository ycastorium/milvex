defmodule Milvex.SearchResultTest do
  use ExUnit.Case, async: true

  alias Milvex.Milvus.Proto.Milvus.SearchResults
  alias Milvex.SearchResult
  alias Milvex.SearchResult.Hit

  alias Milvex.Milvus.Proto.Schema.FieldData
  alias Milvex.Milvus.Proto.Schema.FloatArray
  alias Milvex.Milvus.Proto.Schema.IDs
  alias Milvex.Milvus.Proto.Schema.LongArray
  alias Milvex.Milvus.Proto.Schema.ScalarField
  alias Milvex.Milvus.Proto.Schema.SearchResultData
  alias Milvex.Milvus.Proto.Schema.StringArray
  alias Milvex.Milvus.Proto.Schema.VectorField

  describe "from_proto/1" do
    test "parses nil results" do
      proto = %SearchResults{
        results: nil,
        collection_name: "test_collection"
      }

      result = SearchResult.from_proto(proto)

      assert result.num_queries == 0
      assert result.top_k == 0
      assert result.hits == []
      assert result.collection_name == "test_collection"
    end

    test "parses empty results" do
      proto = %SearchResults{
        results: %SearchResultData{
          num_queries: 1,
          top_k: 10,
          ids: nil,
          scores: [],
          distances: [],
          topks: [0],
          fields_data: [],
          output_fields: []
        },
        collection_name: "test"
      }

      result = SearchResult.from_proto(proto)

      assert result.num_queries == 1
      assert result.top_k == 10
      assert result.hits == [[]]
    end

    test "parses single query results with int64 ids" do
      proto = %SearchResults{
        results: %SearchResultData{
          num_queries: 1,
          top_k: 3,
          ids: %IDs{id_field: {:int_id, %LongArray{data: [100, 200, 300]}}},
          scores: [0.95, 0.85, 0.75],
          distances: [0.05, 0.15, 0.25],
          topks: [3],
          fields_data: [],
          output_fields: []
        },
        collection_name: "vectors"
      }

      result = SearchResult.from_proto(proto)

      assert result.num_queries == 1
      assert result.top_k == 3
      assert length(result.hits) == 1

      [query_hits] = result.hits
      assert length(query_hits) == 3

      [hit1, hit2, hit3] = query_hits
      assert hit1.id == 100
      assert hit1.score == 0.95
      assert hit1.distance == 0.05

      assert hit2.id == 200
      assert hit2.score == 0.85

      assert hit3.id == 300
    end

    test "parses single query results with string ids" do
      proto = %SearchResults{
        results: %SearchResultData{
          num_queries: 1,
          top_k: 2,
          ids: %IDs{id_field: {:str_id, %StringArray{data: ["pk-001", "pk-002"]}}},
          scores: [0.9, 0.8],
          distances: [],
          topks: [2],
          fields_data: [],
          output_fields: []
        },
        collection_name: "test"
      }

      result = SearchResult.from_proto(proto)

      [query_hits] = result.hits
      assert hd(query_hits).id == "pk-001"
      assert hd(query_hits).distance == nil
    end

    test "parses multiple query results" do
      proto = %SearchResults{
        results: %SearchResultData{
          num_queries: 3,
          top_k: 2,
          ids: %IDs{id_field: {:int_id, %LongArray{data: [1, 2, 10, 20, 100, 200]}}},
          scores: [0.9, 0.8, 0.95, 0.85, 0.7, 0.6],
          distances: [],
          topks: [2, 2, 2],
          fields_data: [],
          output_fields: []
        },
        collection_name: "multi"
      }

      result = SearchResult.from_proto(proto)

      assert result.num_queries == 3
      assert length(result.hits) == 3

      [q1_hits, q2_hits, q3_hits] = result.hits

      assert length(q1_hits) == 2
      assert Enum.map(q1_hits, & &1.id) == [1, 2]

      assert length(q2_hits) == 2
      assert Enum.map(q2_hits, & &1.id) == [10, 20]

      assert length(q3_hits) == 2
      assert Enum.map(q3_hits, & &1.id) == [100, 200]
    end

    test "parses results with varying topks per query" do
      proto = %SearchResults{
        results: %SearchResultData{
          num_queries: 2,
          top_k: 5,
          ids: %IDs{id_field: {:int_id, %LongArray{data: [1, 2, 3, 10, 20]}}},
          scores: [0.9, 0.8, 0.7, 0.95, 0.85],
          distances: [],
          topks: [3, 2],
          fields_data: [],
          output_fields: []
        },
        collection_name: "varying"
      }

      result = SearchResult.from_proto(proto)

      [q1, q2] = result.hits
      assert length(q1) == 3
      assert length(q2) == 2
    end

    test "parses results with output fields" do
      proto = %SearchResults{
        results: %SearchResultData{
          num_queries: 1,
          top_k: 2,
          ids: %IDs{id_field: {:int_id, %LongArray{data: [1, 2]}}},
          scores: [0.9, 0.8],
          distances: [],
          topks: [2],
          fields_data: [
            %FieldData{
              field_name: "title",
              type: :VarChar,
              field:
                {:scalars,
                 %ScalarField{data: {:string_data, %StringArray{data: ["Doc 1", "Doc 2"]}}}}
            },
            %FieldData{
              field_name: "score",
              type: :Float,
              field: {:scalars, %ScalarField{data: {:float_data, %FloatArray{data: [1.5, 2.5]}}}}
            }
          ],
          output_fields: ["title", "score"]
        },
        collection_name: "docs"
      }

      result = SearchResult.from_proto(proto)

      [hits] = result.hits
      [hit1, hit2] = hits

      assert hit1.fields["title"] == "Doc 1"
      assert hit1.fields["score"] == 1.5

      assert hit2.fields["title"] == "Doc 2"
      assert hit2.fields["score"] == 2.5
    end

    test "parses results with vector output fields" do
      proto = %SearchResults{
        results: %SearchResultData{
          num_queries: 1,
          top_k: 2,
          ids: %IDs{id_field: {:int_id, %LongArray{data: [1, 2]}}},
          scores: [0.9, 0.8],
          distances: [],
          topks: [2],
          fields_data: [
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
          output_fields: ["embedding"]
        },
        collection_name: "vectors"
      }

      result = SearchResult.from_proto(proto)

      [hits] = result.hits
      assert hd(hits).fields["embedding"] == [0.1, 0.2, 0.3, 0.4]
      assert List.last(hits).fields["embedding"] == [0.5, 0.6, 0.7, 0.8]
    end
  end

  describe "total_hits/1" do
    test "counts all hits across queries" do
      result =
        SearchResult.from_proto(%SearchResults{
          results: %SearchResultData{
            num_queries: 2,
            top_k: 3,
            ids: %IDs{id_field: {:int_id, %LongArray{data: [1, 2, 3, 10, 20]}}},
            scores: [0.9, 0.8, 0.7, 0.95, 0.85],
            distances: [],
            topks: [3, 2],
            fields_data: [],
            output_fields: []
          },
          collection_name: "test"
        })

      assert SearchResult.total_hits(result) == 5
    end

    test "returns 0 for empty results" do
      result =
        SearchResult.from_proto(%SearchResults{
          results: nil,
          collection_name: "test"
        })

      assert SearchResult.total_hits(result) == 0
    end
  end

  describe "get_query_hits/2" do
    setup do
      result =
        SearchResult.from_proto(%SearchResults{
          results: %SearchResultData{
            num_queries: 2,
            top_k: 2,
            ids: %IDs{id_field: {:int_id, %LongArray{data: [1, 2, 10, 20]}}},
            scores: [0.9, 0.8, 0.95, 0.85],
            distances: [],
            topks: [2, 2],
            fields_data: [],
            output_fields: []
          },
          collection_name: "test"
        })

      {:ok, result: result}
    end

    test "returns hits for query index", %{result: result} do
      q0_hits = SearchResult.get_query_hits(result, 0)
      assert length(q0_hits) == 2
      assert Enum.map(q0_hits, & &1.id) == [1, 2]

      q1_hits = SearchResult.get_query_hits(result, 1)
      assert Enum.map(q1_hits, & &1.id) == [10, 20]
    end

    test "returns empty list for out of bounds query", %{result: result} do
      assert SearchResult.get_query_hits(result, 10) == []
    end
  end

  describe "top_hits/1" do
    test "returns first hit for each query" do
      result =
        SearchResult.from_proto(%SearchResults{
          results: %SearchResultData{
            num_queries: 3,
            top_k: 2,
            ids: %IDs{id_field: {:int_id, %LongArray{data: [1, 2, 10, 20, 100, 200]}}},
            scores: [0.9, 0.8, 0.95, 0.85, 0.7, 0.6],
            distances: [],
            topks: [2, 2, 2],
            fields_data: [],
            output_fields: []
          },
          collection_name: "test"
        })

      top = SearchResult.top_hits(result)
      assert length(top) == 3
      assert Enum.map(top, & &1.id) == [1, 10, 100]
    end

    test "returns nil for empty query groups" do
      result =
        SearchResult.from_proto(%SearchResults{
          results: %SearchResultData{
            num_queries: 2,
            top_k: 2,
            ids: %IDs{id_field: {:int_id, %LongArray{data: [1, 2]}}},
            scores: [0.9, 0.8],
            distances: [],
            topks: [2, 0],
            fields_data: [],
            output_fields: []
          },
          collection_name: "test"
        })

      top = SearchResult.top_hits(result)
      assert length(top) == 2
      assert hd(top).id == 1
      assert List.last(top) == nil
    end
  end

  describe "empty?/1" do
    test "returns true for nil results" do
      result =
        SearchResult.from_proto(%SearchResults{
          results: nil,
          collection_name: "test"
        })

      assert SearchResult.empty?(result)
    end

    test "returns true for empty hits" do
      result =
        SearchResult.from_proto(%SearchResults{
          results: %SearchResultData{
            num_queries: 1,
            top_k: 10,
            ids: nil,
            scores: [],
            distances: [],
            topks: [0],
            fields_data: [],
            output_fields: []
          },
          collection_name: "test"
        })

      assert SearchResult.empty?(result)
    end

    test "returns false when hits exist" do
      result =
        SearchResult.from_proto(%SearchResults{
          results: %SearchResultData{
            num_queries: 1,
            top_k: 1,
            ids: %IDs{id_field: {:int_id, %LongArray{data: [1]}}},
            scores: [0.9],
            distances: [],
            topks: [1],
            fields_data: [],
            output_fields: []
          },
          collection_name: "test"
        })

      refute SearchResult.empty?(result)
    end
  end

  describe "Hit struct" do
    test "has expected fields" do
      hit = %Hit{
        id: 123,
        score: 0.95,
        distance: 0.05,
        fields: %{"title" => "Test"}
      }

      assert hit.id == 123
      assert hit.score == 0.95
      assert hit.distance == 0.05
      assert hit.fields["title"] == "Test"
    end
  end
end

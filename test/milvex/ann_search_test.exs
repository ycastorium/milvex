defmodule Milvex.AnnSearchTest do
  use ExUnit.Case, async: true

  alias Milvex.AnnSearch

  describe "new/3 with vector data" do
    test "returns {:ok, search} with valid params" do
      {:ok, search} = AnnSearch.new("embedding", [[0.1, 0.2, 0.3]], limit: 10)

      assert search.anns_field == "embedding"
      assert search.data == [[0.1, 0.2, 0.3]]
      assert search.limit == 10
      assert search.params == nil
      assert search.expr == nil
    end

    test "returns {:ok, search} with all options" do
      {:ok, search} =
        AnnSearch.new("embedding", [[0.1, 0.2]],
          limit: 5,
          params: %{nprobe: 10},
          expr: "category == 'test'"
        )

      assert search.limit == 5
      assert search.params == %{nprobe: 10}
      assert search.expr == "category == 'test'"
    end

    test "returns {:error, _} when anns_field is empty" do
      assert {:error, error} = AnnSearch.new("", [[0.1, 0.2]], limit: 10)
      assert error.field == :anns_field
    end

    test "returns {:error, _} when data is empty list" do
      assert {:error, error} = AnnSearch.new("embedding", [], limit: 10)
      assert error.field == :data
    end

    test "returns {:error, _} when limit is missing" do
      assert {:error, error} = AnnSearch.new("embedding", [[0.1, 0.2]], [])
      assert error.field == :limit
    end

    test "returns {:error, _} when limit is zero" do
      assert {:error, error} = AnnSearch.new("embedding", [[0.1, 0.2]], limit: 0)
      assert error.field == :limit
    end

    test "returns {:error, _} when limit is negative" do
      assert {:error, error} = AnnSearch.new("embedding", [[0.1, 0.2]], limit: -1)
      assert error.field == :limit
    end
  end

  describe "new/3 with text data (BM25)" do
    test "returns {:ok, search} with text query" do
      {:ok, search} = AnnSearch.new("text_sparse", ["search query"], limit: 10)

      assert search.anns_field == "text_sparse"
      assert search.data == ["search query"]
      assert search.limit == 10
    end

    test "returns {:ok, search} with multiple text queries" do
      {:ok, search} = AnnSearch.new("text_sparse", ["query1", "query2"], limit: 5)

      assert search.data == ["query1", "query2"]
    end
  end

  describe "new/3 with mixed data" do
    test "returns {:error, _} when data mixes vectors and strings" do
      assert {:error, error} = AnnSearch.new("embedding", [[0.1, 0.2], "text"], limit: 10)
      assert error.field == :data
    end
  end
end

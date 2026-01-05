defmodule Milvex.HybridSearchTest do
  use ExUnit.Case, async: true
  use Mimic

  alias Milvex.AnnSearch
  alias Milvex.Connection
  alias Milvex.Ranker

  setup :verify_on_exit!

  describe "hybrid_search/5 validation" do
    test "returns {:error, _} when searches is empty" do
      {:ok, ranker} = Ranker.rrf()
      assert {:error, error} = Milvex.hybrid_search(:conn, "collection", [], ranker)
      assert error.field == :searches
    end

    test "returns {:error, _} when weight count doesn't match search count" do
      {:ok, search1} = AnnSearch.new("field1", [[0.1, 0.2]], limit: 10)
      {:ok, search2} = AnnSearch.new("field2", [[0.3, 0.4]], limit: 10)
      {:ok, ranker} = Ranker.weighted([0.5])

      assert {:error, error} =
               Milvex.hybrid_search(:conn, "collection", [search1, search2], ranker)

      assert error.field == :weights
    end

    test "accepts matching weight count and search count" do
      {:ok, search1} = AnnSearch.new("field1", [[0.1, 0.2]], limit: 10)
      {:ok, search2} = AnnSearch.new("field2", [[0.3, 0.4]], limit: 10)
      {:ok, ranker} = Ranker.weighted([0.7, 0.3])

      stub(Connection, :get_channel, fn _ -> {:error, :not_connected} end)

      assert {:error, _} = Milvex.hybrid_search(:conn, "collection", [search1, search2], ranker)
    end

    test "RRF ranker doesn't require weight matching" do
      {:ok, search1} = AnnSearch.new("field1", [[0.1, 0.2]], limit: 10)
      {:ok, search2} = AnnSearch.new("field2", [[0.3, 0.4]], limit: 10)
      {:ok, ranker} = Ranker.rrf()

      stub(Connection, :get_channel, fn _ -> {:error, :not_connected} end)

      assert {:error, _} = Milvex.hybrid_search(:conn, "collection", [search1, search2], ranker)
    end
  end
end

defmodule Milvex.RankerTest do
  use ExUnit.Case, async: true

  alias Milvex.Ranker
  alias Milvex.Ranker.RRFRanker
  alias Milvex.Ranker.WeightedRanker

  describe "weighted/1" do
    test "returns {:ok, ranker} with valid weights" do
      assert {:ok, %WeightedRanker{weights: [0.7, 0.3]}} = Ranker.weighted([0.7, 0.3])
    end

    test "returns {:ok, ranker} with single weight" do
      assert {:ok, %WeightedRanker{weights: [1.0]}} = Ranker.weighted([1.0])
    end

    test "returns {:error, _} with empty list" do
      assert {:error, error} = Ranker.weighted([])
      assert error.field == :weights
    end

    test "returns {:error, _} with non-list" do
      assert {:error, error} = Ranker.weighted(0.5)
      assert error.field == :weights
    end
  end

  describe "rrf/1" do
    test "returns {:ok, ranker} with default k" do
      assert {:ok, %RRFRanker{k: 60}} = Ranker.rrf()
    end

    test "returns {:ok, ranker} with custom k" do
      assert {:ok, %RRFRanker{k: 100}} = Ranker.rrf(k: 100)
    end

    test "returns {:error, _} with invalid k" do
      assert {:error, error} = Ranker.rrf(k: 0)
      assert error.field == :k
    end

    test "returns {:error, _} with negative k" do
      assert {:error, error} = Ranker.rrf(k: -10)
      assert error.field == :k
    end
  end
end

defmodule Milvex.Ranker.WeightedRankerTest do
  use ExUnit.Case, async: true

  alias Milvex.Ranker.WeightedRanker

  describe "struct" do
    test "has weights field" do
      ranker = %WeightedRanker{weights: [0.7, 0.3]}
      assert ranker.weights == [0.7, 0.3]
    end
  end
end

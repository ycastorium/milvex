defmodule Milvex.Ranker.RRFRankerTest do
  use ExUnit.Case, async: true

  alias Milvex.Ranker.RRFRanker

  describe "struct" do
    test "has k field with default 60" do
      ranker = %RRFRanker{}
      assert ranker.k == 60
    end

    test "accepts custom k value" do
      ranker = %RRFRanker{k: 100}
      assert ranker.k == 100
    end
  end
end

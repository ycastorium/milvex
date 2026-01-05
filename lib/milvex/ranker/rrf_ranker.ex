defmodule Milvex.Ranker.RRFRanker do
  @moduledoc """
  Reciprocal Rank Fusion (RRF) reranker for hybrid search.

  Merges results from multiple searches, favoring items that appear consistently.
  The `k` parameter (default: 60) controls the ranking smoothness.
  """

  @type t :: %__MODULE__{
          k: pos_integer()
        }

  defstruct k: 60
end

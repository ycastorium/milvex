defmodule Milvex.Ranker.WeightedRanker do
  @moduledoc """
  Weighted scoring reranker for hybrid search.

  Assigns weights to each sub-search, combining results using weighted average scoring.
  """

  @type t :: %__MODULE__{
          weights: [float()]
        }

  defstruct [:weights]
end

defmodule Milvex.Ranker do
  @moduledoc """
  Builder functions for hybrid search rerankers.

  Provides two reranking strategies:
  - `weighted/1` - Weighted average scoring
  - `rrf/1` - Reciprocal Rank Fusion

  ## Examples

      {:ok, ranker} = Ranker.weighted([0.7, 0.3])
      {:ok, ranker} = Ranker.rrf(k: 60)
  """

  alias Milvex.Errors.Invalid
  alias Milvex.Ranker.RRFRanker
  alias Milvex.Ranker.WeightedRanker

  @doc """
  Creates a weighted ranker with the given weights.

  Each weight corresponds to a sub-search in the hybrid search.
  The number of weights must match the number of searches.

  ## Examples

      {:ok, ranker} = Ranker.weighted([0.8, 0.2])
  """
  @spec weighted([number()]) :: {:ok, WeightedRanker.t()} | {:error, Invalid.t()}
  def weighted([_ | _] = weights) do
    if Enum.all?(weights, &is_number/1) do
      {:ok, %WeightedRanker{weights: weights}}
    else
      {:error, Invalid.exception(field: :weights, message: "all weights must be numbers")}
    end
  end

  def weighted(_) do
    {:error, Invalid.exception(field: :weights, message: "must be a non-empty list of numbers")}
  end

  @doc """
  Creates an RRF (Reciprocal Rank Fusion) ranker.

  ## Options

    - `:k` - Smoothness parameter (default: 60, must be positive)

  ## Examples

      {:ok, ranker} = Ranker.rrf()
      {:ok, ranker} = Ranker.rrf(k: 100)
  """
  @spec rrf(keyword()) :: {:ok, RRFRanker.t()} | {:error, Invalid.t()}
  def rrf(opts \\ []) do
    k = Keyword.get(opts, :k, 60)

    if is_integer(k) and k > 0 do
      {:ok, %RRFRanker{k: k}}
    else
      {:error, Invalid.exception(field: :k, message: "must be a positive integer")}
    end
  end
end

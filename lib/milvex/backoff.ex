defmodule Milvex.Backoff do
  @moduledoc """
  Exponential backoff calculator with jitter support.

  Provides delay calculation for connection retry logic to prevent
  thundering herd problems when multiple clients reconnect simultaneously.
  """

  @doc """
  Calculates the next delay using exponential backoff with optional jitter.

  The formula is: `min(base_delay * multiplier^attempt, max_delay) +/- jitter`

  ## Parameters

    - `attempt` - Current retry attempt number (0-based, first retry is attempt 0)
    - `base_delay` - Base delay in milliseconds
    - `max_delay` - Maximum delay cap in milliseconds
    - `multiplier` - Exponential multiplier (default: 2.0)
    - `jitter` - Jitter factor 0.0-1.0 (default: 0.1)

  ## Examples

      iex> Milvex.Backoff.calculate(0, 1000, 60000, 2.0, 0.0)
      1000

      iex> Milvex.Backoff.calculate(3, 1000, 60000, 2.0, 0.0)
      8000

      iex> Milvex.Backoff.calculate(10, 1000, 60000, 2.0, 0.0)
      60000
  """
  @spec calculate(
          non_neg_integer(),
          pos_integer(),
          pos_integer(),
          float(),
          float()
        ) :: pos_integer()
  def calculate(attempt, base_delay, max_delay, multiplier \\ 2.0, jitter \\ 0.1)
      when is_integer(attempt) and attempt >= 0 and
             is_integer(base_delay) and base_delay > 0 and
             is_integer(max_delay) and max_delay > 0 and
             is_float(multiplier) and multiplier >= 1.0 and
             is_float(jitter) and jitter >= 0.0 and jitter <= 1.0 do
    delay = base_delay * :math.pow(multiplier, attempt)
    capped_delay = min(delay, max_delay)

    capped_delay
    |> apply_jitter(jitter)
    |> round()
    |> max(1)
  end

  defp apply_jitter(delay, +0.0), do: delay

  defp apply_jitter(delay, jitter) do
    jitter_range = delay * jitter
    offset = :rand.uniform() * jitter_range * 2 - jitter_range
    delay + offset
  end
end

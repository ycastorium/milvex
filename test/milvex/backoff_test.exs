defmodule Milvex.BackoffTest do
  use ExUnit.Case, async: true

  alias Milvex.Backoff

  describe "calculate/5" do
    test "returns base delay for first attempt (attempt 0)" do
      delay = Backoff.calculate(0, 1000, 60_000, 2.0, 0.0)

      assert delay == 1000
    end

    test "doubles delay for each subsequent attempt" do
      assert Backoff.calculate(1, 1000, 60_000, 2.0, 0.0) == 2000
      assert Backoff.calculate(2, 1000, 60_000, 2.0, 0.0) == 4000
      assert Backoff.calculate(3, 1000, 60_000, 2.0, 0.0) == 8000
      assert Backoff.calculate(4, 1000, 60_000, 2.0, 0.0) == 16_000
    end

    test "caps delay at max_delay" do
      delay = Backoff.calculate(10, 1000, 60_000, 2.0, 0.0)

      assert delay == 60_000
    end

    test "handles large attempt numbers without overflow" do
      delay = Backoff.calculate(100, 1000, 60_000, 2.0, 0.0)

      assert delay == 60_000
    end

    test "uses custom multiplier" do
      assert Backoff.calculate(1, 1000, 60_000, 1.5, 0.0) == 1500
      assert Backoff.calculate(2, 1000, 60_000, 1.5, 0.0) == 2250
    end

    test "applies jitter within expected range" do
      base_delay = 1000
      jitter = 0.1

      delays =
        for _ <- 1..100 do
          Backoff.calculate(0, base_delay, 60_000, 2.0, jitter)
        end

      min_expected = round(base_delay * (1 - jitter))
      max_expected = round(base_delay * (1 + jitter))

      for delay <- delays do
        assert delay >= min_expected,
               "delay #{delay} should be >= #{min_expected}"

        assert delay <= max_expected,
               "delay #{delay} should be <= #{max_expected}"
      end
    end

    test "jitter produces variance in results" do
      delays =
        for _ <- 1..20 do
          Backoff.calculate(0, 10_000, 60_000, 2.0, 0.1)
        end

      unique_delays = Enum.uniq(delays)
      assert length(unique_delays) > 1, "jitter should produce different values"
    end

    test "zero jitter produces deterministic results" do
      delays =
        for _ <- 1..10 do
          Backoff.calculate(0, 1000, 60_000, 2.0, 0.0)
        end

      assert Enum.all?(delays, &(&1 == 1000))
    end

    test "always returns positive integer" do
      delay = Backoff.calculate(0, 1, 100, 2.0, 0.1)

      assert is_integer(delay)
      assert delay >= 1
    end

    test "works with small base delays" do
      delay = Backoff.calculate(0, 100, 60_000, 2.0, 0.0)

      assert delay == 100
    end

    test "works with conservative defaults from config" do
      delay = Backoff.calculate(5, 1000, 60_000, 2.0, 0.1)

      assert delay >= round(32_000 * 0.9)
      assert delay <= round(32_000 * 1.1)
    end
  end
end

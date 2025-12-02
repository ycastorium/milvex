defmodule Milvex.MixProject do
  use Mix.Project

  def project do
    [
      app: :milvex,
      version: "0.1.0",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      mod: {Milvex.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:assert_eventually, "~> 1.0", only: :test},
      {:benchee, "~> 1.0", only: :dev},
      {:castore, "~> 1.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:gen_state_machine, "~> 3.0"},
      {:grpc, "~> 0.11.5"},
      {:jason, "~> 1.4"},
      {:mimic, "~> 2.2", only: :test},
      {:nx, "~> 0.10", optional: true},
      {:protobuf, "~> 0.15.0"},
      {:splode, "~> 0.2.9"},
      {:testcontainers, "~> 1.13", only: [:test, :dev]},
      {:zoi, "~> 0.11"}
    ]
  end

  defp aliases do
    [
      "bench.field_data": ["run bench/field_data_bench.exs"],
      "bench.data": ["run bench/data_bench.exs"],
      "bench.result": ["run bench/result_bench.exs"],
      "bench.all": [
        "run bench/field_data_bench.exs",
        "run bench/data_bench.exs",
        "run bench/result_bench.exs"
      ]
    ]
  end
end

defmodule Milvex.MixProject do
  use Mix.Project

  def project do
    [
      app: :milvex,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Milvex.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:castore, "~> 1.0"},
      {:gen_state_machine, "~> 3.0"},
      {:grpc, "~> 0.11.5"},
      {:protobuf, "~> 0.15.0"},
      {:splode, "~> 0.2.9"},
      {:zoi, "~> 0.11"},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end
end

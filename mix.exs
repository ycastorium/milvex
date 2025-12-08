defmodule Milvex.MixProject do
  use Mix.Project

  def project do
    [
      app: :milvex,
      version: "0.3.0",
      description: description(),
      package: package(),
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
      start_permanent: Mix.env() == :prod,
      dialyzer: [
        plt_core_path: "_plts/core"
      ],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.cobertura": :test
      ],
      docs: [
        main: "readme",
        extras: [
          "README.md": [title: "Introduction"],
          "guides/getting-started.md": [title: "Getting Started"],
          "guides/architecture.md": [title: "Architecture"],
          "guides/error-handling.md": [title: "Error Handling"],
          "CHANGELOG.md": [title: "Changelog"],
          LICENSE: [title: "License"]
        ],
        groups_for_extras: [
          Guides: [
            "guides/getting-started.md",
            "guides/architecture.md",
            "guides/error-handling.md"
          ],
          About: [
            "README.md",
            "CHANGELOG.md",
            "LICENSE"
          ]
        ],
        groups_for_modules: [
          "Client API": [
            Milvex,
            Milvex.Connection
          ],
          "Data Builders": [
            Milvex.Schema,
            Milvex.Schema.Field,
            Milvex.Data,
            Milvex.Data.FieldData,
            Milvex.Index
          ],
          Results: [
            Milvex.SearchResult,
            Milvex.QueryResult
          ],
          Errors: [
            Milvex.Error,
            Milvex.Errors.Connection,
            Milvex.Errors.Grpc,
            Milvex.Errors.Invalid,
            Milvex.Errors.Unknown
          ],
          Configuration: [
            Milvex.Config,
            Milvex.Backoff,
            Milvex.Application
          ],
          Internal: [
            Milvex.RPC
          ],
          "Generated Proto": ~r/Milvex\.Milvus\.Proto\./
        ],
        nest_modules_by_prefix: [
          Milvex.Schema,
          Milvex.Data,
          Milvex.Errors,
          Milvex.Milvus.Proto
        ]
      ],
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
      {:doctor, "~> 0.22.0", only: :dev},
      {:ex_check, "~> 0.16", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: [:dev, :test]},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:gen_state_machine, "~> 3.0"},
      {:grpc, "~> 0.11.5"},
      {:jason, "~> 1.4"},
      {:mimic, "~> 2.2", only: :test},
      {:mix_audit, ">= 0.0.0", only: [:dev, :test], runtime: false},
      {:nx, "~> 0.10", optional: true},
      {:protobuf, "~> 0.15.0"},
      {:recode, "~> 0.8.0", only: [:dev], runtime: false},
      {:spark, "~> 2.3"},
      {:splode, "~> 0.2.9"},
      {:testcontainers, "~> 1.13", only: [:test, :dev]},
      {:zoi, "~> 0.11"}
    ]
  end

  defp aliases do
    [
      test: ["test --exclude integration"],
      "test.integration": ["test --only integration"],
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

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.post": :test,
        "coveralls.github": :test,
        "coveralls.html": :test,
        "test.integration": :test
      ]
    ]
  end

  defp description() do
    "An Elixir client for Milvus, the open-source vector database."
  end

  defp package() do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/ycastorium/milvex.git"},
      sponsor: "ycastor.eth"
    ]
  end
end

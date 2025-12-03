defmodule Milvex.Config do
  @config_schema Zoi.object(
                   %{
                     host:
                       Zoi.string(description: "Milvus server hostname or IP address")
                       |> Zoi.optional()
                       |> Zoi.default("localhost"),
                     port:
                       Zoi.integer(description: "Milvus server port (1-65535)")
                       |> Zoi.min(1)
                       |> Zoi.max(65_535)
                       |> Zoi.optional(),
                     database:
                       Zoi.string(description: "Database name to connect to")
                       |> Zoi.optional()
                       |> Zoi.default("default"),
                     user:
                       Zoi.string(description: "Username for authentication")
                       |> Zoi.optional(),
                     password:
                       Zoi.string(description: "Password for authentication")
                       |> Zoi.optional(),
                     token:
                       Zoi.string(
                         description:
                           "API token for authentication (alternative to user/password)"
                       )
                       |> Zoi.optional(),
                     timeout:
                       Zoi.integer(description: "Connection timeout in milliseconds")
                       |> Zoi.min(1000)
                       |> Zoi.optional()
                       |> Zoi.default(30_000),
                     ssl:
                       Zoi.boolean(description: "Enable SSL/TLS encryption")
                       |> Zoi.optional()
                       |> Zoi.default(nil),
                     ssl_options:
                       Zoi.any(description: "SSL options passed to the underlying transport")
                       |> Zoi.optional()
                       |> Zoi.default([]),
                     reconnect_base_delay:
                       Zoi.integer(
                         description: "Base delay for reconnection attempts in milliseconds"
                       )
                       |> Zoi.min(100)
                       |> Zoi.max(60_000)
                       |> Zoi.optional()
                       |> Zoi.default(1_000),
                     reconnect_max_delay:
                       Zoi.integer(
                         description: "Maximum delay for reconnection attempts in milliseconds"
                       )
                       |> Zoi.min(1_000)
                       |> Zoi.max(300_000)
                       |> Zoi.optional()
                       |> Zoi.default(60_000),
                     reconnect_multiplier:
                       Zoi.float(description: "Multiplier for exponential backoff")
                       |> Zoi.min(1.0)
                       |> Zoi.max(10.0)
                       |> Zoi.optional()
                       |> Zoi.default(2.0),
                     reconnect_jitter:
                       Zoi.float(description: "Jitter factor (0.0 to 1.0) to randomize delay")
                       |> Zoi.min(0.0)
                       |> Zoi.max(1.0)
                       |> Zoi.optional()
                       |> Zoi.default(0.1),
                     health_check_interval:
                       Zoi.integer(description: "Health check interval in milliseconds")
                       |> Zoi.min(1_000)
                       |> Zoi.optional()
                       |> Zoi.default(30_000)
                   },
                   description: "Configuration options for connecting to a Milvus server",
                   example: %{
                     host: "localhost",
                     port: 19_530,
                     database: "default",
                     timeout: 30_000
                   },
                   metadata: [
                     moduledoc: """
                     Configuration module for Milvus client connections.

                     Handles parsing and validation of connection parameters from keyword lists,
                     maps, or URI strings. Supports various authentication methods including
                     username/password and API tokens.
                     """
                   ]
                 )

  @moduledoc """
  #{Zoi.metadata(@config_schema)[:moduledoc]}
  """

  @type t :: unquote(Zoi.type_spec(@config_schema))

  @doc """
  Returns the configuration schema.

  #{Zoi.description(@config_schema)}

  ## Options

  #{Zoi.describe(@config_schema)}

  ## Example

      iex> Zoi.example(Milvex.Config.schema())
      #{inspect(Zoi.example(@config_schema))}
  """
  def schema, do: @config_schema

  @doc """
  Parse and validate configuration from keyword list or map.

  Returns `{:ok, config}` on success or `{:error, error}` on validation failure.

  ## Examples

      iex> Milvex.Config.parse(host: "localhost", port: 19530)
      {:ok, %{host: "localhost", port: 19530, ...}}

      iex> Milvex.Config.parse(port: -1)
      {:error, %Milvex.Errors.Invalid{...}}
  """
  @spec parse(keyword() | map()) :: {:ok, t()} | {:error, Milvex.Error.t()}
  def parse(config) when is_list(config) do
    config
    |> Enum.into(%{})
    |> parse()
  end

  def parse(config) when is_map(config) do
    config =
      config
      |> normalize_keys()
      |> parse_host_url()

    case Zoi.parse(@config_schema, config) do
      {:ok, validated} ->
        {:ok, finalize_config(validated)}

      {:error, errors} ->
        {:error,
         Milvex.Errors.Invalid.exception(
           field: "config",
           message: Zoi.prettify_errors(errors),
           context: %{errors: errors}
         )}
    end
  end

  @doc """
  Parse configuration from URI string.

  Supports formats:
  - `http://localhost:19530`
  - `https://user:pass@host:19530/database`
  - `milvus://host:19530?timeout=60000`

  ## Examples

      iex> Milvex.Config.parse_uri("http://localhost:19530")
      {:ok, %{host: "localhost", port: 19530, ssl: false, ...}}

      iex> Milvex.Config.parse_uri("https://user:pass@milvus.example.com:443/mydb")
      {:ok, %{host: "milvus.example.com", port: 443, ssl: true, user: "user", ...}}
  """
  @spec parse_uri(String.t()) :: {:ok, t()} | {:error, Milvex.Error.t()}
  def parse_uri(uri) when is_binary(uri) do
    case URI.parse(uri) do
      %URI{host: nil} ->
        {:error,
         Milvex.Errors.Invalid.exception(
           field: "uri",
           message: "Invalid URI format: missing host"
         )}

      %URI{host: ""} ->
        {:error,
         Milvex.Errors.Invalid.exception(
           field: "uri",
           message: "Invalid URI format: empty host"
         )}

      %URI{} = parsed ->
        config = uri_to_config(parsed)
        parse(config)
    end
  end

  defp uri_to_config(%URI{} = uri) do
    base_config = %{
      host: uri.host,
      ssl: uri.scheme in ["https", "milvuss"]
    }

    base_config
    |> maybe_add_port(uri)
    |> maybe_add_auth(uri)
    |> maybe_add_database(uri)
    |> maybe_add_query_params(uri)
  end

  defp maybe_add_port(config, %URI{port: nil}), do: config
  defp maybe_add_port(config, %URI{port: port}), do: Map.put(config, :port, port)

  defp maybe_add_auth(config, %URI{userinfo: nil}), do: config

  defp maybe_add_auth(config, %URI{userinfo: userinfo}) do
    case String.split(userinfo, ":", parts: 2) do
      [user, password] -> Map.merge(config, %{user: user, password: password})
      [user] -> Map.put(config, :user, user)
    end
  end

  defp maybe_add_database(config, %URI{path: nil}), do: config
  defp maybe_add_database(config, %URI{path: ""}), do: config
  defp maybe_add_database(config, %URI{path: "/"}), do: config

  defp maybe_add_database(config, %URI{path: "/" <> database}) when database != "" do
    Map.put(config, :database, database)
  end

  defp maybe_add_query_params(config, %URI{query: nil}), do: config

  defp maybe_add_query_params(config, %URI{query: query}) do
    params = URI.decode_query(query)

    config
    |> maybe_add_timeout(params)
    |> maybe_add_token(params)
  end

  defp maybe_add_timeout(config, %{"timeout" => timeout}) do
    case Integer.parse(timeout) do
      {int_timeout, ""} -> Map.put(config, :timeout, int_timeout)
      _ -> config
    end
  end

  defp maybe_add_timeout(config, _), do: config

  defp maybe_add_token(config, %{"token" => token}), do: Map.put(config, :token, token)
  defp maybe_add_token(config, _), do: config

  defp normalize_keys(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) -> {String.to_existing_atom(k), v}
      {k, v} when is_atom(k) -> {k, v}
    end)
  rescue
    ArgumentError -> map
  end

  defp parse_host_url(%{host: "https://" <> rest} = config) do
    {host, port} = parse_host_and_port(rest)

    config
    |> Map.put(:host, host)
    |> Map.put(:ssl, Map.get(config, :ssl, true))
    |> maybe_put_port(port)
  end

  defp parse_host_url(%{host: "http://" <> rest} = config) do
    {host, port} = parse_host_and_port(rest)

    config
    |> Map.put(:host, host)
    |> Map.put(:ssl, Map.get(config, :ssl, false))
    |> maybe_put_port(port)
  end

  defp parse_host_url(config), do: config

  defp parse_host_and_port(host_string) do
    case String.split(host_string, ":", parts: 2) do
      [host, port_str] ->
        case Integer.parse(port_str) do
          {port, ""} -> {host, port}
          _ -> {host_string, nil}
        end

      [host] ->
        {host, nil}
    end
  end

  defp maybe_put_port(config, nil), do: config
  defp maybe_put_port(config, port), do: Map.put_new(config, :port, port)

  defp finalize_config(config) do
    config
    |> apply_ssl_defaults()
    |> apply_default_port()
  end

  defp apply_ssl_defaults(%{ssl: true, ssl_options: []} = config) do
    %{config | ssl_options: default_ssl_options()}
  end

  defp apply_ssl_defaults(%{ssl: nil} = config) do
    %{config | ssl: false}
  end

  defp apply_ssl_defaults(config), do: config

  defp apply_default_port(%{ssl: true} = config) do
    Map.put_new(config, :port, 443)
  end

  defp apply_default_port(config) do
    Map.put_new(config, :port, 19_530)
  end

  @doc """
  Returns the default SSL options for secure connections.
  """
  @spec default_ssl_options() :: keyword()
  def default_ssl_options do
    [
      verify: :verify_peer,
      depth: 99,
      cacert_file: CAStore.file_path()
    ]
  end
end

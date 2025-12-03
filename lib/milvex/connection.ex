defmodule Milvex.Connection do
  @moduledoc """
  State machine managing gRPC channel lifecycle and health monitoring.

  Each connection maintains a gRPC channel to a Milvus server and provides
  health monitoring with automatic reconnection on failure.

  ## States

  - `:connecting` - Attempting to establish initial connection
  - `:connected` - Channel active, health checks running
  - `:reconnecting` - Lost connection, attempting to restore

  ## Usage

      # Start a connection
      {:ok, conn} = Milvex.Connection.start_link(host: "localhost", port: 19530)

      # Get the gRPC channel for making calls
      {:ok, channel} = Milvex.Connection.get_channel(conn)

      # Perform a health check
      :ok = Milvex.Connection.health_check(conn)

      # Disconnect
      :ok = Milvex.Connection.disconnect(conn)

  ## Named Connections

      # Start a named connection
      {:ok, _} = Milvex.Connection.start_link([host: "localhost"], name: :milvus)

      # Use the named connection
      {:ok, channel} = Milvex.Connection.get_channel(:milvus)
  """

  use GenStateMachine, callback_mode: [:state_functions, :state_enter]

  require Logger

  alias Milvex.Config
  alias Milvex.Milvus.Proto.Milvus.CheckHealthRequest
  alias Milvex.Milvus.Proto.Milvus.MilvusService

  @health_check_interval 30_000
  @reconnect_delay 5_000

  defstruct [:config, :channel, :health_timer]

  @type t :: %__MODULE__{
          config: Config.t(),
          channel: GRPC.Channel.t() | nil,
          health_timer: reference() | nil
        }

  @type state :: :connecting | :connected | :reconnecting

  @doc """
  Starts a connection to a Milvus server.

  ## Options

  - `:name` - Optional name to register the connection process
  - All other options are passed to `Milvex.Config.parse/1`

  ## Examples

      {:ok, conn} = Milvex.Connection.start_link(host: "localhost", port: 19530)
      {:ok, conn} = Milvex.Connection.start_link([host: "localhost"], name: :milvus)
  """
  @spec start_link(keyword()) :: GenStateMachine.on_start()
  def start_link(opts) do
    {name, config_opts} = Keyword.pop(opts, :name)

    gen_opts = if name, do: [name: name], else: []
    GenStateMachine.start_link(__MODULE__, config_opts, gen_opts)
  end

  @doc """
  Gets the gRPC channel from the connection.

  Returns `{:ok, channel}` if connected, or `{:error, error}` if not connected.
  """
  @spec get_channel(GenServer.server()) ::
          {:ok, GRPC.Channel.t()} | {:error, Milvex.Error.t()}
  def get_channel(conn) do
    GenStateMachine.call(conn, :get_channel)
  end

  @doc """
  Disconnects from the Milvus server and stops the connection process.
  """
  @spec disconnect(GenServer.server()) :: :ok
  def disconnect(conn) do
    GenStateMachine.stop(conn, :normal)
  end

  @doc """
  Performs a health check against the Milvus server.

  Returns `:ok` if the server is healthy, or `{:error, error}` otherwise.
  """
  @spec health_check(GenServer.server()) :: :ok | {:error, Milvex.Error.t()}
  def health_check(conn) do
    GenStateMachine.call(conn, :health_check)
  end

  @doc """
  Checks if the connection is currently established.
  """
  @spec connected?(GenServer.server()) :: boolean()
  def connected?(conn) do
    GenStateMachine.call(conn, :connected?)
  end

  @impl true
  def init(config_opts) do
    case Config.parse(config_opts) do
      {:ok, config} ->
        data = %__MODULE__{
          config: config,
          channel: nil,
          health_timer: nil
        }

        {:ok, :connecting, data}

      {:error, error} ->
        {:stop, error}
    end
  end

  # --- :connecting state ---

  def connecting(:enter, _old_state, _data) do
    {:keep_state_and_data, [{:state_timeout, 0, :connect}]}
  end

  def connecting(:state_timeout, :connect, data) do
    case establish_connection(data.config) do
      {:ok, channel} ->
        {:next_state, :connected, %{data | channel: channel}}

      {:error, reason} ->
        Logger.warning("Failed to connect to Milvus: #{inspect(reason)}, retrying...")
        {:keep_state_and_data, [{:state_timeout, @reconnect_delay, :retry}]}
    end
  end

  def connecting(:state_timeout, :retry, data) do
    case establish_connection(data.config) do
      {:ok, channel} ->
        {:next_state, :connected, %{data | channel: channel}}

      {:error, reason} ->
        Logger.warning("Connection retry failed: #{inspect(reason)}, retrying...")
        {:keep_state_and_data, [{:state_timeout, @reconnect_delay, :retry}]}
    end
  end

  def connecting({:call, from}, :get_channel, data) do
    {:keep_state_and_data, [{:reply, from, not_connected_error(data)}]}
  end

  def connecting({:call, from}, :health_check, data) do
    {:keep_state_and_data, [{:reply, from, not_connected_error(data)}]}
  end

  def connecting({:call, from}, :connected?, _data) do
    {:keep_state_and_data, [{:reply, from, false}]}
  end

  # --- :connected state ---

  def connected(:enter, _old_state, data) do
    timer = schedule_health_check()
    {:keep_state, %{data | health_timer: timer}}
  end

  def connected({:call, from}, :get_channel, data) do
    {:keep_state_and_data, [{:reply, from, {:ok, data.channel}}]}
  end

  def connected({:call, from}, :health_check, data) do
    result = perform_health_check(data.channel)
    {:keep_state_and_data, [{:reply, from, result}]}
  end

  def connected({:call, from}, :connected?, _data) do
    {:keep_state_and_data, [{:reply, from, true}]}
  end

  def connected(:info, :health_check, data) do
    case perform_health_check(data.channel) do
      :ok ->
        timer = schedule_health_check()
        {:keep_state, %{data | health_timer: timer}}

      {:error, _reason} ->
        Logger.warning("Health check failed, reconnecting...")
        close_channel(data.channel)
        {:next_state, :reconnecting, %{data | channel: nil, health_timer: nil}}
    end
  end

  # --- :reconnecting state ---

  def reconnecting(:enter, _old_state, _data) do
    {:keep_state_and_data, [{:state_timeout, 0, :reconnect}]}
  end

  def reconnecting(:state_timeout, :reconnect, data), do: reconnect(data)

  def reconnecting(:state_timeout, :retry, data), do: reconnect(data)

  def reconnecting({:call, from}, :get_channel, data) do
    {:keep_state_and_data, [{:reply, from, not_connected_error(data)}]}
  end

  def reconnecting({:call, from}, :health_check, data) do
    {:keep_state_and_data, [{:reply, from, not_connected_error(data)}]}
  end

  def reconnecting({:call, from}, :connected?, _data) do
    {:keep_state_and_data, [{:reply, from, false}]}
  end

  # --- Termination ---

  @impl true
  def terminate(_reason, _state, data) do
    if data.channel do
      close_channel(data.channel)
    end

    if data.health_timer do
      Process.cancel_timer(data.health_timer)
    end

    :ok
  end

  # --- Private helpers ---
  #
  defp reconnect(data) do
    case establish_connection(data.config) do
      {:ok, channel} ->
        Logger.info("Reconnected...")
        {:next_state, :connected, %{data | channel: channel}}

      {:error, reason} ->
        Logger.warning("Reconnection failed: #{inspect(reason)}, retrying...")
        {:keep_state_and_data, [{:state_timeout, @reconnect_delay, :retry}]}
    end
  end

  defp not_connected_error(data) do
    {:error,
     Milvex.Errors.Connection.exception(
       reason: :not_connected,
       host: data.config.host,
       port: data.config.port,
       retriable: true
     )}
  end

  defp establish_connection(config) do
    address = "#{config.host}:#{config.port}"
    opts = build_connection_opts(config)

    case GRPC.Stub.connect(address, opts) do
      {:ok, channel} ->
        {:ok, channel}

      {:error, reason} ->
        {:error,
         Milvex.Errors.Connection.exception(
           reason: reason,
           host: config.host,
           port: config.port,
           retriable: true
         )}
    end
  end

  defp build_connection_opts(config) do
    []
    |> maybe_add_ssl(config)
    |> maybe_add_auth_headers(config)
  end

  defp maybe_add_ssl(opts, %{ssl: true, ssl_options: ssl_options}) do
    cred = GRPC.Credential.new(ssl: ssl_options)
    Keyword.put(opts, :cred, cred)
  end

  defp maybe_add_ssl(opts, _config), do: opts

  defp maybe_add_auth_headers(opts, config) do
    headers = Keyword.get(opts, :headers, [])

    headers =
      case Map.get(config, :token) do
        nil -> headers
        token -> [{"authorization", Base.encode64(token)} | headers]
      end

    headers =
      case Map.get(config, :database) do
        nil -> headers
        "default" -> headers
        db -> [{"dbname", db} | headers]
      end

    if headers == [] do
      opts
    else
      Keyword.put(opts, :headers, headers)
    end
  end

  defp perform_health_check(channel) do
    request = %CheckHealthRequest{}

    case MilvusService.Stub.check_health(channel, request, timeout: 5_000) do
      {:ok, response} ->
        if response.isHealthy do
          :ok
        else
          {:error,
           Milvex.Errors.Grpc.exception(
             operation: "CheckHealth",
             code: :unhealthy,
             message: "Server reported unhealthy status"
           )}
        end

      {:error, %GRPC.RPCError{} = error} ->
        {:error,
         Milvex.Errors.Grpc.exception(
           operation: "CheckHealth",
           code: error.status,
           message: error.message || "Health check failed"
         )}

      {:error, reason} ->
        {:error,
         Milvex.Errors.Connection.exception(
           reason: reason,
           retriable: true
         )}
    end
  end

  defp close_channel(channel) do
    GRPC.Stub.disconnect(channel)
  rescue
    _ -> :ok
  end

  defp schedule_health_check do
    Process.send_after(self(), :health_check, @health_check_interval)
  end
end

defmodule Milvex.MilvusContainer do
  @moduledoc false

  alias Testcontainers.Container
  alias Testcontainers.CommandWaitStrategy
  alias Testcontainers.LogWaitStrategy

  alias DockerEngineAPI.Api.Network, as: NetworkApi
  alias DockerEngineAPI.Api.Container, as: ContainerApi
  alias DockerEngineAPI.Model.NetworkCreateRequest

  @etcd_image "quay.io/coreos/etcd:v3.5.18"
  @minio_image "minio/minio:RELEASE.2024-12-18T13-15-44Z"
  @milvus_image "milvusdb/milvus:v2.6.6"

  @grpc_port 19_530

  defstruct [:network_id, :network_name, :etcd, :minio, :milvus]

  def start do
    with {:ok, _} <- Testcontainers.start_link(),
         {:ok, network_id, network_name} <- create_network(),
         {:ok, etcd} <- start_etcd(network_id),
         etcd_ip <- get_container_ip(etcd.container_id, network_name),
         {:ok, minio} <- start_minio(network_id),
         minio_ip <- get_container_ip(minio.container_id, network_name),
         {:ok, milvus} <- start_milvus(network_id, etcd_ip, minio_ip) do
      {:ok,
       %__MODULE__{
         network_id: network_id,
         network_name: network_name,
         etcd: etcd,
         minio: minio,
         milvus: milvus
       }}
    end
  end

  def stop(%__MODULE__{} = cluster) do
    Testcontainers.stop_container(cluster.milvus.container_id)
    Testcontainers.stop_container(cluster.minio.container_id)
    Testcontainers.stop_container(cluster.etcd.container_id)
    delete_network(cluster.network_id)
    :ok
  end

  def connection_config(%__MODULE__{milvus: milvus}) do
    host = Testcontainers.get_host()
    port = Container.mapped_port(milvus, @grpc_port)
    [host: host, port: port]
  end

  defp create_network do
    conn = get_docker_connection()
    network_name = "milvex-test-#{:erlang.unique_integer([:positive])}"

    request = %NetworkCreateRequest{
      Name: network_name,
      Driver: "bridge",
      CheckDuplicate: true
    }

    case NetworkApi.network_create(conn, request) do
      {:ok, %{Id: id}} -> {:ok, id, network_name}
      {:ok, %DockerEngineAPI.Model.NetworkCreateResponse{Id: id}} -> {:ok, id, network_name}
      error -> error
    end
  end

  defp get_container_ip(container_id, network_name) do
    conn = get_docker_connection()
    {:ok, inspect_result} = ContainerApi.container_inspect(conn, container_id)

    inspect_result
    |> Map.get(:NetworkSettings)
    |> Map.get(:Networks)
    |> Map.get(network_name)
    |> Map.get(:IPAddress)
  end

  defp delete_network(network_id) do
    conn = get_docker_connection()
    NetworkApi.network_delete(conn, network_id)
  end

  defp start_etcd(network_id) do
    container =
      Container.new(@etcd_image)
      |> Container.with_exposed_port(2379)
      |> Container.with_network_mode(network_id)
      |> Container.with_environment("ETCD_AUTO_COMPACTION_MODE", "revision")
      |> Container.with_environment("ETCD_AUTO_COMPACTION_RETENTION", "1000")
      |> Container.with_environment("ETCD_QUOTA_BACKEND_BYTES", "4294967296")
      |> Container.with_environment("ETCD_SNAPSHOT_COUNT", "50000")
      |> Container.with_cmd([
        "etcd",
        "-advertise-client-urls=http://0.0.0.0:2379",
        "-listen-client-urls=http://0.0.0.0:2379",
        "--data-dir=/etcd"
      ])
      |> Container.with_waiting_strategy(
        CommandWaitStrategy.new(["etcdctl", "endpoint", "health"], 60_000)
      )

    Testcontainers.start_container(container)
  end

  defp start_minio(network_id) do
    container =
      Container.new(@minio_image)
      |> Container.with_exposed_port(9000)
      |> Container.with_network_mode(network_id)
      |> Container.with_environment("MINIO_ACCESS_KEY", "minioadmin")
      |> Container.with_environment("MINIO_SECRET_KEY", "minioadmin")
      |> Container.with_environment("MINIO_REGION", "us-east-1")
      |> Container.with_cmd(["minio", "server", "/minio_data", "--console-address", ":9001"])
      |> Container.with_waiting_strategy(LogWaitStrategy.new(~r/API:/, 60_000))

    Testcontainers.start_container(container)
  end

  defp start_milvus(network_id, etcd_ip, minio_ip) do
    container =
      Container.new(@milvus_image)
      |> Container.with_exposed_port(@grpc_port)
      |> Container.with_exposed_port(9091)
      |> Container.with_network_mode(network_id)
      |> Container.with_cmd(["milvus", "run", "standalone"])
      |> Container.with_environment("ETCD_ENDPOINTS", "#{etcd_ip}:2379")
      |> Container.with_environment("MINIO_ADDRESS", "#{minio_ip}:9000")
      |> Container.with_environment("MINIO_REGION", "us-east-1")
      |> Container.with_environment("MQ_TYPE", "woodpecker")
      |> Container.with_waiting_strategy(
        CommandWaitStrategy.new(
          ["curl", "-f", "http://localhost:9091/healthz"],
          180_000
        )
      )

    Testcontainers.start_container(container)
  end

  defp get_docker_connection do
    {conn, _, _} = Testcontainers.Connection.get_connection([])
    conn
  end
end

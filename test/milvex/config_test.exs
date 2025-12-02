defmodule Milvex.ConfigTest do
  use ExUnit.Case, async: true

  alias Milvex.Config

  describe "parse/1 with keyword list" do
    test "uses defaults when no options provided" do
      {:ok, config} = Config.parse([])

      assert config.host == "localhost"
      assert config.port == 19530
      assert config.database == "default"
      assert config.timeout == 30_000
      assert config.ssl == false
      assert config.ssl_options == []
      assert Map.get(config, :user) == nil
      assert Map.get(config, :password) == nil
      assert Map.get(config, :token) == nil
    end

    test "parses custom host and port" do
      {:ok, config} = Config.parse(host: "milvus.example.com", port: 19531)

      assert config.host == "milvus.example.com"
      assert config.port == 19531
    end

    test "parses authentication options" do
      {:ok, config} = Config.parse(user: "admin", password: "secret")

      assert config.user == "admin"
      assert config.password == "secret"
    end

    test "parses token authentication" do
      {:ok, config} = Config.parse(token: "api-token-123")

      assert config.token == "api-token-123"
    end

    test "parses SSL options" do
      {:ok, config} = Config.parse(ssl: true, ssl_options: [verify: :verify_peer])

      assert config.ssl == true
      assert config.ssl_options == [verify: :verify_peer]
    end

    test "validates port range" do
      {:error, error} = Config.parse(port: 0)
      assert error.message =~ "port"

      {:error, error} = Config.parse(port: 70000)
      assert error.message =~ "port"
    end

    test "validates timeout minimum" do
      {:error, error} = Config.parse(timeout: 500)
      assert error.message =~ "timeout"
    end
  end

  describe "parse/1 with map" do
    test "parses map config" do
      {:ok, config} = Config.parse(%{host: "milvus.local", port: 19530})

      assert config.host == "milvus.local"
      assert config.port == 19530
    end

    test "handles string keys" do
      {:ok, config} = Config.parse(%{"host" => "milvus.local", "port" => 19530})

      assert config.host == "milvus.local"
      assert config.port == 19530
    end
  end

  describe "parse_uri/1" do
    test "parses basic http URI" do
      {:ok, config} = Config.parse_uri("http://localhost:19530")

      assert config.host == "localhost"
      assert config.port == 19530
      assert config.ssl == false
    end

    test "parses https URI with SSL enabled" do
      {:ok, config} = Config.parse_uri("https://milvus.example.com:443")

      assert config.host == "milvus.example.com"
      assert config.port == 443
      assert config.ssl == true
    end

    test "parses URI with authentication" do
      {:ok, config} = Config.parse_uri("http://admin:secret@localhost:19530")

      assert config.user == "admin"
      assert config.password == "secret"
    end

    test "parses URI with user only" do
      {:ok, config} = Config.parse_uri("http://admin@localhost:19530")

      assert config.user == "admin"
      assert Map.get(config, :password) == nil
    end

    test "parses URI with database path" do
      {:ok, config} = Config.parse_uri("http://localhost:19530/mydb")

      assert config.database == "mydb"
    end

    test "parses URI with query parameters" do
      {:ok, config} = Config.parse_uri("http://localhost:19530?timeout=60000&token=abc123")

      assert config.timeout == 60_000
      assert config.token == "abc123"
    end

    test "uses scheme default port when not specified" do
      {:ok, config} = Config.parse_uri("http://localhost")

      assert config.host == "localhost"
      # URI.parse returns 80 for http scheme when no port specified
      assert config.port == 80
    end

    test "uses milvus scheme with default port" do
      {:ok, config} = Config.parse_uri("milvus://localhost")

      assert config.host == "localhost"
      # milvus scheme has no default, so we use our default
      assert config.port == 19530
    end

    test "returns error for invalid URI" do
      {:error, error} = Config.parse_uri("not-a-uri")

      assert error.field == "uri"
      assert error.message =~ "Invalid URI"
    end

    test "returns error for empty host" do
      {:error, error} = Config.parse_uri("http://:19530")

      assert error.field == "uri"
    end

    test "parses milvus scheme" do
      {:ok, config} = Config.parse_uri("milvus://localhost:19530")

      assert config.host == "localhost"
      assert config.ssl == false
    end

    test "parses milvuss scheme with SSL" do
      {:ok, config} = Config.parse_uri("milvuss://localhost:19530")

      assert config.host == "localhost"
      assert config.ssl == true
    end
  end

  describe "full configuration scenarios" do
    test "production-like configuration" do
      {:ok, config} =
        Config.parse(
          host: "milvus.prod.example.com",
          port: 19530,
          database: "production",
          user: "app_user",
          password: "secure_password",
          timeout: 60_000,
          ssl: true,
          ssl_options: [verify: :verify_peer, cacertfile: "/path/to/ca.crt"]
        )

      assert config.host == "milvus.prod.example.com"
      assert config.database == "production"
      assert config.ssl == true
      assert config.timeout == 60_000
    end

    test "production URI configuration" do
      {:ok, config} =
        Config.parse_uri(
          "https://app_user:secure_password@milvus.prod.example.com:443/production?timeout=60000"
        )

      assert config.host == "milvus.prod.example.com"
      assert config.port == 443
      assert config.database == "production"
      assert config.user == "app_user"
      assert config.password == "secure_password"
      assert config.timeout == 60_000
      assert config.ssl == true
    end
  end
end

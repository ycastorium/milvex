defmodule Milvex.Collection do
  @moduledoc """
  Declarative DSL for defining Milvus collection schemas.

  This module provides a Spark-based DSL for declaring collection schemas
  in a declarative, compile-time validated way.

  ## Basic Example

      defmodule MyApp.Movies do
        use Milvex.Collection

        collection do
          name "movies"
          description "Movie embeddings collection"

          fields do
            primary_key :id, :int64, auto_id: true
            varchar :title, 512
            scalar :year, :int32
            vector :embedding, 128
          end
        end
      end

  ## Field Types

  ### Primary Key

  Every collection must have exactly one primary key field:

      primary_key :id, :int64                    # Integer primary key
      primary_key :id, :int64, auto_id: true     # Auto-generated IDs
      primary_key :pk, :varchar, max_length: 64  # String primary key

  ### Vector Fields

  Vector fields store embeddings:

      vector :embedding, 128                              # Float vector (default)
      vector :embedding, 768, type: :float16_vector       # Half-precision
      vector :binary_emb, 256, type: :binary_vector       # Binary vector
      sparse_vector :sparse_embedding                      # Sparse vector (no dimension)

  ### Varchar Fields

  Variable-length string fields:

      varchar :title, 256                         # Basic varchar
      varchar :description, 1024, nullable: true  # Nullable
      varchar :category, 64, default: "general"   # With default

  ### Scalar Fields

  Numeric, boolean, and JSON fields:

      scalar :count, :int32
      scalar :score, :float, nullable: true
      scalar :is_active, :bool
      scalar :metadata, :json

  ### Array Fields

  Arrays of scalar types:

      array :tags, :varchar, max_capacity: 100, max_length: 64
      array :scores, :float, max_capacity: 10

  ## Converting to Schema

  The DSL modules can be converted to the existing `Milvex.Schema` format
  for use with the Milvex client:

      schema = Milvex.Collection.to_schema(MyApp.Movies)

  ## Introspection

  You can inspect a collection's configuration at runtime:

      Milvex.Collection.collection_name(MyApp.Movies)
      Milvex.Collection.fields(MyApp.Movies)
      Milvex.Collection.primary_key(MyApp.Movies)
  """

  alias Milvex.Collection.Dsl.Field
  alias Milvex.Milvus.Proto.Common.KeyValuePair
  alias Milvex.Milvus.Proto.Schema.CollectionSchema
  alias Milvex.Milvus.Proto.Schema.FieldSchema

  use Spark.Dsl,
    default_extensions: [
      extensions: [Milvex.Collection.Dsl]
    ]

  @doc """
  Returns the collection name for the given module.
  """
  @spec collection_name(module()) :: String.t()
  def collection_name(module) do
    Spark.Dsl.Extension.get_opt(module, [:collection], :name, nil)
  end

  @doc """
  Returns the collection description for the given module.
  """
  @spec description(module()) :: String.t() | nil
  def description(module) do
    Spark.Dsl.Extension.get_opt(module, [:collection], :description, nil)
  end

  @doc """
  Returns whether dynamic fields are enabled for the collection.
  """
  @spec enable_dynamic_field?(module()) :: boolean()
  def enable_dynamic_field?(module) do
    Spark.Dsl.Extension.get_opt(module, [:collection], :enable_dynamic_field, false)
  end

  @doc """
  Returns all field definitions for the given module.
  """
  @spec fields(module()) :: [Field.t()]
  def fields(module) do
    Spark.Dsl.Extension.get_entities(module, [:collection, :fields])
  end

  @doc """
  Returns the primary key field for the given module.
  """
  @spec primary_key(module()) :: Field.t() | nil
  def primary_key(module) do
    module
    |> fields()
    |> Enum.find(& &1.is_primary_key)
  end

  @doc """
  Returns all vector fields for the given module.
  """
  @spec vector_fields(module()) :: [Field.t()]
  def vector_fields(module) do
    module
    |> fields()
    |> Enum.filter(&Field.vector?/1)
  end

  @doc """
  Returns all scalar fields (non-vector, non-array, non-varchar) for the given module.

  Note: Varchar fields are treated separately in Milvex. Use `fields/1` and filter
  by type if you need to include them.
  """
  @spec scalar_fields(module()) :: [Field.t()]
  def scalar_fields(module) do
    module
    |> fields()
    |> Enum.filter(fn field ->
      Field.scalar?(field) and field.type != :varchar
    end)
  end

  @doc """
  Converts the collection DSL definition to a `Milvex.Schema` struct.

  This allows using DSL-defined collections with the existing Milvex API.

  ## Example

      schema = Milvex.Collection.to_schema(MyApp.Movies)
      Milvex.create_collection(conn, schema)
  """
  @spec to_schema(module()) :: Milvex.Schema.t()
  def to_schema(module) do
    %Milvex.Schema{
      name: collection_name(module),
      description: description(module),
      fields: Enum.map(fields(module), &field_to_schema_field/1),
      enable_dynamic_field: enable_dynamic_field?(module)
    }
  end

  @doc """
  Converts the collection DSL definition directly to a protobuf CollectionSchema.

  ## Example

      proto = Milvex.Collection.to_proto(MyApp.Movies)
  """
  @spec to_proto(module()) :: CollectionSchema.t()
  def to_proto(module) do
    %CollectionSchema{
      name: collection_name(module),
      description: description(module) || "",
      fields: Enum.map(fields(module), &field_to_proto/1),
      enable_dynamic_field: enable_dynamic_field?(module)
    }
  end

  defp field_to_schema_field(%Field{} = field) do
    %Milvex.Schema.Field{
      name: Atom.to_string(field.name),
      data_type: resolve_data_type(field),
      description: field.description,
      is_primary_key: field.is_primary_key || false,
      auto_id: field.auto_id || false,
      dimension: field.dimension,
      max_length: field.max_length,
      element_type: field.element_type,
      max_capacity: field.max_capacity,
      nullable: field.nullable || false,
      is_partition_key: field.partition_key || false,
      is_clustering_key: field.clustering_key || false,
      default_value: field.default
    }
  end

  defp resolve_data_type(%Field{element_type: elem_type}) when not is_nil(elem_type), do: :array
  defp resolve_data_type(%Field{type: type}), do: type

  defp field_to_proto(%Field{} = field) do
    %FieldSchema{
      name: Atom.to_string(field.name),
      description: field.description || "",
      data_type: data_type_to_proto(resolve_data_type(field)),
      is_primary_key: field.is_primary_key || false,
      autoID: field.auto_id || false,
      type_params: build_type_params(field),
      nullable: field.nullable || false,
      is_partition_key: field.partition_key || false,
      is_clustering_key: field.clustering_key || false,
      element_type:
        if(field.element_type, do: data_type_to_proto(field.element_type), else: :None)
    }
  end

  defp build_type_params(field) do
    []
    |> maybe_add_param("dim", field.dimension)
    |> maybe_add_param("max_length", field.max_length)
    |> maybe_add_param("max_capacity", field.max_capacity)
  end

  defp maybe_add_param(params, _key, nil), do: params

  defp maybe_add_param(params, key, value) do
    [%KeyValuePair{key: key, value: to_string(value)} | params]
  end

  defp data_type_to_proto(:bool), do: :Bool
  defp data_type_to_proto(:int8), do: :Int8
  defp data_type_to_proto(:int16), do: :Int16
  defp data_type_to_proto(:int32), do: :Int32
  defp data_type_to_proto(:int64), do: :Int64
  defp data_type_to_proto(:float), do: :Float
  defp data_type_to_proto(:double), do: :Double
  defp data_type_to_proto(:varchar), do: :VarChar
  defp data_type_to_proto(:json), do: :JSON
  defp data_type_to_proto(:text), do: :Text
  defp data_type_to_proto(:array), do: :Array
  defp data_type_to_proto(:binary_vector), do: :BinaryVector
  defp data_type_to_proto(:float_vector), do: :FloatVector
  defp data_type_to_proto(:float16_vector), do: :Float16Vector
  defp data_type_to_proto(:bfloat16_vector), do: :BFloat16Vector
  defp data_type_to_proto(:sparse_float_vector), do: :SparseFloatVector
  defp data_type_to_proto(:int8_vector), do: :Int8Vector
end

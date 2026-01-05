defmodule Milvex.Schema.Field do
  @moduledoc """
  Builder for Milvus collection field schemas.

  Provides a fluent API for constructing field definitions with validation.
  Supports all Milvus data types including scalars, vectors, and complex types.

  ## Examples

      # Primary key field (using builder)
      field = Field.new("id", :int64) |> Field.set_primary_key() |> Field.auto_id()

      # Vector field with dimension
      field = Field.new("embedding", :float_vector) |> Field.dimension(128)

      # VarChar with max length
      field = Field.new("title", :varchar) |> Field.max_length(512)

      # Using smart constructors
      Field.primary_key("id", :int64, auto_id: true)
      Field.vector("embedding", 128)
      Field.varchar("title", 512, nullable: true)
  """

  alias Milvex.Milvus.Proto.Common.KeyValuePair
  alias Milvex.Milvus.Proto.Schema.FieldSchema
  alias Milvex.Milvus.Proto.Schema.StructArrayFieldSchema

  @scalar_types [:bool, :int8, :int16, :int32, :int64, :float, :double, :varchar, :json, :text]
  @vector_types [
    :binary_vector,
    :float_vector,
    :float16_vector,
    :bfloat16_vector,
    :sparse_float_vector,
    :int8_vector
  ]
  @complex_types [:array, :struct, :array_of_struct]
  @all_types @scalar_types ++ @vector_types ++ @complex_types

  @type data_type ::
          :bool
          | :int8
          | :int16
          | :int32
          | :int64
          | :float
          | :double
          | :varchar
          | :json
          | :text
          | :array
          | :struct
          | :array_of_struct
          | :binary_vector
          | :float_vector
          | :float16_vector
          | :bfloat16_vector
          | :sparse_float_vector
          | :int8_vector

  @type t :: %__MODULE__{
          name: String.t(),
          data_type: data_type(),
          description: String.t() | nil,
          is_primary_key: boolean(),
          auto_id: boolean(),
          dimension: pos_integer() | nil,
          max_length: pos_integer() | nil,
          element_type: data_type() | nil,
          max_capacity: pos_integer() | nil,
          nullable: boolean(),
          is_partition_key: boolean(),
          is_clustering_key: boolean(),
          default_value: term() | nil,
          struct_schema: [t()] | nil,
          enable_analyzer: boolean()
        }

  defstruct [
    :name,
    :data_type,
    :description,
    :dimension,
    :max_length,
    :element_type,
    :max_capacity,
    :default_value,
    :struct_schema,
    is_primary_key: false,
    auto_id: false,
    nullable: false,
    is_partition_key: false,
    is_clustering_key: false,
    enable_analyzer: false
  ]

  @doc """
  Creates a new field with the given name and data type.

  ## Parameters
    - `name` - Field name (1-255 characters, alphanumeric and underscores)
    - `data_type` - One of the supported Milvus data types

  ## Examples

      Field.new("id", :int64)
      Field.new("embedding", :float_vector)
  """
  @spec new(String.t(), data_type()) :: t()
  def new(name, data_type) when is_binary(name) and data_type in @all_types do
    %__MODULE__{name: name, data_type: data_type}
  end

  def new(name, data_type) when is_atom(name) do
    new(Atom.to_string(name), data_type)
  end

  @doc """
  Sets the field description.
  """
  @spec description(t(), String.t()) :: t()
  def description(%__MODULE__{} = field, desc) when is_binary(desc) do
    %{field | description: desc}
  end

  @doc """
  Marks the field as the primary key.

  When called with a single Field argument, sets `is_primary_key` to `true`.
  When called with a boolean, sets `is_primary_key` to that value.
  """
  @spec set_primary_key(t(), boolean()) :: t()
  def set_primary_key(%__MODULE__{} = field, value) when is_boolean(value) do
    %{field | is_primary_key: value}
  end

  @doc """
  Marks the field as the primary key (sets to true).
  """
  @spec set_primary_key(t()) :: t()
  def set_primary_key(%__MODULE__{} = field) do
    %{field | is_primary_key: true}
  end

  @doc """
  Enables auto-generation of IDs for this field.
  Only valid for primary key fields with int64 or varchar type.
  """
  @spec auto_id(t(), boolean()) :: t()
  def auto_id(%__MODULE__{} = field, value \\ true) when is_boolean(value) do
    %{field | auto_id: value}
  end

  @doc """
  Sets the dimension for vector fields.
  Required for all vector types except sparse vectors.
  """
  @spec dimension(t(), pos_integer()) :: t()
  def dimension(%__MODULE__{} = field, dim) when is_integer(dim) and dim > 0 do
    %{field | dimension: dim}
  end

  @doc """
  Sets the maximum length for varchar fields.
  Required for varchar type, must be between 1 and 65535.
  """
  @spec max_length(t(), pos_integer()) :: t()
  def max_length(%__MODULE__{} = field, len) when is_integer(len) and len > 0 and len <= 65_535 do
    %{field | max_length: len}
  end

  @doc """
  Sets the element type for array fields.
  """
  @spec element_type(t(), data_type()) :: t()
  def element_type(%__MODULE__{} = field, type) when type in @scalar_types or type == :struct do
    %{field | element_type: type}
  end

  @doc """
  Sets the maximum capacity for array fields.
  """
  @spec max_capacity(t(), pos_integer()) :: t()
  def max_capacity(%__MODULE__{} = field, cap) when is_integer(cap) and cap > 0 do
    %{field | max_capacity: cap}
  end

  @doc """
  Marks the field as nullable.
  """
  @spec nullable(t(), boolean()) :: t()
  def nullable(%__MODULE__{} = field, value \\ true) when is_boolean(value) do
    %{field | nullable: value}
  end

  @doc """
  Marks the field as a partition key.
  """
  @spec partition_key(t(), boolean()) :: t()
  def partition_key(%__MODULE__{} = field, value \\ true) when is_boolean(value) do
    %{field | is_partition_key: value}
  end

  @doc """
  Marks the field as a clustering key.
  """
  @spec clustering_key(t(), boolean()) :: t()
  def clustering_key(%__MODULE__{} = field, value \\ true) when is_boolean(value) do
    %{field | is_clustering_key: value}
  end

  @doc """
  Sets a default value for the field.
  """
  @spec default(t(), term()) :: t()
  def default(%__MODULE__{} = field, value) do
    %{field | default_value: value}
  end

  @doc """
  Enables text analyzer for varchar fields.
  Used for BM25 full-text search.
  """
  @spec enable_analyzer(t(), boolean()) :: t()
  def enable_analyzer(%__MODULE__{} = field, value \\ true) when is_boolean(value) do
    %{field | enable_analyzer: value}
  end

  @doc """
  Creates a primary key field with common defaults.

  ## Options
    - `:auto_id` - Enable auto ID generation (default: false)
    - `:description` - Field description

  ## Examples

      Field.primary_key("id", :int64)
      Field.primary_key("id", :int64, auto_id: true)
      Field.primary_key("pk", :varchar, max_length: 64)
  """
  @spec primary_key(String.t(), data_type(), keyword()) :: t()
  def primary_key(name, type, opts \\ []) when type in [:int64, :varchar] do
    field =
      new(name, type)
      |> set_primary_key(true)
      |> auto_id(Keyword.get(opts, :auto_id, false))

    field =
      if desc = Keyword.get(opts, :description) do
        description(field, desc)
      else
        field
      end

    if type == :varchar do
      max_length(field, Keyword.get(opts, :max_length, 64))
    else
      field
    end
  end

  @doc """
  Creates a vector field with the specified dimension.

  ## Options
    - `:type` - Vector type (default: :float_vector)
    - `:description` - Field description

  ## Examples

      Field.vector("embedding", 128)
      Field.vector("embedding", 768, type: :float16_vector)
  """
  @spec vector(String.t(), pos_integer(), keyword()) :: t()
  def vector(name, dim, opts \\ []) when is_integer(dim) and dim > 0 do
    type = Keyword.get(opts, :type, :float_vector)

    unless type in @vector_types do
      raise ArgumentError, "Invalid vector type: #{inspect(type)}"
    end

    field = new(name, type) |> dimension(dim)

    if desc = Keyword.get(opts, :description) do
      description(field, desc)
    else
      field
    end
  end

  @doc """
  Creates a sparse vector field.

  Sparse vectors don't require a dimension parameter.

  ## Examples

      Field.sparse_vector("sparse_embedding")
  """
  @spec sparse_vector(String.t(), keyword()) :: t()
  def sparse_vector(name, opts \\ []) do
    field = new(name, :sparse_float_vector)

    if desc = Keyword.get(opts, :description) do
      description(field, desc)
    else
      field
    end
  end

  @doc """
  Creates a varchar field with the specified max length.

  ## Options
    - `:nullable` - Allow null values (default: false)
    - `:description` - Field description
    - `:default` - Default value
    - `:enable_analyzer` - Enable text analyzer for BM25 search (default: false)

  ## Examples

      Field.varchar("title", 256)
      Field.varchar("description", 1024, nullable: true)
      Field.varchar("content", 4096, enable_analyzer: true)
  """
  @spec varchar(String.t(), pos_integer(), keyword()) :: t()
  def varchar(name, len, opts \\ []) when is_integer(len) and len > 0 do
    field =
      new(name, :varchar)
      |> max_length(len)
      |> nullable(Keyword.get(opts, :nullable, false))
      |> enable_analyzer(Keyword.get(opts, :enable_analyzer, false))

    field =
      if desc = Keyword.get(opts, :description) do
        description(field, desc)
      else
        field
      end

    if default_val = Keyword.get(opts, :default) do
      default(field, default_val)
    else
      field
    end
  end

  @doc """
  Creates a scalar field of the specified type.

  ## Options
    - `:nullable` - Allow null values (default: false)
    - `:description` - Field description
    - `:default` - Default value

  ## Examples

      Field.scalar("age", :int32)
      Field.scalar("score", :float, nullable: true)
  """
  @spec scalar(String.t(), data_type(), keyword()) :: t()
  def scalar(name, type, opts \\ []) when type in @scalar_types and type != :varchar do
    field =
      new(name, type)
      |> nullable(Keyword.get(opts, :nullable, false))

    field =
      if desc = Keyword.get(opts, :description) do
        description(field, desc)
      else
        field
      end

    if default_val = Keyword.get(opts, :default) do
      default(field, default_val)
    else
      field
    end
  end

  @doc """
  Creates an array field with the specified element type.

  ## Options
    - `:max_capacity` - Maximum number of elements (required)
    - `:nullable` - Allow null values (default: false)
    - `:description` - Field description
    - `:struct_schema` - Required when element_type is :struct, list of Field.t()
    - `:max_length` - For varchar element types

  ## Examples

      # Array of scalars
      Field.array("tags", :varchar, max_capacity: 100, max_length: 64)

      # Array of structs
      struct_fields = [
        Field.varchar("text", 4096),
        Field.vector("embedding", 1024)
      ]
      Field.array("sentences", :struct, max_capacity: 50, struct_schema: struct_fields)
  """
  @spec array(String.t(), data_type(), keyword()) :: t()
  def array(name, elem_type, opts \\ [])
      when elem_type in @scalar_types or elem_type == :struct do
    cap = Keyword.fetch!(opts, :max_capacity)

    data_type = if elem_type == :struct, do: :array_of_struct, else: :array

    field =
      new(name, data_type)
      |> element_type(elem_type)
      |> max_capacity(cap)
      |> nullable(Keyword.get(opts, :nullable, false))

    field =
      cond do
        elem_type == :varchar ->
          max_length(field, Keyword.get(opts, :max_length, 256))

        elem_type == :struct ->
          struct_schema = Keyword.fetch!(opts, :struct_schema)
          %{field | struct_schema: struct_schema}

        true ->
          field
      end

    if desc = Keyword.get(opts, :description) do
      description(field, desc)
    else
      field
    end
  end

  @doc """
  Creates a struct field with nested fields.

  Struct fields are typically used as the element type for array_of_struct fields.

  ## Options
    - `:fields` - List of Field.t() defining the struct schema (required)
    - `:description` - Field description

  ## Examples

      struct_fields = [
        Field.varchar("text", 4096),
        Field.vector("embedding", 1024),
        Field.varchar("speaker_id", 36)
      ]
      Field.struct("sentence", fields: struct_fields)
  """
  @spec struct(String.t(), keyword()) :: t()
  def struct(name, opts \\ []) do
    fields = Keyword.fetch!(opts, :fields)

    field = %__MODULE__{
      name: if(is_atom(name), do: Atom.to_string(name), else: name),
      data_type: :struct,
      struct_schema: fields
    }

    if desc = Keyword.get(opts, :description) do
      description(field, desc)
    else
      field
    end
  end

  @doc """
  Validates the field configuration.

  Returns `{:ok, field}` if valid, `{:error, error}` otherwise.
  """
  @spec validate(t()) :: {:ok, t()} | {:error, Milvex.Error.t()}
  def validate(%__MODULE__{} = field) do
    with :ok <- validate_name(field),
         :ok <- validate_vector_dimension(field),
         :ok <- validate_varchar_length(field),
         :ok <- validate_array_config(field),
         :ok <- validate_primary_key(field) do
      {:ok, field}
    end
  end

  @doc """
  Validates the field and raises on error.
  """
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = field) do
    case validate(field) do
      {:ok, field} -> field
      {:error, error} -> raise error
    end
  end

  defp validate_name(%{name: name}) do
    cond do
      byte_size(name) == 0 ->
        {:error, invalid_error(:name, "cannot be empty")}

      byte_size(name) > 255 ->
        {:error, invalid_error(:name, "cannot exceed 255 characters")}

      not Regex.match?(~r/^[a-zA-Z_][a-zA-Z0-9_]*$/, name) ->
        {:error,
         invalid_error(
           :name,
           "must start with a letter or underscore and contain only alphanumeric characters and underscores"
         )}

      true ->
        :ok
    end
  end

  defp validate_vector_dimension(%{data_type: type, dimension: dim})
       when type in @vector_types and type != :sparse_float_vector do
    cond do
      is_nil(dim) ->
        {:error, invalid_error(:dimension, "is required for #{type} fields")}

      dim < 1 ->
        {:error, invalid_error(:dimension, "must be a positive integer")}

      type == :binary_vector and rem(dim, 8) != 0 ->
        {:error, invalid_error(:dimension, "must be a multiple of 8 for binary vectors")}

      true ->
        :ok
    end
  end

  defp validate_vector_dimension(_), do: :ok

  defp validate_varchar_length(%{data_type: :varchar, max_length: len}) do
    cond do
      is_nil(len) ->
        {:error, invalid_error(:max_length, "is required for varchar fields")}

      len < 1 or len > 65_535 ->
        {:error, invalid_error(:max_length, "must be between 1 and 65535")}

      true ->
        :ok
    end
  end

  defp validate_varchar_length(_), do: :ok

  defp validate_array_config(%{data_type: :array, element_type: elem_type, max_capacity: cap}) do
    cond do
      is_nil(elem_type) ->
        {:error, invalid_error(:element_type, "is required for array fields")}

      is_nil(cap) ->
        {:error, invalid_error(:max_capacity, "is required for array fields")}

      cap < 1 ->
        {:error, invalid_error(:max_capacity, "must be a positive integer")}

      true ->
        :ok
    end
  end

  defp validate_array_config(%{
         data_type: :array_of_struct,
         element_type: elem_type,
         max_capacity: cap,
         struct_schema: struct_schema
       }) do
    cond do
      is_nil(elem_type) or elem_type != :struct ->
        {:error, invalid_error(:element_type, "must be :struct for array_of_struct fields")}

      is_nil(cap) ->
        {:error, invalid_error(:max_capacity, "is required for array_of_struct fields")}

      cap < 1 ->
        {:error, invalid_error(:max_capacity, "must be a positive integer")}

      is_nil(struct_schema) or struct_schema == [] ->
        {:error, invalid_error(:struct_schema, "is required for array_of_struct fields")}

      true ->
        :ok
    end
  end

  defp validate_array_config(_), do: :ok

  defp validate_primary_key(%{is_primary_key: true, data_type: type, auto_id: auto_id}) do
    cond do
      type not in [:int64, :varchar] ->
        {:error, invalid_error(:data_type, "primary key must be int64 or varchar")}

      auto_id and type == :varchar ->
        {:error, invalid_error(:auto_id, "auto ID is only supported for int64 primary keys")}

      true ->
        :ok
    end
  end

  defp validate_primary_key(_), do: :ok

  defp invalid_error(field, message) do
    Milvex.Errors.Invalid.exception(field: field, message: message)
  end

  @doc """
  Converts the field to a protobuf FieldSchema struct.
  """
  @spec to_proto(t()) :: FieldSchema.t()
  def to_proto(%__MODULE__{} = field) do
    %FieldSchema{
      name: field.name,
      description: field.description || "",
      data_type: data_type_to_proto(field.data_type),
      is_primary_key: field.is_primary_key,
      autoID: field.auto_id,
      type_params: build_type_params(field),
      nullable: field.nullable,
      is_partition_key: field.is_partition_key,
      is_clustering_key: field.is_clustering_key,
      element_type:
        if(field.element_type, do: data_type_to_proto(field.element_type), else: :None)
    }
  end

  @doc """
  Creates a Field from a protobuf FieldSchema struct.
  """
  @spec from_proto(FieldSchema.t()) :: t()
  def from_proto(%FieldSchema{} = proto) do
    data_type = data_type_from_proto(proto.data_type)
    type_params = parse_type_params(proto.type_params)

    %__MODULE__{
      name: proto.name,
      description: if(proto.description == "", do: nil, else: proto.description),
      data_type: data_type,
      is_primary_key: proto.is_primary_key,
      auto_id: proto.autoID,
      dimension: type_params[:dim],
      max_length: type_params[:max_length],
      max_capacity: type_params[:max_capacity],
      nullable: proto.nullable,
      is_partition_key: proto.is_partition_key,
      is_clustering_key: proto.is_clustering_key,
      enable_analyzer: type_params[:enable_analyzer] || false,
      element_type:
        if(proto.element_type != :None, do: data_type_from_proto(proto.element_type), else: nil)
    }
  end

  defp build_type_params(field) do
    []
    |> maybe_add_param("dim", field.dimension)
    |> maybe_add_param("max_length", field.max_length)
    |> maybe_add_param("max_capacity", field.max_capacity)
    |> maybe_add_bool_param("enable_analyzer", field.enable_analyzer)
  end

  defp maybe_add_param(params, _key, nil), do: params

  defp maybe_add_param(params, key, value) do
    [%KeyValuePair{key: key, value: to_string(value)} | params]
  end

  defp maybe_add_bool_param(params, _key, false), do: params

  defp maybe_add_bool_param(params, key, true) do
    [%KeyValuePair{key: key, value: "true"} | params]
  end

  defp parse_type_params(params) do
    Enum.reduce(params, %{}, fn %KeyValuePair{key: key, value: value}, acc ->
      case key do
        "dim" -> Map.put(acc, :dim, String.to_integer(value))
        "max_length" -> Map.put(acc, :max_length, String.to_integer(value))
        "max_capacity" -> Map.put(acc, :max_capacity, String.to_integer(value))
        "enable_analyzer" -> Map.put(acc, :enable_analyzer, value == "true")
        _ -> acc
      end
    end)
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
  defp data_type_to_proto(:struct), do: :Struct
  defp data_type_to_proto(:array_of_struct), do: :ArrayOfStruct
  defp data_type_to_proto(:binary_vector), do: :BinaryVector
  defp data_type_to_proto(:float_vector), do: :FloatVector
  defp data_type_to_proto(:float16_vector), do: :Float16Vector
  defp data_type_to_proto(:bfloat16_vector), do: :BFloat16Vector
  defp data_type_to_proto(:sparse_float_vector), do: :SparseFloatVector
  defp data_type_to_proto(:int8_vector), do: :Int8Vector

  defp data_type_from_proto(:Bool), do: :bool
  defp data_type_from_proto(:Int8), do: :int8
  defp data_type_from_proto(:Int16), do: :int16
  defp data_type_from_proto(:Int32), do: :int32
  defp data_type_from_proto(:Int64), do: :int64
  defp data_type_from_proto(:Float), do: :float
  defp data_type_from_proto(:Double), do: :double
  defp data_type_from_proto(:VarChar), do: :varchar
  defp data_type_from_proto(:JSON), do: :json
  defp data_type_from_proto(:Text), do: :text
  defp data_type_from_proto(:Array), do: :array
  defp data_type_from_proto(:Struct), do: :struct
  defp data_type_from_proto(:ArrayOfStruct), do: :array_of_struct
  defp data_type_from_proto(:BinaryVector), do: :binary_vector
  defp data_type_from_proto(:FloatVector), do: :float_vector
  defp data_type_from_proto(:Float16Vector), do: :float16_vector
  defp data_type_from_proto(:BFloat16Vector), do: :bfloat16_vector
  defp data_type_from_proto(:SparseFloatVector), do: :sparse_float_vector
  defp data_type_from_proto(:Int8Vector), do: :int8_vector
  defp data_type_from_proto(_), do: :unknown

  @doc """
  Returns list of all supported data types.
  """
  @spec data_types() :: [data_type()]
  def data_types, do: @all_types

  @doc """
  Returns list of scalar data types.
  """
  @spec scalar_types() :: [data_type()]
  def scalar_types, do: @scalar_types

  @doc """
  Returns list of vector data types.
  """
  @spec vector_types() :: [data_type()]
  def vector_types, do: @vector_types

  @doc """
  Checks if the given type is a vector type.
  """
  @spec vector_type?(data_type()) :: boolean()
  def vector_type?(type), do: type in @vector_types

  @doc """
  Checks if the given type is a scalar type.
  """
  @spec scalar_type?(data_type()) :: boolean()
  def scalar_type?(type), do: type in @scalar_types

  @doc """
  Checks if the field is an array of struct type.
  """
  @spec array_of_struct?(t()) :: boolean()
  def array_of_struct?(%__MODULE__{data_type: :array_of_struct}), do: true
  def array_of_struct?(_), do: false

  @doc """
  Checks if the field is a struct type.
  """
  @spec struct?(t()) :: boolean()
  def struct?(%__MODULE__{data_type: :struct}), do: true
  def struct?(_), do: false

  @doc """
  Converts an array_of_struct field to a StructArrayFieldSchema proto.

  Used for the `struct_array_fields` in CollectionSchema.

  Nested fields are wrapped as Array (for scalars) or ArrayOfVector (for vectors)
  as required by Milvus's StructArrayFieldSchema format.
  """
  @spec to_struct_array_field_schema(t()) :: StructArrayFieldSchema.t()
  def to_struct_array_field_schema(%__MODULE__{data_type: :array_of_struct} = field) do
    max_cap = field.max_capacity

    %StructArrayFieldSchema{
      name: field.name,
      description: field.description || "",
      fields: Enum.map(field.struct_schema, &to_nested_struct_field_proto(&1, max_cap)),
      type_params: build_type_params(field)
    }
  end

  defp to_nested_struct_field_proto(%__MODULE__{data_type: type} = field, max_capacity)
       when type in @vector_types do
    type_params = build_nested_type_params(field, max_capacity)

    %FieldSchema{
      name: field.name,
      description: field.description || "",
      data_type: :ArrayOfVector,
      is_primary_key: false,
      autoID: false,
      type_params: type_params,
      nullable: field.nullable,
      is_partition_key: false,
      is_clustering_key: false,
      element_type: data_type_to_proto(type)
    }
  end

  defp to_nested_struct_field_proto(%__MODULE__{} = field, max_capacity) do
    type_params = build_nested_type_params(field, max_capacity)

    %FieldSchema{
      name: field.name,
      description: field.description || "",
      data_type: :Array,
      is_primary_key: false,
      autoID: false,
      type_params: type_params,
      nullable: field.nullable,
      is_partition_key: false,
      is_clustering_key: false,
      element_type: data_type_to_proto(field.data_type)
    }
  end

  defp build_nested_type_params(field, max_capacity) do
    []
    |> maybe_add_param("dim", field.dimension)
    |> maybe_add_param("max_length", field.max_length)
    |> maybe_add_param("max_capacity", max_capacity)
  end

  @doc """
  Creates a Field from a StructArrayFieldSchema proto.
  """
  @spec from_struct_array_field_schema(StructArrayFieldSchema.t()) :: t()
  def from_struct_array_field_schema(%StructArrayFieldSchema{} = proto) do
    type_params = parse_type_params(proto.type_params)

    %__MODULE__{
      name: proto.name,
      description: if(proto.description == "", do: nil, else: proto.description),
      data_type: :array_of_struct,
      element_type: :struct,
      max_capacity: type_params[:max_capacity],
      struct_schema: Enum.map(proto.fields, &from_nested_struct_field_proto/1)
    }
  end

  defp from_nested_struct_field_proto(%FieldSchema{data_type: :ArrayOfVector} = proto) do
    type_params = parse_type_params(proto.type_params)
    data_type = data_type_from_proto(proto.element_type)

    %__MODULE__{
      name: proto.name,
      description: if(proto.description == "", do: nil, else: proto.description),
      data_type: data_type,
      dimension: type_params[:dim],
      nullable: proto.nullable
    }
  end

  defp from_nested_struct_field_proto(%FieldSchema{data_type: :Array} = proto) do
    type_params = parse_type_params(proto.type_params)
    data_type = data_type_from_proto(proto.element_type)

    %__MODULE__{
      name: proto.name,
      description: if(proto.description == "", do: nil, else: proto.description),
      data_type: data_type,
      max_length: type_params[:max_length],
      nullable: proto.nullable
    }
  end

  defp from_nested_struct_field_proto(proto) do
    from_proto(proto)
  end
end

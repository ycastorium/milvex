defmodule Milvex.Schema do
  @moduledoc """
  Builder for Milvus collection schemas.

  Provides a fluent API for constructing collection schema definitions with validation.
  Schemas define the structure of data stored in Milvus collections, including
  fields, primary keys, and vector configurations.

  ## Examples

      alias Milvex.Schema
      alias Milvex.Schema.Field

      # Using the builder pattern
      schema =
        Schema.new("movies")
        |> Schema.description("Movie embeddings collection")
        |> Schema.add_field(Field.primary_key("id", :int64, auto_id: true))
        |> Schema.add_field(Field.varchar("title", 512))
        |> Schema.add_field(Field.vector("embedding", 128))
        |> Schema.enable_dynamic_field()
        |> Schema.validate!()

      # Using the build/1 helper
      schema = Schema.build(
        name: "movies",
        description: "Movie embeddings collection",
        enable_dynamic_field: true,
        fields: [
          Field.primary_key("id", :int64, auto_id: true),
          Field.varchar("title", 512),
          Field.vector("embedding", 128)
        ]
      )
  """

  alias Milvex.Function
  alias Milvex.Milvus.Proto.Schema.CollectionSchema
  alias Milvex.Schema.Field

  @type t :: %__MODULE__{
          name: String.t(),
          description: String.t() | nil,
          fields: [Field.t()],
          functions: [Function.t()],
          enable_dynamic_field: boolean()
        }

  defstruct [
    :name,
    :description,
    fields: [],
    functions: [],
    enable_dynamic_field: false
  ]

  @doc """
  Creates a new schema with the given collection name.

  ## Parameters
    - `name` - Collection name (1-255 characters)

  ## Examples

      Schema.new("movies")
  """
  @spec new(String.t()) :: t()
  def new(name) when is_binary(name) do
    %__MODULE__{name: name}
  end

  def new(name) when is_atom(name) do
    new(Atom.to_string(name))
  end

  @doc """
  Sets the collection description.
  """
  @spec description(t(), String.t()) :: t()
  def description(%__MODULE__{} = schema, desc) when is_binary(desc) do
    %{schema | description: desc}
  end

  @doc """
  Adds a field to the schema.

  ## Examples

      schema
      |> Schema.add_field(Field.new("id", :int64) |> Field.primary_key())
      |> Schema.add_field(Field.vector("embedding", 128))
  """
  @spec add_field(t(), Field.t()) :: t()
  def add_field(%__MODULE__{} = schema, %Field{} = field) do
    %{schema | fields: schema.fields ++ [field]}
  end

  @doc """
  Adds multiple fields to the schema.

  ## Examples

      Schema.add_fields(schema, [
        Field.primary_key("id", :int64),
        Field.vector("embedding", 128)
      ])
  """
  @spec add_fields(t(), [Field.t()]) :: t()
  def add_fields(%__MODULE__{} = schema, fields) when is_list(fields) do
    Enum.reduce(fields, schema, &add_field(&2, &1))
  end

  @doc """
  Adds a function to the schema.

  ## Examples

      schema
      |> Schema.add_function(Function.bm25("bm25_fn", input: "content", output: "sparse"))
  """
  @spec add_function(t(), Function.t()) :: t()
  def add_function(%__MODULE__{} = schema, %Function{} = function) do
    %{schema | functions: schema.functions ++ [function]}
  end

  @doc """
  Enables or disables dynamic fields for the collection.

  When enabled, the collection can store fields not defined in the schema.
  """
  @spec enable_dynamic_field(t(), boolean()) :: t()
  def enable_dynamic_field(%__MODULE__{} = schema, enabled \\ true) when is_boolean(enabled) do
    %{schema | enable_dynamic_field: enabled}
  end

  @doc """
  Builds a schema from a keyword list or map.

  ## Options
    - `:name` - Collection name (required)
    - `:description` - Collection description
    - `:fields` - List of Field structs (required)
    - `:enable_dynamic_field` - Enable dynamic fields (default: false)

  ## Examples

      Schema.build(
        name: "movies",
        fields: [
          Field.primary_key("id", :int64),
          Field.vector("embedding", 128)
        ]
      )
  """
  @spec build(keyword() | map()) :: {:ok, t()} | {:error, Milvex.Error.t()}
  def build(opts) when is_list(opts), do: build(Map.new(opts))

  def build(opts) when is_map(opts) do
    with {:ok, name} <- fetch_required(opts, :name, "name is required"),
         {:ok, fields} <- fetch_required(opts, :fields, "fields are required") do
      schema =
        new(name)
        |> maybe_set(:description, opts[:description], &description/2)
        |> add_fields(fields)
        |> enable_dynamic_field(Map.get(opts, :enable_dynamic_field, false))

      validate(schema)
    end
  end

  @doc """
  Builds a schema from options and raises on error.
  """
  @spec build!(keyword() | map()) :: t()
  def build!(opts) do
    case build(opts) do
      {:ok, schema} -> schema
      {:error, error} -> raise error
    end
  end

  defp fetch_required(opts, key, error_msg) do
    case Map.fetch(opts, key) do
      {:ok, value} when not is_nil(value) -> {:ok, value}
      _ -> {:error, invalid_error(key, error_msg)}
    end
  end

  defp maybe_set(schema, _key, nil, _setter), do: schema
  defp maybe_set(schema, _key, value, setter), do: setter.(schema, value)

  @doc """
  Validates the schema configuration.

  Checks:
  - Collection name is valid (1-255 chars, alphanumeric + underscore)
  - At least one field is defined
  - Exactly one primary key field exists
  - All field names are unique
  - All individual fields pass validation

  Returns `{:ok, schema}` if valid, `{:error, error}` otherwise.
  """
  @spec validate(t()) :: {:ok, t()} | {:error, Milvex.Error.t()}
  def validate(%__MODULE__{} = schema) do
    with :ok <- validate_name(schema),
         :ok <- validate_has_fields(schema),
         :ok <- validate_primary_key(schema),
         :ok <- validate_unique_field_names(schema),
         :ok <- validate_all_fields(schema),
         :ok <- validate_functions(schema) do
      {:ok, schema}
    end
  end

  @doc """
  Validates the schema and raises on error.
  """
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = schema) do
    case validate(schema) do
      {:ok, schema} -> schema
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

  defp validate_has_fields(%{fields: []}),
    do: {:error, invalid_error(:fields, "at least one field is required")}

  defp validate_has_fields(_), do: :ok

  defp validate_primary_key(%{fields: fields}) do
    primary_keys = Enum.filter(fields, & &1.is_primary_key)

    case length(primary_keys) do
      0 ->
        {:error, invalid_error(:primary_key, "exactly one primary key field is required")}

      1 ->
        :ok

      n ->
        {:error, invalid_error(:primary_key, "found #{n} primary keys, exactly one is required")}
    end
  end

  defp validate_unique_field_names(%{fields: fields}) do
    names = Enum.map(fields, & &1.name)
    duplicates = names -- Enum.uniq(names)

    if duplicates == [] do
      :ok
    else
      {:error,
       invalid_error(:fields, "duplicate field names: #{Enum.join(Enum.uniq(duplicates), ", ")}")}
    end
  end

  defp validate_all_fields(%{fields: fields}) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      case Field.validate(field) do
        {:ok, _} -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp validate_functions(%{functions: [], fields: _}), do: :ok

  defp validate_functions(%{functions: functions, fields: fields}) do
    field_map = Map.new(fields, &{&1.name, &1})

    Enum.reduce_while(functions, :ok, fn func, :ok ->
      with :ok <- validate_function_inputs(func, field_map),
           :ok <- validate_function_outputs(func, field_map) do
        {:cont, :ok}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_function_inputs(func, field_map) do
    missing_or_invalid =
      func.input_field_names
      |> Enum.filter(fn name ->
        case Map.get(field_map, name) do
          nil -> true
          field -> func.type == :BM25 and not field.enable_analyzer
        end
      end)

    if Enum.empty?(missing_or_invalid) do
      :ok
    else
      if func.type == :BM25 do
        {:error,
         invalid_error(
           :functions,
           "function '#{func.name}' input fields #{inspect(missing_or_invalid)} must exist and have enable_analyzer: true"
         )}
      else
        {:error,
         invalid_error(
           :functions,
           "function '#{func.name}' references missing input fields: #{inspect(missing_or_invalid)}"
         )}
      end
    end
  end

  defp validate_function_outputs(func, field_map) do
    missing =
      func.output_field_names
      |> Enum.filter(fn name -> not Map.has_key?(field_map, name) end)

    if Enum.empty?(missing) do
      :ok
    else
      {:error,
       invalid_error(
         :functions,
         "function '#{func.name}' references missing output fields: #{inspect(missing)}"
       )}
    end
  end

  defp invalid_error(field, message) do
    Milvex.Errors.Invalid.exception(field: field, message: message)
  end

  @doc """
  Converts the schema to a protobuf CollectionSchema struct.

  Splits fields into regular fields and struct_array_fields as required
  by the Milvus proto schema.
  """
  @spec to_proto(t()) :: CollectionSchema.t()
  def to_proto(%__MODULE__{} = schema) do
    {regular_fields, struct_array_fields} =
      Enum.split_with(schema.fields, fn f -> f.data_type != :array_of_struct end)

    %CollectionSchema{
      name: schema.name,
      description: schema.description || "",
      fields: Enum.map(regular_fields, &Field.to_proto/1),
      struct_array_fields: Enum.map(struct_array_fields, &Field.to_struct_array_field_schema/1),
      enable_dynamic_field: schema.enable_dynamic_field,
      functions: Enum.map(schema.functions, &Function.to_proto/1)
    }
  end

  @doc """
  Creates a Schema from a protobuf CollectionSchema struct.

  Returns `nil` if the input is `nil`.
  Combines regular fields and struct_array_fields into a single fields list.
  """
  @spec from_proto(CollectionSchema.t() | nil) :: t() | nil
  def from_proto(nil), do: nil

  def from_proto(%CollectionSchema{} = proto) do
    regular_fields = Enum.map(proto.fields, &Field.from_proto/1)

    struct_array_fields =
      (proto.struct_array_fields || [])
      |> Enum.map(&Field.from_struct_array_field_schema/1)

    functions =
      (proto.functions || [])
      |> Enum.map(&Function.from_proto/1)

    %__MODULE__{
      name: proto.name,
      description: if(proto.description == "", do: nil, else: proto.description),
      fields: regular_fields ++ struct_array_fields,
      functions: functions,
      enable_dynamic_field: proto.enable_dynamic_field
    }
  end

  @doc """
  Returns the primary key field from the schema.

  Returns `nil` if no primary key is defined.
  """
  @spec primary_key_field(t()) :: Field.t() | nil
  def primary_key_field(%__MODULE__{fields: fields}) do
    Enum.find(fields, & &1.is_primary_key)
  end

  @doc """
  Returns all vector fields from the schema.
  """
  @spec vector_fields(t()) :: [Field.t()]
  def vector_fields(%__MODULE__{fields: fields}) do
    Enum.filter(fields, &Field.vector_type?(&1.data_type))
  end

  @doc """
  Returns all scalar fields from the schema.
  """
  @spec scalar_fields(t()) :: [Field.t()]
  def scalar_fields(%__MODULE__{fields: fields}) do
    Enum.filter(fields, &Field.scalar_type?(&1.data_type))
  end

  @doc """
  Finds a field by name.

  Returns `nil` if the field is not found.
  """
  @spec get_field(t(), String.t()) :: Field.t() | nil
  def get_field(%__MODULE__{fields: fields}, name) when is_binary(name) do
    Enum.find(fields, &(&1.name == name))
  end

  def get_field(schema, name) when is_atom(name) do
    get_field(schema, Atom.to_string(name))
  end

  @doc """
  Returns the names of all fields in the schema.
  """
  @spec field_names(t()) :: [String.t()]
  def field_names(%__MODULE__{fields: fields}) do
    Enum.map(fields, & &1.name)
  end

  @doc """
  Returns all array_of_struct fields from the schema.
  """
  @spec struct_array_fields(t()) :: [Field.t()]
  def struct_array_fields(%__MODULE__{fields: fields}) do
    Enum.filter(fields, &Field.array_of_struct?/1)
  end
end

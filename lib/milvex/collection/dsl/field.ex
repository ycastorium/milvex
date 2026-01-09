defmodule Milvex.Collection.Dsl.Field do
  @moduledoc """
  Struct representing a field definition in the Milvex Collection DSL.

  This struct is the target for all field entity definitions and contains
  all possible field configuration options. Not all options are valid for
  all field types - validation is handled by the verifiers.
  """

  @type t :: %__MODULE__{
          name: atom(),
          type: atom() | nil,
          dimension: pos_integer() | nil,
          max_length: pos_integer() | nil,
          max_capacity: pos_integer() | nil,
          element_type: atom() | nil,
          struct_schema: [Milvex.Schema.Field.t()] | nil,
          auto_id: boolean(),
          nullable: boolean(),
          partition_key: boolean(),
          clustering_key: boolean(),
          enable_analyzer: boolean(),
          default: term() | nil,
          description: String.t() | nil
        }

  defstruct [
    :name,
    :type,
    :dimension,
    :max_length,
    :max_capacity,
    :element_type,
    :struct_schema,
    :default,
    :description,
    :__spark_metadata__,
    is_primary_key: false,
    auto_id: false,
    nullable: false,
    partition_key: false,
    clustering_key: false,
    enable_analyzer: false
  ]

  @scalar_types [
    :bool,
    :int8,
    :int16,
    :int32,
    :int64,
    :float,
    :double,
    :json,
    :text,
    :varchar,
    :timestamp
  ]
  @vector_types [
    :binary_vector,
    :float_vector,
    :float16_vector,
    :bfloat16_vector,
    :sparse_float_vector,
    :int8_vector
  ]

  @doc """
  Returns true if this field is a primary key.

  A field is considered a primary key if it has a non-nil type that is
  either `:int64` or `:varchar` and no dimension (distinguishing from vectors).
  Primary keys are identified by the entity that created them.
  """
  @spec primary_key?(t()) :: boolean()
  def primary_key?(%__MODULE__{} = field) do
    field.type in [:int64, :varchar] and is_nil(field.dimension) and
      is_nil(field.element_type) and is_nil(field.max_capacity)
  end

  @doc """
  Returns true if this field is a vector field.
  """
  @spec vector?(t()) :: boolean()
  def vector?(%__MODULE__{type: type}) when type in @vector_types, do: true
  def vector?(_), do: false

  @doc """
  Returns true if this field is a sparse vector field.
  """
  @spec sparse_vector?(t()) :: boolean()
  def sparse_vector?(%__MODULE__{type: :sparse_float_vector}), do: true
  def sparse_vector?(_), do: false

  @doc """
  Returns true if this field is a scalar field.
  """
  @spec scalar?(t()) :: boolean()
  def scalar?(%__MODULE__{type: type, element_type: nil}) when type in @scalar_types, do: true
  def scalar?(_), do: false

  @doc """
  Returns true if this field is an array field.
  """
  @spec array?(t()) :: boolean()
  def array?(%__MODULE__{element_type: elem_type}) when not is_nil(elem_type), do: true
  def array?(_), do: false

  @doc """
  Returns true if this field is an array of structs field.
  """
  @spec array_of_structs?(t()) :: boolean()
  def array_of_structs?(%__MODULE__{element_type: :struct}), do: true
  def array_of_structs?(_), do: false
end

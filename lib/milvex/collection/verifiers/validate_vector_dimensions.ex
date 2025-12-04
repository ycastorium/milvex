defmodule Milvex.Collection.Verifiers.ValidateVectorDimensions do
  @moduledoc """
  Verifies that vector fields have valid dimensions.

  - Dense vectors (non-sparse) require a positive dimension
  - Binary vectors require dimensions to be a multiple of 8
  - Sparse vectors do not require a dimension
  """

  use Spark.Dsl.Verifier

  @dense_vector_types [
    :float_vector,
    :float16_vector,
    :bfloat16_vector,
    :binary_vector,
    :int8_vector
  ]

  @impl true
  def verify(dsl_state) do
    fields = Spark.Dsl.Extension.get_entities(dsl_state, [:collection, :fields])
    module = Spark.Dsl.Verifier.get_persisted(dsl_state, :module)

    errors =
      fields
      |> Enum.filter(&vector_field?/1)
      |> Enum.flat_map(&validate_vector(&1, module))

    case errors do
      [] -> :ok
      [first | _] -> {:error, first}
    end
  end

  defp vector_field?(%{type: type}) when type in @dense_vector_types, do: true
  defp vector_field?(%{type: :sparse_float_vector}), do: true
  defp vector_field?(_), do: false

  defp validate_vector(%{type: :sparse_float_vector}, _module), do: []

  defp validate_vector(%{type: type, dimension: dim, name: name}, module)
       when type in @dense_vector_types do
    cond do
      is_nil(dim) ->
        [
          Spark.Error.DslError.exception(
            message: "Vector field #{inspect(name)} requires a dimension",
            path: [:collection, :fields, name],
            module: module
          )
        ]

      dim < 1 ->
        [
          Spark.Error.DslError.exception(
            message: "Vector field #{inspect(name)} dimension must be positive",
            path: [:collection, :fields, name],
            module: module
          )
        ]

      type == :binary_vector and rem(dim, 8) != 0 ->
        [
          Spark.Error.DslError.exception(
            message:
              "Binary vector field #{inspect(name)} dimension must be a multiple of 8, got #{dim}",
            path: [:collection, :fields, name],
            module: module
          )
        ]

      true ->
        []
    end
  end
end

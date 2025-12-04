defmodule Milvex.Collection.Verifiers.ValidateArrayConfig do
  @moduledoc """
  Verifies that array fields have valid configuration.

  Array fields must have:
  - A valid element_type
  - A positive max_capacity
  - max_length if element_type is :varchar
  """

  use Spark.Dsl.Verifier

  @impl true
  def verify(dsl_state) do
    fields = Spark.Dsl.Extension.get_entities(dsl_state, [:collection, :fields])
    module = Spark.Dsl.Verifier.get_persisted(dsl_state, :module)

    errors =
      fields
      |> Enum.filter(&array_field?/1)
      |> Enum.flat_map(&validate_array(&1, module))

    case errors do
      [] -> :ok
      [first | _] -> {:error, first}
    end
  end

  defp array_field?(%{element_type: elem_type}) when not is_nil(elem_type), do: true
  defp array_field?(_), do: false

  defp validate_array(
         %{element_type: elem_type, max_capacity: cap, max_length: len, name: name},
         module
       ) do
    errors = []

    errors =
      if is_nil(cap) or cap < 1 do
        [
          Spark.Error.DslError.exception(
            message: "Array field #{inspect(name)} requires a positive max_capacity",
            path: [:collection, :fields, name],
            module: module
          )
          | errors
        ]
      else
        errors
      end

    errors =
      if elem_type == :varchar and (is_nil(len) or len < 1 or len > 65_535) do
        [
          Spark.Error.DslError.exception(
            message:
              "Array field #{inspect(name)} with varchar elements requires max_length between 1 and 65535",
            path: [:collection, :fields, name],
            module: module
          )
          | errors
        ]
      else
        errors
      end

    errors
  end
end

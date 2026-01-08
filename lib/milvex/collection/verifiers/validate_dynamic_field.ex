defmodule Milvex.Collection.Verifiers.ValidateDynamicField do
  @moduledoc """
  Verifies that dynamic fields are only used with valid scalar types.

  Dynamic fields are only supported for scalar types: bool, int8, int16, int32,
  int64, float, double, varchar, text, and json. They cannot be used with vectors,
  arrays, or structs.
  """

  use Spark.Dsl.Verifier

  @valid_dynamic_types [:bool, :int8, :int16, :int32, :int64, :float, :double, :varchar, :text, :json]

  @impl true
  def verify(dsl_state) do
    fields = Spark.Dsl.Extension.get_entities(dsl_state, [:collection, :fields])
    module = Spark.Dsl.Verifier.get_persisted(dsl_state, :module)

    errors =
      fields
      |> Enum.filter(&Map.get(&1, :dynamic, false))
      |> Enum.flat_map(&validate_dynamic_field(&1, module))

    case errors do
      [] -> :ok
      [first | _] -> {:error, first}
    end
  end

  defp validate_dynamic_field(%{type: type, name: name}, module)
       when type not in @valid_dynamic_types do
    [
      Spark.Error.DslError.exception(
        message:
          "Dynamic fields are only supported for scalar types (#{Enum.join(@valid_dynamic_types, ", ")}), not #{type}",
        path: [:collection, :fields, name],
        module: module
      )
    ]
  end

  defp validate_dynamic_field(_field, _module), do: []
end

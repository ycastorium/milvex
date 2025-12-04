defmodule Milvex.Collection.Verifiers.ValidateVarcharLength do
  @moduledoc """
  Verifies that varchar fields have valid max_length configuration.

  Varchar fields must have a max_length between 1 and 65535.
  """

  use Spark.Dsl.Verifier

  @impl true
  def verify(dsl_state) do
    fields = Spark.Dsl.Extension.get_entities(dsl_state, [:collection, :fields])
    module = Spark.Dsl.Verifier.get_persisted(dsl_state, :module)

    errors =
      fields
      |> Enum.filter(&varchar_field?/1)
      |> Enum.flat_map(&validate_varchar(&1, module))

    case errors do
      [] -> :ok
      [first | _] -> {:error, first}
    end
  end

  defp varchar_field?(%{type: :varchar, element_type: nil}), do: true
  defp varchar_field?(_), do: false

  defp validate_varchar(%{max_length: len, name: name}, module) do
    cond do
      is_nil(len) ->
        [
          Spark.Error.DslError.exception(
            message: "Varchar field #{inspect(name)} requires max_length",
            path: [:collection, :fields, name],
            module: module
          )
        ]

      len < 1 or len > 65_535 ->
        [
          Spark.Error.DslError.exception(
            message: "Varchar field #{inspect(name)} max_length must be between 1 and 65535",
            path: [:collection, :fields, name],
            module: module
          )
        ]

      true ->
        []
    end
  end
end

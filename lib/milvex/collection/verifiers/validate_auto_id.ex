defmodule Milvex.Collection.Verifiers.ValidateAutoId do
  @moduledoc """
  Verifies that auto_id is only used with int64 primary keys.

  Auto ID generation is only supported for int64 primary key fields,
  not for varchar primary keys.
  """

  use Spark.Dsl.Verifier

  alias Milvex.Collection.Dsl.Field

  @impl true
  def verify(dsl_state) do
    fields = Spark.Dsl.Extension.get_entities(dsl_state, [:collection, :fields])
    module = Spark.Dsl.Verifier.get_persisted(dsl_state, :module)

    errors =
      fields
      |> Enum.filter(&primary_key_with_auto_id?/1)
      |> Enum.flat_map(&validate_auto_id(&1, module))

    case errors do
      [] -> :ok
      [first | _] -> {:error, first}
    end
  end

  defp primary_key_with_auto_id?(%Field{
         type: type,
         auto_id: true,
         dimension: nil,
         element_type: nil
       })
       when type in [:int64, :varchar] do
    true
  end

  defp primary_key_with_auto_id?(_), do: false

  defp validate_auto_id(%Field{type: :varchar, name: name}, module) do
    [
      Spark.Error.DslError.exception(
        message: "Auto ID is only supported for int64 primary keys, not varchar",
        path: [:collection, :fields, name],
        module: module
      )
    ]
  end

  defp validate_auto_id(%Field{type: :int64}, _module), do: []
end

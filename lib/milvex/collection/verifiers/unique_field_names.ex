defmodule Milvex.Collection.Verifiers.UniqueFieldNames do
  @moduledoc """
  Verifies that all field names in the collection are unique.
  """

  use Spark.Dsl.Verifier

  @impl true
  def verify(dsl_state) do
    fields = Spark.Dsl.Extension.get_entities(dsl_state, [:collection, :fields])
    module = Spark.Dsl.Verifier.get_persisted(dsl_state, :module)

    names = Enum.map(fields, & &1.name)
    duplicates = names -- Enum.uniq(names)

    if duplicates == [] do
      :ok
    else
      {:error,
       Spark.Error.DslError.exception(
         message: "Duplicate field names: #{inspect(Enum.uniq(duplicates))}",
         path: [:collection, :fields],
         module: module
       )}
    end
  end
end

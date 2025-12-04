defmodule Milvex.Collection.Verifiers.RequirePrimaryKey do
  @moduledoc """
  Verifies that exactly one primary key field is defined in the collection.
  """

  use Spark.Dsl.Verifier

  @impl true
  def verify(dsl_state) do
    fields = Spark.Dsl.Extension.get_entities(dsl_state, [:collection, :fields])
    module = Spark.Dsl.Verifier.get_persisted(dsl_state, :module)

    primary_keys =
      fields
      |> Enum.filter(& &1.is_primary_key)
      |> length()

    case primary_keys do
      0 ->
        {:error,
         Spark.Error.DslError.exception(
           message: "Exactly one primary key field is required",
           path: [:collection, :fields],
           module: module
         )}

      1 ->
        :ok

      n ->
        {:error,
         Spark.Error.DslError.exception(
           message: "Found #{n} primary keys, but exactly one is required",
           path: [:collection, :fields],
           module: module
         )}
    end
  end
end

defmodule Milvex.Collection.Verifiers.ValidateCollectionName do
  @moduledoc """
  Verifies that the collection name is valid according to Milvus naming rules.

  Collection names must:
  - Be 1-255 characters
  - Start with a letter or underscore
  - Contain only alphanumeric characters and underscores
  """

  use Spark.Dsl.Verifier

  @impl true
  def verify(dsl_state) do
    name = Spark.Dsl.Extension.get_opt(dsl_state, [:collection], :name, nil)
    module = Spark.Dsl.Verifier.get_persisted(dsl_state, :module)

    cond do
      is_nil(name) ->
        {:error,
         Spark.Error.DslError.exception(
           message: "Collection name is required",
           path: [:collection, :name],
           module: module
         )}

      byte_size(name) == 0 ->
        {:error,
         Spark.Error.DslError.exception(
           message: "Collection name cannot be empty",
           path: [:collection, :name],
           module: module
         )}

      byte_size(name) > 255 ->
        {:error,
         Spark.Error.DslError.exception(
           message: "Collection name cannot exceed 255 characters",
           path: [:collection, :name],
           module: module
         )}

      not Regex.match?(~r/^[a-zA-Z_][a-zA-Z0-9_]*$/, name) ->
        {:error,
         Spark.Error.DslError.exception(
           message:
             "Collection name must start with a letter or underscore and contain only alphanumeric characters and underscores",
           path: [:collection, :name],
           module: module
         )}

      true ->
        :ok
    end
  end
end

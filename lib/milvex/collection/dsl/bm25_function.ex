defmodule Milvex.Collection.Dsl.BM25Function do
  @moduledoc """
  Struct representing a BM25 function definition in the Milvex Collection DSL.

  BM25 functions convert text fields to sparse vector embeddings for full-text search.
  """

  @type t :: %__MODULE__{
          name: atom(),
          input: atom() | [atom()],
          output: atom()
        }

  defstruct [
    :name,
    :input,
    :output,
    :__spark_metadata__
  ]
end

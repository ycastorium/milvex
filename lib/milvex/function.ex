defmodule Milvex.Function do
  @moduledoc """
  Builder for Milvus function schemas.

  Functions define transformations on fields, such as BM25 full-text search
  that converts text to sparse embeddings.

  ## Examples

      # BM25 function for full-text search
      function = Function.bm25("bm25_fn", input: "content", output: "sparse")

      # Using with fluent builder
      function = Function.new("bm25_fn", :BM25)
      |> Function.input_field_names(["content"])
      |> Function.output_field_names(["sparse"])
  """

  alias Milvex.Milvus.Proto.Common.KeyValuePair
  alias Milvex.Milvus.Proto.Schema.FunctionSchema

  @type function_type :: :BM25 | :TextEmbedding | :Rerank

  @type t :: %__MODULE__{
          name: String.t(),
          type: function_type(),
          input_field_names: [String.t()],
          output_field_names: [String.t()],
          params: %{String.t() => String.t()}
        }

  defstruct [
    :name,
    :type,
    input_field_names: [],
    output_field_names: [],
    params: %{}
  ]

  @doc """
  Creates a new function with the given name and type.

  ## Parameters
    - `name` - Function name
    - `type` - Function type (:BM25, :TextEmbedding, :Rerank)

  ## Examples

      Function.new("bm25_fn", :BM25)
  """
  @spec new(String.t(), function_type()) :: t()
  def new(name, type) when is_binary(name) and type in [:BM25, :TextEmbedding, :Rerank] do
    %__MODULE__{name: name, type: type}
  end

  def new(name, type) when is_atom(name) do
    new(Atom.to_string(name), type)
  end

  @doc """
  Sets the input field names for the function.
  """
  @spec input_field_names(t(), [String.t() | atom()]) :: t()
  def input_field_names(%__MODULE__{} = func, names) when is_list(names) do
    normalized = Enum.map(names, &to_string/1)
    %{func | input_field_names: normalized}
  end

  @doc """
  Sets the output field names for the function.
  """
  @spec output_field_names(t(), [String.t() | atom()]) :: t()
  def output_field_names(%__MODULE__{} = func, names) when is_list(names) do
    normalized = Enum.map(names, &to_string/1)
    %{func | output_field_names: normalized}
  end

  @doc """
  Adds a parameter to the function.
  """
  @spec param(t(), String.t(), String.t()) :: t()
  def param(%__MODULE__{} = func, key, value) when is_binary(key) and is_binary(value) do
    %{func | params: Map.put(func.params, key, value)}
  end

  @doc """
  Creates a BM25 function for full-text search.

  BM25 converts text fields to sparse vector embeddings for full-text search.

  ## Options
    - `:input` - Input field name (required)
    - `:output` - Output field name (required)

  ## Examples

      Function.bm25("bm25_fn", input: "content", output: "sparse")
      Function.bm25("bm25_fn", input: ["title", "content"], output: "sparse")
  """
  @spec bm25(String.t(), keyword()) :: t()
  def bm25(name, opts) when is_binary(name) do
    input = Keyword.fetch!(opts, :input)
    output = Keyword.fetch!(opts, :output)

    input_fields = if is_list(input), do: input, else: [input]
    output_fields = if is_list(output), do: output, else: [output]

    new(name, :BM25)
    |> input_field_names(input_fields)
    |> output_field_names(output_fields)
  end

  @doc """
  Converts the function to a protobuf FunctionSchema struct.
  """
  @spec to_proto(t()) :: FunctionSchema.t()
  def to_proto(%__MODULE__{} = func) do
    %FunctionSchema{
      name: func.name,
      type: func.type,
      input_field_names: func.input_field_names,
      output_field_names: func.output_field_names,
      params: build_params(func.params)
    }
  end

  @doc """
  Creates a Function from a protobuf FunctionSchema struct.
  """
  @spec from_proto(FunctionSchema.t()) :: t()
  def from_proto(%FunctionSchema{} = proto) do
    %__MODULE__{
      name: proto.name,
      type: proto.type,
      input_field_names: proto.input_field_names || [],
      output_field_names: proto.output_field_names || [],
      params: parse_params(proto.params)
    }
  end

  defp build_params(params) when is_map(params) do
    Enum.map(params, fn {key, value} ->
      %KeyValuePair{key: key, value: value}
    end)
  end

  defp parse_params(params) when is_list(params) do
    Enum.reduce(params, %{}, fn %KeyValuePair{key: key, value: value}, acc ->
      Map.put(acc, key, value)
    end)
  end
end

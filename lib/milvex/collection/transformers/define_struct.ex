defmodule Milvex.Collection.Transformers.DefineStruct do
  @moduledoc """
  Transformer that generates a struct and @type t from the Collection DSL fields.

  This makes Collection modules work like Ecto schemas - the field names from the
  DSL become struct fields with appropriate types.

  ## Example

      defmodule MyApp.Movies do
        use Milvex.Collection

        collection do
          name "movies"
          fields do
            primary_key :id, :int64
            varchar :title, 256
            vector :embedding, 128
          end
        end
      end

      # Generates:
      # @type t :: %MyApp.Movies{id: integer(), title: String.t(), embedding: [float()]}
      # defstruct [:id, :title, :embedding]

      movie = %MyApp.Movies{id: 1, title: "Inception", embedding: [0.1, 0.2]}
  """

  use Spark.Dsl.Transformer

  alias Spark.Dsl.Transformer

  @impl true
  def transform(dsl_state) do
    fields = Transformer.get_entities(dsl_state, [:collection, :fields])

    field_names = Enum.map(fields, & &1.name)
    type_specs = build_type_specs(fields)

    type_ast =
      quote do
        %__MODULE__{unquote_splicing(type_specs)}
      end

    dsl_state =
      Transformer.eval(
        dsl_state,
        [field_names: field_names, type_ast: type_ast],
        quote do
          @type t :: unquote(type_ast)

          defstruct unquote(field_names)

          @doc false
          def __collection__, do: true
        end
      )

    {:ok, dsl_state}
  end

  defp build_type_specs(fields) do
    Enum.map(fields, fn field ->
      type_ast = field_to_type(field)
      {field.name, type_ast}
    end)
  end

  defp field_to_type(%{type: :bool}), do: quote(do: boolean())
  defp field_to_type(%{type: :int8}), do: quote(do: integer())
  defp field_to_type(%{type: :int16}), do: quote(do: integer())
  defp field_to_type(%{type: :int32}), do: quote(do: integer())
  defp field_to_type(%{type: :int64}), do: quote(do: integer())
  defp field_to_type(%{type: :float}), do: quote(do: float())
  defp field_to_type(%{type: :double}), do: quote(do: float())
  defp field_to_type(%{type: :varchar}), do: quote(do: String.t())
  defp field_to_type(%{type: :text}), do: quote(do: String.t())
  defp field_to_type(%{type: :json}), do: quote(do: map())

  defp field_to_type(%{type: :float_vector}), do: quote(do: [float()])
  defp field_to_type(%{type: :float16_vector}), do: quote(do: [float()])
  defp field_to_type(%{type: :bfloat16_vector}), do: quote(do: [float()])
  defp field_to_type(%{type: :int8_vector}), do: quote(do: [integer()])
  defp field_to_type(%{type: :binary_vector}), do: quote(do: binary())
  defp field_to_type(%{type: :sparse_float_vector}), do: quote(do: map())

  defp field_to_type(%{element_type: :bool}) when not is_nil(:bool), do: quote(do: [boolean()])

  defp field_to_type(%{element_type: elem}) when elem in [:int8, :int16, :int32, :int64] do
    quote(do: [integer()])
  end

  defp field_to_type(%{element_type: elem}) when elem in [:float, :double] do
    quote(do: [float()])
  end

  defp field_to_type(%{element_type: :varchar}), do: quote(do: [String.t()])
  defp field_to_type(%{element_type: :json}), do: quote(do: [map()])

  defp field_to_type(_), do: quote(do: term())
end

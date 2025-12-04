defmodule Milvex.Collection.Dsl do
  @moduledoc """
  Spark DSL extension for defining Milvus collection schemas.

  This module provides the DSL entities and sections for declaring
  collection schemas in a declarative way.

  ## Example

      defmodule MyApp.Movies do
        use Milvex.Collection

        collection do
          name "movies"
          description "Movie embeddings collection"
          enable_dynamic_field true

          fields do
            primary_key :id, :int64, auto_id: true
            varchar :title, 512
            scalar :year, :int32
            vector :embedding, 128
          end
        end
      end
  """

  @scalar_types [:bool, :int8, :int16, :int32, :int64, :float, :double, :json, :text]
  @array_element_types [
    :bool,
    :int8,
    :int16,
    :int32,
    :int64,
    :float,
    :double,
    :json,
    :text,
    :varchar
  ]
  @vector_types [
    :binary_vector,
    :float_vector,
    :float16_vector,
    :bfloat16_vector,
    :sparse_float_vector,
    :int8_vector
  ]
  @primary_key_types [:int64, :varchar]

  @primary_key %Spark.Dsl.Entity{
    name: :primary_key,
    describe: """
    Declares a primary key field for the collection.

    Primary keys must be either `:int64` or `:varchar` type. Only one primary key
    is allowed per collection.
    """,
    examples: [
      "primary_key :id, :int64",
      "primary_key :id, :int64, auto_id: true",
      "primary_key :pk, :varchar, max_length: 64"
    ],
    target: Milvex.Collection.Dsl.Field,
    args: [:name, :type],
    schema: [
      name: [
        type: :atom,
        required: true,
        doc: "The name of the primary key field"
      ],
      type: [
        type: {:one_of, @primary_key_types},
        required: true,
        doc: "The type of the primary key (`:int64` or `:varchar`)"
      ],
      is_primary_key: [
        type: :boolean,
        default: true,
        doc: "Marks this field as the primary key"
      ],
      auto_id: [
        type: :boolean,
        default: false,
        doc: "Enable auto-generation of IDs. Only valid for `:int64` type."
      ],
      max_length: [
        type: :pos_integer,
        doc: "Maximum length for varchar primary keys. Required when type is `:varchar`."
      ],
      description: [
        type: :string,
        doc: "Optional description for the field"
      ]
    ]
  }

  @vector %Spark.Dsl.Entity{
    name: :vector,
    describe: """
    Declares a vector field for storing embeddings.

    Vector fields require a dimension parameter specifying the number of dimensions.
    The default type is `:float_vector`.
    """,
    examples: [
      "vector :embedding, 128",
      "vector :embedding, 768, type: :float16_vector",
      "vector :binary_emb, 256, type: :binary_vector"
    ],
    target: Milvex.Collection.Dsl.Field,
    args: [:name, :dimension],
    schema: [
      name: [
        type: :atom,
        required: true,
        doc: "The name of the vector field"
      ],
      dimension: [
        type: :pos_integer,
        required: true,
        doc: "The dimension of the vector"
      ],
      type: [
        type: {:one_of, @vector_types -- [:sparse_float_vector]},
        default: :float_vector,
        doc: "The vector type (defaults to `:float_vector`)"
      ],
      description: [
        type: :string,
        doc: "Optional description for the field"
      ]
    ]
  }

  @sparse_vector %Spark.Dsl.Entity{
    name: :sparse_vector,
    describe: """
    Declares a sparse vector field.

    Sparse vectors do not require a dimension parameter.
    """,
    examples: [
      "sparse_vector :sparse_embedding"
    ],
    target: Milvex.Collection.Dsl.Field,
    args: [:name],
    schema: [
      name: [
        type: :atom,
        required: true,
        doc: "The name of the sparse vector field"
      ],
      type: [
        type: :atom,
        default: :sparse_float_vector,
        doc: "The vector type (always `:sparse_float_vector` for sparse vectors)"
      ],
      description: [
        type: :string,
        doc: "Optional description for the field"
      ]
    ]
  }

  @varchar %Spark.Dsl.Entity{
    name: :varchar,
    describe: """
    Declares a varchar (variable-length string) field.

    Requires a max_length parameter between 1 and 65535.
    """,
    examples: [
      "varchar :title, 256",
      "varchar :description, 1024, nullable: true",
      "varchar :category, 64, default: \"uncategorized\""
    ],
    target: Milvex.Collection.Dsl.Field,
    args: [:name, :max_length],
    schema: [
      name: [
        type: :atom,
        required: true,
        doc: "The name of the varchar field"
      ],
      type: [
        type: :atom,
        default: :varchar,
        doc: "The data type (always `:varchar`)"
      ],
      max_length: [
        type: :pos_integer,
        required: true,
        doc: "Maximum length of the string (1-65535)"
      ],
      nullable: [
        type: :boolean,
        default: false,
        doc: "Whether the field can be null"
      ],
      default: [
        type: :string,
        doc: "Default value for the field"
      ],
      partition_key: [
        type: :boolean,
        default: false,
        doc: "Mark this field as a partition key"
      ],
      clustering_key: [
        type: :boolean,
        default: false,
        doc: "Mark this field as a clustering key"
      ],
      description: [
        type: :string,
        doc: "Optional description for the field"
      ]
    ]
  }

  @scalar %Spark.Dsl.Entity{
    name: :scalar,
    describe: """
    Declares a scalar field (numeric, boolean, or JSON).

    Supported types: `:bool`, `:int8`, `:int16`, `:int32`, `:int64`,
    `:float`, `:double`, `:json`, `:text`
    """,
    examples: [
      "scalar :age, :int32",
      "scalar :score, :float, nullable: true",
      "scalar :metadata, :json"
    ],
    target: Milvex.Collection.Dsl.Field,
    args: [:name, :type],
    schema: [
      name: [
        type: :atom,
        required: true,
        doc: "The name of the scalar field"
      ],
      type: [
        type: {:one_of, @scalar_types},
        required: true,
        doc: "The scalar type"
      ],
      nullable: [
        type: :boolean,
        default: false,
        doc: "Whether the field can be null"
      ],
      default: [
        type: :any,
        doc: "Default value for the field"
      ],
      partition_key: [
        type: :boolean,
        default: false,
        doc: "Mark this field as a partition key"
      ],
      clustering_key: [
        type: :boolean,
        default: false,
        doc: "Mark this field as a clustering key"
      ],
      description: [
        type: :string,
        doc: "Optional description for the field"
      ]
    ]
  }

  @array %Spark.Dsl.Entity{
    name: :array,
    describe: """
    Declares an array field with a specified element type.

    Requires `max_capacity` option specifying the maximum number of elements.
    For varchar arrays, also requires `max_length` for element size.
    """,
    examples: [
      "array :tags, :varchar, max_capacity: 100, max_length: 64",
      "array :scores, :float, max_capacity: 10"
    ],
    target: Milvex.Collection.Dsl.Field,
    args: [:name, :element_type],
    schema: [
      name: [
        type: :atom,
        required: true,
        doc: "The name of the array field"
      ],
      element_type: [
        type: {:one_of, @array_element_types},
        required: true,
        doc: "The type of elements in the array"
      ],
      max_capacity: [
        type: :pos_integer,
        required: true,
        doc: "Maximum number of elements in the array"
      ],
      max_length: [
        type: :pos_integer,
        doc: "Maximum length for varchar elements (required when element_type is :varchar)"
      ],
      nullable: [
        type: :boolean,
        default: false,
        doc: "Whether the field can be null"
      ],
      description: [
        type: :string,
        doc: "Optional description for the field"
      ]
    ]
  }

  @fields %Spark.Dsl.Section{
    name: :fields,
    describe: """
    Section for declaring fields in the collection.

    At minimum, a collection must have one primary key field and at least one other field.
    """,
    examples: [
      """
      fields do
        primary_key :id, :int64, auto_id: true
        varchar :title, 512
        vector :embedding, 128
      end
      """
    ],
    entities: [
      @primary_key,
      @vector,
      @sparse_vector,
      @varchar,
      @scalar,
      @array
    ]
  }

  @collection %Spark.Dsl.Section{
    name: :collection,
    describe: """
    Defines a Milvus collection schema.

    A collection represents a table in Milvus that stores vectors and their associated data.
    """,
    examples: [
      """
      collection do
        name "movies"
        description "Movie embeddings"

        fields do
          primary_key :id, :int64, auto_id: true
          varchar :title, 512
          vector :embedding, 128
        end
      end
      """
    ],
    schema: [
      name: [
        type: :string,
        required: true,
        doc: "The name of the collection (1-255 characters, alphanumeric and underscores)"
      ],
      description: [
        type: :string,
        doc: "Optional description for the collection"
      ],
      enable_dynamic_field: [
        type: :boolean,
        default: false,
        doc: "Enable storage of fields not defined in the schema"
      ]
    ],
    sections: [@fields]
  }

  use Spark.Dsl.Extension,
    sections: [@collection],
    transformers: [
      Milvex.Collection.Transformers.DefineStruct
    ],
    verifiers: [
      Milvex.Collection.Verifiers.RequirePrimaryKey,
      Milvex.Collection.Verifiers.UniqueFieldNames,
      Milvex.Collection.Verifiers.ValidateCollectionName,
      Milvex.Collection.Verifiers.ValidateVectorDimensions,
      Milvex.Collection.Verifiers.ValidateVarcharLength,
      Milvex.Collection.Verifiers.ValidateArrayConfig,
      Milvex.Collection.Verifiers.ValidateAutoId
    ]
end

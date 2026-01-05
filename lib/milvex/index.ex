defmodule Milvex.Index do
  @moduledoc """
  Builder for Milvus index configurations.

  Provides a fluent API for constructing index definitions with validation.
  Supports all common Milvus index types and distance metrics.

  ## Examples

      # Using builder
      index = Index.new("embedding", :hnsw, :cosine)
              |> Index.name("my_index")
              |> Index.params(%{M: 16, efConstruction: 256})

      # Using smart constructors
      Index.flat("embedding", :l2)
      Index.ivf_flat("embedding", :ip, nlist: 1024)
      Index.hnsw("embedding", :cosine, m: 16, ef_construction: 256)
      Index.autoindex("embedding", :l2)
  """

  alias Milvex.Milvus.Proto.Common.KeyValuePair

  @index_types [
    :flat,
    :ivf_flat,
    :ivf_sq8,
    :ivf_pq,
    :hnsw,
    :autoindex,
    :diskann,
    :gpu_ivf_flat,
    :gpu_ivf_pq,
    :scann,
    :sparse_inverted_index
  ]

  @metric_types [:l2, :ip, :cosine, :hamming, :jaccard, :max_sim_cosine, :max_sim_ip, :bm25]

  @index_schema Zoi.object(%{
                  name: Zoi.nullish(Zoi.string()),
                  field_name: Zoi.string() |> Zoi.min(1) |> Zoi.max(255),
                  index_type: Zoi.enum(@index_types),
                  metric_type: Zoi.enum(@metric_types),
                  params: Zoi.any() |> Zoi.optional() |> Zoi.default(%{})
                })

  @type index_type ::
          :flat
          | :ivf_flat
          | :ivf_sq8
          | :ivf_pq
          | :hnsw
          | :autoindex
          | :diskann
          | :gpu_ivf_flat
          | :gpu_ivf_pq
          | :scann
          | :sparse_inverted_index

  @type metric_type ::
          :l2
          | :ip
          | :cosine
          | :hamming
          | :jaccard
          | :max_sim_cosine
          | :max_sim_ip
          | :bm25

  @type inverted_index_algo :: :daat_maxscore | :daat_wand | :taat_naive

  @type t :: %__MODULE__{
          name: String.t() | nil,
          field_name: String.t(),
          index_type: index_type(),
          metric_type: metric_type(),
          params: map()
        }

  defstruct [
    :name,
    :field_name,
    :index_type,
    :metric_type,
    params: %{}
  ]

  @doc """
  Creates a new index configuration.

  ## Parameters
    - `field_name` - Name of the vector field to index
    - `index_type` - Type of index to create
    - `metric_type` - Distance metric to use

  ## Examples

      Index.new("embedding", :hnsw, :cosine)
      Index.new("vectors", :ivf_flat, :l2)
  """
  @spec new(String.t(), index_type(), metric_type()) :: t()
  def new(field_name, index_type, metric_type)
      when is_binary(field_name) and index_type in @index_types and metric_type in @metric_types do
    %__MODULE__{
      field_name: field_name,
      index_type: index_type,
      metric_type: metric_type
    }
  end

  def new(field_name, index_type, metric_type) when is_atom(field_name) do
    new(Atom.to_string(field_name), index_type, metric_type)
  end

  @doc """
  Sets the index name.

  If not set, Milvus will auto-generate a name.
  """
  @spec name(t(), String.t()) :: t()
  def name(%__MODULE__{} = index, name) when is_binary(name) do
    %{index | name: name}
  end

  @doc """
  Sets or merges additional index parameters.

  Parameters are index-type specific. Common parameters include:
  - IVF indexes: `nlist` (number of cluster units)
  - HNSW: `M` (max connections), `efConstruction` (search depth during build)
  - IVF_PQ: `m` (number of subquantizers), `nbits` (bits per subquantizer)
  """
  @spec params(t(), map()) :: t()
  def params(%__MODULE__{} = index, params) when is_map(params) do
    %{index | params: Map.merge(index.params, params)}
  end

  @doc """
  Creates a FLAT index (brute-force search).

  FLAT provides 100% recall but is slower for large datasets.
  Best for small datasets or when perfect accuracy is required.

  ## Examples

      Index.flat("embedding", :l2)
      Index.flat("embedding", :cosine)
  """
  @spec flat(String.t(), metric_type()) :: t()
  def flat(field_name, metric_type) do
    new(field_name, :flat, metric_type)
  end

  @doc """
  Creates an IVF_FLAT index.

  Inverted File index with flat quantization. Good balance of
  speed and accuracy for medium-sized datasets.

  ## Options
    - `:nlist` - Number of cluster units (default: 1024)
    - `:name` - Index name (optional)

  ## Examples

      Index.ivf_flat("embedding", :l2)
      Index.ivf_flat("embedding", :ip, nlist: 2048)
  """
  @spec ivf_flat(String.t(), metric_type(), keyword()) :: t()
  def ivf_flat(field_name, metric_type, opts \\ []) do
    nlist = Keyword.get(opts, :nlist, 1024)

    index = new(field_name, :ivf_flat, metric_type) |> params(%{nlist: nlist})

    if index_name = Keyword.get(opts, :name) do
      name(index, index_name)
    else
      index
    end
  end

  @doc """
  Creates an HNSW index.

  Hierarchical Navigable Small World graph. Excellent performance
  for high-dimensional vectors with good recall.

  ## Options
    - `:m` - Maximum number of connections per node (default: 16)
    - `:ef_construction` - Search depth during index building (default: 256)
    - `:name` - Index name (optional)

  ## Examples

      Index.hnsw("embedding", :cosine)
      Index.hnsw("embedding", :l2, m: 32, ef_construction: 512)
  """
  @spec hnsw(String.t(), metric_type(), keyword()) :: t()
  def hnsw(field_name, metric_type, opts \\ []) do
    m = Keyword.get(opts, :m, 16)
    ef_construction = Keyword.get(opts, :ef_construction, 256)

    index =
      new(field_name, :hnsw, metric_type)
      |> params(%{M: m, efConstruction: ef_construction})

    if index_name = Keyword.get(opts, :name) do
      name(index, index_name)
    else
      index
    end
  end

  @doc """
  Creates an AUTOINDEX.

  Lets Milvus automatically choose the best index type and parameters
  based on data characteristics.

  ## Options
    - `:name` - Index name (optional)

  ## Examples

      Index.autoindex("embedding", :l2)
  """
  @spec autoindex(String.t(), metric_type(), keyword()) :: t()
  def autoindex(field_name, metric_type, opts \\ []) do
    index = new(field_name, :autoindex, metric_type)

    if index_name = Keyword.get(opts, :name) do
      name(index, index_name)
    else
      index
    end
  end

  @doc """
  Creates an IVF_SQ8 index.

  IVF with scalar quantization. More memory-efficient than IVF_FLAT
  with slight accuracy trade-off.

  ## Options
    - `:nlist` - Number of cluster units (default: 1024)
    - `:name` - Index name (optional)

  ## Examples

      Index.ivf_sq8("embedding", :l2)
      Index.ivf_sq8("embedding", :ip, nlist: 2048)
  """
  @spec ivf_sq8(String.t(), metric_type(), keyword()) :: t()
  def ivf_sq8(field_name, metric_type, opts \\ []) do
    nlist = Keyword.get(opts, :nlist, 1024)

    index = new(field_name, :ivf_sq8, metric_type) |> params(%{nlist: nlist})

    if index_name = Keyword.get(opts, :name) do
      name(index, index_name)
    else
      index
    end
  end

  @doc """
  Creates an IVF_PQ index.

  IVF with product quantization. Very memory-efficient but with
  lower accuracy. Best for very large datasets.

  ## Options
    - `:nlist` - Number of cluster units (default: 1024)
    - `:m` - Number of subquantizers (default: 8)
    - `:nbits` - Bits per subquantizer (default: 8)
    - `:name` - Index name (optional)

  ## Examples

      Index.ivf_pq("embedding", :l2)
      Index.ivf_pq("embedding", :ip, nlist: 2048, m: 16)
  """
  @spec ivf_pq(String.t(), metric_type(), keyword()) :: t()
  def ivf_pq(field_name, metric_type, opts \\ []) do
    nlist = Keyword.get(opts, :nlist, 1024)
    m = Keyword.get(opts, :m, 8)
    nbits = Keyword.get(opts, :nbits, 8)

    index =
      new(field_name, :ivf_pq, metric_type)
      |> params(%{nlist: nlist, m: m, nbits: nbits})

    if index_name = Keyword.get(opts, :name) do
      name(index, index_name)
    else
      index
    end
  end

  @doc """
  Creates a DiskANN index.

  Disk-based ANN index for very large datasets that don't fit in memory.

  ## Options
    - `:name` - Index name (optional)

  ## Examples

      Index.diskann("embedding", :l2)
  """
  @spec diskann(String.t(), metric_type(), keyword()) :: t()
  def diskann(field_name, metric_type, opts \\ []) do
    index = new(field_name, :diskann, metric_type)

    if index_name = Keyword.get(opts, :name) do
      name(index, index_name)
    else
      index
    end
  end

  @doc """
  Creates a SCANN index.

  Google's ScaNN (Scalable Nearest Neighbors) implementation.
  Good balance of speed and accuracy.

  ## Options
    - `:nlist` - Number of cluster units (default: 1024)
    - `:name` - Index name (optional)

  ## Examples

      Index.scann("embedding", :l2)
      Index.scann("embedding", :cosine, nlist: 2048)
  """
  @spec scann(String.t(), metric_type(), keyword()) :: t()
  def scann(field_name, metric_type, opts \\ []) do
    nlist = Keyword.get(opts, :nlist, 1024)

    index = new(field_name, :scann, metric_type) |> params(%{nlist: nlist})

    if index_name = Keyword.get(opts, :name) do
      name(index, index_name)
    else
      index
    end
  end

  @doc """
  Creates a SPARSE_INVERTED_INDEX for BM25 full-text search.

  Used for SPARSE_FLOAT_VECTOR fields that receive BM25 function output.
  Supports inverted index algorithms optimized for sparse vectors.

  ## Options
    - `:inverted_index_algo` - Algorithm to use (default: `:daat_maxscore`)
      - `:daat_maxscore` - Document-at-a-time with MaxScore optimization
      - `:daat_wand` - Document-at-a-time with WAND optimization
      - `:taat_naive` - Term-at-a-time naive approach
    - `:bm25_k1` - BM25 k1 parameter (default: 1.2)
    - `:bm25_b` - BM25 b parameter (default: 0.75)
    - `:drop_ratio_build` - Drop ratio during index build (default: 0.2)
    - `:name` - Index name (optional)

  ## Examples

      Index.sparse_bm25("text_sparse")
      Index.sparse_bm25("text_sparse", inverted_index_algo: :daat_wand)
      Index.sparse_bm25("text_sparse", bm25_k1: 1.5, bm25_b: 0.8)
      Index.sparse_bm25("text_sparse", drop_ratio_build: 0.1)
  """
  @spec sparse_bm25(String.t(), keyword()) :: t()
  def sparse_bm25(field_name, opts \\ []) do
    algo = Keyword.get(opts, :inverted_index_algo, :daat_maxscore)
    k1 = Keyword.get(opts, :bm25_k1, 1.2)
    b = Keyword.get(opts, :bm25_b, 0.75)
    drop_ratio = Keyword.get(opts, :drop_ratio_build, 0.2)

    index_params = %{
      drop_ratio_build: drop_ratio,
      bm25_k1: k1,
      bm25_b: b,
      inverted_index_algo: inverted_algo_to_string(algo)
    }

    index = new(field_name, :sparse_inverted_index, :bm25) |> params(index_params)

    if index_name = Keyword.get(opts, :name) do
      name(index, index_name)
    else
      index
    end
  end

  @doc """
  Validates the index configuration.

  Returns `{:ok, index}` if valid, `{:error, error}` otherwise.
  """
  @spec validate(t()) :: {:ok, t()} | {:error, Milvex.Error.t()}
  def validate(%__MODULE__{} = index) do
    case Zoi.parse(@index_schema, Map.from_struct(index)) do
      {:ok, validated} ->
        with :ok <- validate_index_params(index) do
          {:ok, struct(__MODULE__, validated)}
        end

      {:error, errors} ->
        {:error,
         Milvex.Errors.Invalid.exception(
           field: "index",
           message: Zoi.prettify_errors(errors)
         )}
    end
  end

  @doc """
  Validates the index and raises on error.
  """
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = index) do
    case validate(index) do
      {:ok, index} -> index
      {:error, error} -> raise error
    end
  end

  defp validate_index_params(%{index_type: :ivf_flat, params: params}) do
    validate_nlist(params)
  end

  defp validate_index_params(%{index_type: :ivf_sq8, params: params}) do
    validate_nlist(params)
  end

  defp validate_index_params(%{index_type: :ivf_pq, params: params}) do
    with :ok <- validate_nlist(params),
         :ok <- validate_positive_param(params, :m, "m") do
      validate_positive_param(params, :nbits, "nbits")
    end
  end

  defp validate_index_params(%{index_type: :hnsw, params: params}) do
    with :ok <- validate_positive_param(params, :M, "M") do
      validate_positive_param(params, :efConstruction, "efConstruction")
    end
  end

  defp validate_index_params(_), do: :ok

  defp validate_nlist(params) do
    validate_positive_param(params, :nlist, "nlist")
  end

  defp validate_positive_param(params, key, name) do
    case Map.get(params, key) do
      nil -> :ok
      val when is_integer(val) and val > 0 -> :ok
      _ -> {:error, invalid_error(:params, "#{name} must be a positive integer")}
    end
  end

  defp invalid_error(field, message) do
    Milvex.Errors.Invalid.exception(field: field, message: message)
  end

  @doc """
  Converts the index configuration to protobuf extra_params.

  Returns a list of KeyValuePair structs for use in CreateIndexRequest.
  """
  @spec to_extra_params(t()) :: [KeyValuePair.t()]
  def to_extra_params(%__MODULE__{} = index) do
    params = [
      %KeyValuePair{key: "index_type", value: index_type_to_string(index.index_type)},
      %KeyValuePair{key: "metric_type", value: metric_type_to_string(index.metric_type)}
    ]

    params ++ params_to_key_value_pairs(index.params)
  end

  defp params_to_key_value_pairs(params) do
    params
    |> Enum.map(fn {key, value} ->
      %KeyValuePair{key: to_string(key), value: to_string(value)}
    end)
  end

  defp index_type_to_string(:flat), do: "FLAT"
  defp index_type_to_string(:ivf_flat), do: "IVF_FLAT"
  defp index_type_to_string(:ivf_sq8), do: "IVF_SQ8"
  defp index_type_to_string(:ivf_pq), do: "IVF_PQ"
  defp index_type_to_string(:hnsw), do: "HNSW"
  defp index_type_to_string(:autoindex), do: "AUTOINDEX"
  defp index_type_to_string(:diskann), do: "DISKANN"
  defp index_type_to_string(:gpu_ivf_flat), do: "GPU_IVF_FLAT"
  defp index_type_to_string(:gpu_ivf_pq), do: "GPU_IVF_PQ"
  defp index_type_to_string(:scann), do: "SCANN"
  defp index_type_to_string(:sparse_inverted_index), do: "SPARSE_INVERTED_INDEX"

  defp metric_type_to_string(:l2), do: "L2"
  defp metric_type_to_string(:ip), do: "IP"
  defp metric_type_to_string(:cosine), do: "COSINE"
  defp metric_type_to_string(:hamming), do: "HAMMING"
  defp metric_type_to_string(:jaccard), do: "JACCARD"
  defp metric_type_to_string(:max_sim_cosine), do: "MAX_SIM_COSINE"
  defp metric_type_to_string(:max_sim_ip), do: "MAX_SIM_IP"
  defp metric_type_to_string(:bm25), do: "BM25"

  defp inverted_algo_to_string(:daat_maxscore), do: "DAAT_MAXSCORE"
  defp inverted_algo_to_string(:daat_wand), do: "DAAT_WAND"
  defp inverted_algo_to_string(:taat_naive), do: "TAAT_NAIVE"

  @doc """
  Returns list of all supported index types.
  """
  @spec index_types() :: [index_type()]
  def index_types, do: @index_types

  @doc """
  Returns list of all supported metric types.
  """
  @spec metric_types() :: [metric_type()]
  def metric_types, do: @metric_types
end

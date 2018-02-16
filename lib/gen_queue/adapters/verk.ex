defmodule GenQueue.Adapters.Verk do
  @moduledoc """
  An adapter for `GenQueue` to enable functionaility with `Verk`.
  """

  @type job :: module | {module} | {module, any}
  @type pushed_job :: {module, list, map}

  use GenQueue.Adapter

  def start_link(_gen_queue, _opts) do
    Verk.Supervisor.start_link()
  end

  @doc """
  Push a job for Verk to consume.

  ## Parameters:
    * `gen_queue` - Any GenQueue module
    * `job` - Any valid job format
    * `opts` - A keyword list of job options

  ## Options
    * `:queue` - The queue to push the job to. Defaults to "default".
    * `:delay` - Either a `DateTime` or millseconds-based integer.

  ## Returns:
    * `{:ok, {module, args, opts}}` if the operation was successful
    * `{:error, reason}` if there was an error
  """
  def handle_push(_gen_queue, module, opts) when is_atom(module) do
    do_enqueue(module, [], build_opts_map(opts))
  end

  def handle_push(_gen_queue, {module}, opts) do
    do_enqueue(module, [], build_opts_map(opts))
  end

  def handle_push(_gen_queue, {module, args}, opts) when is_list(args) do
    do_enqueue(module, args, build_opts_map(opts))
  end

  def handle_push(_gen_queue, {module, arg}, opts) do
    do_enqueue(module, [arg], build_opts_map(opts))
  end

  @doc false
  def handle_pop(_gen_queue, _opts) do
    {:error, :not_implemented}
  end

  @doc false
  def handle_flush(_gen_queue, _opts) do
    {:error, :not_implemented}
  end

  @doc false
  def handle_length(_gen_queue, _opts) do
    {:error, :not_implemented}
  end

  @doc false
  def build_opts_map(opts) do
    opts
    |> Enum.into(%{})
    |> Map.put_new(:queue, "default")
  end

  defp do_enqueue(module, args, %{delay: offset} = opts) when is_integer(offset) do
    delay = :os.system_time(:seconds) + round(offset / 1000)
    do_schedule(module, args, opts, DateTime.from_unix!(delay))
  end

  defp do_enqueue(module, args, %{delay: %DateTime{} = delay} = opts) do
    do_schedule(module, args, opts, delay)
  end

  defp do_enqueue(module, args, opts) do
    verk_job = build_verk_job(module, args, opts)
    case Verk.enqueue(verk_job) do
      {:ok, jid} -> {:ok, {module, args, Map.put(opts, :jid, jid)}}
      error -> error
    end
  end

  defp do_schedule(module, args, opts, delay) do
    verk_job = build_verk_job(module, args, opts)
    case Verk.schedule(verk_job, delay) do
      {:ok, jid} -> {:ok, {module, args, Map.put(opts, :jid, jid)}}
      error -> error
    end
  end

  defp build_verk_job(module, args, opts) do
    struct(Verk.Job, Map.merge(opts, %{class: module, args: args}))
  end
end

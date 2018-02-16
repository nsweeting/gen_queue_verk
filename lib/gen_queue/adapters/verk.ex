defmodule GenQueue.Adapters.Verk do
  use GenQueue.Adapter

  @default_opts %{
    queue: "default"
  }

  def start_link(_gen_queue, _opts) do
    Verk.Supervisor.start_link()
  end

  def handle_push(_gen_queue, module, opts) when is_atom(module) do
    do_enqueue(module, [], build_opts_map(opts))
  end

  def handle_push(_gen_queue, {module}, opts) do
    do_enqueue(module, [], build_opts_map(opts))
  end

  def handle_push(_gen_queue, {module, args}, opts) do
    do_enqueue(module, args, build_opts_map(opts))
  end

  def build_opts_map(opts) do
    opts = Enum.into(opts, %{})
    Map.merge(@default_opts, opts)
  end

  defp do_enqueue(module, args, %{at: _} = opts) do
    verk_job = build_verk_job(module, args, opts)
    case Verk.schedule(verk_job, opts.at) do
      {:ok, jid} -> {:ok, {module, args, Map.put(opts, :jid, jid)}}
      error -> error
    end
  end

  defp do_enqueue(module, args, opts) do
    verk_job = build_verk_job(module, args, opts)
    case Verk.enqueue(verk_job) do
      {:ok, jid} -> {:ok, {module, args, Map.put(opts, :jid, jid)}}
      error -> error
    end
  end

  defp build_verk_job(module, args, opts) do
    struct(Verk.Job, Map.merge(opts, %{class: module, args: args}))
  end
end

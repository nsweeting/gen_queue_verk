defmodule GenQueue.Adapters.VerkMock do
  use GenQueue.Adapter

  alias GenQueue.Adapters.Verk, as: VerkAdapter

  def start_link(_gen_queue, _opts) do
    :ignore
  end

  def handle_push(gen_queue, module, opts) when is_atom(module) do
    do_return(gen_queue, module, [], VerkAdapter.build_opts_map(opts))
  end

  def handle_push(gen_queue, {module}, opts) do
    do_return(gen_queue, module, [], VerkAdapter.build_opts_map(opts))
  end

  def handle_push(gen_queue, {module, args}, opts) do
    do_return(gen_queue, module, args, VerkAdapter.build_opts_map(opts))
  end

  defp do_return(gen_queue, module, args, opts) do
    job = {module, args, Map.put(opts, :jid, generate_jid())}
    GenQueue.Test.send_item(gen_queue, job)
    {:ok, job}
  end

  defp generate_jid do
    <<part1::32, part2::32>> = :crypto.strong_rand_bytes(8)
   "#{part1}#{part2}"
  end
end